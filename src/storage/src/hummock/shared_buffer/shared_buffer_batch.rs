// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::{Deref, RangeBounds};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{table_key, FullKey, TableKey, UserKey};

use crate::hummock::iterator::{
    Backward, DirectionEnum, Forward, HummockIterator, HummockIteratorDirection,
};
use crate::hummock::utils::{range_overlap, MemoryTracker};
use crate::hummock::value::HummockValue;
use crate::hummock::{DeleteRangeTombstone, HummockEpoch, HummockResult, MemoryLimiter};
use crate::storage_value::StorageValue;

/// The key is `table_key`, which does not contain table id or epoch.
pub(crate) type SharedBufferItem = (Bytes, HummockValue<Bytes>);
pub type SharedBufferBatchId = u64;

pub(crate) struct SharedBufferBatchInner {
    payload: Vec<SharedBufferItem>,
    delete_range_tombstones: Vec<DeleteRangeTombstone>,
    largest_table_key: Vec<u8>,
    size: usize,
    _tracker: Option<MemoryTracker>,
    batch_id: SharedBufferBatchId,
}

impl SharedBufferBatchInner {
    fn new(
        payload: Vec<SharedBufferItem>,
        delete_range_tombstones: Vec<DeleteRangeTombstone>,
        size: usize,
        _tracker: Option<MemoryTracker>,
    ) -> Self {
        let mut largest_table_key = vec![];
        for tombstone in &delete_range_tombstones {
            // Although `end_user_key` of tombstone is exclusive, we still use it as a boundary of
            // `SharedBufferBatch` because it just expands an useless query and does not affect
            // correctness.
            let end_table_key = table_key(&tombstone.end_user_key);
            if largest_table_key.as_slice().lt(end_table_key) {
                largest_table_key.clear();
                largest_table_key.extend_from_slice(end_table_key);
            }
        }
        if let Some(item) = payload.last() {
            if item.0.gt(&largest_table_key) {
                largest_table_key.clear();
                largest_table_key.extend_from_slice(item.0.as_ref());
            }
        }
        SharedBufferBatchInner {
            payload,
            delete_range_tombstones,
            size,
            largest_table_key,
            _tracker,
            batch_id: SHARED_BUFFER_BATCH_ID_GENERATOR.fetch_add(1, Relaxed),
        }
    }
}

impl Deref for SharedBufferBatchInner {
    type Target = [SharedBufferItem];

    fn deref(&self) -> &Self::Target {
        self.payload.as_slice()
    }
}

impl Debug for SharedBufferBatchInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SharedBufferBatchInner {{ payload: {:?}, size: {} }}",
            self.payload, self.size
        )
    }
}

impl PartialEq for SharedBufferBatchInner {
    fn eq(&self, other: &Self) -> bool {
        self.payload == other.payload
    }
}

/// A write batch stored in the shared buffer.
#[derive(Clone, Debug, PartialEq)]
pub struct SharedBufferBatch {
    inner: Arc<SharedBufferBatchInner>,
    epoch: HummockEpoch,
    pub table_id: TableId,
}

static SHARED_BUFFER_BATCH_ID_GENERATOR: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));

impl SharedBufferBatch {
    pub fn for_test(
        sorted_items: Vec<SharedBufferItem>,
        epoch: HummockEpoch,
        table_id: TableId,
    ) -> Self {
        let size = Self::measure_batch_size(&sorted_items);

        Self {
            inner: Arc::new(SharedBufferBatchInner::new(
                sorted_items,
                vec![],
                size,
                None,
            )),
            epoch,
            table_id,
        }
    }

    pub fn measure_batch_size(batches: &[SharedBufferItem]) -> usize {
        // size = Sum(length of full key + length of user value)
        batches
            .iter()
            .map(|(k, v)| {
                k.len() + {
                    match v {
                        HummockValue::Put(val) => val.len(),
                        HummockValue::Delete => 0,
                    }
                }
            })
            .sum()
    }

    pub fn filter<R, B>(&self, table_id: TableId, table_key_range: &R) -> bool
    where
        R: RangeBounds<TableKey<B>>,
        B: AsRef<[u8]>,
    {
        self.table_id == table_id
            && range_overlap(
                table_key_range,
                *self.start_table_key(),
                *self.end_table_key(),
            )
    }

    pub fn get(&self, table_key: TableKey<&[u8]>) -> Option<HummockValue<Bytes>> {
        // Perform binary search on table key because the items in SharedBufferBatch is ordered by
        // table key.
        match self.inner.binary_search_by(|m| (m.0[..]).cmp(*table_key)) {
            Ok(i) => Some(self.inner[i].1.clone()),
            Err(_) => None,
        }
    }

    pub fn into_directed_iter<D: HummockIteratorDirection>(self) -> SharedBufferBatchIterator<D> {
        SharedBufferBatchIterator::<D>::new(self.inner, self.table_id, self.epoch)
    }

    pub fn into_forward_iter(self) -> SharedBufferBatchIterator<Forward> {
        self.into_directed_iter()
    }

    pub fn into_backward_iter(self) -> SharedBufferBatchIterator<Backward> {
        self.into_directed_iter()
    }

    pub fn get_payload(&self) -> &[SharedBufferItem] {
        &self.inner
    }

    pub fn start_table_key(&self) -> TableKey<&[u8]> {
        TableKey(&self.inner.first().unwrap().0)
    }

    pub fn end_table_key(&self) -> TableKey<&[u8]> {
        TableKey(&self.inner.last().unwrap().0)
    }

    /// return inclusive left endpoint, which means that all data in this batch should be larger or
    /// equal than this key.
    pub fn start_user_key(&self) -> UserKey<&[u8]> {
        if !self.inner.delete_range_tombstones.is_empty()
            && (self.inner.is_empty()
                || table_key(
                    self.inner
                        .delete_range_tombstones
                        .first()
                        .unwrap()
                        .start_user_key
                        .as_slice(),
                )
                .le(&self.inner.first().unwrap().0))
        {
            UserKey::decode(
                self.inner
                    .delete_range_tombstones
                    .first()
                    .unwrap()
                    .start_user_key
                    .as_slice(),
            )
        } else {
            UserKey::new(self.table_id, self.start_table_key())
        }
    }

    /// return inclusive right endpoint, which means that all data in this batch should be smaller
    /// or equal than this key.
    pub fn end_user_key(&self) -> UserKey<&[u8]> {
        UserKey::new(self.table_id, TableKey(&self.inner.largest_table_key))
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn size(&self) -> usize {
        self.inner.size
    }

    pub fn batch_id(&self) -> SharedBufferBatchId {
        self.inner.batch_id
    }

    pub fn build_shared_buffer_item_batches(
        kv_pairs: Vec<(Bytes, StorageValue)>,
    ) -> Vec<SharedBufferItem> {
        kv_pairs
            .into_iter()
            .map(|(key, value)| (key, value.into()))
            .collect()
    }

    pub async fn build_shared_buffer_batch(
        epoch: HummockEpoch,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        delete_ranges: Vec<(Bytes, Bytes)>,
        table_id: TableId,
        memory_limit: Option<&MemoryLimiter>,
    ) -> Self {
        let sorted_items = Self::build_shared_buffer_item_batches(kv_pairs);
        let delete_range_tombstones = delete_ranges
            .into_iter()
            .map(|(start_table_key, end_table_key)| {
                DeleteRangeTombstone::new(
                    table_id,
                    start_table_key.to_vec(),
                    end_table_key.to_vec(),
                    epoch,
                )
            })
            .collect_vec();
        #[cfg(test)]
        {
            Self::check_tombstone_prefix(table_id, &delete_range_tombstones);
        }
        let size = Self::measure_batch_size(&sorted_items);
        let tracker = if let Some(limiter) = memory_limit {
            limiter.require_memory(size as u64).await
        } else {
            None
        };
        let inner =
            SharedBufferBatchInner::new(sorted_items, delete_range_tombstones, size, tracker);
        SharedBufferBatch {
            inner: Arc::new(inner),
            table_id,
            epoch,
        }
    }

    pub fn get_delete_range_tombstones(&self) -> Vec<DeleteRangeTombstone> {
        self.inner.delete_range_tombstones.clone()
    }

    #[cfg(test)]
    fn check_tombstone_prefix(table_id: TableId, tombstones: &[DeleteRangeTombstone]) {
        for tombstone in tombstones {
            assert_eq!(
                UserKey::decode(&tombstone.start_user_key).table_id,
                table_id,
                "delete range tombstone in a shared buffer batch must begin with the same table id"
            );
            assert_eq!(
                UserKey::decode(&tombstone.end_user_key).table_id,
                table_id,
                "delete range tombstone in a shared buffer batch must begin with the same table id"
            );
        }
    }
}

pub struct SharedBufferBatchIterator<D: HummockIteratorDirection> {
    inner: Arc<SharedBufferBatchInner>,
    current_idx: usize,
    table_id: TableId,
    epoch: HummockEpoch,
    _phantom: PhantomData<D>,
}

impl<D: HummockIteratorDirection> SharedBufferBatchIterator<D> {
    pub(crate) fn new(
        inner: Arc<SharedBufferBatchInner>,
        table_id: TableId,
        epoch: HummockEpoch,
    ) -> Self {
        Self {
            inner,
            current_idx: 0,
            table_id,
            epoch,
            _phantom: Default::default(),
        }
    }

    fn current_item(&self) -> &SharedBufferItem {
        assert!(self.is_valid());
        let idx = match D::direction() {
            DirectionEnum::Forward => self.current_idx,
            DirectionEnum::Backward => self.inner.len() - self.current_idx - 1,
        };
        self.inner.get(idx).unwrap()
    }
}

impl<D: HummockIteratorDirection> HummockIterator for SharedBufferBatchIterator<D> {
    type Direction = D;

    type NextFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type RewindFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type SeekFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            assert!(self.is_valid());
            self.current_idx += 1;
            Ok(())
        }
    }

    fn key(&self) -> FullKey<&[u8]> {
        FullKey::new(self.table_id, TableKey(&self.current_item().0), self.epoch)
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.current_item().1.as_slice()
    }

    fn is_valid(&self) -> bool {
        self.current_idx < self.inner.len()
    }

    fn rewind(&mut self) -> Self::RewindFuture<'_> {
        async move {
            self.current_idx = 0;
            Ok(())
        }
    }

    fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> Self::SeekFuture<'a> {
        async move {
            debug_assert_eq!(key.user_key.table_id, self.table_id);
            // Perform binary search on table key because the items in SharedBufferBatch is ordered
            // by table key.
            let partition_point = self
                .inner
                .binary_search_by(|probe| probe.0[..].cmp(*key.user_key.table_key));
            let seek_key_epoch = key.epoch;
            match D::direction() {
                DirectionEnum::Forward => match partition_point {
                    Ok(i) => {
                        self.current_idx = i;
                        // The user key part must be the same if we reach here.
                        if self.epoch > seek_key_epoch {
                            // Move onto the next key for forward iteration if the current key
                            // has a larger epoch
                            self.current_idx += 1;
                        }
                    }
                    Err(i) => self.current_idx = i,
                },
                DirectionEnum::Backward => {
                    match partition_point {
                        Ok(i) => {
                            self.current_idx = self.inner.len() - i - 1;
                            // The user key part must be the same if we reach here.
                            if self.epoch < seek_key_epoch {
                                // Move onto the prev key for backward iteration if the current key
                                // has a smaller epoch
                                self.current_idx += 1;
                            }
                        }
                        // Seek to one item before the seek partition_point:
                        // If i == 0, the iterator will be invalidated with self.current_idx ==
                        // self.inner.len().
                        Err(i) => self.current_idx = self.inner.len() - i,
                    }
                }
            }
            Ok(())
        }
    }

    fn collect_local_statistic(&self, _stats: &mut crate::monitor::StoreLocalStatistic) {}
}

#[cfg(test)]
mod tests {

    use itertools::Itertools;

    use super::*;
    use crate::hummock::iterator::test_utils::{
        iterator_test_key_of_epoch, iterator_test_table_key_of,
    };

    fn transform_shared_buffer(
        batches: Vec<(Vec<u8>, HummockValue<Bytes>)>,
    ) -> Vec<(Bytes, HummockValue<Bytes>)> {
        batches
            .into_iter()
            .map(|(k, v)| (k.into(), v))
            .collect_vec()
    }

    #[tokio::test]
    async fn test_shared_buffer_batch_basic() {
        let epoch = 1;
        let shared_buffer_items: Vec<(Vec<u8>, HummockValue<Bytes>)> = vec![
            (
                iterator_test_table_key_of(0),
                HummockValue::put(Bytes::from("value1")),
            ),
            (
                iterator_test_table_key_of(1),
                HummockValue::put(Bytes::from("value1")),
            ),
            (
                iterator_test_table_key_of(2),
                HummockValue::put(Bytes::from("value1")),
            ),
        ];
        let shared_buffer_batch = SharedBufferBatch::for_test(
            transform_shared_buffer(shared_buffer_items.clone()),
            epoch,
            Default::default(),
        );

        // Sketch
        assert_eq!(
            *shared_buffer_batch.start_table_key(),
            shared_buffer_items[0].0
        );
        assert_eq!(
            *shared_buffer_batch.end_table_key(),
            shared_buffer_items[2].0
        );

        // Point lookup
        for (k, v) in &shared_buffer_items {
            assert_eq!(
                shared_buffer_batch.get(TableKey(k.as_slice())),
                Some(v.clone())
            );
        }
        assert_eq!(
            shared_buffer_batch.get(TableKey(iterator_test_table_key_of(3).as_slice())),
            None
        );
        assert_eq!(
            shared_buffer_batch.get(TableKey(iterator_test_table_key_of(4).as_slice())),
            None
        );

        // Forward iterator
        let mut iter = shared_buffer_batch.clone().into_forward_iter();
        iter.rewind().await.unwrap();
        let mut output = vec![];
        while iter.is_valid() {
            output.push((
                iter.key().user_key.table_key.to_vec(),
                iter.value().to_bytes(),
            ));
            iter.next().await.unwrap();
        }
        assert_eq!(output, shared_buffer_items);

        // Backward iterator
        let mut backward_iter = shared_buffer_batch.clone().into_backward_iter();
        backward_iter.rewind().await.unwrap();
        let mut output = vec![];
        while backward_iter.is_valid() {
            output.push((
                backward_iter.key().user_key.table_key.to_vec(),
                backward_iter.value().to_bytes(),
            ));
            backward_iter.next().await.unwrap();
        }
        output.reverse();
        assert_eq!(output, shared_buffer_items);
    }

    #[tokio::test]
    async fn test_shared_buffer_batch_seek() {
        let epoch = 1;
        let shared_buffer_items = vec![
            (
                iterator_test_table_key_of(1),
                HummockValue::put(Bytes::from("value1")),
            ),
            (
                iterator_test_table_key_of(2),
                HummockValue::put(Bytes::from("value2")),
            ),
            (
                iterator_test_table_key_of(3),
                HummockValue::put(Bytes::from("value3")),
            ),
        ];
        let shared_buffer_batch = SharedBufferBatch::for_test(
            transform_shared_buffer(shared_buffer_items.clone()),
            epoch,
            Default::default(),
        );

        // FORWARD: Seek to a key < 1st key, expect all three items to return
        let mut iter = shared_buffer_batch.clone().into_forward_iter();
        iter.seek(iterator_test_key_of_epoch(0, epoch).to_ref())
            .await
            .unwrap();
        for item in &shared_buffer_items {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0);
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // FORWARD: Seek to a key > the last key, expect no items to return
        let mut iter = shared_buffer_batch.clone().into_forward_iter();
        iter.seek(iterator_test_key_of_epoch(4, epoch).to_ref())
            .await
            .unwrap();
        assert!(!iter.is_valid());

        // FORWARD: Seek to 2nd key with current epoch, expect last two items to return
        let mut iter = shared_buffer_batch.clone().into_forward_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch).to_ref())
            .await
            .unwrap();
        for item in &shared_buffer_items[1..] {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0);
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // FORWARD: Seek to 2nd key with future epoch, expect last two items to return
        let mut iter = shared_buffer_batch.clone().into_forward_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch + 1).to_ref())
            .await
            .unwrap();
        for item in &shared_buffer_items[1..] {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0.as_slice());
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // FORWARD: Seek to 2nd key with old epoch, expect last item to return
        let mut iter = shared_buffer_batch.clone().into_forward_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch - 1).to_ref())
            .await
            .unwrap();
        let item = shared_buffer_items.last().unwrap();
        assert!(iter.is_valid());
        assert_eq!(*iter.key().user_key.table_key, item.0.as_slice());
        assert_eq!(iter.value(), item.1.as_slice());
        iter.next().await.unwrap();
        assert!(!iter.is_valid());

        // BACKWARD: Seek to a key < 1st key, expect no items to return
        let mut iter = shared_buffer_batch.clone().into_backward_iter();
        iter.seek(iterator_test_key_of_epoch(0, epoch).to_ref())
            .await
            .unwrap();
        assert!(!iter.is_valid());

        // BACKWARD: Seek to a key > the last key, expect all items to return
        let mut iter = shared_buffer_batch.clone().into_backward_iter();
        iter.seek(iterator_test_key_of_epoch(4, epoch).to_ref())
            .await
            .unwrap();
        for item in shared_buffer_items.iter().rev() {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0.as_slice());
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // BACKWARD: Seek to 2nd key with current epoch, expect first two items to return
        let mut iter = shared_buffer_batch.clone().into_backward_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch).to_ref())
            .await
            .unwrap();
        for item in shared_buffer_items[0..=1].iter().rev() {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0.as_slice());
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // BACKWARD: Seek to 2nd key with future epoch, expect first item to return
        let mut iter = shared_buffer_batch.clone().into_backward_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch + 1).to_ref())
            .await
            .unwrap();
        assert!(iter.is_valid());
        let item = shared_buffer_items.first().unwrap();
        assert_eq!(*iter.key().user_key.table_key, item.0.as_slice());
        assert_eq!(iter.value(), item.1.as_slice());
        iter.next().await.unwrap();
        assert!(!iter.is_valid());

        // BACKWARD: Seek to 2nd key with old epoch, expect first two item to return
        let mut iter = shared_buffer_batch.clone().into_backward_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch - 1).to_ref())
            .await
            .unwrap();
        for item in shared_buffer_items[0..=1].iter().rev() {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0.as_slice());
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());
    }

    #[tokio::test]
    #[should_panic]
    async fn test_invalid_table_id() {
        let epoch = 1;
        let shared_buffer_batch = SharedBufferBatch::for_test(vec![], epoch, Default::default());
        // Seeking to non-current epoch should panic
        let mut iter = shared_buffer_batch.into_forward_iter();
        iter.seek(FullKey::for_test(TableId::new(1), vec![], epoch).to_ref())
            .await
            .unwrap();
    }
}
