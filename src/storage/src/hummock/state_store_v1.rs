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

use std::cmp::Ordering;
use std::future::Future;
use std::ops::Bound::{Excluded, Included};
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::Ordering as MemOrdering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use itertools::Itertools;
use minitrace::future::FutureExt;
use minitrace::Span;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_hummock_sdk::key::{
    bound_table_key_range, map_table_key_range, next_key, user_key, FullKey, TableKey,
    TableKeyRange, UserKey,
};
use risingwave_hummock_sdk::{can_concat, HummockReadEpoch};
use risingwave_pb::hummock::LevelType;
use tokio::sync::oneshot;
use tracing::log::warn;

use super::iterator::{
    ConcatIteratorInner, DirectedUserIterator, DirectionEnum, HummockIteratorUnion,
};
use super::utils::{search_sst_idx, validate_epoch};
use super::{
    get_from_order_sorted_uncommitted_data, get_from_sstable_info, hit_sstable_bloom_filter,
    HummockStorageV1, SstableIteratorType,
};
use crate::error::{StorageError, StorageResult};
use crate::hummock::event_handler::HummockEvent;
use crate::hummock::iterator::{DirectedUserIteratorBuilder, HummockIteratorDirection};
use crate::hummock::local_version::ReadVersion;
use crate::hummock::shared_buffer::build_ordered_merge_iter;
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::utils::prune_ssts;
use crate::hummock::{ForwardIter, HummockEpoch, HummockError, HummockIteratorType, HummockResult};
use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{
    define_state_store_associated_type, define_state_store_read_associated_type,
    define_state_store_write_associated_type, StateStore, StateStoreIter,
};

impl HummockStorageV1 {
    /// Gets the value of a specified `key`.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    /// if `key` has consistent hash virtual node value, then such value is stored in `value_meta`
    ///
    /// If `Ok(Some())` is returned, the key is found. If `Ok(None)` is returned,
    /// the key is not found. If `Err()` is returned, the searching for the key
    /// failed due to other non-EOF errors.
    async fn get<'a>(
        &'a self,
        table_key: TableKey<&'a [u8]>,
        epoch: HummockEpoch,
        read_options: ReadOptions,
    ) -> StorageResult<Option<Bytes>> {
        let table_id = read_options.table_id;
        let mut local_stats = StoreLocalStatistic::default();
        let ReadVersion {
            shared_buffer_data,
            pinned_version,
            sync_uncommitted_data,
        } = self.read_filter(epoch, &read_options, &(table_key..=table_key))?;

        let mut table_counts = 0;
        let full_key = FullKey::new(table_id, table_key, epoch);

        // Query shared buffer. Return the value without iterating SSTs if found
        for uncommitted_data in shared_buffer_data {
            // iterate over uncommitted data in order index in descending order
            let (value, table_count) = get_from_order_sorted_uncommitted_data(
                self.sstable_store.clone(),
                uncommitted_data,
                full_key,
                &mut local_stats,
                read_options.check_bloom_filter,
            )
            .await?;
            if let Some(v) = value {
                local_stats.report(self.stats.as_ref());
                return Ok(v.into_user_value());
            }
            table_counts += table_count;
        }
        for sync_uncommitted_data in sync_uncommitted_data {
            let (value, table_count) = get_from_order_sorted_uncommitted_data(
                self.sstable_store.clone(),
                sync_uncommitted_data,
                full_key,
                &mut local_stats,
                read_options.check_bloom_filter,
            )
            .await?;
            if let Some(v) = value {
                local_stats.report(self.stats.as_ref());
                return Ok(v.into_user_value());
            }
            table_counts += table_count;
        }

        // Because SST meta records encoded key range,
        // the filter key needs to be encoded as well.
        let encoded_user_key = UserKey::for_test(read_options.table_id, table_key).encode();
        // See comments in HummockStorage::iter_inner for details about using compaction_group_id in
        // read/write path.
        assert!(pinned_version.is_valid());
        for level in pinned_version.levels(table_id) {
            if level.table_infos.is_empty() {
                continue;
            }
            match level.level_type() {
                LevelType::Overlapping | LevelType::Unspecified => {
                    let sstable_infos =
                        prune_ssts(level.table_infos.iter(), table_id, &(table_key..=table_key));
                    for sstable_info in sstable_infos {
                        table_counts += 1;
                        if let Some(v) = get_from_sstable_info(
                            self.sstable_store.clone(),
                            sstable_info,
                            full_key,
                            read_options.check_bloom_filter,
                            &mut local_stats,
                        )
                        .await?
                        {
                            local_stats.report(self.stats.as_ref());
                            return Ok(v.into_user_value());
                        }
                    }
                }
                LevelType::Nonoverlapping => {
                    let mut table_info_idx = level.table_infos.partition_point(|table| {
                        let ord = user_key(&table.key_range.as_ref().unwrap().left)
                            .cmp(encoded_user_key.as_ref());
                        ord == Ordering::Less || ord == Ordering::Equal
                    });
                    if table_info_idx == 0 {
                        continue;
                    }
                    table_info_idx = table_info_idx.saturating_sub(1);
                    let ord = user_key(
                        &level.table_infos[table_info_idx]
                            .key_range
                            .as_ref()
                            .unwrap()
                            .right,
                    )
                    .cmp(encoded_user_key.as_ref());
                    // the case that the key falls into the gap between two ssts
                    if ord == Ordering::Less {
                        continue;
                    }

                    table_counts += 1;
                    if let Some(v) = get_from_sstable_info(
                        self.sstable_store.clone(),
                        &level.table_infos[table_info_idx],
                        full_key,
                        read_options.check_bloom_filter,
                        &mut local_stats,
                    )
                    .await?
                    {
                        local_stats.report(self.stats.as_ref());
                        return Ok(v.into_user_value());
                    }
                }
            }
        }

        local_stats.report(self.stats.as_ref());
        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["sub-iter"])
            .observe(table_counts as f64);
        Ok(None)
    }

    fn read_filter<R, B>(
        &self,
        epoch: HummockEpoch,
        read_options: &ReadOptions,
        table_key_range: &R,
    ) -> HummockResult<ReadVersion>
    where
        R: RangeBounds<TableKey<B>>,
        B: AsRef<[u8]>,
    {
        let read_version =
            self.local_version_manager
                .read_filter(epoch, read_options.table_id, table_key_range);

        // Check epoch validity
        validate_epoch(read_version.pinned_version.safe_epoch(), epoch)?;

        Ok(read_version)
    }

    async fn iter_inner<T>(
        &self,
        epoch: HummockEpoch,
        table_key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> StorageResult<HummockStateStoreIter>
    where
        T: HummockIteratorType,
    {
        let table_id = read_options.table_id;
        let min_epoch = gen_min_epoch(epoch, read_options.retention_seconds.as_ref());
        let iter_read_options = Arc::new(SstableIteratorReadOptions::default());
        let mut overlapped_iters = vec![];
        let user_key_range = bound_table_key_range(table_id, &table_key_range);

        let ReadVersion {
            shared_buffer_data,
            pinned_version,
            sync_uncommitted_data,
        } = self.read_filter(epoch, &read_options, &table_key_range)?;

        let mut local_stats = StoreLocalStatistic::default();
        for uncommitted_data in shared_buffer_data {
            overlapped_iters.push(HummockIteratorUnion::Second(
                build_ordered_merge_iter::<T>(
                    &uncommitted_data,
                    self.sstable_store.clone(),
                    self.stats.clone(),
                    &mut local_stats,
                    iter_read_options.clone(),
                )
                .in_span(Span::enter_with_local_parent(
                    "build_ordered_merge_iter_shared_buffer",
                ))
                .await?,
            ));
        }
        for sync_uncommitted_data in sync_uncommitted_data {
            overlapped_iters.push(HummockIteratorUnion::Second(
                build_ordered_merge_iter::<T>(
                    &sync_uncommitted_data,
                    self.sstable_store.clone(),
                    self.stats.clone(),
                    &mut local_stats,
                    iter_read_options.clone(),
                )
                .in_span(Span::enter_with_local_parent(
                    "build_ordered_merge_iter_uncommitted",
                ))
                .await?,
            ))
        }
        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["memory-iter"])
            .observe(overlapped_iters.len() as f64);

        // Generate iterators for versioned ssts by filter out ssts that do not overlap with the
        // user key range derived from the given `table_key_range` and `table_id`.

        // The correctness of using compaction_group_id in read path and write path holds only
        // because we are currently:
        //
        // a) adopting static compaction group. It means a table_id->compaction_group mapping would
        // never change since creation until the table is dropped.
        //
        // b) enforcing shared buffer to split output SSTs by compaction group. It means no SSTs
        // would contain tables from different compaction_group, even for those in L0.
        //
        // When adopting dynamic compaction group in the future, be sure to revisit this assumption.

        // Because SST meta records encoded key range,
        // the filter key range needs to be encoded as well.
        let encoded_user_key_range = (
            user_key_range.0.as_ref().map(UserKey::encode),
            user_key_range.1.as_ref().map(UserKey::encode),
        );
        assert!(pinned_version.is_valid());
        for level in pinned_version.levels(table_id) {
            let table_infos = prune_ssts(level.table_infos.iter(), table_id, &table_key_range);
            if table_infos.is_empty() {
                continue;
            }
            if level.level_type == LevelType::Nonoverlapping as i32 {
                debug_assert!(can_concat(&table_infos));
                let start_table_idx = match encoded_user_key_range.start_bound() {
                    Included(key) | Excluded(key) => search_sst_idx(&table_infos, key),
                    _ => 0,
                };
                let end_table_idx = match encoded_user_key_range.end_bound() {
                    Included(key) | Excluded(key) => search_sst_idx(&table_infos, key),
                    _ => table_infos.len().saturating_sub(1),
                };
                assert!(start_table_idx < table_infos.len() && end_table_idx < table_infos.len());
                let matched_table_infos = &table_infos[start_table_idx..=end_table_idx];

                let pruned_sstables = match T::Direction::direction() {
                    DirectionEnum::Backward => matched_table_infos.iter().rev().collect_vec(),
                    DirectionEnum::Forward => matched_table_infos.iter().collect_vec(),
                };

                let mut sstables = vec![];
                for sstable_info in pruned_sstables {
                    if let Some(bloom_filter_key) = read_options.prefix_hint.as_ref() {
                        let sstable = self
                            .sstable_store
                            .sstable(sstable_info, &mut local_stats)
                            .in_span(Span::enter_with_local_parent("get_sstable"))
                            .await?;

                        if hit_sstable_bloom_filter(
                            sstable.value(),
                            UserKey::for_test(read_options.table_id, bloom_filter_key)
                                .encode()
                                .as_slice(),
                            &mut local_stats,
                        ) {
                            sstables.push((*sstable_info).clone());
                        }
                    } else {
                        sstables.push((*sstable_info).clone());
                    }
                }

                overlapped_iters.push(HummockIteratorUnion::Third(ConcatIteratorInner::<
                    T::SstableIteratorType,
                >::new(
                    sstables,
                    self.sstable_store(),
                    iter_read_options.clone(),
                )));
            } else {
                for table_info in table_infos.into_iter().rev() {
                    let sstable = self
                        .sstable_store
                        .sstable(table_info, &mut local_stats)
                        .in_span(Span::enter_with_local_parent("get_sstable"))
                        .await?;
                    if let Some(bloom_filter_key) = read_options.prefix_hint.as_ref() {
                        if !hit_sstable_bloom_filter(
                            sstable.value(),
                            bloom_filter_key,
                            &mut local_stats,
                        ) {
                            continue;
                        }
                    }

                    overlapped_iters.push(HummockIteratorUnion::Fourth(
                        T::SstableIteratorType::create(
                            sstable,
                            self.sstable_store(),
                            iter_read_options.clone(),
                        ),
                    ));
                }
            }
        }

        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["sub-iter"])
            .observe(overlapped_iters.len() as f64);

        // The input of the user iterator is a `HummockIteratorUnion` of 4 different types. We use
        // the union because the underlying merge iterator
        let mut user_iterator = T::UserIteratorBuilder::create(
            overlapped_iters,
            user_key_range,
            epoch,
            min_epoch,
            Some(pinned_version),
        );

        user_iterator
            .rewind()
            .in_span(Span::enter_with_local_parent("rewind"))
            .await?;

        local_stats.report(self.stats.as_ref());
        Ok(HummockStateStoreIter::new(
            user_iterator,
            self.stats.clone(),
        ))
    }
}

impl StateStoreRead for HummockStorageV1 {
    type Iter = HummockStateStoreIter;

    define_state_store_read_associated_type!();

    fn get<'a>(
        &'a self,
        key: &'a [u8],
        epoch: HummockEpoch,
        read_options: ReadOptions,
    ) -> Self::GetFuture<'_> {
        self.get(TableKey(key), epoch, read_options)
    }

    /// Returns an iterator that scan from the begin key to the end key
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: HummockEpoch,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
        if let Some(prefix_hint) = read_options.prefix_hint.as_ref() {
            let next_key = next_key(prefix_hint);

            // learn more detail about start_bound with storage_table.rs.
            match key_range.start_bound() {
                // it guarantees that the start bound must be included (some different case)
                // 1. Include(pk + col_bound) => prefix_hint <= start_bound <
                // next_key(prefix_hint)
                //
                // for case2, frontend need to reject this, avoid excluded start_bound and
                // transform it to included(next_key), without this case we can just guarantee
                // that start_bound < next_key
                //
                // 2. Include(next_key(pk +
                // col_bound)) => prefix_hint <= start_bound <= next_key(prefix_hint)
                //
                // 3. Include(pk) => prefix_hint <= start_bound < next_key(prefix_hint)
                Included(range_start) | Excluded(range_start) => {
                    assert!(range_start.as_slice() >= prefix_hint.as_slice());
                    assert!(range_start.as_slice() < next_key.as_slice() || next_key.is_empty());
                }

                _ => unreachable!(),
            }

            match key_range.end_bound() {
                Included(range_end) => {
                    assert!(range_end.as_slice() >= prefix_hint.as_slice());
                    assert!(range_end.as_slice() < next_key.as_slice() || next_key.is_empty());
                }

                // 1. Excluded(end_bound_of_prefix(pk + col)) => prefix_hint < end_bound <=
                // next_key(prefix_hint)
                //
                // 2. Excluded(pk + bound) => prefix_hint < end_bound <=
                // next_key(prefix_hint)
                Excluded(range_end) => {
                    assert!(range_end.as_slice() > prefix_hint.as_slice());
                    assert!(range_end.as_slice() <= next_key.as_slice() || next_key.is_empty());
                }

                std::ops::Bound::Unbounded => {
                    assert!(next_key.is_empty());
                }
            }
        } else {
            // not check
        }

        let iter =
            self.iter_inner::<ForwardIter>(epoch, map_table_key_range(key_range), read_options);
        #[cfg(not(madsim))]
        return iter.in_span(self.tracing.new_tracer("hummock_iter"));
        #[cfg(madsim)]
        iter
    }
}

impl StateStoreWrite for HummockStorageV1 {
    define_state_store_write_associated_type!();

    /// Writes a batch to storage. The batch should be:
    /// * Ordered. KV pairs will be directly written to the table, so it must be ordered.
    /// * Locally unique. There should not be two or more operations on the same key in one write
    ///   batch.
    /// * Globally unique. The streaming operators should ensure that different operators won't
    ///   operate on the same key. The operator operating on one keyspace should always wait for all
    ///   changes to be committed before reading and writing new keys to the engine. That is because
    ///   that the table with lower epoch might be committed after a table with higher epoch has
    ///   been committed. If such case happens, the outcome is non-predictable.
    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        delete_ranges: Vec<(Bytes, Bytes)>,
        write_options: WriteOptions,
    ) -> Self::IngestBatchFuture<'_> {
        async move {
            if kv_pairs.is_empty() {
                return Ok(0);
            }

            let epoch = write_options.epoch;
            // See comments in HummockStorage::iter_inner for details about using
            // compaction_group_id in read/write path.
            let size = self
                .local_version_manager
                .write_shared_buffer(epoch, kv_pairs, delete_ranges, write_options.table_id)
                .await?;
            Ok(size)
        }
    }
}

impl LocalStateStore for HummockStorageV1 {}

impl StateStore for HummockStorageV1 {
    type Local = Self;

    type NewLocalFuture<'a> = impl Future<Output = Self::Local> + Send + 'a;

    define_state_store_associated_type!();

    /// Waits until the local hummock version contains the epoch. If `wait_epoch` is `Current`,
    /// we will only check whether it is le `sealed_epoch` and won't wait.
    fn try_wait_epoch(&self, wait_epoch: HummockReadEpoch) -> Self::WaitEpochFuture<'_> {
        async move {
            // Ok(self.local_version_manager.try_wait_epoch(epoch).await?)
            let wait_epoch = match wait_epoch {
                HummockReadEpoch::Committed(epoch) => epoch,
                HummockReadEpoch::Current(epoch) => {
                    // let sealed_epoch = self.local_version.read().get_sealed_epoch();
                    let sealed_epoch = (*self.seal_epoch).load(MemOrdering::SeqCst);
                    assert!(
                            epoch <= sealed_epoch
                                && epoch != HummockEpoch::MAX
                            ,
                            "current epoch can't read, because the epoch in storage is not updated, epoch{}, sealed epoch{}"
                            ,epoch
                            ,sealed_epoch
                        );
                    return Ok(());
                }
                HummockReadEpoch::NoWait(_) => return Ok(()),
            };
            if wait_epoch == HummockEpoch::MAX {
                panic!("epoch should not be u64::MAX");
            }

            let mut receiver = self.version_update_notifier_tx.subscribe();
            // avoid unnecessary check in the loop if the value does not change
            let max_committed_epoch = *receiver.borrow_and_update();
            if max_committed_epoch >= wait_epoch {
                return Ok(());
            }
            loop {
                match tokio::time::timeout(Duration::from_secs(30), receiver.changed()).await {
                    Err(elapsed) => {
                        // The reason that we need to retry here is batch scan in
                        // chain/rearrange_chain is waiting for an
                        // uncommitted epoch carried by the CreateMV barrier, which
                        // can take unbounded time to become committed and propagate
                        // to the CN. We should consider removing the retry as well as wait_epoch
                        // for chain/rearrange_chain if we enforce
                        // chain/rearrange_chain to be scheduled on the same
                        // CN with the same distribution as the upstream MV.
                        // See #3845 for more details.
                        tracing::warn!(
                            "wait_epoch {:?} timeout when waiting for version update elapsed {:?}s",
                            wait_epoch,
                            elapsed
                        );
                        continue;
                    }
                    Ok(Err(_)) => {
                        return StorageResult::Err(StorageError::Hummock(
                            HummockError::wait_epoch("tx dropped"),
                        ));
                    }
                    Ok(Ok(_)) => {
                        let max_committed_epoch = *receiver.borrow();
                        if max_committed_epoch >= wait_epoch {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    fn sync(&self, epoch: u64) -> Self::SyncFuture<'_> {
        async move {
            if epoch == INVALID_EPOCH {
                warn!("syncing invalid epoch");
                return Ok(SyncResult {
                    sync_size: 0,
                    uncommitted_ssts: vec![],
                });
            }
            let (tx, rx) = oneshot::channel();
            self.hummock_event_sender
                .send(HummockEvent::SyncEpoch {
                    new_sync_epoch: epoch,
                    sync_result_sender: tx,
                })
                .expect("should send success");
            Ok(rx.await.expect("should wait success")?)
        }
    }

    fn seal_epoch(&self, epoch: u64, is_checkpoint: bool) {
        if epoch == INVALID_EPOCH {
            warn!("sealing invalid epoch");
            return;
        }
        self.hummock_event_sender
            .send(HummockEvent::SealEpoch {
                epoch,
                is_checkpoint,
            })
            .expect("should send success");
    }

    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        async move {
            let (tx, rx) = oneshot::channel();
            self.hummock_event_sender
                .send(HummockEvent::Clear(tx))
                .expect("should send success");
            rx.await.expect("should wait success");
            Ok(())
        }
    }

    fn new_local(&self, _table_id: TableId) -> Self::NewLocalFuture<'_> {
        async { self.clone() }
    }
}

pub struct HummockStateStoreIter {
    inner: DirectedUserIterator,
    metrics: Arc<StateStoreMetrics>,
}

impl HummockStateStoreIter {
    #[allow(dead_code)]
    fn new(inner: DirectedUserIterator, metrics: Arc<StateStoreMetrics>) -> Self {
        Self { inner, metrics }
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        self.inner.collect_local_statistic(stats);
    }
}

impl StateStoreIter for HummockStateStoreIter {
    // TODO: directly return `&[u8]` to user instead of `Bytes`.
    type Item = (FullKey<Vec<u8>>, Bytes);

    type NextFuture<'a> =
        impl Future<Output = crate::error::StorageResult<Option<Self::Item>>> + Send + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            let iter = &mut self.inner;

            if iter.is_valid() {
                let kv = (iter.key().clone(), Bytes::copy_from_slice(iter.value()));
                iter.next().await?;
                Ok(Some(kv))
            } else {
                Ok(None)
            }
        }
    }
}

impl Drop for HummockStateStoreIter {
    fn drop(&mut self) {
        let mut stats = StoreLocalStatistic::default();
        self.collect_local_statistic(&mut stats);
        stats.report(&self.metrics);
    }
}
