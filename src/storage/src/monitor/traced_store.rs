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

use std::ops::Bound;

use bytes::Bytes;
use futures::Future;
use risingwave_hummock_trace::{
    init_collector, trace, trace_result, OperationResult, RecordId, TraceSpan, StorageType,
};

use crate::error::StorageResult;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{HummockStorage, SstableIdManagerRef};
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{
    define_state_store_associated_type, define_state_store_read_associated_type,
    define_state_store_write_associated_type, StateStore, StateStoreIter,
};


#[derive(Clone)]
pub struct TracedStateStore<S> {
    inner: S,
    storage_type: StorageType
}

impl<S> TracedStateStore<S> {
    pub fn new(inner: S, storage_type: StorageType) -> Self {
        init_collector();
        Self {
            inner,
            storage_type
        }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S: StateStoreRead> TracedStateStore<S> {
    async fn traced_iter<'a, I>(
        &self,
        inner: I,
        record_id: RecordId,
    ) -> StorageResult<<TracedStateStore<S> as StateStoreRead>::Iter>
    where
        I: Future<Output = StorageResult<S::Iter>>,
    {
        let inner = inner.await?;

        let traced = TracedStateStoreIter { inner, record_id };

        Ok(traced)
    }
}

impl<S: LocalStateStore> LocalStateStore for TracedStateStore<S> {}

impl<S: StateStore> StateStore for TracedStateStore<S> {
    type Local = TracedStateStore<S::Local>;

    type NewLocalFuture<'a> = impl Future<Output = Self::Local> + Send + 'a;

    define_state_store_associated_type!();

    fn try_wait_epoch(
        &self,
        epoch: risingwave_hummock_sdk::HummockReadEpoch,
    ) -> Self::WaitEpochFuture<'_> {
        async move { self.inner.try_wait_epoch(epoch).await }
    }

    fn sync(&self, epoch: u64) -> Self::SyncFuture<'_> {
        async move {
            let span = trace!(SYNC, epoch);
            let sync_result = self.inner.sync(epoch).await;
            trace_result!(SYNC, span, sync_result);
            sync_result
        }
    }

    fn seal_epoch(&self, epoch: u64, is_checkpoint: bool) {
        trace!(SEAL, epoch, is_checkpoint);
        self.inner.seal_epoch(epoch, is_checkpoint);
    }

    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        async move { self.inner.clear_shared_buffer().await }
    }

    fn new_local(&self, table_id: risingwave_common::catalog::TableId) -> Self::NewLocalFuture<'_> {
        async move { TracedStateStore::new(self.inner.new_local(table_id).await, StorageType::Local) }
    }
}

impl<S: StateStoreRead> StateStoreRead for TracedStateStore<S> {
    type Iter = TracedStateStoreIter<S::Iter>;

    define_state_store_read_associated_type!();

    fn get<'a>(
        &'a self,
        key: &'a [u8],
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::GetFuture<'_> {
        async move {
            let span: TraceSpan = trace!(GET, key, epoch, read_options);
            let res: StorageResult<Option<Bytes>> = self.inner.get(key, epoch, read_options).await;
            trace_result!(GET, span, res);
            res
        }
    }

    fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
        async move {
            let span = trace!(ITER, key_range, epoch, read_options);
            let iter = self
                .traced_iter(self.inner.iter(key_range, epoch, read_options), span.id())
                .await;
            trace_result!(ITER, span, iter);
            iter
        }
    }
}

impl<S: StateStoreWrite> StateStoreWrite for TracedStateStore<S> {
    define_state_store_write_associated_type!();

    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        delete_ranges: Vec<(Bytes, Bytes)>,
        write_options: WriteOptions,
    ) -> Self::IngestBatchFuture<'_> {
        async move {
            let span: TraceSpan = trace!(INGEST, kv_pairs, delete_ranges, write_options);
            let res: StorageResult<usize> = self
                .inner
                .ingest_batch(kv_pairs, delete_ranges, write_options)
                .await;
            trace_result!(INGEST, span, res);
            res
        }
    }
}

impl TracedStateStore<HummockStorage> {
    pub fn sstable_store(&self) -> SstableStoreRef {
        self.inner.sstable_store()
    }

    pub fn sstable_id_manager(&self) -> SstableIdManagerRef {
        self.inner.sstable_id_manager().clone()
    }
}

pub struct TracedStateStoreIter<I> {
    inner: I,
    record_id: RecordId,
}

impl<I> StateStoreIter for TracedStateStoreIter<I>
where
    I: StateStoreIter<Item = (Bytes, Bytes)>,
{
    type Item = (Bytes, Bytes);

    type NextFuture<'a> =
        impl Future<Output = crate::error::StorageResult<Option<Self::Item>>> + Send + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            let span = trace!(ITER_NEXT, self.record_id);
            let kv_pair = self.inner.next().await.expect("failed to call iter next");
            trace_result!(ITER_NEXT, span, kv_pair);
            Ok(kv_pair)
        }
    }
}
