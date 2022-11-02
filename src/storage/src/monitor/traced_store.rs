use bytes::Bytes;
use futures::Future;
use risingwave_common::hm_trace::TraceLocalId;
use risingwave_hummock_trace::{
    init_collector, trace, trace_result, Operation, RecordId, TraceOpResult, TraceSpan,
};

use crate::error::StorageResult;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{HummockStorage, SstableIdManagerRef};
use crate::store::*;
use crate::{define_state_store_associated_type, StateStore};

#[derive(Clone)]
pub struct TracedStateStore<S> {
    inner: S,
}

impl<S> TracedStateStore<S> {
    pub fn new(inner: S) -> Self {
        init_collector();
        Self { inner }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S: StateStore> TracedStateStore<S> {
    async fn traced_iter<'a, I>(
        &self,
        inner: I,
        record_id: RecordId,
    ) -> StorageResult<<TracedStateStore<S> as StateStore>::Iter>
    where
        I: Future<Output = StorageResult<S::Iter>>,
    {
        let inner = inner.await?;

        let traced = TracedStateStoreIter { inner, record_id };

        Ok(traced)
    }
}

impl<S: StateStore> StateStore for TracedStateStore<S> {
    type Iter = TracedStateStoreIter<S::Iter>;

    define_state_store_associated_type!();

    fn get<'a>(
        &'a self,
        key: &'a [u8],
        check_bloom_filter: bool,
        read_options: crate::store::ReadOptions,
    ) -> Self::GetFuture<'_> {
        async move {
            let span: TraceSpan = trace!(GET, key, check_bloom_filter, read_options);
            let res: StorageResult<Option<Bytes>> =
                self.inner.get(key, check_bloom_filter, read_options).await;
            trace_result!(GET, span, res);
            res
        }
    }

    fn scan(
        &self,
        prefix_hint: Option<Vec<u8>>,
        key_range: (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>),
        limit: Option<usize>,
        read_options: crate::store::ReadOptions,
    ) -> Self::ScanFuture<'_> {
        async move {
            self.inner
                .scan(prefix_hint, key_range, limit, read_options)
                .await
        }
    }

    fn backward_scan(
        &self,
        key_range: (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>),
        limit: Option<usize>,
        read_options: crate::store::ReadOptions,
    ) -> Self::BackwardScanFuture<'_> {
        async move {
            self.inner
                .backward_scan(key_range, limit, read_options)
                .await
        }
    }

    fn ingest_batch(
        &self,
        kv_pairs: Vec<(bytes::Bytes, crate::storage_value::StorageValue)>,
        write_options: crate::store::WriteOptions,
    ) -> Self::IngestBatchFuture<'_> {
        async move {
            let span: TraceSpan = trace!(INGEST, kv_pairs, write_options);
            let res: StorageResult<usize> = self.inner.ingest_batch(kv_pairs, write_options).await;
            trace_result!(INGEST, span, res);
            res
        }
    }

    fn iter(
        &self,
        prefix_hint: Option<Vec<u8>>,
        key_range: (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>),
        read_options: crate::store::ReadOptions,
    ) -> Self::IterFuture<'_> {
        let span = trace!(ITER, prefix_hint, key_range, read_options);
        self.traced_iter(
            self.inner.iter(prefix_hint, key_range, read_options),
            span.id(),
        )
    }

    fn backward_iter(
        &self,
        key_range: (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>),
        read_options: crate::store::ReadOptions,
    ) -> Self::BackwardIterFuture<'_> {
        self.traced_iter(self.inner.backward_iter(key_range, read_options), 0)
    }

    fn try_wait_epoch(
        &self,
        epoch: risingwave_hummock_sdk::HummockReadEpoch,
    ) -> Self::WaitEpochFuture<'_> {
        async move {
            trace!(WAITEPOCH, epoch);
            self.inner.try_wait_epoch(epoch).await
        }
    }

    fn sync(&self, epoch: u64) -> Self::SyncFuture<'_> {
        async move {
            trace!(SYNC, epoch);
            self.inner.sync(epoch).await
        }
    }

    fn seal_epoch(&self, epoch: u64, is_checkpoint: bool) {
        trace!(SEAL, epoch, is_checkpoint);
        self.inner.seal_epoch(epoch, is_checkpoint);
    }

    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        async move { self.inner.clear_shared_buffer().await }
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

pub struct TracedStateStoreIter<I>
where
    I: StateStoreIter<Item = (Bytes, Bytes)>,
{
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
        async move { self.inner.next().await }
    }
}
