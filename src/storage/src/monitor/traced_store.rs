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

use bytes::Bytes;
use futures::{Future, TryFutureExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::opts::{NewLocalOptions, ReadOptions};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_hummock_trace::{
    init_collector, should_use_trace, trace, ConcurrentId, MayTraceSpan, OperationResult,
    StorageType, TraceResult, TraceSpan, TracedBytes, LOCAL_ID,
};

use super::identity;
use crate::error::{StorageError, StorageResult};
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{HummockStorage, SstableObjectIdManagerRef};
use crate::store::*;
use crate::{
    define_local_state_store_associated_type, define_state_store_associated_type,
    define_state_store_read_associated_type, StateStore,
};

#[derive(Clone)]
pub struct TracedStateStore<S> {
    inner: S,
    storage_type: StorageType,
}

impl<S> TracedStateStore<S> {
    pub fn new(inner: S, storage_type: StorageType) -> Self {
        if should_use_trace() {
            init_collector();
            tracing::info!("Hummock Tracing Enabled");
        }
        Self {
            inner,
            storage_type,
        }
    }

    pub fn new_global(inner: S) -> Self {
        Self::new(inner, StorageType::Global)
    }

    pub fn new_local(inner: S, options: NewLocalOptions) -> Self {
        let id = get_concurrent_id();
        let storage_type: StorageType = StorageType::Local(id, options.table_id);

        let _span: MayTraceSpan = TraceSpan::new_local_storage_span(options, storage_type);

        Self {
            inner,
            storage_type,
        }
    }

    async fn traced_iter<'a, 's, St: StateStoreIterItemStream + 's>(
        &'a self,
        table_id: TableId,
        iter_stream_future: impl Future<Output = StorageResult<St>> + 'a,
        span: MayTraceSpan,
    ) -> StorageResult<TracedStateStoreIterStream<'s, St>> {
        let iter_stream = iter_stream_future.await?;
        let traced = TracedStateStoreIter {
            inner: iter_stream,
            span,
        };
        Ok(traced.into_stream())
    }

    async fn traced_get(
        &self,
        key: Bytes,
        epoch: Option<u64>,
        read_options: ReadOptions,
        get_future: impl Future<Output = StorageResult<Option<Bytes>>>,
    ) -> StorageResult<Option<Bytes>> {
        let span =
            TraceSpan::new_get_span(key.clone(), epoch, read_options.clone(), self.storage_type);

        let res = get_future.await;

        span.may_send_result(OperationResult::Get(TraceResult::from(
            res.as_ref()
                .map(|o| o.as_ref().map(|b| TracedBytes::from(b.clone()))),
        )));
        Ok(res?)
    }
}

type TracedStateStoreIterStream<'s, S: StateStoreIterItemStream + 's> =
    impl StateStoreIterItemStream + 's;

impl<S: LocalStateStore> LocalStateStore for TracedStateStore<S> {
    type FlushFuture<'a> = impl Future<Output = StorageResult<usize>> + 'a;
    type GetFuture<'a> = impl GetFutureTrait<'a>;
    type IterFuture<'a> = impl Future<Output = StorageResult<Self::IterStream<'a>>> + Send + 'a;
    type IterStream<'a> = impl StateStoreIterItemStream + 'a;

    define_local_state_store_associated_type!();

    fn may_exist(
        &self,
        key_range: IterKeyRange,
        read_options: ReadOptions,
    ) -> Self::MayExistFuture<'_> {
        async move { self.inner.may_exist(key_range, read_options).await }
    }

    fn get(&self, key: Bytes, read_options: ReadOptions) -> Self::GetFuture<'_> {
        self.traced_get(
            key.clone(),
            None,
            read_options.clone(),
            self.inner.get(key, read_options),
        )
    }

    fn iter(&self, key_range: IterKeyRange, read_options: ReadOptions) -> Self::IterFuture<'_> {
        let span = TraceSpan::new_iter_span(
            key_range.clone(),
            None,
            read_options.clone(),
            self.storage_type,
        );
        self.traced_iter(
            read_options.table_id,
            self.inner.iter(key_range, read_options),
            span,
        )
        .map_ok(identity)
    }

    fn insert(&mut self, key: Bytes, new_val: Bytes, old_val: Option<Bytes>) -> StorageResult<()> {
        let span = TraceSpan::new_insert_span(
            key.clone(),
            new_val.clone(),
            old_val.clone(),
            self.storage_type,
        );
        let res = self.inner.insert(key, new_val, old_val);

        span.may_send_result(OperationResult::Insert(
            res.as_ref().map(|o| o.clone()).into(),
        ));
        res
    }

    fn delete(&mut self, key: Bytes, old_val: Bytes) -> StorageResult<()> {
        let span = TraceSpan::new_delete_span(key.clone(), old_val.clone(), self.storage_type);

        let res = self.inner.delete(key, old_val);

        span.may_send_result(OperationResult::Delete(
            res.as_ref().map(|o| o.clone()).into(),
        ));

        res
    }

    fn flush(&mut self, delete_ranges: Vec<(Bytes, Bytes)>) -> Self::FlushFuture<'_> {
        self.inner.flush(delete_ranges)
    }

    fn epoch(&self) -> u64 {
        self.inner.epoch()
    }

    fn is_dirty(&self) -> bool {
        self.inner.is_dirty()
    }

    fn init(&mut self, epoch: u64) {
        self.inner.init(epoch)
    }

    fn seal_current_epoch(&mut self, next_epoch: u64) {
        self.inner.seal_current_epoch(next_epoch)
    }
}

impl<S: StateStore> StateStore for TracedStateStore<S> {
    type Local = TracedStateStore<S::Local>;

    type NewLocalFuture<'a> = impl Future<Output = Self::Local> + Send + 'a;

    define_state_store_associated_type!();

    fn try_wait_epoch(&self, epoch: HummockReadEpoch) -> Self::WaitEpochFuture<'_> {
        async move { self.inner.try_wait_epoch(epoch).await }
    }

    fn sync(&self, epoch: u64) -> Self::SyncFuture<'_> {
        async move {
            let span = TraceSpan::new_sync_span(epoch, self.storage_type);

            let sync_result = self.inner.sync(epoch).await;

            span.may_send_result(OperationResult::Sync(
                sync_result.as_ref().map(|res| res.sync_size.clone()).into(),
            ));
            sync_result
        }
    }

    fn seal_epoch(&self, epoch: u64, is_checkpoint: bool) {
        let _span = TraceSpan::new_seal_span(epoch, is_checkpoint, self.storage_type);
        self.inner.seal_epoch(epoch, is_checkpoint);
    }

    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        async move { self.inner.clear_shared_buffer().await }
    }

    fn new_local(&self, options: NewLocalOptions) -> Self::NewLocalFuture<'_> {
        async move { TracedStateStore::new_local(self.inner.new_local(options.clone()).await, options) }
    }

    fn validate_read_epoch(&self, epoch: HummockReadEpoch) -> StorageResult<()> {
        self.inner.validate_read_epoch(epoch)
    }
}

impl<S: StateStoreRead> StateStoreRead for TracedStateStore<S> {
    type IterStream = impl StateStoreReadIterStream;

    define_state_store_read_associated_type!();

    fn get(&self, key: Bytes, epoch: u64, read_options: ReadOptions) -> Self::GetFuture<'_> {
        self.traced_get(
            key.clone(),
            Some(epoch),
            read_options.clone(),
            self.inner.get(key, epoch, read_options),
        )
    }

    fn iter(
        &self,
        key_range: IterKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
        let span = TraceSpan::new_iter_span(
            key_range.clone(),
            Some(epoch),
            read_options.clone(),
            self.storage_type,
        );
        self.traced_iter(
            read_options.table_id,
            self.inner.iter(key_range, epoch, read_options),
            span,
        )
        .map_ok(identity)
    }
}

impl TracedStateStore<HummockStorage> {
    pub fn sstable_store(&self) -> SstableStoreRef {
        self.inner.sstable_store()
    }

    pub fn sstable_object_id_manager(&self) -> &SstableObjectIdManagerRef {
        self.inner.sstable_object_id_manager()
    }
}

impl<S> TracedStateStore<S> {
    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S> Drop for TracedStateStore<S> {
    fn drop(&mut self) {
        if let StorageType::Local(_, _) = self.storage_type {
            trace!(DROPLOCAL, self.storage_type);
        }
    }
}

pub struct TracedStateStoreIter<S> {
    inner: S,
    span: MayTraceSpan,
}

impl<S> TracedStateStoreIter<S> {
    fn new(inner: S, span: MayTraceSpan) -> Self {
        TracedStateStoreIter { inner, span }
    }
}

impl<S: StateStoreIterItemStream> TracedStateStoreIter<S> {
    #[try_stream(ok = StateStoreIterItem, error = StorageError)]
    async fn into_stream_inner(self) {
        let inner = self.inner;
        futures::pin_mut!(inner);

        while let Some((key, value)) = inner
            .try_next()
            .await
            .inspect_err(|e| tracing::error!("Failed in next: {:?}", e))?
        {
            self.span
                .may_send_result(OperationResult::IterNext(TraceResult::Ok(Some((
                    TracedBytes::from(key.user_key.table_key.to_vec()),
                    TracedBytes::from(value.clone()),
                )))));
            yield (key, value);
        }
    }

    fn into_stream(self) -> impl StateStoreIterItemStream {
        Self::into_stream_inner(self)
    }
}

pub fn get_concurrent_id() -> ConcurrentId {
    LOCAL_ID.get()
}
