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
use futures::{Future, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_hummock_trace::{
    init_collector, new_global_span, send_result, should_use_trace, trace_result, ConcurrentId,
    Operation, OperationResult, StorageType, TraceReadOptions, TraceResult, TraceSpan, TracedBytes,
    LOCAL_ID,
};

use crate::error::{StorageError, StorageResult};
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

    pub fn new_local(inner: S, table_id: TableId) -> Self {
        let id = get_concurrent_id();
        let storage_type = StorageType::Local(id, table_id.table_id);

        let _span = new_global_span!(Operation::NewLocalStorage, storage_type);

        Self {
            inner,
            storage_type,
        }
    }
}

impl<S: StateStoreRead> TracedStateStore<S> {
    pub fn inner(&self) -> &S {
        &self.inner
    }

    async fn traced_iter(
        &self,
        iter_stream_future: impl Future<Output = StorageResult<S::IterStream>>,
        span: Option<TraceSpan>,
    ) -> StorageResult<TracedStateStoreIterStream<S>> {
        // wait for iterator creation (e.g. seek)
        let iter_stream = match iter_stream_future.await {
            Err(e) => {
                if let Some(span) = span {
                    span.send_result(OperationResult::Iter(TraceResult::Err));
                }
                return Err(e);
            }
            Ok(inner) => {
                if let Some(span) = &span {
                    span.send_result(OperationResult::Iter(TraceResult::Ok(())));
                }
                inner
            }
        };
        let traced = TracedStateStoreIter {
            inner: iter_stream,
            span,
        };
        Ok(traced.into_stream())
    }
}

impl<S: LocalStateStore> LocalStateStore for TracedStateStore<S> {}

impl<S: StateStore> StateStore for TracedStateStore<S> {
    type Local = TracedStateStore<S::Local>;

    type NewLocalFuture<'a> = impl Future<Output = Self::Local> + Send + 'a;

    define_state_store_associated_type!();

    fn try_wait_epoch(&self, epoch: HummockReadEpoch) -> Self::WaitEpochFuture<'_> {
        async move { self.inner.try_wait_epoch(epoch).await }
    }

    fn sync(&self, epoch: u64) -> Self::SyncFuture<'_> {
        async move {
            let span = new_global_span!(Operation::Sync(epoch), self.storage_type);
            let sync_result = self.inner.sync(epoch).await;
            trace_result!(SYNC, span, sync_result);
            sync_result
        }
    }

    fn seal_epoch(&self, epoch: u64, is_checkpoint: bool) {
        let _span = new_global_span!(Operation::Seal(epoch, is_checkpoint), self.storage_type);
        self.inner.seal_epoch(epoch, is_checkpoint);
    }

    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        async move { self.inner.clear_shared_buffer().await }
    }

    fn new_local(&self, table_id: TableId) -> Self::NewLocalFuture<'_> {
        async move { TracedStateStore::new_local(self.inner.new_local(table_id).await, table_id) }
    }
}

pub type TracedStateStoreIterStream<S: StateStoreRead> = impl StateStoreReadIterStream;

impl<S: StateStoreRead> StateStoreRead for TracedStateStore<S> {
    type IterStream = TracedStateStoreIterStream<S>;

    define_state_store_read_associated_type!();

    fn get<'a>(
        &'a self,
        key: &'a [u8],
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::GetFuture<'_> {
        async move {
            let span = new_global_span!(
                Operation::get(
                    TracedBytes::from(key.to_vec()),
                    epoch,
                    read_options.prefix_hint.clone(),
                    read_options.check_bloom_filter,
                    read_options.retention_seconds,
                    read_options.table_id.table_id,
                    read_options.ignore_range_tombstone,
                ),
                self.storage_type
            );

            let res: StorageResult<Option<Bytes>> = self.inner.get(key, epoch, read_options).await;

            send_result!(
                span,
                OperationResult::Get(TraceResult::from(
                    res.as_ref()
                        .map(|o| { o.as_ref().map(|b| TracedBytes::from(b.clone())) })
                ))
            );

            res
        }
    }

    fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
        let span = new_global_span!(
            Operation::Iter {
                key_range: (
                    key_range.0.as_ref().map(|v| TracedBytes::from(v.clone())),
                    key_range.1.as_ref().map(|v| TracedBytes::from(v.clone()))
                ),
                epoch: epoch,
                read_options: TraceReadOptions {
                    prefix_hint: read_options.prefix_hint.clone(),
                    table_id: read_options.table_id.table_id,
                    retention_seconds: read_options.retention_seconds,
                    check_bloom_filter: read_options.check_bloom_filter,
                    ignore_range_tombstone: read_options.ignore_range_tombstone,
                }
            },
            self.storage_type
        );
        self.traced_iter(self.inner.iter(key_range, epoch, read_options), span)
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
            // Don't trace empty ingest to save disk space
            if kv_pairs.is_empty() && delete_ranges.is_empty() {
                return Ok(0);
            }
            let span = new_global_span!(
                Operation::ingest(
                    kv_pairs
                        .iter()
                        .map(|(k, v)| (
                            TracedBytes::from(k.clone()),
                            v.user_value.clone().map(|b| TracedBytes::from(b.clone()))
                        ))
                        .collect(),
                    delete_ranges
                        .iter()
                        .map(|(k, v)| (TracedBytes::from(k.clone()), TracedBytes::from(v.clone())))
                        .collect(),
                    write_options.epoch,
                    write_options.table_id.table_id,
                ),
                self.storage_type
            );

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

    pub fn sstable_id_manager(&self) -> &SstableIdManagerRef {
        self.inner.sstable_id_manager()
    }
}

impl<S> Drop for TracedStateStore<S> {
    fn drop(&mut self) {
        if let StorageType::Local(_, _) = self.storage_type {
            let _ = new_global_span!(Operation::DropLocalStorage, self.storage_type);
        }
    }
}

pub struct TracedStateStoreIter<S> {
    inner: S,
    span: Option<TraceSpan>,
}

impl<S: StateStoreIterItemStream> TracedStateStoreIter<S> {
    #[try_stream(ok = StateStoreIterItem, error = StorageError)]
    async fn into_stream_inner(self) {
        let inner = self.inner;
        futures::pin_mut!(inner);
        while let Some((key, value)) = inner.try_next().await? {
            if let Some(span) = &self.span {
                span.send(Operation::IterNext(span.id()));
            }

            send_result!(
                self.span,
                OperationResult::IterNext(TraceResult::Ok(Some((&key, &value)).map(|(k, v)| (
                    TracedBytes::from(k.user_key.table_key.to_vec()),
                    TracedBytes::from(v.clone())
                ))))
            );

            yield (key, value);
        }
    }

    fn into_stream(self) -> impl StateStoreIterItemStream {
        Self::into_stream_inner(self)
    }
}

impl<I> StateStoreIter for TracedStateStoreIter<I>
where
    I: StateStoreIter<Item = (FullKey<Vec<u8>>, Bytes)>,
{
    type Item = (FullKey<Vec<u8>>, Bytes);

    type NextFuture<'a> = impl NextFutureTrait<'a, Self::Item>;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            let kv_pair: _ = self.inner.next().await?;
            trace_result!(ITER_NEXT, self.span, kv_pair);
            Ok(kv_pair)
        }
    }
}

pub fn get_concurrent_id() -> ConcurrentId {
    LOCAL_ID.get()
}
