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

use std::future::Future;
use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::Epoch;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::{HummockReadEpoch, LocalSstableInfo};

use crate::error::StorageResult;
use crate::monitor::{MonitoredStateStore, StateStoreMetrics};
use crate::storage_value::StorageValue;
use crate::write_batch::WriteBatch;

pub trait StaticSendSync = Send + Sync + 'static;

pub trait NextFutureTrait<'a, Item> = Future<Output = StorageResult<Option<Item>>> + Send + 'a;
pub trait StateStoreIter: Send + 'static {
    type Item: Send;
    type NextFuture<'a>: NextFutureTrait<'a, Self::Item>;

    fn next(&mut self) -> Self::NextFuture<'_>;
}

pub trait StateStoreIterExt: StateStoreIter {
    type CollectFuture<'a>: Future<Output = StorageResult<Vec<<Self as StateStoreIter>::Item>>>
        + Send
        + 'a;

    fn collect(&mut self, limit: Option<usize>) -> Self::CollectFuture<'_>;
}

impl<I: StateStoreIter> StateStoreIterExt for I {
    type CollectFuture<'a> =
        impl Future<Output = StorageResult<Vec<<Self as StateStoreIter>::Item>>> + Send + 'a;

    fn collect(&mut self, limit: Option<usize>) -> Self::CollectFuture<'_> {
        async move {
            let mut kvs = Vec::with_capacity(limit.unwrap_or_default());

            for _ in 0..limit.unwrap_or(usize::MAX) {
                match self.next().await? {
                    Some(kv) => kvs.push(kv),
                    None => break,
                }
            }

            Ok(kvs)
        }
    }
}

#[macro_export]
macro_rules! define_state_store_read_associated_type {
    () => {
        type GetFuture<'a> = impl GetFutureTrait<'a>;
        type IterFuture<'a> = impl IterFutureTrait<'a, Self::Iter>;
    };
}

pub trait GetFutureTrait<'a> = Future<Output = StorageResult<Option<Bytes>>> + Send + 'a;
pub trait IterFutureTrait<'a, I: StateStoreIter<Item = (FullKey<Vec<u8>>, Bytes)>> =
    Future<Output = StorageResult<I>> + Send + 'a;
pub trait StateStoreRead: StaticSendSync {
    type Iter: StateStoreIter<Item = (FullKey<Vec<u8>>, Bytes)> + 'static;

    type GetFuture<'a>: GetFutureTrait<'a>;
    type IterFuture<'a>: IterFutureTrait<'a, Self::Iter>;

    /// Point gets a value from the state store.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn get<'a>(
        &'a self,
        key: &'a [u8],
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::GetFuture<'_>;

    /// Opens and returns an iterator for given `prefix_hint` and `full_key_range`
    /// Internally, `prefix_hint` will be used to for checking `bloom_filter` and
    /// `full_key_range` used for iter. (if the `prefix_hint` not None, it should be be included in
    /// `key_range`) The returned iterator will iterate data based on a snapshot corresponding to
    /// the given `epoch`.
    fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_>;
}

pub trait ScanFutureTrait<'a> =
    Future<Output = StorageResult<Vec<(FullKey<Vec<u8>>, Bytes)>>> + Send + 'a;

pub trait StateStoreReadExt: StaticSendSync {
    type ScanFuture<'a>: ScanFutureTrait<'a>;

    /// Scans `limit` number of keys from a key range. If `limit` is `None`, scans all elements.
    /// Internally, `prefix_hint` will be used to for checking `bloom_filter` and
    /// `full_key_range` used for iter.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    ///
    ///
    /// By default, this simply calls `StateStore::iter` to fetch elements.
    fn scan(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        limit: Option<usize>,
        read_options: ReadOptions,
    ) -> Self::ScanFuture<'_>;
}

impl<S: StateStoreRead> StateStoreReadExt for S {
    type ScanFuture<'a> = impl ScanFutureTrait<'a>;

    fn scan(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        limit: Option<usize>,
        read_options: ReadOptions,
    ) -> Self::ScanFuture<'_> {
        async move {
            self.iter(key_range, epoch, read_options)
                .await?
                .collect(limit)
                .await
        }
    }
}

#[macro_export]
macro_rules! define_state_store_write_associated_type {
    () => {
        type IngestBatchFuture<'a> = impl IngestBatchFutureTrait<'a>;
    };
}

pub trait IngestBatchFutureTrait<'a> = Future<Output = StorageResult<usize>> + Send + 'a;
pub trait StateStoreWrite: StaticSendSync {
    type IngestBatchFuture<'a>: IngestBatchFutureTrait<'a>;

    /// Writes a batch to storage. The batch should be:
    /// * Ordered. KV pairs will be directly written to the table, so it must be ordered.
    /// * Locally unique. There should not be two or more operations on the same key in one write
    ///   batch.
    ///
    /// Ingests a batch of data into the state store. One write batch should never contain operation
    /// on the same key. e.g. Put(233, x) then Delete(233).
    /// An epoch should be provided to ingest a write batch. It is served as:
    /// - A handle to represent an atomic write session. All ingested write batches associated with
    ///   the same `Epoch` have the all-or-nothing semantics, meaning that partial changes are not
    ///   queryable and will be rolled back if instructed.
    /// - A version of a kv pair. kv pair associated with larger `Epoch` is guaranteed to be newer
    ///   then kv pair with smaller `Epoch`. Currently this version is only used to derive the
    ///   per-key modification history (e.g. in compaction), not across different keys.
    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        delete_ranges: Vec<(Bytes, Bytes)>,
        write_options: WriteOptions,
    ) -> Self::IngestBatchFuture<'_>;

    /// Creates a `WriteBatch` associated with this state store.
    fn start_write_batch(&self, write_options: WriteOptions) -> WriteBatch<'_, Self>
    where
        Self: Sized,
    {
        WriteBatch::new(self, write_options)
    }
}

#[derive(Default, Debug)]
pub struct SyncResult {
    /// The size of all synced shared buffers.
    pub sync_size: usize,
    /// The sst_info of sync.
    pub uncommitted_ssts: Vec<LocalSstableInfo>,
}
pub trait EmptyFutureTrait<'a> = Future<Output = StorageResult<()>> + Send + 'a;
pub trait SyncFutureTrait<'a> = Future<Output = StorageResult<SyncResult>> + Send + 'a;

#[macro_export]
macro_rules! define_state_store_associated_type {
    () => {
        type WaitEpochFuture<'a> = impl EmptyFutureTrait<'a>;
        type SyncFuture<'a> = impl SyncFutureTrait<'a>;
        type ClearSharedBufferFuture<'a> = impl EmptyFutureTrait<'a>;
    };
}

pub trait StateStore: StateStoreRead + StaticSendSync + Clone {
    type Local: LocalStateStore;

    type WaitEpochFuture<'a>: EmptyFutureTrait<'a>;

    type SyncFuture<'a>: SyncFutureTrait<'a>;

    type ClearSharedBufferFuture<'a>: EmptyFutureTrait<'a>;

    type NewLocalFuture<'a>: Future<Output = Self::Local> + Send + 'a;

    /// If epoch is `Committed`, we will wait until the epoch is committed and its data is ready to
    /// read. If epoch is `Current`, we will only check if the data can be read with this epoch.
    fn try_wait_epoch(&self, epoch: HummockReadEpoch) -> Self::WaitEpochFuture<'_>;

    fn sync(&self, epoch: u64) -> Self::SyncFuture<'_>;

    /// update max current epoch in storage.
    fn seal_epoch(&self, epoch: u64, is_checkpoint: bool);

    /// Creates a [`MonitoredStateStore`] from this state store, with given `stats`.
    fn monitored(self, stats: Arc<StateStoreMetrics>) -> MonitoredStateStore<Self> {
        MonitoredStateStore::new(self, stats)
    }

    /// Clears contents in shared buffer.
    /// This method should only be called when dropping all actors in the local compute node.
    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        todo!()
    }

    fn new_local(&self, table_id: TableId) -> Self::NewLocalFuture<'_>;
}

/// A state store that is dedicated for streaming operator, which only reads the uncommitted data
/// written by itself. Each local state store is not `Clone`, and is owned by a streaming state
/// table.
pub trait LocalStateStore: StateStoreRead + StateStoreWrite + StaticSendSync {
    /// Inserts a key-value entry associated with a given `epoch` into the state store.
    fn insert(&self, _key: Bytes, _val: Bytes) -> StorageResult<()> {
        unimplemented!()
    }

    /// Deletes a key-value entry from the state store. Only the key-value entry with epoch smaller
    /// than the given `epoch` will be deleted.
    fn delete(&self, _key: Bytes) -> StorageResult<()> {
        unimplemented!()
    }

    /// Triggers a flush to persistent storage for the in-memory states.
    fn flush(&self) -> StorageResult<usize> {
        unimplemented!()
    }

    /// Updates the monotonically increasing write epoch to `new_epoch`.
    /// All writes after this function is called will be tagged with `new_epoch`. In other words,
    /// the previous write epoch is sealed.
    fn advance_write_epoch(&mut self, _new_epoch: u64) -> StorageResult<()> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct ReadOptions {
    /// A hint for prefix key to check bloom filter.
    /// If the `prefix_hint` is not None, it should be included in
    /// `key` or `key_range` in the read API.
    pub prefix_hint: Option<Vec<u8>>,
    pub check_bloom_filter: bool,

    pub retention_seconds: Option<u32>,
    pub table_id: TableId,
}

pub fn gen_min_epoch(base_epoch: u64, retention_seconds: Option<&u32>) -> u64 {
    let base_epoch = Epoch(base_epoch);
    match retention_seconds {
        Some(retention_seconds_u32) => {
            base_epoch
                .subtract_ms((retention_seconds_u32 * 1000) as u64)
                .0
        }
        None => 0,
    }
}

#[derive(Default, Clone)]
pub struct WriteOptions {
    pub epoch: u64,
    pub table_id: TableId,
}
