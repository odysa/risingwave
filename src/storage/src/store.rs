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
use risingwave_hummock_sdk::{HummockReadEpoch, LocalSstableInfo};

use crate::error::StorageResult;
use crate::monitor::{MonitoredStateStore, StateStoreMetrics};
use crate::storage_value::StorageValue;
use crate::write_batch::WriteBatch;

#[derive(Default, Debug)]
pub struct SyncResult {
    /// The size of all synced shared buffers.
    pub sync_size: usize,
    /// The sst_info of sync.
    pub uncommitted_ssts: Vec<LocalSstableInfo>,
}

pub trait GetFutureTrait<'a> = Future<Output = StorageResult<Option<Bytes>>> + Send + 'a;
pub trait ScanFutureTrait<'a> = Future<Output = StorageResult<Vec<(Bytes, Bytes)>>> + Send + 'a;
pub trait IterFutureTrait<'a, I: StateStoreIter<Item = (Bytes, Bytes)>> =
    Future<Output = StorageResult<I>> + Send + 'a;
pub trait EmptyFutureTrait<'a> = Future<Output = StorageResult<()>> + Send + 'a;
pub trait SyncFutureTrait<'a> = Future<Output = StorageResult<SyncResult>> + Send + 'a;
pub trait IngestBatchFutureTrait<'a> = Future<Output = StorageResult<usize>> + Send + 'a;

#[macro_export]
macro_rules! define_state_store_associated_type {
    () => {
        type GetFuture<'a> = impl GetFutureTrait<'a>;
        type IngestBatchFuture<'a> = impl IngestBatchFutureTrait<'a>;
        type WaitEpochFuture<'a> = impl EmptyFutureTrait<'a>;
        type SyncFuture<'a> = impl SyncFutureTrait<'a>;

        type BackwardIterFuture<'a> = impl IterFutureTrait<'a, Self::Iter>;

        type IterFuture<'a> = impl IterFutureTrait<'a, Self::Iter>;

        type BackwardScanFuture<'a> = impl ScanFutureTrait<'a>;

        type ScanFuture<'a> = impl ScanFutureTrait<'a>;

        type ClearSharedBufferFuture<'a> = impl EmptyFutureTrait<'a>;
    };
}

pub trait StateStore: Send + Sync + 'static + Clone {
    type Iter: StateStoreIter<Item = (Bytes, Bytes)> + 'static;

    type GetFuture<'a>: GetFutureTrait<'a>;

    type ScanFuture<'a>: ScanFutureTrait<'a>;

    type BackwardScanFuture<'a>: ScanFutureTrait<'a>;

    type IngestBatchFuture<'a>: IngestBatchFutureTrait<'a>;

    type WaitEpochFuture<'a>: EmptyFutureTrait<'a>;

    type SyncFuture<'a>: SyncFutureTrait<'a>;

    type IterFuture<'a>: IterFutureTrait<'a, Self::Iter>;

    type BackwardIterFuture<'a>: IterFutureTrait<'a, Self::Iter>;

    type ClearSharedBufferFuture<'a>: EmptyFutureTrait<'a>;

    /// Point gets a value from the state store.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn get<'a>(
        &'a self,
        key: &'a [u8],
        check_bloom_filter: bool,
        read_options: ReadOptions,
    ) -> Self::GetFuture<'_>;

    /// Scans `limit` number of keys from a key range. If `limit` is `None`, scans all elements.
    /// Internally, `prefix_hint` will be used to for checking `bloom_filter` and
    /// `full_key_range` used for iter.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    ///
    ///
    /// By default, this simply calls `StateStore::iter` to fetch elements.
    fn scan(
        &self,
        prefix_hint: Option<Vec<u8>>,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        limit: Option<usize>,
        read_options: ReadOptions,
    ) -> Self::ScanFuture<'_>;

    fn backward_scan(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        limit: Option<usize>,
        read_options: ReadOptions,
    ) -> Self::BackwardScanFuture<'_>;

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
        write_options: WriteOptions,
    ) -> Self::IngestBatchFuture<'_>;

    /// Opens and returns an iterator for given `prefix_hint` and `full_key_range`
    /// Internally, `prefix_hint` will be used to for checking `bloom_filter` and
    /// `full_key_range` used for iter. (if the `prefix_hint` not None, it should be be included in
    /// `key_range`) The returned iterator will iterate data based on a snapshot corresponding to
    /// the given `epoch`.
    fn iter(
        &self,
        prefix_hint: Option<Vec<u8>>,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_>;

    /// Opens and returns a backward iterator for given `key_range`.
    /// The returned iterator will iterate data based on a snapshot corresponding to the given
    /// `epoch`
    fn backward_iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        read_options: ReadOptions,
    ) -> Self::BackwardIterFuture<'_>;

    /// Creates a `WriteBatch` associated with this state store.
    fn start_write_batch(&self, write_options: WriteOptions) -> WriteBatch<'_, Self> {
        WriteBatch::new(self, write_options)
    }

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
}

pub trait StateStoreIter: Send + 'static {
    type Item;
    type NextFuture<'a>: Future<Output = StorageResult<Option<Self::Item>>> + Send;

    fn next(&mut self) -> Self::NextFuture<'_>;
}

#[derive(Default, Clone)]
pub struct ReadOptions {
    pub epoch: u64,
    pub table_id: TableId,
    pub retention_seconds: Option<u32>, // second
}

#[derive(Default, Clone)]
pub struct WriteOptions {
    pub epoch: u64,
    pub table_id: TableId,
}

impl ReadOptions {
    pub fn min_epoch(&self) -> u64 {
        use risingwave_common::util::epoch::Epoch;
        let epoch = Epoch(self.epoch);
        match self.retention_seconds.as_ref() {
            Some(retention_seconds_u32) => {
                epoch.subtract_ms((retention_seconds_u32 * 1000) as u64).0
            }
            None => 0,
        }
    }
}
