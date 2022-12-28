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

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use itertools::Itertools;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use risingwave_common::catalog::TableId;
use risingwave_common::config::{load_config, StorageConfig};
use risingwave_hummock_sdk::compact::CompactorRuntimeConfig;
use risingwave_hummock_sdk::filter_key_extractor::{
    FilterKeyExtractorImpl, FilterKeyExtractorManager, FullKeyFilterKeyExtractor,
};
use risingwave_hummock_test::get_test_notification_client;
use risingwave_meta::hummock::compaction::compaction_config::CompactionConfigBuilder;
use risingwave_meta::hummock::test_utils::setup_compute_env_with_config;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::parse_remote_object_store;
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_pb::hummock::{CompactionConfig, CompactionGroup, TableOption};
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::hummock::backup_reader::BackupReader;
use risingwave_storage::hummock::compactor::{CompactionExecutor, CompactorContext, Context};
use risingwave_storage::hummock::sstable_store::SstableStoreRef;
use risingwave_storage::hummock::store::state_store::LocalHummockStorage;
use risingwave_storage::hummock::{
    CompactorSstableStore, HummockStorage, MemoryLimiter, SstableIdManager, SstableStore,
    TieredCache,
};
use risingwave_storage::monitor::StateStoreMetrics;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::{ReadOptions, StateStoreRead, StateStoreWrite, WriteOptions};
use risingwave_storage::StateStore;

use crate::CompactionTestOpts;

pub fn start_delete_range(opts: CompactionTestOpts) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        tracing::info!("Compaction delete-range test start with options {:?}", opts);
        let prefix = opts.state_store.strip_prefix("hummock+");
        match prefix {
            Some(s) => {
                assert!(
                    s.starts_with("s3://") || s.starts_with("minio://"),
                    "Only support S3 and MinIO object store"
                );
            }
            None => {
                panic!("Invalid state store");
            }
        }
        let ret = compaction_test_main(opts).await;

        match ret {
            Ok(_) => {
                tracing::info!("Compaction delete-range test Success");
            }
            Err(e) => {
                panic!("Compaction delete-range test Fail: {}", e);
            }
        }
    })
}
pub async fn compaction_test_main(opts: CompactionTestOpts) -> anyhow::Result<()> {
    let config = load_config(&opts.config_path);
    let mut storage_config = config.storage;
    storage_config.enable_state_store_v1 = false;
    let compaction_config = CompactionConfigBuilder::new().build();
    compaction_test(
        compaction_config,
        storage_config,
        &opts.state_store,
        1000000,
        2000,
    )
    .await
}

async fn compaction_test(
    compaction_config: CompactionConfig,
    storage_config: StorageConfig,
    state_store_type: &str,
    test_range: u64,
    test_count: u64,
) -> anyhow::Result<()> {
    let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env_with_config(8080, compaction_config.clone()).await;
    let meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));

    let delete_key_table = ProstTable {
        id: 1,
        schema_id: 1,
        database_id: 1,
        name: "delete-key-table".to_string(),
        columns: vec![],
        pk: vec![],
        dependent_relations: vec![],
        distribution_key: vec![],
        stream_key: vec![],
        owner: 0,
        properties: Default::default(),
        fragment_id: 0,
        vnode_col_index: None,
        value_indices: vec![],
        definition: "".to_string(),
        handle_pk_conflict: false,
        read_prefix_len_hint: 0,
        optional_associated_source_id: None,
        table_type: 0,
        append_only: false,
        row_id_index: None,
    };
    let mut delete_range_table = delete_key_table.clone();
    delete_range_table.id = 2;
    delete_range_table.name = "delete-range-table".to_string();
    let mut group1 = CompactionGroup {
        id: 3,
        parent_id: 0,
        member_table_ids: vec![1],
        compaction_config: Some(compaction_config.clone()),
        table_id_to_options: Default::default(),
    };
    group1.table_id_to_options.insert(
        1,
        TableOption {
            retention_seconds: 0,
        },
    );
    let mut group2 = CompactionGroup {
        id: 4,
        parent_id: 0,
        member_table_ids: vec![2],
        compaction_config: Some(compaction_config.clone()),
        table_id_to_options: Default::default(),
    };
    group2.table_id_to_options.insert(
        2,
        TableOption {
            retention_seconds: 0,
        },
    );
    hummock_manager_ref
        .init_metadata_for_replay(
            vec![delete_key_table, delete_range_table],
            vec![group1, group2],
        )
        .await?;

    let config = Arc::new(storage_config);

    let state_store_metrics = Arc::new(StateStoreMetrics::unused());
    let object_store_metrics = Arc::new(ObjectStoreMetrics::unused());
    let remote_object_store = parse_remote_object_store(
        state_store_type.strip_prefix("hummock+").unwrap(),
        object_store_metrics.clone(),
        false,
        "Hummock",
    )
    .await;
    let sstable_store = Arc::new(SstableStore::new(
        Arc::new(remote_object_store),
        config.data_directory.to_string(),
        config.block_cache_capacity_mb * (1 << 20),
        config.meta_cache_capacity_mb * (1 << 20),
        TieredCache::none(),
    ));

    let store = HummockStorage::new(
        config.clone(),
        sstable_store.clone(),
        BackupReader::unused(),
        meta_client.clone(),
        get_test_notification_client(env, hummock_manager_ref.clone(), worker_node),
        state_store_metrics.clone(),
        Arc::new(risingwave_tracing::RwTracingService::disabled()),
    )
    .await?;
    let sstable_id_manager = store.sstable_id_manager().clone();
    let filter_key_extractor_manager = store.filter_key_extractor_manager().clone();
    filter_key_extractor_manager.update(
        1,
        Arc::new(FilterKeyExtractorImpl::FullKey(
            FullKeyFilterKeyExtractor {},
        )),
    );
    filter_key_extractor_manager.update(
        2,
        Arc::new(FilterKeyExtractorImpl::FullKey(
            FullKeyFilterKeyExtractor {},
        )),
    );

    let (compactor_thrd, compactor_shutdown_tx) = run_compactor_thread(
        config,
        sstable_store,
        meta_client.clone(),
        filter_key_extractor_manager,
        sstable_id_manager,
        state_store_metrics,
    );
    run_compare_result(&store, meta_client.clone(), test_range, test_count)
        .await
        .unwrap();
    let version = store.get_pinned_version().version();
    let remote_version = meta_client.get_current_version().await.unwrap();
    println!(
        "version-{}, remote version-{}",
        version.id, remote_version.id
    );
    for (group, levels) in &version.levels {
        let l0 = levels.l0.as_ref().unwrap();
        let sz = levels
            .levels
            .iter()
            .map(|level| level.total_file_size)
            .sum::<u64>();
        let count = levels
            .levels
            .iter()
            .map(|level| level.table_infos.len())
            .sum::<usize>();
        println!(
            "group-{}: base: {} {} , l0 sz: {}, count: {}",
            group,
            sz,
            count,
            l0.total_file_size,
            l0.sub_levels
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>()
        );
    }
    compactor_shutdown_tx.send(()).unwrap();
    compactor_thrd.await.unwrap();
    Ok(())
}

async fn run_compare_result(
    hummock: &HummockStorage,
    meta_client: Arc<MockHummockMetaClient>,
    test_range: u64,
    test_count: u64,
) -> Result<(), String> {
    let mut normal = NormalState::new(hummock, 1, 0).await;
    let mut delete_range = DeleteRangeState::new(hummock, 2, 0).await;
    const RANGE_BASE: u64 = 400;
    let range_mod = test_range / RANGE_BASE;

    let mut rng = StdRng::seed_from_u64(10097);
    let mut overlap_ranges = vec![];
    for epoch in 1..test_count {
        for idx in 0..1000 {
            let op = rng.next_u32() % 50;
            let key_number = rng.next_u64() % test_range;
            if op == 0 {
                let end_key = key_number + (rng.next_u64() % range_mod) + 1;
                overlap_ranges.push((key_number, end_key, epoch, idx));
                let start_key = format!("{:010}", key_number);
                let end_key = format!("{:010}", end_key);
                normal
                    .delete_range(start_key.as_bytes(), end_key.as_bytes())
                    .await;
                delete_range
                    .delete_range(start_key.as_bytes(), end_key.as_bytes())
                    .await;
            } else if op < 5 {
                let key = format!("{:010}", key_number);
                let a = normal.get(key.as_bytes()).await;
                let b = delete_range.get(key.as_bytes()).await;
                assert!(
                    a.eq(&b),
                    "query {} {:?} vs {:?} in epoch-{}",
                    key_number,
                    a.map(|raw| String::from_utf8(raw.to_vec()).unwrap()),
                    b.map(|raw| String::from_utf8(raw.to_vec()).unwrap()),
                    epoch,
                );
            } else if op < 10 {
                let end_key = key_number + (rng.next_u64() % range_mod) + 1;
                let start_key = format!("{:010}", key_number);
                let end_key = format!("{:010}", end_key);
                let ret1 = normal.scan(start_key.as_bytes(), end_key.as_bytes()).await;
                let ret2 = delete_range
                    .scan(start_key.as_bytes(), end_key.as_bytes())
                    .await;
                assert_eq!(ret1, ret2);
            } else {
                let overlap = overlap_ranges
                    .iter()
                    .any(|(left, right, _, _)| *left <= key_number && key_number < *right);
                if overlap {
                    continue;
                }
                let key = format!("{:010}", key_number);
                let val = format!("val-{:010}-{:016}-{:016}", idx, key_number, epoch);
                normal.insert(key.as_bytes(), val.as_bytes());
                delete_range.insert(key.as_bytes(), val.as_bytes());
            }
        }
        normal.commit(epoch).await?;
        delete_range.commit(epoch).await?;
        // let checkpoint = epoch % 10 == 0;
        let ret = hummock.seal_and_sync_epoch(epoch).await.unwrap();
        meta_client
            .commit_epoch(epoch, ret.uncommitted_ssts)
            .await
            .map_err(|e| format!("{:?}", e))?;
        if epoch % 200 == 0 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    Ok(())
}

struct NormalState {
    storage: LocalHummockStorage,
    cache: BTreeMap<Vec<u8>, StorageValue>,
    table_id: TableId,
    epoch: u64,
}

struct DeleteRangeState {
    inner: NormalState,
    delete_ranges: Vec<(Bytes, Bytes)>,
}

impl DeleteRangeState {
    async fn new(hummock: &HummockStorage, table_id: u32, epoch: u64) -> Self {
        Self {
            inner: NormalState::new(hummock, table_id, epoch).await,
            delete_ranges: vec![],
        }
    }
}

#[async_trait::async_trait]
trait CheckState {
    async fn delete_range(&mut self, left: &[u8], right: &[u8]);
    async fn get(&self, key: &[u8]) -> Option<Bytes>;
    async fn scan(&self, left: &[u8], right: &[u8]) -> Vec<(Bytes, Bytes)>;
    fn insert(&mut self, key: &[u8], val: &[u8]);
    async fn commit(&mut self, epoch: u64) -> Result<(), String>;
}

impl NormalState {
    async fn new(hummock: &HummockStorage, table_id: u32, epoch: u64) -> Self {
        let table_id = TableId::new(table_id);
        let storage = hummock.new_local(table_id).await;
        Self {
            cache: BTreeMap::default(),
            storage,
            epoch,
            table_id,
        }
    }

    async fn commit_impl(
        &mut self,
        delete_ranges: Vec<(Bytes, Bytes)>,
        epoch: u64,
    ) -> Result<(), String> {
        let data = std::mem::take(&mut self.cache)
            .into_iter()
            .map(|(key, val)| (Bytes::from(key), val))
            .collect_vec();
        self.storage
            .ingest_batch(
                data,
                delete_ranges,
                WriteOptions {
                    epoch,
                    table_id: self.table_id,
                },
            )
            .await
            .map_err(|e| format!("{:?}", e))?;
        self.epoch = epoch;
        Ok(())
    }

    async fn get_from_storage(&self, key: &[u8], ignore_range_tombstone: bool) -> Option<Bytes> {
        self.storage
            .get(
                key,
                self.epoch,
                ReadOptions {
                    prefix_hint: None,
                    ignore_range_tombstone,
                    check_bloom_filter: false,
                    retention_seconds: None,
                    table_id: self.table_id,
                    read_version_from_backup: false,
                },
            )
            .await
            .unwrap()
    }

    async fn get_impl(&self, key: &[u8], ignore_range_tombstone: bool) -> Option<Bytes> {
        if let Some(val) = self.cache.get(key) {
            return val.user_value.clone();
        }
        self.get_from_storage(key, ignore_range_tombstone).await
    }

    async fn scan_impl(
        &self,
        left: &[u8],
        right: &[u8],
        ignore_range_tombstone: bool,
    ) -> Vec<(Bytes, Bytes)> {
        let mut iter = Box::pin(
            self.storage
                .iter(
                    (
                        Bound::Included(left.to_vec()),
                        Bound::Excluded(right.to_vec()),
                    ),
                    self.epoch,
                    ReadOptions {
                        prefix_hint: None,
                        ignore_range_tombstone,
                        check_bloom_filter: false,
                        retention_seconds: None,
                        table_id: self.table_id,
                        read_version_from_backup: false,
                    },
                )
                .await
                .unwrap(),
        );
        let mut ret = vec![];
        while let Some(item) = iter.next().await {
            let (full_key, val) = item.unwrap();
            let tkey = full_key.user_key.table_key.0.clone();
            if let Some(cache_val) = self.cache.get(&tkey) {
                if cache_val.user_value.is_some() {
                    ret.push((Bytes::from(tkey), cache_val.user_value.clone().unwrap()));
                } else {
                    continue;
                }
            } else {
                ret.push((Bytes::from(tkey), val));
            }
        }
        for (key, val) in self.cache.range((
            Bound::Included(left.to_vec()),
            Bound::Excluded(right.to_vec()),
        )) {
            if let Some(uval) = val.user_value.as_ref() {
                ret.push((Bytes::from(key.clone()), uval.clone()));
            }
        }
        ret.sort_by(|a, b| a.0.cmp(&b.0));
        ret
    }
}

#[async_trait::async_trait]
impl CheckState for NormalState {
    async fn delete_range(&mut self, left: &[u8], right: &[u8]) {
        self.cache
            .retain(|key, _| key.as_slice().lt(left) || key.as_slice().ge(right));
        let mut iter = Box::pin(
            self.storage
                .iter(
                    (
                        Bound::Included(left.to_vec()),
                        Bound::Excluded(right.to_vec()),
                    ),
                    self.epoch,
                    ReadOptions {
                        prefix_hint: None,
                        ignore_range_tombstone: true,
                        check_bloom_filter: false,
                        retention_seconds: None,
                        table_id: self.table_id,
                        read_version_from_backup: false,
                    },
                )
                .await
                .unwrap(),
        );
        while let Some(item) = iter.next().await {
            let (full_key, _) = item.unwrap();
            self.cache
                .insert(full_key.user_key.table_key.0, StorageValue::new_delete());
        }
    }

    fn insert(&mut self, key: &[u8], val: &[u8]) {
        self.cache
            .insert(key.to_vec(), StorageValue::new_put(val.to_vec()));
    }

    async fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.get_impl(key, true).await
    }

    async fn scan(&self, left: &[u8], right: &[u8]) -> Vec<(Bytes, Bytes)> {
        self.scan_impl(left, right, true).await
    }

    async fn commit(&mut self, epoch: u64) -> Result<(), String> {
        self.commit_impl(vec![], epoch).await
    }
}

#[async_trait::async_trait]
impl CheckState for DeleteRangeState {
    async fn delete_range(&mut self, left: &[u8], right: &[u8]) {
        self.delete_ranges
            .push((Bytes::copy_from_slice(left), Bytes::copy_from_slice(right)));
    }

    async fn get(&self, key: &[u8]) -> Option<Bytes> {
        for (left, right) in &self.delete_ranges {
            if left.as_ref().le(key) && right.as_ref().gt(key) {
                return None;
            }
        }
        self.inner.get_impl(key, false).await
    }

    async fn scan(&self, left: &[u8], right: &[u8]) -> Vec<(Bytes, Bytes)> {
        let mut ret = self.inner.scan_impl(left, right, false).await;
        ret.retain(|(key, _)| {
            for (left, right) in &self.delete_ranges {
                if left.as_ref().le(key) && right.as_ref().gt(key) {
                    return false;
                }
            }
            true
        });
        ret
    }

    fn insert(&mut self, key: &[u8], val: &[u8]) {
        self.inner.insert(key, val);
    }

    async fn commit(&mut self, epoch: u64) -> Result<(), String> {
        let delete_ranges = std::mem::take(&mut self.delete_ranges);
        self.inner.commit_impl(delete_ranges, epoch).await
    }
}

fn run_compactor_thread(
    config: Arc<StorageConfig>,
    sstable_store: SstableStoreRef,
    meta_client: Arc<MockHummockMetaClient>,
    filter_key_extractor_manager: Arc<FilterKeyExtractorManager>,
    sstable_id_manager: Arc<SstableIdManager>,
    state_store_metrics: Arc<StateStoreMetrics>,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let compact_sstable_store = Arc::new(CompactorSstableStore::new(
        sstable_store.clone(),
        MemoryLimiter::unlimit(),
    ));

    let context = Arc::new(Context {
        options: config,
        hummock_meta_client: meta_client.clone(),
        sstable_store,
        stats: state_store_metrics,
        is_share_buffer_compact: false,
        compaction_executor: Arc::new(CompactionExecutor::new(None)),
        filter_key_extractor_manager,
        read_memory_limiter: MemoryLimiter::unlimit(),
        sstable_id_manager,
        task_progress_manager: Default::default(),
    });
    let context = CompactorContext::with_config(
        context,
        compact_sstable_store,
        CompactorRuntimeConfig {
            max_concurrent_task_number: 4,
        },
    );
    let compactor_context = Arc::new(context);
    risingwave_storage::hummock::compactor::Compactor::start_compactor(
        compactor_context,
        meta_client,
    )
}

#[cfg(test)]
mod tests {

    use risingwave_common::config::StorageConfig;
    use risingwave_meta::hummock::compaction::compaction_config::CompactionConfigBuilder;

    use super::compaction_test;

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_small_data() {
        let storage_config = StorageConfig {
            enable_state_store_v1: false,
            ..Default::default()
        };
        let mut compaction_config = CompactionConfigBuilder::new().build();
        compaction_config.max_sub_compaction = 1;
        compaction_config.level0_tier_compact_file_number = 2;
        compaction_config.max_bytes_for_level_base = 512 * 1024;
        compaction_config.sub_level_max_compaction_bytes = 256 * 1024;
        compaction_test(
            compaction_config,
            storage_config,
            "hummock+memory",
            10000,
            100,
        )
        .await
        .unwrap();
    }
}
