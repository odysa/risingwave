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

mod hummock_replay;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use clap::Parser;
use hummock_replay::{get_test_notification_client, HummockInterface};
use risingwave_common::config::StorageConfig;
use risingwave_hummock_trace::{HummockReplay, Result, TraceReaderImpl};
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_object_store::object::parse_remote_object_store;
use risingwave_storage::hummock::compaction_group_client::{
    CompactionGroupClientImpl, MetaCompactionGroupClient,
};
use risingwave_storage::hummock::{HummockStorage, SstableStore, TieredCache};
use risingwave_storage::monitor::{ObjectStoreMetrics, StateStoreMetrics};

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    path: String,
}

fn main() {}

async fn run_replay(path: &Path) -> Result<()> {
    let f = File::open(path)?;
    let reader = TraceReaderImpl::new(f)?;
    let hummock = create_hummock("".to_string())
        .await
        .expect("fail to create hummock");
    let replay_interface = Box::new(HummockInterface::new(hummock));
    let (mut replayer, handle) = HummockReplay::new(reader, replay_interface);

    replayer.run().unwrap();

    handle.await.expect("fail to wait replaying thread");

    Ok(())
}

async fn create_hummock(object_store: String) -> Result<HummockStorage> {
    let config = Arc::new(StorageConfig::default());

    let state_store_stats = Arc::new(StateStoreMetrics::unused());
    let object_store_stats = Arc::new(ObjectStoreMetrics::unused());
    let object_store = parse_remote_object_store(&object_store, object_store_stats).await;

    let sstable_store = {
        let tiered_cache = TieredCache::none();
        Arc::new(SstableStore::new(
            Arc::new(object_store),
            config.data_directory.to_string(),
            config.block_cache_capacity_mb * (1 << 20),
            config.meta_cache_capacity_mb * (1 << 20),
            tiered_cache,
        ))
    };

    let (hummock_meta_client, notification_client) = {
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let notification_client = get_test_notification_client(
            env.clone(),
            hummock_manager_ref.clone(),
            worker_node.clone(),
        );

        (
            Arc::new(MockHummockMetaClient::new(
                hummock_manager_ref.clone(),
                worker_node.id,
            )),
            notification_client,
        )
    };

    let compaction_group_client = Arc::new(CompactionGroupClientImpl::Meta(Arc::new(
        MetaCompactionGroupClient::new(hummock_meta_client.clone()),
    )));

    let storage = HummockStorage::new(
        config,
        sstable_store,
        hummock_meta_client.clone(),
        notification_client,
        state_store_stats,
        compaction_group_client,
    )
    .await
    .expect("fail to new a HummockStorage");
    Ok(storage)
}
