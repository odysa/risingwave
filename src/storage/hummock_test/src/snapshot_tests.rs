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
use std::sync::Arc;

use bytes::Bytes;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
use risingwave_storage::hummock::test_utils::default_config_for_test;
use risingwave_storage::hummock::*;
use risingwave_storage::monitor::StateStoreMetrics;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::{ReadOptions, StateStoreIter, StateStoreWrite, WriteOptions};
use risingwave_storage::StateStore;

use crate::test_utils::{
    get_test_notification_client, with_hummock_storage_v1, with_hummock_storage_v2,
    HummockStateStoreTestTrait,
};

macro_rules! assert_count_range_scan {
    ($storage:expr, $range:expr, $expect_count:expr, $epoch:expr) => {{
        use std::ops::RangeBounds;
        let range = $range;
        let bounds: (Bound<Vec<u8>>, Bound<Vec<u8>>) = (
            range.start_bound().map(|x: &Bytes| x.to_vec()),
            range.end_bound().map(|x: &Bytes| x.to_vec()),
        );
        let mut it = $storage
            .iter(
                bounds,
                $epoch,
                ReadOptions {
                    check_bloom_filter: false,
                    prefix_hint: None,
                    table_id: Default::default(),
                    retention_seconds: None,
                },
            )
            .await
            .unwrap();
        let mut count = 0;
        loop {
            match it.next().await.unwrap() {
                Some(_) => count += 1,
                None => break,
            }
        }
        assert_eq!(count, $expect_count);
    }};
}

#[allow(unused_macros)]
macro_rules! assert_count_backward_range_scan {
    ($storage:expr, $range:expr, $expect_count:expr, $epoch:expr) => {{
        use std::ops::RangeBounds;
        let range = $range;
        let bounds: (Bound<Vec<u8>>, Bound<Vec<u8>>) = (
            range.start_bound().map(|x: &Bytes| x.to_vec()),
            range.end_bound().map(|x: &Bytes| x.to_vec()),
        );
        let mut it = $storage
            .backward_iter(
                bounds,
                ReadOptions {
                    epoch: $epoch,
                    table_id: Default::default(),
                    retention_seconds: None,
                },
            )
            .await
            .unwrap();
        let mut count = 0;
        loop {
            match it.next().await.unwrap() {
                Some(_) => count += 1,
                None => break,
            }
        }
        assert_eq!(count, $expect_count);
    }};
}

async fn test_snapshot_inner(
    hummock_storage: impl HummockStateStoreTestTrait,
    mock_hummock_meta_client: Arc<MockHummockMetaClient>,
    enable_sync: bool,
    enable_commit: bool,
) {
    let epoch1: u64 = 1;
    hummock_storage
        .ingest_batch(
            vec![
                (Bytes::from("1"), StorageValue::new_put("test")),
                (Bytes::from("2"), StorageValue::new_put("test")),
            ],
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    if enable_sync {
        let ssts = hummock_storage
            .seal_and_sync_epoch(epoch1)
            .await
            .unwrap()
            .uncommitted_ssts;
        if enable_commit {
            mock_hummock_meta_client
                .commit_epoch(epoch1, ssts)
                .await
                .unwrap();
            hummock_storage
                .try_wait_epoch(HummockReadEpoch::Committed(epoch1))
                .await
                .unwrap();
        }
    }
    assert_count_range_scan!(hummock_storage, .., 2, epoch1);

    let epoch2 = epoch1 + 1;
    hummock_storage
        .ingest_batch(
            vec![
                (Bytes::from("1"), StorageValue::new_delete()),
                (Bytes::from("3"), StorageValue::new_put("test")),
                (Bytes::from("4"), StorageValue::new_put("test")),
            ],
            vec![],
            WriteOptions {
                epoch: epoch2,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    if enable_sync {
        let ssts = hummock_storage
            .seal_and_sync_epoch(epoch2)
            .await
            .unwrap()
            .uncommitted_ssts;
        if enable_commit {
            mock_hummock_meta_client
                .commit_epoch(epoch2, ssts)
                .await
                .unwrap();
            hummock_storage
                .try_wait_epoch(HummockReadEpoch::Committed(epoch2))
                .await
                .unwrap();
        }
    }
    assert_count_range_scan!(hummock_storage, .., 3, epoch2);
    assert_count_range_scan!(hummock_storage, .., 2, epoch1);

    let epoch3 = epoch2 + 1;
    hummock_storage
        .ingest_batch(
            vec![
                (Bytes::from("2"), StorageValue::new_delete()),
                (Bytes::from("3"), StorageValue::new_delete()),
                (Bytes::from("4"), StorageValue::new_delete()),
            ],
            vec![],
            WriteOptions {
                epoch: epoch3,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    if enable_sync {
        let ssts = hummock_storage
            .seal_and_sync_epoch(epoch3)
            .await
            .unwrap()
            .uncommitted_ssts;
        if enable_commit {
            mock_hummock_meta_client
                .commit_epoch(epoch3, ssts)
                .await
                .unwrap();
            hummock_storage
                .try_wait_epoch(HummockReadEpoch::Committed(epoch3))
                .await
                .unwrap();
        }
    }
    assert_count_range_scan!(hummock_storage, .., 0, epoch3);
    assert_count_range_scan!(hummock_storage, .., 3, epoch2);
    assert_count_range_scan!(hummock_storage, .., 2, epoch1);
}

async fn test_snapshot_range_scan_inner(
    hummock_storage: impl HummockStateStoreTestTrait,
    mock_hummock_meta_client: Arc<MockHummockMetaClient>,
    enable_sync: bool,
    enable_commit: bool,
) {
    let epoch: u64 = 1;

    hummock_storage
        .ingest_batch(
            vec![
                (Bytes::from("1"), StorageValue::new_put("test")),
                (Bytes::from("2"), StorageValue::new_put("test")),
                (Bytes::from("3"), StorageValue::new_put("test")),
                (Bytes::from("4"), StorageValue::new_put("test")),
            ],
            vec![],
            WriteOptions {
                epoch,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    if enable_sync {
        let ssts = hummock_storage
            .seal_and_sync_epoch(epoch)
            .await
            .unwrap()
            .uncommitted_ssts;
        if enable_commit {
            mock_hummock_meta_client
                .commit_epoch(epoch, ssts)
                .await
                .unwrap();
            hummock_storage
                .try_wait_epoch(HummockReadEpoch::Committed(epoch))
                .await
                .unwrap();
        }
    }
    macro_rules! key {
        ($idx:expr) => {
            Bytes::from(stringify!($idx))
        };
    }

    assert_count_range_scan!(hummock_storage, key!(2)..=key!(3), 2, epoch);
    assert_count_range_scan!(hummock_storage, key!(2)..key!(3), 1, epoch);
    assert_count_range_scan!(hummock_storage, key!(2).., 3, epoch);
    assert_count_range_scan!(hummock_storage, ..=key!(3), 3, epoch);
    assert_count_range_scan!(hummock_storage, ..key!(3), 2, epoch);
    assert_count_range_scan!(hummock_storage, .., 4, epoch);
}

#[ignore]
async fn test_snapshot_backward_range_scan_inner(enable_sync: bool, enable_commit: bool) {
    let sstable_store = mock_sstable_store();
    let hummock_options = Arc::new(default_config_for_test());
    let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));

    // TODO: may also test for v2 when the unit test is enabled.
    let hummock_storage = HummockStorageV1::new(
        hummock_options,
        sstable_store,
        mock_hummock_meta_client.clone(),
        get_test_notification_client(env, hummock_manager_ref, worker_node),
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();

    let epoch = 1;
    hummock_storage
        .ingest_batch(
            vec![
                (Bytes::from("1"), StorageValue::new_put("test")),
                (Bytes::from("2"), StorageValue::new_put("test")),
                (Bytes::from("3"), StorageValue::new_put("test")),
                (Bytes::from("4"), StorageValue::new_put("test")),
                (Bytes::from("5"), StorageValue::new_put("test")),
                (Bytes::from("6"), StorageValue::new_put("test")),
            ],
            vec![],
            WriteOptions {
                epoch,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    if enable_sync {
        let ssts = hummock_storage
            .seal_and_sync_epoch(epoch)
            .await
            .unwrap()
            .uncommitted_ssts;
        if enable_commit {
            mock_hummock_meta_client
                .commit_epoch(epoch, ssts)
                .await
                .unwrap();
            hummock_storage
                .try_wait_epoch(HummockReadEpoch::Committed(epoch))
                .await
                .unwrap();
        }
    }
    hummock_storage
        .ingest_batch(
            vec![
                (Bytes::from("5"), StorageValue::new_put("test")),
                (Bytes::from("6"), StorageValue::new_put("test")),
                (Bytes::from("7"), StorageValue::new_put("test")),
                (Bytes::from("8"), StorageValue::new_put("test")),
            ],
            vec![],
            WriteOptions {
                epoch: epoch + 1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    if enable_sync {
        let ssts = hummock_storage
            .seal_and_sync_epoch(epoch + 1)
            .await
            .unwrap()
            .uncommitted_ssts;
        if enable_commit {
            mock_hummock_meta_client
                .commit_epoch(epoch + 1, ssts)
                .await
                .unwrap();
            hummock_storage
                .try_wait_epoch(HummockReadEpoch::Committed(epoch + 1))
                .await
                .unwrap();
        }
    }

    #[allow(unused_macros)]
    macro_rules! key {
        ($idx:expr) => {
            Bytes::from(stringify!($idx))
        };
    }

    // TODO: re-enable it when backward range scan is supported again on hummock
    // assert_count_backward_range_scan!(hummock_storage, key!(3)..=key!(2), 2, epoch);
    // assert_count_backward_range_scan!(hummock_storage, key!(3)..key!(2), 1, epoch);
    // assert_count_backward_range_scan!(hummock_storage, key!(3)..key!(1), 2, epoch);
    // assert_count_backward_range_scan!(hummock_storage, key!(3)..=key!(1), 3, epoch);
    // assert_count_backward_range_scan!(hummock_storage, key!(3)..key!(0), 3, epoch);
    // assert_count_backward_range_scan!(hummock_storage, .., 6, epoch);
    // assert_count_backward_range_scan!(hummock_storage, .., 8, epoch + 1);
    // assert_count_backward_range_scan!(hummock_storage, key!(7)..key!(2), 5, epoch + 1);
}

#[tokio::test]
async fn test_snapshot_v1() {
    let (storage, meta_client) = with_hummock_storage_v1().await;
    test_snapshot_inner(storage, meta_client, false, false).await;
}

#[tokio::test]
async fn test_snapshot_v2() {
    let (storage, meta_client) = with_hummock_storage_v2().await;
    test_snapshot_inner(storage, meta_client, false, false).await;
}

#[tokio::test]
async fn test_snapshot_with_sync_v1() {
    let (storage, meta_client) = with_hummock_storage_v1().await;
    test_snapshot_inner(storage, meta_client, true, false).await;
}

#[tokio::test]
async fn test_snapshot_with_sync_v2() {
    let (storage, meta_client) = with_hummock_storage_v2().await;
    test_snapshot_inner(storage, meta_client, true, false).await;
}

#[tokio::test]
async fn test_snapshot_with_commit_v1() {
    let (storage, meta_client) = with_hummock_storage_v1().await;
    test_snapshot_inner(storage, meta_client, true, true).await;
}

#[tokio::test]
async fn test_snapshot_with_commit_v2() {
    let (storage, meta_client) = with_hummock_storage_v2().await;
    test_snapshot_inner(storage, meta_client, true, true).await;
}

#[tokio::test]
async fn test_snapshot_range_scan_v1() {
    let (storage, meta_client) = with_hummock_storage_v1().await;
    test_snapshot_range_scan_inner(storage, meta_client, false, false).await;
}

#[tokio::test]
async fn test_snapshot_range_scan_v2() {
    let (storage, meta_client) = with_hummock_storage_v2().await;
    test_snapshot_range_scan_inner(storage, meta_client, false, false).await;
}

#[tokio::test]
async fn test_snapshot_range_scan_with_sync_v1() {
    let (storage, meta_client) = with_hummock_storage_v1().await;
    test_snapshot_range_scan_inner(storage, meta_client, true, false).await;
}

#[tokio::test]
async fn test_snapshot_range_scan_with_sync_v2() {
    let (storage, meta_client) = with_hummock_storage_v2().await;
    test_snapshot_range_scan_inner(storage, meta_client, true, false).await;
}

#[tokio::test]
async fn test_snapshot_range_scan_with_commit_v1() {
    let (storage, meta_client) = with_hummock_storage_v1().await;
    test_snapshot_range_scan_inner(storage, meta_client, true, true).await;
}

#[tokio::test]
async fn test_snapshot_range_scan_with_commit_v2() {
    let (storage, meta_client) = with_hummock_storage_v2().await;
    test_snapshot_range_scan_inner(storage, meta_client, true, true).await;
}

#[ignore]
#[tokio::test]
async fn test_snapshot_backward_range_scan() {
    test_snapshot_backward_range_scan_inner(false, false).await;
}

#[ignore]
#[tokio::test]
async fn test_snapshot_backward_range_scan_with_sync() {
    test_snapshot_backward_range_scan_inner(true, false).await;
}

#[ignore]
#[tokio::test]
async fn test_snapshot_backward_range_scan_with_commit() {
    test_snapshot_backward_range_scan_inner(true, true).await;
}
