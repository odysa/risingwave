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
use std::ops::Bound::Unbounded;
use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::{HummockEpoch, HummockReadEpoch, HummockSstableId, LocalSstableInfo};
use risingwave_meta::hummock::test_utils::{
    register_table_ids_to_compaction_group, setup_compute_env,
    update_filter_key_extractor_for_table_ids,
};
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
use risingwave_storage::hummock::test_utils::{count_iter, default_config_for_test};
use risingwave_storage::hummock::{HummockStorage, HummockStorageV1};
use risingwave_storage::monitor::StateStoreMetrics;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::{
    ReadOptions, StateStore, StateStoreRead, StateStoreWrite, SyncResult, WriteOptions,
};
use risingwave_storage::StateStoreIter;

use crate::test_utils::{
    get_test_notification_client, with_hummock_storage_v1, with_hummock_storage_v2,
    HummockStateStoreTestTrait, HummockV2MixedStateStore,
};

#[tokio::test]
async fn test_basic_v1() {
    let (hummock_storage, meta_client) = with_hummock_storage_v1().await;
    test_basic_inner(hummock_storage, meta_client).await;
}

#[tokio::test]
async fn test_basic_v2() {
    let (hummock_storage, meta_client) = with_hummock_storage_v2().await;
    test_basic_inner(hummock_storage, meta_client).await;
}

async fn test_basic_inner(
    hummock_storage: impl HummockStateStoreTestTrait,
    meta_client: Arc<MockHummockMetaClient>,
) {
    let anchor = Bytes::from("aa");

    // First batch inserts the anchor and others.
    let mut batch1 = vec![
        (anchor.clone(), StorageValue::new_put("111")),
        (Bytes::from("bb"), StorageValue::new_put("222")),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Second batch modifies the anchor.
    let mut batch2 = vec![
        (Bytes::from("cc"), StorageValue::new_put("333")),
        (anchor.clone(), StorageValue::new_put("111111")),
    ];

    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Third batch deletes the anchor
    let mut batch3 = vec![
        (Bytes::from("dd"), StorageValue::new_put("444")),
        (Bytes::from("ee"), StorageValue::new_put("555")),
        (anchor.clone(), StorageValue::new_delete()),
    ];

    // Make sure the batch is sorted.
    batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // epoch 0 is reserved by storage service
    let epoch1: u64 = 1;

    // try to write an empty batch, and hummock should write nothing
    let size = hummock_storage
        .ingest_batch(
            vec![],
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    assert_eq!(size, 0);

    // Write the first batch.
    hummock_storage
        .ingest_batch(
            batch1,
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage
        .get(
            &anchor,
            epoch1,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));
    let value = hummock_storage
        .get(
            &Bytes::from("bb"),
            epoch1,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("222"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = hummock_storage
        .get(
            &Bytes::from("ab"),
            epoch1,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write the second batch.
    let epoch2 = epoch1 + 1;
    hummock_storage
        .ingest_batch(
            batch2,
            vec![],
            WriteOptions {
                epoch: epoch2,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage
        .get(
            &anchor,
            epoch2,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111111"));

    // Write the third batch.
    let epoch3 = epoch2 + 1;
    hummock_storage
        .ingest_batch(
            batch3,
            vec![],
            WriteOptions {
                epoch: epoch3,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage
        .get(
            &anchor,
            epoch3,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    // Get non-existent maximum key.
    let value = hummock_storage
        .get(
            &Bytes::from("ff"),
            epoch3,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write aa bb
    let mut iter = hummock_storage
        .iter(
            (Bound::Unbounded, Bound::Included(b"ee".to_vec())),
            epoch1,
            ReadOptions {
                check_bloom_filter: false,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 2);

    // Get the anchor value at the first snapshot
    let value = hummock_storage
        .get(
            &anchor,
            epoch1,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));

    // Get the anchor value at the second snapshot
    let value = hummock_storage
        .get(
            &anchor,
            epoch2,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111111"));
    // Update aa, write cc
    let mut iter = hummock_storage
        .iter(
            (Bound::Unbounded, Bound::Included(b"ee".to_vec())),
            epoch2,
            ReadOptions {
                check_bloom_filter: false,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 3);

    // Delete aa, write dd,ee
    let mut iter = hummock_storage
        .iter(
            (Bound::Unbounded, Bound::Included(b"ee".to_vec())),
            epoch3,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 4);
    let ssts = hummock_storage
        .seal_and_sync_epoch(epoch1)
        .await
        .unwrap()
        .uncommitted_ssts;
    meta_client.commit_epoch(epoch1, ssts).await.unwrap();
    hummock_storage
        .try_wait_epoch(HummockReadEpoch::Committed(epoch1))
        .await
        .unwrap();
    let value = hummock_storage
        .get(
            &Bytes::from("bb"),
            epoch2,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("222"));
    let value = hummock_storage
        .get(
            &Bytes::from("dd"),
            epoch2,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap();
    assert!(value.is_none());
}

#[tokio::test]
async fn test_state_store_sync_v1() {
    let (hummock_storage, meta_client) = with_hummock_storage_v1().await;
    test_state_store_sync_inner(hummock_storage, meta_client).await;
}

#[tokio::test]
async fn test_state_store_sync_v2() {
    let (hummock_storage, meta_client) = with_hummock_storage_v2().await;
    test_state_store_sync_inner(hummock_storage, meta_client).await;
}

async fn test_state_store_sync_inner(
    hummock_storage: impl HummockStateStoreTestTrait,
    _meta_client: Arc<MockHummockMetaClient>,
) {
    let mut config = default_config_for_test();
    config.shared_buffer_capacity_mb = 64;
    config.write_conflict_detection_enabled = false;

    let mut epoch: HummockEpoch = hummock_storage.get_pinned_version().max_committed_epoch() + 1;

    // ingest 16B batch
    let mut batch1 = vec![
        (Bytes::from("aaaa"), StorageValue::new_put("1111")),
        (Bytes::from("bbbb"), StorageValue::new_put("2222")),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    hummock_storage
        .ingest_batch(
            batch1,
            vec![],
            WriteOptions {
                epoch,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    // ingest 24B batch
    let mut batch2 = vec![
        (Bytes::from("cccc"), StorageValue::new_put("3333")),
        (Bytes::from("dddd"), StorageValue::new_put("4444")),
        (Bytes::from("eeee"), StorageValue::new_put("5555")),
    ];
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    hummock_storage
        .ingest_batch(
            batch2,
            vec![],
            WriteOptions {
                epoch,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    // TODO: Uncomment the following lines after flushed sstable can be accessed.
    // FYI: https://github.com/risingwavelabs/risingwave/pull/1928#discussion_r852698719
    // shared buffer threshold size should have been reached and will trigger a flush
    // then ingest the batch
    // assert_eq!(
    //     (24 + 8 * 3) as u64,
    //     hummock_storage.shared_buffer_manager().size() as u64
    // );

    epoch += 1;

    // ingest more 8B then will trigger a sync behind the scene
    let mut batch3 = vec![(Bytes::from("eeee"), StorageValue::new_put("5555"))];
    batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    hummock_storage
        .ingest_batch(
            batch3,
            vec![],
            WriteOptions {
                epoch,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    // TODO: Uncomment the following lines after flushed sstable can be accessed.
    // FYI: https://github.com/risingwavelabs/risingwave/pull/1928#discussion_r852698719
    // 16B in total with 8B epoch appended to the key
    // assert_eq!(
    //     16 as u64,
    //     hummock_storage.shared_buffer_manager().size() as u64
    // );

    // trigger a sync
    hummock_storage
        .seal_and_sync_epoch(epoch - 1)
        .await
        .unwrap();
    hummock_storage.seal_and_sync_epoch(epoch).await.unwrap();

    // TODO: Uncomment the following lines after flushed sstable can be accessed.
    // FYI: https://github.com/risingwavelabs/risingwave/pull/1928#discussion_r852698719
    // assert_eq!(0, hummock_storage.shared_buffer_manager().size());
}

#[tokio::test]
/// Fix this when we finished epoch management.
#[ignore]
async fn test_reload_storage() {
    let sstable_store = mock_sstable_store();
    let hummock_options = Arc::new(default_config_for_test());
    let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));

    // TODO: may also test for v2 when the unit test is enabled.
    let hummock_storage = HummockStorageV1::new(
        hummock_options.clone(),
        sstable_store.clone(),
        meta_client.clone(),
        get_test_notification_client(
            env.clone(),
            hummock_manager_ref.clone(),
            worker_node.clone(),
        ),
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();
    let anchor = Bytes::from("aa");

    // First batch inserts the anchor and others.
    let mut batch1 = vec![
        (anchor.clone(), StorageValue::new_put("111")),
        (Bytes::from("bb"), StorageValue::new_put("222")),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Second batch modifies the anchor.
    let mut batch2 = vec![
        (Bytes::from("cc"), StorageValue::new_put("333")),
        (anchor.clone(), StorageValue::new_put("111111")),
    ];

    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // epoch 0 is reserved by storage service
    let epoch1: u64 = 1;

    // Write the first batch.
    hummock_storage
        .ingest_batch(
            batch1,
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    // Mock something happened to storage internal, and storage is reloaded.
    drop(hummock_storage);
    let hummock_storage = HummockStorage::for_test(
        hummock_options.clone(),
        sstable_store.clone(),
        meta_client.clone(),
        get_test_notification_client(env, hummock_manager_ref, worker_node),
    )
    .await
    .unwrap();

    let hummock_storage = HummockV2MixedStateStore::new(hummock_storage).await;

    // Get the value after flushing to remote.
    let value = hummock_storage
        .get(
            &anchor,
            epoch1,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = hummock_storage
        .get(
            &Bytes::from("ab"),
            epoch1,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write the second batch.
    let epoch2 = epoch1 + 1;
    hummock_storage
        .ingest_batch(
            batch2,
            vec![],
            WriteOptions {
                epoch: epoch2,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage
        .get(
            &anchor,
            epoch2,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111111"));

    // Write aa bb
    let mut iter = hummock_storage
        .iter(
            (Bound::Unbounded, Bound::Included(b"ee".to_vec())),
            epoch1,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 2);

    // Get the anchor value at the first snapshot
    let value = hummock_storage
        .get(
            &anchor,
            epoch1,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));

    // Get the anchor value at the second snapshot
    let value = hummock_storage
        .get(
            &anchor,
            epoch2,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111111"));
    // Update aa, write cc
    let mut iter = hummock_storage
        .iter(
            (Bound::Unbounded, Bound::Included(b"ee".to_vec())),
            epoch2,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            },
        )
        .await
        .unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 3);
}

#[tokio::test]
async fn test_write_anytime_v1() {
    let (hummock_storage, meta_client) = with_hummock_storage_v1().await;
    test_write_anytime_inner(hummock_storage, meta_client).await;
}

#[tokio::test]
async fn test_write_anytime_v2() {
    let (hummock_storage, meta_client) = with_hummock_storage_v2().await;
    test_write_anytime_inner(hummock_storage, meta_client).await;
}

async fn test_write_anytime_inner(
    hummock_storage: impl HummockStateStoreTestTrait,
    _meta_client: Arc<MockHummockMetaClient>,
) {
    let initial_epoch = hummock_storage.get_pinned_version().max_committed_epoch();

    let epoch1 = initial_epoch + 1;

    let assert_old_value = |epoch| {
        let hummock_storage = hummock_storage.clone();
        async move {
            // check point get
            assert_eq!(
                "111".as_bytes(),
                hummock_storage
                    .get(
                        "aa".as_bytes(),
                        epoch,
                        ReadOptions {
                            check_bloom_filter: true,
                            prefix_hint: None,
                            table_id: Default::default(),
                            retention_seconds: None,
                        }
                    )
                    .await
                    .unwrap()
                    .unwrap()
            );
            assert_eq!(
                "222".as_bytes(),
                hummock_storage
                    .get(
                        "bb".as_bytes(),
                        epoch,
                        ReadOptions {
                            check_bloom_filter: true,
                            prefix_hint: None,
                            table_id: Default::default(),
                            retention_seconds: None,
                        }
                    )
                    .await
                    .unwrap()
                    .unwrap()
            );
            assert_eq!(
                "333".as_bytes(),
                hummock_storage
                    .get(
                        "cc".as_bytes(),
                        epoch,
                        ReadOptions {
                            check_bloom_filter: true,
                            prefix_hint: None,
                            table_id: Default::default(),
                            retention_seconds: None,
                        }
                    )
                    .await
                    .unwrap()
                    .unwrap()
            );
            // check iter
            let mut iter = hummock_storage
                .iter(
                    (
                        Bound::Included(b"aa".to_vec()),
                        Bound::Included(b"cc".to_vec()),
                    ),
                    epoch,
                    ReadOptions {
                        check_bloom_filter: false,
                        prefix_hint: None,
                        table_id: Default::default(),
                        retention_seconds: None,
                    },
                )
                .await
                .unwrap();
            assert_eq!(
                (
                    FullKey::for_test(TableId::default(), b"aa".to_vec(), epoch),
                    Bytes::from("111")
                ),
                iter.next().await.unwrap().unwrap()
            );
            assert_eq!(
                (
                    FullKey::for_test(TableId::default(), b"bb".to_vec(), epoch),
                    Bytes::from("222")
                ),
                iter.next().await.unwrap().unwrap()
            );
            assert_eq!(
                (
                    FullKey::for_test(TableId::default(), b"cc".to_vec(), epoch),
                    Bytes::from("333")
                ),
                iter.next().await.unwrap().unwrap()
            );
            assert!(iter.next().await.unwrap().is_none());
        }
    };

    let batch1 = vec![
        (Bytes::from("aa"), StorageValue::new_put("111")),
        (Bytes::from("bb"), StorageValue::new_put("222")),
        (Bytes::from("cc"), StorageValue::new_put("333")),
    ];

    hummock_storage
        .ingest_batch(
            batch1.clone(),
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    assert_old_value(epoch1).await;

    let assert_new_value = |epoch| {
        let hummock_storage = hummock_storage.clone();
        async move {
            // check point get
            assert_eq!(
                "111_new".as_bytes(),
                hummock_storage
                    .get(
                        "aa".as_bytes(),
                        epoch,
                        ReadOptions {
                            check_bloom_filter: true,
                            prefix_hint: None,
                            table_id: Default::default(),
                            retention_seconds: None,
                        }
                    )
                    .await
                    .unwrap()
                    .unwrap()
            );
            assert!(hummock_storage
                .get(
                    "bb".as_bytes(),
                    epoch,
                    ReadOptions {
                        check_bloom_filter: true,
                        prefix_hint: None,
                        table_id: Default::default(),
                        retention_seconds: None,
                    }
                )
                .await
                .unwrap()
                .is_none());
            assert_eq!(
                "333".as_bytes(),
                hummock_storage
                    .get(
                        "cc".as_bytes(),
                        epoch,
                        ReadOptions {
                            check_bloom_filter: true,
                            prefix_hint: None,
                            table_id: Default::default(),
                            retention_seconds: None,
                        }
                    )
                    .await
                    .unwrap()
                    .unwrap()
            );
            let mut iter = hummock_storage
                .iter(
                    (
                        Bound::Included(b"aa".to_vec()),
                        Bound::Included(b"cc".to_vec()),
                    ),
                    epoch,
                    ReadOptions {
                        check_bloom_filter: false,
                        prefix_hint: None,
                        table_id: Default::default(),
                        retention_seconds: None,
                    },
                )
                .await
                .unwrap();
            assert_eq!(
                (
                    FullKey::for_test(TableId::default(), b"aa".to_vec(), epoch),
                    Bytes::from("111_new")
                ),
                iter.next().await.unwrap().unwrap()
            );
            assert_eq!(
                (
                    FullKey::for_test(TableId::default(), b"cc".to_vec(), epoch),
                    Bytes::from("333")
                ),
                iter.next().await.unwrap().unwrap()
            );
            assert!(iter.next().await.unwrap().is_none());
        }
    };

    // Update aa, delete bb, cc unchanged
    let batch2 = vec![
        (Bytes::from("aa"), StorageValue::new_put("111_new")),
        (Bytes::from("bb"), StorageValue::new_delete()),
    ];

    hummock_storage
        .ingest_batch(
            batch2,
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    assert_new_value(epoch1).await;

    let epoch2 = epoch1 + 1;

    // Write to epoch2
    hummock_storage
        .ingest_batch(
            batch1,
            vec![],
            WriteOptions {
                epoch: epoch2,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    // Assert epoch 1 unchanged
    assert_new_value(epoch1).await;
    // Assert epoch 2 correctness
    assert_old_value(epoch2).await;

    let ssts1 = hummock_storage
        .seal_and_sync_epoch(epoch1)
        .await
        .unwrap()
        .uncommitted_ssts;
    assert_new_value(epoch1).await;
    assert_old_value(epoch2).await;

    let ssts2 = hummock_storage
        .seal_and_sync_epoch(epoch2)
        .await
        .unwrap()
        .uncommitted_ssts;
    assert_new_value(epoch1).await;
    assert_old_value(epoch2).await;

    assert!(!ssts1.is_empty());
    assert!(!ssts2.is_empty());
}

#[tokio::test]
async fn test_delete_get_v1() {
    let (hummock_storage, meta_client) = with_hummock_storage_v1().await;
    test_delete_get_inner(hummock_storage, meta_client).await;
}

#[tokio::test]
async fn test_delete_get_v2() {
    let (hummock_storage, meta_client) = with_hummock_storage_v2().await;
    test_delete_get_inner(hummock_storage, meta_client).await;
}

async fn test_delete_get_inner(
    hummock_storage: impl HummockStateStoreTestTrait,
    meta_client: Arc<MockHummockMetaClient>,
) {
    let initial_epoch = hummock_storage.get_pinned_version().max_committed_epoch();
    let epoch1 = initial_epoch + 1;
    let batch1 = vec![
        (Bytes::from("aa"), StorageValue::new_put("111")),
        (Bytes::from("bb"), StorageValue::new_put("222")),
    ];
    hummock_storage
        .ingest_batch(
            batch1,
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    let ssts = hummock_storage
        .seal_and_sync_epoch(epoch1)
        .await
        .unwrap()
        .uncommitted_ssts;
    meta_client.commit_epoch(epoch1, ssts).await.unwrap();
    let epoch2 = initial_epoch + 2;
    let batch2 = vec![(Bytes::from("bb"), StorageValue::new_delete())];
    hummock_storage
        .ingest_batch(
            batch2,
            vec![],
            WriteOptions {
                epoch: epoch2,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    let ssts = hummock_storage
        .seal_and_sync_epoch(epoch2)
        .await
        .unwrap()
        .uncommitted_ssts;
    meta_client.commit_epoch(epoch2, ssts).await.unwrap();
    hummock_storage
        .try_wait_epoch(HummockReadEpoch::Committed(epoch2))
        .await
        .unwrap();
    assert!(hummock_storage
        .get(
            "bb".as_bytes(),
            epoch2,
            ReadOptions {
                check_bloom_filter: true,
                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
            }
        )
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn test_multiple_epoch_sync_v1() {
    let (hummock_storage, meta_client) = with_hummock_storage_v1().await;
    test_multiple_epoch_sync_inner(hummock_storage, meta_client).await;
}

#[tokio::test]
async fn test_multiple_epoch_sync_v2() {
    let (hummock_storage, meta_client) = with_hummock_storage_v2().await;
    test_multiple_epoch_sync_inner(hummock_storage, meta_client).await;
}

async fn test_multiple_epoch_sync_inner(
    hummock_storage: impl HummockStateStoreTestTrait,
    meta_client: Arc<MockHummockMetaClient>,
) {
    let initial_epoch = hummock_storage.get_pinned_version().max_committed_epoch();
    let epoch1 = initial_epoch + 1;
    let batch1 = vec![
        (Bytes::from("aa"), StorageValue::new_put("111")),
        (Bytes::from("bb"), StorageValue::new_put("222")),
    ];
    hummock_storage
        .ingest_batch(
            batch1,
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    let epoch2 = initial_epoch + 2;
    let batch2 = vec![(Bytes::from("bb"), StorageValue::new_delete())];
    hummock_storage
        .ingest_batch(
            batch2,
            vec![],
            WriteOptions {
                epoch: epoch2,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    let epoch3 = initial_epoch + 3;
    let batch3 = vec![
        (Bytes::from("aa"), StorageValue::new_put("444")),
        (Bytes::from("bb"), StorageValue::new_put("555")),
    ];
    hummock_storage
        .ingest_batch(
            batch3,
            vec![],
            WriteOptions {
                epoch: epoch3,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    let test_get = || {
        let hummock_storage_clone = hummock_storage.clone();
        async move {
            assert_eq!(
                hummock_storage_clone
                    .get(
                        "bb".as_bytes(),
                        epoch1,
                        ReadOptions {
                            check_bloom_filter: true,
                            prefix_hint: None,
                            table_id: Default::default(),
                            retention_seconds: None,
                        }
                    )
                    .await
                    .unwrap()
                    .unwrap(),
                "222".as_bytes()
            );
            assert!(hummock_storage_clone
                .get(
                    "bb".as_bytes(),
                    epoch2,
                    ReadOptions {
                        check_bloom_filter: true,
                        prefix_hint: None,
                        table_id: Default::default(),
                        retention_seconds: None,
                    }
                )
                .await
                .unwrap()
                .is_none());
            assert_eq!(
                hummock_storage_clone
                    .get(
                        "bb".as_bytes(),
                        epoch3,
                        ReadOptions {
                            check_bloom_filter: true,
                            prefix_hint: None,
                            table_id: Default::default(),
                            retention_seconds: None,
                        }
                    )
                    .await
                    .unwrap()
                    .unwrap(),
                "555".as_bytes()
            );
        }
    };
    test_get().await;
    hummock_storage.seal_epoch(epoch1, false);
    let sync_result2 = hummock_storage.seal_and_sync_epoch(epoch2).await.unwrap();
    let sync_result3 = hummock_storage.seal_and_sync_epoch(epoch3).await.unwrap();
    test_get().await;
    meta_client
        .commit_epoch(epoch2, sync_result2.uncommitted_ssts)
        .await
        .unwrap();
    meta_client
        .commit_epoch(epoch3, sync_result3.uncommitted_ssts)
        .await
        .unwrap();
    hummock_storage
        .try_wait_epoch(HummockReadEpoch::Committed(epoch3))
        .await
        .unwrap();
    test_get().await;
}

#[tokio::test]
async fn test_gc_watermark_and_clear_shared_buffer() {
    let sstable_store = mock_sstable_store();
    let hummock_options = Arc::new(default_config_for_test());
    let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));

    let hummock_storage = HummockStorage::for_test(
        hummock_options,
        sstable_store,
        meta_client.clone(),
        get_test_notification_client(env, hummock_manager_ref, worker_node),
    )
    .await
    .unwrap();

    let hummock_storage = HummockV2MixedStateStore::new(hummock_storage).await;

    assert_eq!(
        hummock_storage
            .sstable_id_manager()
            .global_watermark_sst_id(),
        HummockSstableId::MAX
    );

    let initial_epoch = hummock_storage.get_pinned_version().max_committed_epoch();
    let epoch1 = initial_epoch + 1;
    let batch1 = vec![
        (Bytes::from("aa"), StorageValue::new_put("111")),
        (Bytes::from("bb"), StorageValue::new_put("222")),
    ];
    hummock_storage
        .ingest_batch(
            batch1,
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    assert_eq!(
        hummock_storage
            .sstable_id_manager()
            .global_watermark_sst_id(),
        HummockSstableId::MAX
    );

    let epoch2 = initial_epoch + 2;
    let batch2 = vec![(Bytes::from("bb"), StorageValue::new_delete())];
    hummock_storage
        .ingest_batch(
            batch2,
            vec![],
            WriteOptions {
                epoch: epoch2,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    assert_eq!(
        hummock_storage
            .sstable_id_manager()
            .global_watermark_sst_id(),
        HummockSstableId::MAX
    );
    let min_sst_id = |sync_result: &SyncResult| {
        sync_result
            .uncommitted_ssts
            .iter()
            .map(|LocalSstableInfo { sst_info, .. }| sst_info.id)
            .min()
            .unwrap()
    };
    let sync_result1 = hummock_storage.seal_and_sync_epoch(epoch1).await.unwrap();
    let min_sst_id_epoch1 = min_sst_id(&sync_result1);
    assert_eq!(
        hummock_storage
            .sstable_id_manager()
            .global_watermark_sst_id(),
        min_sst_id_epoch1,
    );
    let sync_result2 = hummock_storage.seal_and_sync_epoch(epoch2).await.unwrap();
    let min_sst_id_epoch2 = min_sst_id(&sync_result2);
    assert_eq!(
        hummock_storage
            .sstable_id_manager()
            .global_watermark_sst_id(),
        min_sst_id_epoch1,
    );
    meta_client
        .commit_epoch(epoch1, sync_result1.uncommitted_ssts)
        .await
        .unwrap();
    hummock_storage
        .try_wait_epoch(HummockReadEpoch::Committed(epoch1))
        .await
        .unwrap();

    assert_eq!(
        hummock_storage
            .sstable_id_manager()
            .global_watermark_sst_id(),
        min_sst_id_epoch2,
    );

    hummock_storage.clear_shared_buffer().await.unwrap();

    let read_version = hummock_storage.local.read_version();

    let read_version = read_version.read();
    assert!(read_version.staging().imm.is_empty());
    assert!(read_version.staging().sst.is_empty());
    assert_eq!(read_version.committed().max_committed_epoch(), epoch1);
    assert_eq!(
        hummock_storage
            .sstable_id_manager()
            .global_watermark_sst_id(),
        HummockSstableId::MAX
    );
}

// Make sure `table_id` in `ReadOptions` works as expected.
#[tokio::test]
async fn test_table_id_filter() {
    let sstable_store = mock_sstable_store();
    let hummock_options = Arc::new(default_config_for_test());
    let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));

    let hummock_storage = HummockStorage::for_test(
        hummock_options,
        sstable_store,
        meta_client.clone(),
        get_test_notification_client(env, hummock_manager_ref.clone(), worker_node),
    )
    .await
    .unwrap();

    let table_ids = vec![1, 2];
    register_table_ids_to_compaction_group(
        &hummock_manager_ref,
        table_ids.as_ref(),
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    update_filter_key_extractor_for_table_ids(
        hummock_storage.filter_key_extractor_manager().clone(),
        table_ids.as_ref(),
    );

    let hummock_storage = HummockV2MixedStateStore::new(hummock_storage).await;

    let initial_epoch = hummock_storage.get_pinned_version().max_committed_epoch();

    let gen_value =
        |value, table_id| Bytes::from(format!("{}_{}", value, table_id).as_bytes().to_vec());

    let gen_batches = |table_id| {
        vec![
            vec![(
                Bytes::from("aa"),
                StorageValue::new_put(gen_value("111", table_id)),
            )],
            vec![(
                Bytes::from("bb"),
                StorageValue::new_put(gen_value("222", table_id)),
            )],
        ]
    };

    let epochs: Vec<HummockEpoch> = vec![initial_epoch + 1, initial_epoch + 2];

    for table_id in &table_ids {
        let batches = gen_batches(*table_id);
        for (idx, epoch) in epochs.iter().enumerate() {
            hummock_storage
                .ingest_batch(
                    batches[idx].clone(),
                    vec![],
                    WriteOptions {
                        epoch: *epoch,
                        table_id: TableId::new(*table_id),
                    },
                )
                .await
                .unwrap();
        }
    }

    assert_eq!(
        hummock_storage
            .local
            .read_version()
            .read()
            .staging()
            .imm
            .len(),
        epochs.len() * table_ids.len()
    );

    let ssts = hummock_storage
        .seal_and_sync_epoch(epochs[0])
        .await
        .unwrap()
        .uncommitted_ssts;
    meta_client.commit_epoch(epochs[0], ssts).await.unwrap();
    hummock_storage
        .try_wait_epoch(HummockReadEpoch::Committed(epochs[0]))
        .await
        .unwrap();

    assert_eq!(
        hummock_storage
            .local
            .read_version()
            .read()
            .staging()
            .imm
            .len(),
        (epochs.len() - 1) * table_ids.len()
    );

    let assert_for_table_id = |table_id: u32| {
        let hummock_storage = hummock_storage.clone();
        let epochs = epochs.clone();
        let read_epoch = *epochs.last().unwrap();
        async move {
            // Assert point get.
            assert_eq!(
                hummock_storage
                    .get(
                        b"aa",
                        read_epoch,
                        ReadOptions {
                            check_bloom_filter: true,
                            prefix_hint: None,
                            table_id: TableId::new(table_id),
                            retention_seconds: None,
                        },
                    )
                    .await
                    .unwrap()
                    .unwrap(),
                gen_value("111", table_id)
            );

            assert_eq!(
                hummock_storage
                    .get(
                        b"bb",
                        read_epoch,
                        ReadOptions {
                            check_bloom_filter: true,
                            prefix_hint: None,
                            table_id: TableId::new(table_id),
                            retention_seconds: None,
                        },
                    )
                    .await
                    .unwrap()
                    .unwrap(),
                gen_value("222", table_id)
            );

            assert!(hummock_storage
                .get(
                    b"cc",
                    read_epoch,
                    ReadOptions {
                        check_bloom_filter: true,
                        prefix_hint: None,
                        table_id: TableId::new(table_id),
                        retention_seconds: None,
                    },
                )
                .await
                .unwrap()
                .is_none());

            // Assert iter.
            let mut iter = hummock_storage
                .iter(
                    (Unbounded, Unbounded),
                    read_epoch,
                    ReadOptions {
                        check_bloom_filter: true,
                        prefix_hint: None,
                        table_id: TableId::new(table_id),
                        retention_seconds: None,
                    },
                )
                .await
                .unwrap();

            assert_eq!(
                iter.next().await.unwrap(),
                Some((
                    FullKey::for_test(TableId::new(table_id), b"aa".to_vec(), epochs[0]),
                    gen_value("111", table_id)
                ))
            );
            assert_eq!(
                iter.next().await.unwrap(),
                Some((
                    FullKey::for_test(TableId::new(table_id), b"bb".to_vec(), epochs[1]),
                    gen_value("222", table_id)
                ))
            );
            assert!(iter.next().await.unwrap().is_none());
        }
    };

    for id in table_ids {
        assert_for_table_id(id).await;
    }
}
