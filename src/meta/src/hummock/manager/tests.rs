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

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::table_stats::{to_prost_table_stats_map, TableStats, TableStatsMap};
// use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::{
    HummockContextId, HummockEpoch, HummockVersionId, LocalSstableInfo, FIRST_VERSION_ID,
};
use risingwave_pb::common::{HostAddress, WorkerType};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::pin_version_response::Payload;
use risingwave_pb::hummock::{
    HummockPinnedSnapshot, HummockPinnedVersion, HummockSnapshot, KeyRange, SstableInfo,
};

use crate::hummock::compaction::ManualCompactionOption;
use crate::hummock::error::Error;
use crate::hummock::test_utils::*;
use crate::hummock::{start_compaction_scheduler, CompactionScheduler, HummockManagerRef};
use crate::manager::WorkerId;
use crate::model::MetadataModel;
use crate::storage::MemStore;

fn pin_versions_sum(pin_versions: &[HummockPinnedVersion]) -> usize {
    pin_versions.iter().len()
}

fn pin_snapshots_epoch(pin_snapshots: &[HummockPinnedSnapshot]) -> Vec<u64> {
    pin_snapshots
        .iter()
        .map(|p| p.minimal_pinned_snapshot)
        .collect_vec()
}

#[tokio::test]
async fn test_unpin_snapshot_before() {
    let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let epoch = 0;

    for _ in 0..2 {
        let pin_result = hummock_manager.pin_snapshot(context_id).await.unwrap();
        assert_eq!(pin_result.committed_epoch, epoch);
        let pinned_snapshots = HummockPinnedSnapshot::list(env.meta_store()).await.unwrap();
        assert_eq!(pinned_snapshots[0].context_id, context_id);
        assert_eq!(
            pinned_snapshots[0].minimal_pinned_snapshot,
            pin_result.committed_epoch
        );
    }

    // unpin nonexistent target will not return error
    for _ in 0..3 {
        hummock_manager
            .unpin_snapshot_before(
                context_id,
                HummockSnapshot {
                    committed_epoch: epoch,
                    current_epoch: epoch,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            pin_snapshots_epoch(&HummockPinnedSnapshot::list(env.meta_store()).await.unwrap()),
            vec![epoch]
        );
    }

    // unpin nonexistent target will not return error
    for _ in 0..3 {
        hummock_manager
            .unpin_snapshot_before(
                context_id,
                HummockSnapshot {
                    committed_epoch: epoch + 1,
                    current_epoch: epoch + 1,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            pin_snapshots_epoch(&HummockPinnedSnapshot::list(env.meta_store()).await.unwrap()),
            vec![epoch]
        );
    }
}

#[tokio::test]
async fn test_hummock_compaction_task() {
    let (_, hummock_manager, _, worker_node) = setup_compute_env(80).await;
    let sst_num = 2;

    // No compaction task available.
    assert!(hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
        .await
        .unwrap()
        .is_none());

    // Add some sstables and commit.
    let epoch: u64 = 1;
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, sst_num).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &original_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    commit_from_meta_node(
        hummock_manager.borrow(),
        epoch,
        to_local_sstable_info(&original_tables),
    )
    .await
    .unwrap();

    // Get a compaction task.
    let mut compact_task = hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
        .await
        .unwrap()
        .unwrap();
    // Get the compactor and assign task.
    let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
    compactor_manager.add_compactor(worker_node.id, u64::MAX);
    let compactor = hummock_manager.get_idle_compactor().await.unwrap();
    hummock_manager
        .assign_compaction_task(&compact_task, compactor.context_id())
        .await
        .unwrap();
    assert_eq!(compactor.context_id(), worker_node.id);
    assert_eq!(
        compact_task
            .get_input_ssts()
            .first()
            .unwrap()
            .get_level_idx(),
        0
    );
    assert_eq!(compact_task.get_task_id(), 2);

    // Cancel the task and succeed.
    assert!(hummock_manager
        .cancel_compact_task(&mut compact_task, TaskStatus::ManualCanceled)
        .await
        .unwrap());
    // Cancel a non-existent task and succeed.
    assert!(hummock_manager
        .cancel_compact_task(&mut compact_task, TaskStatus::ManualCanceled)
        .await
        .unwrap());

    // Get compactor.
    let compactor = hummock_manager.get_idle_compactor().await.unwrap();
    // Get a compaction task.
    let mut compact_task = hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
        .await
        .unwrap()
        .unwrap();
    hummock_manager
        .assign_compaction_task(&compact_task, compactor.context_id())
        .await
        .unwrap();
    assert_eq!(compact_task.get_task_id(), 3);
    // Finish the task and succeed.
    compact_task.set_task_status(TaskStatus::Success);

    assert!(hummock_manager
        .report_compact_task(compactor.context_id(), &mut compact_task, None)
        .await
        .unwrap());
    // Finish the task and told the task is not found, which may have been processed previously.
    assert!(!hummock_manager
        .report_compact_task(compactor.context_id(), &mut compact_task, None)
        .await
        .unwrap());
}

#[tokio::test]
async fn test_hummock_table() {
    let (_env, hummock_manager, _cluster_manager, _worker_node) = setup_compute_env(80).await;

    let epoch: u64 = 1;
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &original_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    commit_from_meta_node(
        hummock_manager.borrow(),
        epoch,
        to_local_sstable_info(&original_tables),
    )
    .await
    .unwrap();

    let pinned_version = hummock_manager.get_current_version().await;
    let levels =
        pinned_version.get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into());
    assert_eq!(
        Ordering::Equal,
        levels
            .l0
            .as_ref()
            .unwrap()
            .sub_levels
            .iter()
            .chain(levels.levels.iter())
            .flat_map(|level| level.table_infos.iter())
            .map(|info| info.id)
            .sorted()
            .cmp(original_tables.iter().map(|ot| ot.id).sorted())
    );

    // Confirm tables got are equal to original tables
    assert_eq!(
        get_sorted_sstable_ids(&original_tables),
        get_sorted_committed_sstable_ids(&pinned_version)
    );
}

#[tokio::test]
async fn test_hummock_transaction() {
    let (_env, hummock_manager, _cluster_manager, _worker_node) = setup_compute_env(80).await;
    let mut committed_tables = vec![];

    // Add and commit tables in epoch1.
    // BEFORE:  committed_epochs = []
    // AFTER:   committed_epochs = [epoch1]
    let epoch1: u64 = 1;
    {
        // Add tables in epoch1
        let tables_in_epoch1 = generate_test_tables(epoch1, get_sst_ids(&hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            &hummock_manager,
            &tables_in_epoch1,
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        // Get tables before committing epoch1. No tables should be returned.
        let current_version = hummock_manager.get_current_version().await;
        assert_eq!(current_version.max_committed_epoch, INVALID_EPOCH);
        assert!(get_sorted_committed_sstable_ids(&current_version).is_empty());

        // Commit epoch1
        commit_from_meta_node(
            hummock_manager.borrow(),
            epoch1,
            to_local_sstable_info(&tables_in_epoch1),
        )
        .await
        .unwrap();
        committed_tables.extend(tables_in_epoch1.clone());

        // Get tables after committing epoch1. All tables committed in epoch1 should be returned
        let current_version = hummock_manager.get_current_version().await;
        assert_eq!(current_version.max_committed_epoch, epoch1);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(&current_version)
        );
    }

    // Add and commit tables in epoch2.
    // BEFORE:  committed_epochs = [epoch1]
    // AFTER:   committed_epochs = [epoch1, epoch2]
    let epoch2 = epoch1 + 1;
    {
        // Add tables in epoch2
        let tables_in_epoch2 = generate_test_tables(epoch2, get_sst_ids(&hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            &hummock_manager,
            &tables_in_epoch2,
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        // Get tables before committing epoch2. tables_in_epoch1 should be returned and
        // tables_in_epoch2 should be invisible.
        let current_version = hummock_manager.get_current_version().await;
        assert_eq!(current_version.max_committed_epoch, epoch1);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(&current_version)
        );

        // Commit epoch2
        commit_from_meta_node(
            hummock_manager.borrow(),
            epoch2,
            to_local_sstable_info(&tables_in_epoch2),
        )
        .await
        .unwrap();
        committed_tables.extend(tables_in_epoch2);

        // Get tables after committing epoch2. tables_in_epoch1 and tables_in_epoch2 should be
        // returned
        let current_version = hummock_manager.get_current_version().await;
        assert_eq!(current_version.max_committed_epoch, epoch2);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(&current_version)
        );
    }
}

#[tokio::test]
async fn test_release_context_resource() {
    let (env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(1).await;
    let context_id_1 = worker_node.id;

    let fake_host_address_2 = HostAddress {
        host: "127.0.0.1".to_string(),
        port: 2,
    };
    let fake_parallelism = 4;
    let worker_node_2 = cluster_manager
        .add_worker_node(
            WorkerType::ComputeNode,
            fake_host_address_2,
            fake_parallelism,
        )
        .await
        .unwrap();
    let context_id_2 = worker_node_2.id;

    assert_eq!(
        pin_versions_sum(&HummockPinnedVersion::list(env.meta_store()).await.unwrap()),
        0
    );
    hummock_manager.pin_version(context_id_1).await.unwrap();
    hummock_manager.pin_version(context_id_2).await.unwrap();
    hummock_manager.pin_snapshot(context_id_1).await.unwrap();
    hummock_manager.pin_snapshot(context_id_2).await.unwrap();
    assert_eq!(
        pin_versions_sum(&HummockPinnedVersion::list(env.meta_store()).await.unwrap()),
        2
    );
    assert_eq!(
        HummockPinnedSnapshot::list(env.meta_store())
            .await
            .unwrap()
            .len(),
        2
    );
    hummock_manager
        .release_contexts(&vec![context_id_1])
        .await
        .unwrap();
    let pinned_versions = HummockPinnedVersion::list(env.meta_store()).await.unwrap();
    assert_eq!(pin_versions_sum(&pinned_versions), 1);
    assert_eq!(pinned_versions[0].context_id, context_id_2);
    let pinned_snapshots = HummockPinnedSnapshot::list(env.meta_store()).await.unwrap();
    assert_eq!(pinned_snapshots[0].context_id, context_id_2);
    // it's OK to call again
    hummock_manager
        .release_contexts(&vec![context_id_1])
        .await
        .unwrap();
    hummock_manager
        .release_contexts(&vec![context_id_2])
        .await
        .unwrap();
    assert_eq!(
        pin_versions_sum(&HummockPinnedVersion::list(env.meta_store()).await.unwrap()),
        0
    );
}

#[tokio::test]
async fn test_context_id_validation() {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let invalid_context_id = HummockContextId::MAX;
    let context_id = worker_node.id;

    // Invalid context id is rejected.
    let error = hummock_manager
        .pin_version(invalid_context_id)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidContext(_)));

    // Valid context id is accepted.
    hummock_manager.pin_version(context_id).await.unwrap();
    // Pin multiple times is OK.
    hummock_manager.pin_version(context_id).await.unwrap();
}

#[tokio::test]
async fn test_hummock_manager_basic() {
    let (_env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(1).await;
    let context_id_1 = worker_node.id;

    let fake_host_address_2 = HostAddress {
        host: "127.0.0.1".to_string(),
        port: 2,
    };
    let fake_parallelism = 4;
    let worker_node_2 = cluster_manager
        .add_worker_node(
            WorkerType::ComputeNode,
            fake_host_address_2,
            fake_parallelism,
        )
        .await
        .unwrap();
    let context_id_2 = worker_node_2.id;

    // initial version id
    assert_eq!(
        hummock_manager.get_current_version().await.id,
        FIRST_VERSION_ID
    );

    let mut epoch = 1;
    let commit_one = |epoch: HummockEpoch, hummock_manager: HummockManagerRef<MemStore>| async move {
        let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            &hummock_manager,
            &original_tables,
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        commit_from_meta_node(
            hummock_manager.borrow(),
            epoch,
            to_local_sstable_info(&original_tables),
        )
        .await
        .unwrap();
    };

    commit_one(epoch, hummock_manager.clone()).await;
    epoch += 1;

    let sync_group_version_id = FIRST_VERSION_ID;

    // increased version id
    assert_eq!(
        hummock_manager.get_current_version().await.id,
        sync_group_version_id + 1
    );

    // min pinned version id if no clients
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        HummockVersionId::MAX
    );
    for _ in 0..2 {
        hummock_manager
            .unpin_version_before(context_id_1, u64::MAX)
            .await
            .unwrap();
        assert_eq!(
            hummock_manager.get_min_pinned_version_id().await,
            HummockVersionId::MAX
        );

        // should pin latest because u64::MAX
        let version = match hummock_manager.pin_version(context_id_1).await.unwrap() {
            Payload::VersionDeltas(_) => {
                unreachable!("should get full version")
            }
            Payload::PinnedVersion(version) => version,
        };
        assert_eq!(version.get_id(), sync_group_version_id + 1);
        assert_eq!(
            hummock_manager.get_min_pinned_version_id().await,
            sync_group_version_id + 1
        );
    }

    commit_one(epoch, hummock_manager.clone()).await;
    // epoch += 1;

    for _ in 0..2 {
        // should pin latest because deltas cannot contain INVALID_EPOCH
        let version = match hummock_manager.pin_version(context_id_2).await.unwrap() {
            Payload::VersionDeltas(_) => {
                unreachable!("should get full version")
            }
            Payload::PinnedVersion(version) => version,
        };
        assert_eq!(version.get_id(), sync_group_version_id + 2);
        // pinned by context_id_1
        assert_eq!(
            hummock_manager.get_min_pinned_version_id().await,
            sync_group_version_id + 1
        );
    }

    // ssts_to_delete is always empty because no compaction is ever invoked.
    assert!(hummock_manager.get_ssts_to_delete().await.is_empty());
    assert_eq!(
        hummock_manager
            .delete_version_deltas(usize::MAX)
            .await
            .unwrap(),
        (0, 0)
    );
    assert_eq!(
        hummock_manager.proceed_version_checkpoint().await.unwrap(),
        sync_group_version_id
    );
    assert!(hummock_manager.get_ssts_to_delete().await.is_empty());
    assert_eq!(
        hummock_manager
            .delete_version_deltas(usize::MAX)
            .await
            .unwrap(),
        (sync_group_version_id as usize, 0)
    );

    hummock_manager
        .unpin_version_before(context_id_1, u64::MAX)
        .await
        .unwrap();
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        sync_group_version_id + 2
    );
    assert!(hummock_manager.get_ssts_to_delete().await.is_empty());
    assert_eq!(
        hummock_manager
            .delete_version_deltas(usize::MAX)
            .await
            .unwrap(),
        (0, 0)
    );
    assert_eq!(
        hummock_manager.proceed_version_checkpoint().await.unwrap(),
        1
    );
    assert!(hummock_manager.get_ssts_to_delete().await.is_empty());
    assert_eq!(
        hummock_manager
            .delete_version_deltas(usize::MAX)
            .await
            .unwrap(),
        (1, 0)
    );

    hummock_manager
        .unpin_version_before(context_id_2, u64::MAX)
        .await
        .unwrap();
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        HummockVersionId::MAX
    );

    assert_eq!(
        hummock_manager.proceed_version_checkpoint().await.unwrap(),
        0
    );
}

#[tokio::test]
async fn test_pin_snapshot_response_lost() {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    let mut epoch: u64 = 1;
    let test_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &test_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    // [ ] -> [ e0 ]
    commit_from_meta_node(
        hummock_manager.borrow(),
        epoch,
        to_local_sstable_info(&test_tables),
    )
    .await
    .unwrap();
    epoch += 1;

    // Pin a snapshot with smallest last_pin
    // [ e0 ] -> [ e0:pinned ]
    let mut epoch_recorded_in_frontend = hummock_manager.pin_snapshot(context_id).await.unwrap();
    assert_eq!(epoch_recorded_in_frontend.committed_epoch, epoch - 1);

    let test_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &test_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    // [ e0:pinned ] -> [ e0:pinned, e1 ]
    commit_from_meta_node(
        hummock_manager.borrow(),
        epoch,
        to_local_sstable_info(&test_tables),
    )
    .await
    .unwrap();
    epoch += 1;

    // Assume the response of the previous rpc is lost.
    // [ e0:pinned, e1 ] -> [ e0, e1:pinned ]
    epoch_recorded_in_frontend = hummock_manager.pin_snapshot(context_id).await.unwrap();
    assert_eq!(epoch_recorded_in_frontend.committed_epoch, epoch - 1);

    // Assume the response of the previous rpc is lost.
    // [ e0, e1:pinned ] -> [ e0, e1:pinned ]
    epoch_recorded_in_frontend = hummock_manager.pin_snapshot(context_id).await.unwrap();
    assert_eq!(epoch_recorded_in_frontend.committed_epoch, epoch - 1);

    let test_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &test_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    // [ e0, e1:pinned ] -> [ e0, e1:pinned, e2 ]
    commit_from_meta_node(
        hummock_manager.borrow(),
        epoch,
        to_local_sstable_info(&test_tables),
    )
    .await
    .unwrap();
    epoch += 1;

    // Use correct snapshot id.
    // [ e0, e1:pinned, e2 ] -> [ e0, e1:pinned, e2:pinned ]
    epoch_recorded_in_frontend = hummock_manager.pin_snapshot(context_id).await.unwrap();
    assert_eq!(epoch_recorded_in_frontend.committed_epoch, epoch - 1);

    let test_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &test_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    // [ e0, e1:pinned, e2:pinned ] -> [ e0, e1:pinned, e2:pinned, e3 ]
    commit_from_meta_node(
        hummock_manager.borrow(),
        epoch,
        to_local_sstable_info(&test_tables),
    )
    .await
    .unwrap();
    epoch += 1;

    // Use u64::MAX as epoch to pin greatest snapshot
    // [ e0, e1:pinned, e2:pinned, e3 ] -> [ e0, e1:pinned, e2:pinned, e3::pinned ]
    epoch_recorded_in_frontend = hummock_manager.pin_snapshot(context_id).await.unwrap();
    assert_eq!(epoch_recorded_in_frontend.committed_epoch, epoch - 1);
}

#[tokio::test]
async fn test_print_compact_task() {
    let (_, hummock_manager, _cluster_manager, _) = setup_compute_env(80).await;
    // Add some sstables and commit.
    let epoch: u64 = 1;
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &original_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    commit_from_meta_node(
        hummock_manager.borrow(),
        epoch,
        to_local_sstable_info(&original_tables),
    )
    .await
    .unwrap();

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        compact_task
            .get_input_ssts()
            .first()
            .unwrap()
            .get_level_idx(),
        0
    );

    let s = compact_task_to_string(&compact_task);
    assert!(s.contains("Compaction task id: 1, group-id: 2, target level: 0"));
}

#[tokio::test]
async fn test_invalid_sst_id() {
    let (_, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let epoch = 1;
    let ssts = generate_test_tables(epoch, vec![1]);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &ssts,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    let ssts = to_local_sstable_info(&ssts);
    // reject due to invalid context id
    let sst_to_worker = ssts
        .iter()
        .map(|LocalSstableInfo { sst_info, .. }| (sst_info.id, WorkerId::MAX))
        .collect();
    let error = hummock_manager
        .commit_epoch(epoch, ssts.clone(), sst_to_worker)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidSst(1)));

    let sst_to_worker = ssts
        .iter()
        .map(|LocalSstableInfo { sst_info, .. }| (sst_info.id, context_id))
        .collect();
    hummock_manager
        .commit_epoch(epoch, ssts, sst_to_worker)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_trigger_manual_compaction() {
    let (_, hummock_manager, _, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    {
        let option = ManualCompactionOption::default();
        // to check no compaction task
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
            .await;

        assert_eq!(
            "trigger_manual_compaction No compactor is available. compaction_group 2",
            result.err().unwrap().to_string()
        );
    }

    // No compaction task available.
    let compactor_manager_ref = hummock_manager.compactor_manager_ref_for_test();
    let receiver = compactor_manager_ref.add_compactor(context_id, u64::MAX);
    {
        let option = ManualCompactionOption::default();
        // to check no compaction task
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
            .await;

        assert_eq!(
            "trigger_manual_compaction No compaction_task is available. compaction_group 2",
            result.err().unwrap().to_string()
        );
    }

    // Generate data for compaction task
    let _ = add_test_tables(&hummock_manager, context_id).await;
    {
        // to check compactor send task fail
        drop(receiver);
        {
            let option = ManualCompactionOption::default();
            let result = hummock_manager
                .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
                .await;
            assert!(result.is_err());
        }
    }

    compactor_manager_ref.remove_compactor(context_id);
    let _receiver = compactor_manager_ref.add_compactor(context_id, u64::MAX);

    {
        let option = ManualCompactionOption {
            level: 6,
            key_range: KeyRange {
                ..Default::default()
            },
            ..Default::default()
        };

        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
            .await;
        assert!(result.is_ok());
    }

    let task_id: u64 = 4;
    let compact_task = hummock_manager
        .compaction_task_from_assignment_for_test(task_id)
        .await
        .unwrap()
        .compact_task
        .unwrap();
    assert_eq!(task_id, compact_task.task_id);

    {
        let option = ManualCompactionOption::default();
        // all sst pending , test no compaction avail
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
            .await;
        assert!(result.is_err());
    }
}

#[tokio::test]
async fn test_trigger_compaction_deterministic() {
    let (env, hummock_manager, _, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    // No compaction task available.
    let compactor_manager_ref = hummock_manager.compactor_manager_ref_for_test();
    let compaction_scheduler = Arc::new(CompactionScheduler::new(
        env.clone(),
        hummock_manager.clone(),
        compactor_manager_ref.clone(),
    ));

    let _ = compactor_manager_ref.add_compactor(context_id, u64::MAX);
    let (_handle, shutdown_tx) = start_compaction_scheduler(compaction_scheduler);

    // Generate data for compaction task
    let _ = add_test_tables(&hummock_manager, context_id).await;

    let cur_version = hummock_manager.get_current_version().await;
    let compaction_groups = hummock_manager.compaction_group_ids().await;

    let ret = hummock_manager
        .trigger_compaction_deterministic(cur_version.id, compaction_groups)
        .await;
    assert!(ret.is_ok());
    shutdown_tx
        .send(())
        .expect("shutdown compaction scheduler error");
}

#[tokio::test]
async fn test_reset_current_version() {
    let (_env, hummock_manager, _, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    // Generate data for compaction task
    let _ = add_test_tables(&hummock_manager, context_id).await;

    let cur_version = hummock_manager.get_current_version().await;

    let old_version = hummock_manager.reset_current_version().await.unwrap();
    assert_eq!(cur_version.id, old_version.id);
    assert_eq!(
        cur_version.max_committed_epoch,
        old_version.max_committed_epoch
    );
    let new_version = hummock_manager.get_current_version().await;
    assert_eq!(new_version.id, FIRST_VERSION_ID);
    assert_eq!(new_version.max_committed_epoch, INVALID_EPOCH);
}

// This is a non-deterministic test
#[cfg(madsim)]
#[tokio::test]
async fn test_hummock_compaction_task_heartbeat() {
    use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
    use risingwave_pb::hummock::CompactTaskProgress;

    use crate::hummock::HummockManager;
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let sst_num = 2;

    let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
    let _tx = compactor_manager.add_compactor(context_id, 100);
    let (join_handle, shutdown_tx) =
        HummockManager::start_compaction_heartbeat(hummock_manager.clone()).await;

    // No compaction task available.
    assert!(hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
        .await
        .unwrap()
        .is_none());

    // Add some sstables and commit.
    let epoch: u64 = 1;
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, sst_num).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &original_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    commit_from_meta_node(
        hummock_manager.borrow(),
        epoch,
        to_local_sstable_info(&original_tables),
    )
    .await
    .unwrap();

    let compactor = hummock_manager.get_idle_compactor().await.unwrap();
    // Get a compaction task.
    let mut compact_task = hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
        .await
        .unwrap()
        .unwrap();
    hummock_manager
        .assign_compaction_task(&compact_task, compactor.context_id())
        .await
        .unwrap();

    assert_eq!(
        compact_task
            .get_input_ssts()
            .first()
            .unwrap()
            .get_level_idx(),
        0
    );
    assert_eq!(compact_task.get_task_id(), 2);
    // send task
    compactor
        .send_task(Task::CompactTask(compact_task.clone()))
        .await
        .unwrap();

    for i in 0..10 {
        // send heartbeats to the task over 2.5 seconds
        let req = CompactTaskProgress {
            task_id: compact_task.task_id,
            num_ssts_sealed: i + 1,
            num_ssts_uploaded: 0,
        };
        compactor_manager.update_task_heartbeats(context_id, &vec![req]);
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    // Cancel the task immediately and succeed.
    compact_task.set_task_status(TaskStatus::ExecuteFailed);

    assert!(hummock_manager
        .report_compact_task(context_id, &mut compact_task, None)
        .await
        .unwrap());

    let compactor = hummock_manager.get_idle_compactor().await.unwrap();
    // Get a compaction task.
    let mut compact_task = hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
        .await
        .unwrap()
        .unwrap();
    hummock_manager
        .assign_compaction_task(&compact_task, compactor.context_id())
        .await
        .unwrap();
    assert_eq!(compact_task.get_task_id(), 3);
    // send task
    compactor
        .send_task(Task::CompactTask(compact_task.clone()))
        .await
        .unwrap();

    // do not send heartbeats to the task for 2.5 seconds (ttl = 1s, heartbeat check freq. = 1s)
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;

    // Cancel the task after heartbeat has triggered and fail.
    compact_task.set_task_status(TaskStatus::ExecuteFailed);
    assert!(!hummock_manager
        .report_compact_task(context_id, &mut compact_task, None)
        .await
        .unwrap());
    shutdown_tx.send(()).unwrap();
    join_handle.await.unwrap();
}

// This is a non-deterministic test
#[cfg(madsim)]
#[tokio::test]
async fn test_hummock_compaction_task_heartbeat_removal_on_node_removal() {
    use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
    use risingwave_pb::hummock::CompactTaskProgress;

    use crate::hummock::HummockManager;
    let (_env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let sst_num = 2;

    let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
    let _tx = compactor_manager.add_compactor(context_id, 100);
    let (join_handle, shutdown_tx) =
        HummockManager::start_compaction_heartbeat(hummock_manager.clone()).await;

    // No compaction task available.
    assert!(hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
        .await
        .unwrap()
        .is_none());

    // Add some sstables and commit.
    let epoch: u64 = 1;
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, sst_num).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &original_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    commit_from_meta_node(
        hummock_manager.borrow(),
        epoch,
        to_local_sstable_info(&original_tables),
    )
    .await
    .unwrap();

    let compactor = hummock_manager.get_idle_compactor().await.unwrap();
    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
        .await
        .unwrap()
        .unwrap();
    hummock_manager
        .assign_compaction_task(&compact_task, compactor.context_id())
        .await
        .unwrap();
    assert_eq!(
        compact_task
            .get_input_ssts()
            .first()
            .unwrap()
            .get_level_idx(),
        0
    );
    assert_eq!(compact_task.get_task_id(), 2);
    // send task
    compactor
        .send_task(Task::CompactTask(compact_task.clone()))
        .await
        .unwrap();

    // send heartbeats to the task immediately
    let req = CompactTaskProgress {
        task_id: compact_task.task_id,
        num_ssts_sealed: 1,
        num_ssts_uploaded: 1,
    };
    compactor_manager.update_task_heartbeats(context_id, &vec![req.clone()]);

    // Removing the node from cluster will invalidate context id.
    cluster_manager
        .delete_worker_node(worker_node.host.unwrap())
        .await
        .unwrap();
    hummock_manager
        .release_contexts([context_id])
        .await
        .unwrap();

    // Check that no heartbeats exist for the relevant context.
    assert!(!compactor_manager.purge_heartbeats_for_context(worker_node.id));

    shutdown_tx.send(()).unwrap();
    join_handle.await.unwrap();
}

#[tokio::test]
async fn test_extend_ssts_to_delete() {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let sst_infos = add_test_tables(hummock_manager.as_ref(), context_id).await;
    let max_committed_sst_id = sst_infos
        .iter()
        .map(|ssts| ssts.iter().max_by_key(|s| s.id).map(|s| s.id).unwrap())
        .max()
        .unwrap();
    let orphan_sst_num = 10;
    let orphan_sst_ids = sst_infos
        .iter()
        .flatten()
        .map(|s| s.id)
        .chain(max_committed_sst_id + 1..=max_committed_sst_id + orphan_sst_num)
        .collect_vec();
    assert!(hummock_manager.get_ssts_to_delete().await.is_empty());
    assert_eq!(
        hummock_manager
            .extend_ssts_to_delete_from_scan(&orphan_sst_ids)
            .await,
        orphan_sst_num as usize
    );
    assert_eq!(
        hummock_manager.get_ssts_to_delete().await.len(),
        orphan_sst_num as usize
    );

    // Checkpoint
    assert_eq!(
        hummock_manager.proceed_version_checkpoint().await.unwrap(),
        3
    );
    assert_eq!(
        hummock_manager
            .extend_ssts_to_delete_from_scan(&orphan_sst_ids)
            .await,
        orphan_sst_num as usize
    );
    // Another 3 SSTs from useless delta logs after checkpoint
    assert_eq!(
        hummock_manager.get_ssts_to_delete().await.len(),
        orphan_sst_num as usize + 3
    );
}

#[tokio::test]
async fn test_version_stats() {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let init_stats = hummock_manager.get_version_stats().await;
    assert!(init_stats.table_stats.is_empty());

    // Commit epoch
    let epoch = 1;
    register_table_ids_to_compaction_group(
        &hummock_manager,
        &[1, 2, 3],
        StaticCompactionGroupId::StateDefault as _,
    )
    .await;
    let table_stats_change = TableStats {
        total_key_size: 1000,
        total_value_size: 100,
        total_key_count: 10,
    };
    let ssts_with_table_ids = vec![vec![1, 2], vec![2, 3]];
    let sst_ids = get_sst_ids(&hummock_manager, ssts_with_table_ids.len() as _).await;
    let ssts = ssts_with_table_ids
        .into_iter()
        .enumerate()
        .map(|(idx, table_ids)| LocalSstableInfo {
            compaction_group_id: StaticCompactionGroupId::StateDefault as _,
            sst_info: SstableInfo {
                id: sst_ids[idx],
                key_range: Some(KeyRange {
                    left: iterator_test_key_of_epoch(1, 1, 1),
                    right: iterator_test_key_of_epoch(1, 1, 1),
                    right_exclusive: false,
                }),
                file_size: 1024 * 1024 * 1024,
                table_ids: table_ids.clone(),
                ..Default::default()
            },
            table_stats: table_ids
                .iter()
                .map(|table_id| (*table_id, table_stats_change.clone()))
                .into_iter()
                .collect(),
        })
        .collect_vec();
    let sst_to_worker = ssts
        .iter()
        .map(|LocalSstableInfo { sst_info, .. }| (sst_info.id, worker_node.id))
        .collect();
    hummock_manager
        .commit_epoch(epoch, ssts, sst_to_worker)
        .await
        .unwrap();

    let stats_after_commit = hummock_manager.get_version_stats().await;
    assert_eq!(stats_after_commit.table_stats.len(), 3);
    let table1_stats = stats_after_commit.table_stats.get(&1).unwrap();
    let table2_stats = stats_after_commit.table_stats.get(&2).unwrap();
    let table3_stats = stats_after_commit.table_stats.get(&3).unwrap();
    assert_eq!(table1_stats.total_key_count, 10);
    assert_eq!(table1_stats.total_value_size, 100);
    assert_eq!(table1_stats.total_key_size, 1000);
    assert_eq!(table2_stats.total_key_count, 20);
    assert_eq!(table2_stats.total_value_size, 200);
    assert_eq!(table2_stats.total_key_size, 2000);
    assert_eq!(table3_stats.total_key_count, 10);
    assert_eq!(table3_stats.total_value_size, 100);
    assert_eq!(table3_stats.total_key_size, 1000);

    // Report compaction
    hummock_manager
        .compactor_manager_ref_for_test()
        .add_compactor(worker_node.id, u64::MAX);
    let compactor = hummock_manager.get_idle_compactor().await.unwrap();
    let mut compact_task = hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
        .await
        .unwrap()
        .unwrap();
    hummock_manager
        .assign_compaction_task(&compact_task, compactor.context_id())
        .await
        .unwrap();
    compact_task.task_status = TaskStatus::Success as _;
    let compact_table_stats_change = TableStatsMap::from([
        (
            2,
            TableStats {
                total_key_size: -1000,
                total_value_size: -100,
                total_key_count: -10,
            },
        ),
        (
            3,
            TableStats {
                total_key_size: -1000,
                total_value_size: -100,
                total_key_count: -10,
            },
        ),
    ]);
    hummock_manager
        .report_compact_task(
            worker_node.id,
            &mut compact_task,
            Some(to_prost_table_stats_map(compact_table_stats_change)),
        )
        .await
        .unwrap();
    let stats_after_compact = hummock_manager.get_version_stats().await;
    let compact_table1_stats = stats_after_compact.table_stats.get(&1).unwrap();
    let compact_table2_stats = stats_after_compact.table_stats.get(&2).unwrap();
    let compact_table3_stats = stats_after_compact.table_stats.get(&3).unwrap();
    assert_eq!(compact_table1_stats, table1_stats);
    assert_eq!(compact_table2_stats.total_key_count, 10);
    assert_eq!(compact_table2_stats.total_value_size, 100);
    assert_eq!(compact_table2_stats.total_key_size, 1000);
    assert_eq!(compact_table3_stats.total_key_count, 0);
    assert_eq!(compact_table3_stats.total_value_size, 0);
    assert_eq!(compact_table3_stats.total_key_size, 0);
}
