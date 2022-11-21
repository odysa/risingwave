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

use std::collections::{HashMap, HashSet};
use std::ops::DerefMut;

use fail::fail_point;
use function_name::named;
use itertools::Itertools;
use risingwave_hummock_sdk::{HummockContextId, HummockEpoch, HummockSstableId, LocalSstableInfo};
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::{HummockVersion, ValidationTask};

use crate::hummock::error::{Error, Result};
use crate::hummock::manager::{
    commit_multi_var, read_lock, start_measure_real_process_timer, write_lock,
};
use crate::hummock::HummockManager;
use crate::model::{BTreeMapTransaction, ValTransaction};
use crate::storage::{MetaStore, Transaction};

impl<S> HummockManager<S>
where
    S: MetaStore,
{
    /// Release resources pinned by these contexts, including:
    /// - Version
    /// - Snapshot
    /// - Compaction task
    #[named]
    pub async fn release_contexts(
        &self,
        context_ids: impl AsRef<[HummockContextId]>,
    ) -> Result<()> {
        fail_point!("release_contexts_metastore_err", |_| Err(Error::MetaStore(
            anyhow::anyhow!("failpoint metastore error")
        )));
        fail_point!("release_contexts_internal_err", |_| Err(Error::Internal(
            anyhow::anyhow!("failpoint internal error")
        )));
        let mut compaction_guard = write_lock!(self, compaction).await;
        let compaction = compaction_guard.deref_mut();
        let (compact_statuses, compact_task_assignment) =
            compaction.cancel_assigned_tasks_for_context_ids(context_ids.as_ref())?;
        for context_id in context_ids.as_ref() {
            self.compactor_manager
                .purge_heartbeats_for_context(*context_id);
        }
        let mut versioning_guard = write_lock!(self, versioning).await;
        let versioning = versioning_guard.deref_mut();
        let mut pinned_versions = BTreeMapTransaction::new(&mut versioning.pinned_versions);
        let mut pinned_snapshots = BTreeMapTransaction::new(&mut versioning.pinned_snapshots);
        for context_id in context_ids.as_ref() {
            pinned_versions.remove(*context_id);
            pinned_snapshots.remove(*context_id);
        }
        commit_multi_var!(
            self,
            None,
            Transaction::default(),
            compact_statuses,
            compact_task_assignment,
            pinned_versions,
            pinned_snapshots
        )?;

        #[cfg(test)]
        {
            drop(versioning_guard);
            drop(compaction_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    /// Checks whether `context_id` is valid.
    pub async fn check_context(&self, context_id: HummockContextId) -> bool {
        self.cluster_manager
            .get_worker_by_id(context_id)
            .await
            .is_some()
    }

    /// Release invalid contexts, aka worker node ids which are no longer valid in `ClusterManager`.
    #[named]
    pub(super) async fn release_invalid_contexts(&self) -> Result<Vec<HummockContextId>> {
        let active_context_ids = {
            let compaction_guard = read_lock!(self, compaction).await;
            let versioning_guard = read_lock!(self, versioning).await;
            let _timer = start_measure_real_process_timer!(self);
            let mut active_context_ids = HashSet::new();
            active_context_ids.extend(
                compaction_guard
                    .compact_task_assignment
                    .values()
                    .map(|c| c.context_id),
            );
            active_context_ids.extend(versioning_guard.pinned_versions.keys());
            active_context_ids.extend(versioning_guard.pinned_snapshots.keys());
            active_context_ids
        };

        let mut invalid_context_ids = vec![];
        for active_context_id in &active_context_ids {
            if !self.check_context(*active_context_id).await {
                invalid_context_ids.push(*active_context_id);
            }
        }

        self.release_contexts(&invalid_context_ids).await?;

        Ok(invalid_context_ids)
    }

    pub(crate) async fn commit_epoch_sanity_check(
        &self,
        epoch: HummockEpoch,
        sstables: &Vec<LocalSstableInfo>,
        sst_to_context: &HashMap<HummockSstableId, HummockContextId>,
        current_version: &HummockVersion,
    ) -> Result<()> {
        for (sst_id, context_id) in sst_to_context {
            #[cfg(test)]
            {
                if *context_id == crate::manager::META_NODE_ID {
                    continue;
                }
            }
            if !self.check_context(*context_id).await {
                return Err(Error::InvalidSst(*sst_id));
            }
        }

        if epoch <= current_version.max_committed_epoch {
            return Err(anyhow::anyhow!(
                "Epoch {} <= max_committed_epoch {}",
                epoch,
                current_version.max_committed_epoch
            )
            .into());
        }

        async {
            if !self.env.opts.enable_committed_sst_sanity_check {
                return;
            }
            if sstables.is_empty() {
                return;
            }
            let compactor = match self.compactor_manager.next_compactor() {
                None => {
                    tracing::warn!("Skip committed SST sanity check due to no available worker");
                    return;
                }
                Some(compactor) => compactor,
            };
            let sst_infos = sstables
                .iter()
                .map(|LocalSstableInfo { sst_info, .. }| sst_info.clone())
                .collect_vec();
            if compactor
                .send_task(Task::ValidationTask(ValidationTask {
                    sst_infos,
                    sst_id_to_worker_id: sst_to_context.clone(),
                    epoch,
                }))
                .await
                .is_err()
            {
                tracing::warn!("Skip committed SST sanity check due to send failure");
            }
        }
        .await;
        Ok(())
    }
}
