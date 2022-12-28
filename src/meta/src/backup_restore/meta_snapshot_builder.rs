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

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use risingwave_backup::error::BackupResult;
use risingwave_backup::meta_snapshot::{ClusterMetadata, MetaSnapshot};
use risingwave_backup::MetaSnapshotId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_pb::catalog::{Database, Index, Schema, Sink, Source, Table, View};
use risingwave_pb::hummock::{HummockVersion, HummockVersionDelta, HummockVersionStats};
use risingwave_pb::user::UserInfo;

use crate::model::MetadataModel;
use crate::storage::{MetaStore, Snapshot, DEFAULT_COLUMN_FAMILY};

pub struct MetaSnapshotBuilder<S> {
    snapshot: MetaSnapshot,
    meta_store: Arc<S>,
}

impl<S: MetaStore> MetaSnapshotBuilder<S> {
    pub fn new(meta_store: Arc<S>) -> Self {
        Self {
            snapshot: MetaSnapshot::default(),
            meta_store,
        }
    }

    pub async fn build(&mut self, id: MetaSnapshotId) -> BackupResult<()> {
        self.snapshot.id = id;
        // Caveat: snapshot impl of etcd meta store doesn't prevent it from expiration.
        // So expired snapshot read may return error. If that happens,
        // tune auto-compaction-mode and auto-compaction-retention on demand.
        let meta_store_snapshot = self.meta_store.snapshot().await;
        let default_cf = self.build_default_cf(&meta_store_snapshot).await?;
        // hummock_version and version_stats is guaranteed to exist in a initialized cluster.
        let hummock_version = {
            let mut redo_state = HummockVersion::list_at_snapshot::<S>(&meta_store_snapshot)
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow!("hummock version checkpoint not found in meta store"))?;
            let hummock_version_deltas =
                HummockVersionDelta::list_at_snapshot::<S>(&meta_store_snapshot).await?;
            for version_delta in &hummock_version_deltas {
                if version_delta.prev_id == redo_state.id {
                    redo_state.apply_version_delta(version_delta);
                }
            }
            redo_state
        };
        let version_stats = HummockVersionStats::list_at_snapshot::<S>(&meta_store_snapshot)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("hummock version stats not found in meta store"))?;
        let compaction_groups =
            crate::hummock::compaction_group::CompactionGroup::list_at_snapshot::<S>(
                &meta_store_snapshot,
            )
            .await?
            .iter()
            .map(MetadataModel::to_protobuf)
            .collect();
        let table_fragments =
            crate::model::TableFragments::list_at_snapshot::<S>(&meta_store_snapshot)
                .await?
                .iter()
                .map(MetadataModel::to_protobuf)
                .collect();
        let user_info = UserInfo::list_at_snapshot::<S>(&meta_store_snapshot).await?;

        let database = Database::list_at_snapshot::<S>(&meta_store_snapshot).await?;
        let schema = Schema::list_at_snapshot::<S>(&meta_store_snapshot).await?;
        let table = Table::list_at_snapshot::<S>(&meta_store_snapshot).await?;
        let index = Index::list_at_snapshot::<S>(&meta_store_snapshot).await?;
        let sink = Sink::list_at_snapshot::<S>(&meta_store_snapshot).await?;
        let source = Source::list_at_snapshot::<S>(&meta_store_snapshot).await?;
        let view = View::list_at_snapshot::<S>(&meta_store_snapshot).await?;

        self.snapshot.metadata = ClusterMetadata {
            default_cf,
            hummock_version,
            version_stats,
            compaction_groups,
            database,
            schema,
            table,
            index,
            sink,
            source,
            view,
            table_fragments,
            user_info,
        };
        Ok(())
    }

    pub fn finish(self) -> BackupResult<MetaSnapshot> {
        // Any sanity check goes here.
        Ok(self.snapshot)
    }

    async fn build_default_cf(
        &self,
        snapshot: &S::Snapshot,
    ) -> BackupResult<HashMap<Vec<u8>, Vec<u8>>> {
        // It's fine any lazy initialized value is not found in meta store.
        let default_cf =
            HashMap::from_iter(snapshot.list_cf(DEFAULT_COLUMN_FAMILY).await?.into_iter());
        Ok(default_cf)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use itertools::Itertools;
    use risingwave_backup::error::BackupError;
    use risingwave_backup::meta_snapshot::MetaSnapshot;
    use risingwave_common::error::ToErrorStr;
    use risingwave_pb::hummock::{HummockVersion, HummockVersionStats};

    use crate::backup_restore::meta_snapshot_builder::MetaSnapshotBuilder;
    use crate::model::MetadataModel;
    use crate::storage::{MemStore, MetaStore, DEFAULT_COLUMN_FAMILY};

    #[tokio::test]
    async fn test_snapshot_builder() {
        let meta_store = Arc::new(MemStore::new());

        let mut builder = MetaSnapshotBuilder::new(meta_store.clone());
        let err = builder.build(1).await.unwrap_err();
        let err = assert_matches!(err, BackupError::Other(e) => e);
        assert_eq!(
            "hummock version checkpoint not found in meta store",
            err.to_error_str()
        );

        let hummock_version = HummockVersion {
            id: 1,
            ..Default::default()
        };
        hummock_version.insert(meta_store.deref()).await.unwrap();
        let mut builder = MetaSnapshotBuilder::new(meta_store.clone());
        let err = builder.build(1).await.unwrap_err();
        let err = assert_matches!(err, BackupError::Other(e) => e);
        assert_eq!(
            "hummock version stats not found in meta store",
            err.to_error_str()
        );

        let hummock_version_stats = HummockVersionStats {
            hummock_version_id: hummock_version.id,
            ..Default::default()
        };
        hummock_version_stats
            .insert(meta_store.deref())
            .await
            .unwrap();
        let mut builder = MetaSnapshotBuilder::new(meta_store.clone());
        builder.build(1).await.unwrap();

        let dummy_key = vec![0u8, 1u8, 2u8];
        let mut builder = MetaSnapshotBuilder::new(meta_store.clone());
        meta_store
            .put_cf(DEFAULT_COLUMN_FAMILY, dummy_key.clone(), vec![100])
            .await
            .unwrap();
        builder.build(1).await.unwrap();
        let snapshot = builder.finish().unwrap();
        let encoded = snapshot.encode();
        let decoded = MetaSnapshot::decode(&encoded).unwrap();
        assert_eq!(snapshot, decoded);
        assert_eq!(snapshot.id, 1);
        assert_eq!(
            snapshot.metadata.default_cf.keys().cloned().collect_vec(),
            vec![dummy_key.clone()]
        );
        assert_eq!(
            snapshot.metadata.default_cf.values().cloned().collect_vec(),
            vec![vec![100]]
        );
        assert_eq!(snapshot.metadata.hummock_version.id, 1);
        assert_eq!(snapshot.metadata.version_stats.hummock_version_id, 1);
    }
}
