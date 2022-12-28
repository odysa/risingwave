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

use std::time::Duration;

use risingwave_pb::backup_service::BackupJobStatus;

use crate::common::MetaServiceOpts;

pub async fn backup_meta() -> anyhow::Result<()> {
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta_client = meta_opts.create_meta_client().await?;
    let job_id = meta_client.backup_meta().await?;
    loop {
        let job_status = meta_client.get_backup_job_status(job_id).await?;
        match job_status {
            BackupJobStatus::Running => {
                tracing::info!("backup job is still running: job {}", job_id);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            BackupJobStatus::Succeeded => {
                tracing::info!("backup job succeeded: job {}", job_id);
                break;
            }
            _ => {
                tracing::info!("backup job failed: job {}", job_id);
                break;
            }
        }
    }
    Ok(())
}

pub async fn delete_meta_snapshots(snapshot_ids: &[u64]) -> anyhow::Result<()> {
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta_client = meta_opts.create_meta_client().await?;
    meta_client.delete_meta_snapshot(snapshot_ids).await?;
    tracing::info!("delete meta snapshots succeeded: {:?}", snapshot_ids);
    Ok(())
}
