// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use anyhow::anyhow;
use risingwave_common::telemetry::report::{TelemetryInfoFetcher, TelemetryReportCreator};
use risingwave_common::telemetry::{
    current_timestamp, SystemData, TelemetryNodeType, TelemetryReport, TelemetryReportBase,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::{MetadataModelError, MetadataModelResult};
use crate::storage::{MetaStore, Snapshot};

// Column in meta store
pub const TELEMETRY_CF: &str = "cf/telemetry";
/// `telemetry` in bytes
pub const TELEMETRY_KEY: &[u8] = &[74, 65, 0x6c, 65, 0x6d, 65, 74, 72, 79];

#[derive(Debug, Default)]
pub struct TrackingID(String);

impl TrackingID {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn from_bytes(bytes: Vec<u8>) -> anyhow::Result<Self> {
        Ok(Self(String::from_utf8(bytes)?))
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0.into_bytes()
    }

    pub fn to_string(self) -> String {
        self.0
    }

    pub async fn put_int_meta_store(&self, meta_store: Arc<impl MetaStore>) -> anyhow::Result<()> {
        let uuid = self.0.clone();
        match meta_store
            .put_cf(TELEMETRY_CF, TELEMETRY_KEY.to_vec(), uuid.into_bytes())
            .await
        {
            Err(e) => Err(anyhow!("failed to create uuid, {}", e)),
            Ok(_) => Ok(()),
        }
    }

    pub async fn from_meta_store(meta_store: Arc<impl MetaStore>) -> anyhow::Result<Self> {
        match meta_store.get_cf(TELEMETRY_CF, TELEMETRY_KEY).await {
            Ok(bytes) => Self::from_bytes(bytes),
            Err(e) => Err(anyhow::format_err!("failed to get from meta store {}", e)),
        }
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MetaTelemetryReport {
    #[serde(flatten)]
    base: TelemetryReportBase,
}

impl MetaTelemetryReport {
    pub(crate) fn new(tracking_id: String, session_id: String, up_time: u64) -> Self {
        Self {
            base: TelemetryReportBase {
                tracking_id,
                session_id,
                system_data: SystemData::new(),
                up_time,
                time_stamp: current_timestamp(),
                node_type: TelemetryNodeType::Meta,
            },
        }
    }
}

impl TelemetryReport for MetaTelemetryReport {
    fn to_json(&self) -> anyhow::Result<String> {
        let json = serde_json::to_string(self)?;
        Ok(json)
    }
}

pub(crate) struct MetaTelemetryInfoFetcher<S: MetaStore> {
    meta_store: Arc<S>,
}

impl<S: MetaStore> MetaTelemetryInfoFetcher<S> {
    pub(crate) fn new(meta_store: Arc<S>) -> Self {
        Self { meta_store }
    }
}

#[async_trait::async_trait]
impl<S: MetaStore> TelemetryInfoFetcher for MetaTelemetryInfoFetcher<S> {
    async fn fetch_telemetry_info(&self) -> anyhow::Result<String> {
        let tracking_id = get_or_create_tracking_id(self.meta_store.clone()).await?;

        Ok(tracking_id.to_string())
    }
}

/// fetch or create a `tracking_id` from etcd
async fn get_or_create_tracking_id(
    meta_store: Arc<impl MetaStore>,
) -> Result<TrackingID, anyhow::Error> {
    match TrackingID::from_meta_store(meta_store.clone()).await {
        Ok(tracking_id) => Ok(tracking_id),
        Err(_) => {
            let tracking_id = TrackingID::new();
            tracking_id.put_int_meta_store(meta_store).await?;
            Ok(tracking_id)
        }
    }
}

#[derive(Copy, Clone)]
pub(crate) struct MetaReportCreator {}

impl MetaReportCreator {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl TelemetryReportCreator for MetaReportCreator {
    fn create_report(
        &self,
        tracking_id: String,
        session_id: String,
        up_time: u64,
    ) -> anyhow::Result<MetaTelemetryReport> {
        Ok(MetaTelemetryReport::new(tracking_id, session_id, up_time))
    }

    fn report_type(&self) -> &str {
        "meta"
    }
}

pub(crate) async fn get_tracking_id_at_snapshot<S: MetaStore>(
    snapshot: &S::Snapshot,
) -> MetadataModelResult<TrackingID> {
    let bytes = snapshot.get_cf(TELEMETRY_CF, TELEMETRY_KEY).await?;
    let tracking_id = TrackingID::from_bytes(bytes).map_err(MetadataModelError::internal)?;
    Ok(tracking_id)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::storage::MemStore;

    #[tokio::test]
    async fn test_get_or_create_tracking_id_existing_id() {
        let meta_store = Arc::new(MemStore::new());
        let uuid = Uuid::new_v4().to_string();
        meta_store
            .put_cf(
                TELEMETRY_CF,
                TELEMETRY_KEY.to_vec(),
                uuid.clone().into_bytes(),
            )
            .await
            .unwrap();
        let result = get_or_create_tracking_id(Arc::clone(&meta_store))
            .await
            .unwrap();
        assert_eq!(result.to_string(), uuid);
    }

    #[tokio::test]
    async fn test_get_or_create_tracking_id_new_id() {
        let meta_store = Arc::new(MemStore::new());
        let result = get_or_create_tracking_id(Arc::clone(&meta_store))
            .await
            .unwrap();
        assert!(String::from_utf8(result.into_bytes()).is_ok());
    }
}
