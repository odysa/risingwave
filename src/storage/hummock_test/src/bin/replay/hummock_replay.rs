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

use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result as RwResult;
use risingwave_common::util::addr::HostAddr;
use risingwave_common_service::observer_manager::{Channel, NotificationClient};
use risingwave_hummock_trace::{Replayable, Result};
use risingwave_meta::hummock::{HummockManager, HummockManagerRef};
use risingwave_meta::manager::{MessageStatus, MetaSrvEnv, NotificationManagerRef, WorkerKey};
use risingwave_meta::storage::{MemStore, MetaStore};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{MetaSnapshot, SubscribeResponse, SubscribeType};
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::{ReadOptions, SyncResult, WriteOptions};
use risingwave_storage::StateStore;
use tokio::sync::mpsc::UnboundedReceiver;

pub(crate) struct HummockInterface(HummockStorage);

impl HummockInterface {
    pub(crate) fn new(store: HummockStorage) -> Self {
        Self(store)
    }
}

#[async_trait::async_trait]
impl Replayable for HummockInterface {
    async fn get(
        &self,
        key: &Vec<u8>,
        check_bloom_filter: bool,
        epoch: u64,
        table_id: u32,
        retention_seconds: Option<u32>,
    ) -> Option<Vec<u8>> {
        let value = self
            .0
            .get(
                key,
                check_bloom_filter,
                ReadOptions {
                    epoch,
                    table_id: TableId { table_id },
                    retention_seconds,
                },
            )
            .await
            .unwrap();
        value.map(|b| b.to_vec())
    }

    async fn ingest(
        &self,
        mut kv_pairs: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        epoch: u64,
        table_id: u32,
    ) -> Result<usize> {
        let kv_pairs = kv_pairs
            .drain(..)
            .map(|(key, value)| {
                (
                    Bytes::from(key),
                    StorageValue {
                        user_value: value.map(|v| Bytes::from(v)),
                    },
                )
            })
            .collect();

        let size: usize = self
            .0
            .ingest_batch(
                kv_pairs,
                WriteOptions {
                    epoch,
                    table_id: TableId { table_id },
                },
            )
            .await
            .unwrap();
        Ok(size)
    }

    async fn iter(&self) {
        todo!()
    }

    async fn sync(&self, id: u64) {
        let _: SyncResult = self.0.sync(id).await.unwrap();
    }

    async fn seal_epoch(&self, epoch_id: u64, is_checkpoint: bool) {
        self.0.seal_epoch(epoch_id, is_checkpoint);
    }

    async fn update_version(&self, version_id: u64) {
        todo!()
    }
}

pub struct TestNotificationClient<S: MetaStore> {
    addr: HostAddr,
    notification_manager: NotificationManagerRef<S>,
    hummock_manager: HummockManagerRef<S>,
}

pub struct TestChannel<T>(UnboundedReceiver<std::result::Result<T, MessageStatus>>);

#[async_trait::async_trait]
impl<T: Send + 'static> Channel for TestChannel<T> {
    type Item = T;

    async fn message(&mut self) -> std::result::Result<Option<T>, MessageStatus> {
        match self.0.recv().await {
            None => Ok(None),
            Some(result) => result.map(|r| Some(r)),
        }
    }
}

impl<S: MetaStore> TestNotificationClient<S> {
    pub fn new(
        addr: HostAddr,
        notification_manager: NotificationManagerRef<S>,
        hummock_manager: HummockManagerRef<S>,
    ) -> Self {
        Self {
            addr,
            notification_manager,
            hummock_manager,
        }
    }
}

#[async_trait::async_trait]
impl<S: MetaStore> NotificationClient for TestNotificationClient<S> {
    type Channel = TestChannel<SubscribeResponse>;

    async fn subscribe(&self, subscribe_type: SubscribeType) -> RwResult<Self::Channel> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let hummock_manager_guard = self.hummock_manager.get_read_guard().await;
        let meta_snapshot = MetaSnapshot {
            hummock_version: Some(hummock_manager_guard.current_version.clone()),
            ..Default::default()
        };
        tx.send(Ok(SubscribeResponse {
            status: None,
            operation: Operation::Snapshot as i32,
            info: Some(Info::Snapshot(meta_snapshot)),
            version: self.notification_manager.current_version().await,
        }))
        .unwrap();
        self.notification_manager
            .insert_sender(subscribe_type, WorkerKey(self.addr.to_protobuf()), tx)
            .await;
        Ok(TestChannel(rx))
    }
}

pub fn get_test_notification_client(
    env: MetaSrvEnv<MemStore>,
    hummock_manager_ref: Arc<HummockManager<MemStore>>,
    worker_node: WorkerNode,
) -> TestNotificationClient<MemStore> {
    TestNotificationClient::new(
        worker_node.get_host().unwrap().into(),
        env.notification_manager_ref(),
        hummock_manager_ref,
    )
}
