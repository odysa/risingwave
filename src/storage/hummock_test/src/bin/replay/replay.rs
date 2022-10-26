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

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_hummock_trace::{Replayable, Result};
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::{ReadOptions, SyncResult, WriteOptions};
use risingwave_storage::StateStore;

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
        println!("replay: get {:?}, epoch {}", key, epoch);
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
        println!("replay: get fin");
        if let Some(b) = value.clone() {
            println!("get value {}", String::from_utf8(b.to_vec()).unwrap());
        }
        value.map(|b| b.to_vec())
    }

    async fn ingest(
        &self,
        mut kv_pairs: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        epoch: u64,
        table_id: u32,
    ) -> Result<usize> {
        println!("replay: ingest {:?} {} {}", kv_pairs, epoch, table_id);

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

        if let Ok(size) = self
            .0
            .ingest_batch(
                kv_pairs,
                WriteOptions {
                    epoch,
                    table_id: TableId { table_id },
                },
            )
            .await
        {
            // println!("success!");
        } else {
            println!("failed to ingest {} {}", epoch, table_id);
        }
        Ok(0)
    }

    async fn iter(&self) {
        todo!()
    }

    async fn sync(&self, id: u64) {
        println!("replay: sync {}", id);
        let _: SyncResult = self.0.sync(id).await.unwrap();
    }

    async fn seal_epoch(&self, epoch_id: u64, is_checkpoint: bool) {
        println!("replay: seal {}", epoch_id);
        self.0.seal_epoch(epoch_id, is_checkpoint);
    }

    async fn update_version(&self, _: u64) {
        // intentionally left blank because hummock does not allow users to update version
    }
}
