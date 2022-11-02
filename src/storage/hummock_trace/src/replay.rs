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
use std::hash::Hash;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crossbeam::channel::{bounded, Receiver, Sender};
use crossbeam::queue::SegQueue;
use futures::future::join_all;
#[cfg(test)]
use mockall::automock;
use parking_lot::RwLock;
use risingwave_common::hm_trace::TraceLocalId;
use risingwave_pb::meta::subscribe_response::{Info, Operation as RespOperation};
use tokio::task::JoinHandle;

use crate::error::Result;
use crate::read::TraceReader;
use crate::{Operation, ReadEpochStatus, Record, RecordId};

static GLOBAL_OPS_COUNT: AtomicU64 = AtomicU64::new(0);

#[cfg_attr(test, automock)]
#[async_trait::async_trait]
pub trait Replayable: Send + Sync {
    async fn get(
        &self,
        key: Vec<u8>,
        check_bloom_filter: bool,
        epoch: u64,
        table_id: u32,
        retention_seconds: Option<u32>,
    ) -> Option<Vec<u8>>;
    async fn ingest(
        &self,
        kv_pairs: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        epoch: u64,
        table_id: u32,
    ) -> Result<usize>;
    async fn iter(
        &self,
        prefix_hint: Option<Vec<u8>>,
        left_bound: Bound<Vec<u8>>,
        right_bound: Bound<Vec<u8>>,
        epoch: u64,
        table_id: u32,
        retention_seconds: Option<u32>,
    ) -> Result<Box<dyn ReplayIter>>;
    async fn sync(&self, id: u64);
    async fn seal_epoch(&self, epoch_id: u64, is_checkpoint: bool);
    async fn update_version(&self, version_id: u64);
    async fn wait_epoch(&self, epoch: ReadEpochStatus) -> Result<()>;
    async fn notify_hummock(&self, info: Info, op: RespOperation) -> Result<u64>;
    async fn wait_version_update(&self);
}

#[async_trait::async_trait]
pub trait ReplayIter: Send + Sync {
    async fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)>;
}

pub struct HummockReplay<R: TraceReader> {
    reader: R,
    tx: Sender<ReplayMessage>,
}

impl<T: TraceReader> HummockReplay<T> {
    pub fn new(reader: T) -> Self {
        let (tx, _) = bounded::<ReplayMessage>(50);
        Self { reader, tx }
    }

    pub async fn simple_run(&mut self, replay: Arc<Box<dyn Replayable>>) -> Result<()> {
        let mut handle_map: HashMap<u64, JoinHandle<()>> = HashMap::new();
        let mut total: u64 = 0;
        loop {
            match self.reader.read() {
                Ok(r) => {
                    match r.op() {
                        Operation::Finish => {
                            let record_id = r.record_id();
                            if let Some(handle) = handle_map.remove(&record_id) {
                                handle.await.expect("failed to wait a task");
                            }
                        }
                        _ => {
                            let replay = replay.clone();
                            let record_id = r.record_id();
                            let handle = tokio::spawn(handle_record(r, replay));
                            handle_map.insert(record_id, handle);
                            total += 1;
                        }
                    }
                    if total % 10000 == 0 {
                        println!("replayed {} ops", total);
                    }
                }
                Err(_) => break,
            }
        }
        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        let total: u64 = 0;
        loop {
            match self.reader.read() {
                Ok(r) => {
                    match r.local_id() {
                        TraceLocalId::Actor(_) => {}
                        TraceLocalId::Executor(_) => {}
                        TraceLocalId::None => {}
                    };

                    if total % 10000 == 0 {
                        println!("replayed {} ops", total);
                    }
                }
                Err(_) => break,
            }
        }
        Ok(())
    }
}

async fn replay_worker(rx: Receiver<ReplayMessage>, replay: Arc<Box<dyn Replayable>>) {
    let mut iters_map = HashMap::new();
    loop {
        if let Ok(msg) = rx.recv() {
            match msg {
                ReplayMessage::Task(record_group) => {
                    for r in record_group {
                        let Record(_, record_id, op) = r;
                        match op {
                            Operation::Get(
                                key,
                                check_bloom_filter,
                                epoch,
                                table_id,
                                retention_seconds,
                            ) => {
                                replay
                                    .get(
                                        key,
                                        check_bloom_filter,
                                        epoch,
                                        table_id,
                                        retention_seconds,
                                    )
                                    .await;
                            }
                            Operation::Ingest(kv_pairs, epoch, table_id) => {
                                let _ = replay.ingest(kv_pairs, epoch, table_id).await.unwrap();
                            }
                            Operation::Iter(
                                prefix_hint,
                                left_bound,
                                right_bound,
                                epoch,
                                table_id,
                                retention_seconds,
                            ) => {
                                let iter = replay
                                    .iter(
                                        prefix_hint,
                                        left_bound,
                                        right_bound,
                                        epoch,
                                        table_id,
                                        retention_seconds,
                                    )
                                    .await
                                    .unwrap();
                                iters_map.insert(record_id, iter);
                            }
                            Operation::Sync(epoch_id) => {
                                replay.sync(epoch_id).await;
                            }
                            Operation::Seal(epoch_id, is_checkpoint) => {
                                replay.seal_epoch(epoch_id, is_checkpoint).await;
                            }
                            Operation::IterNext(id, expected) => {
                                let iter = iters_map.get_mut(&id);
                                if let Some(iter) = iter {
                                    let actual = iter.next().await;
                                    assert_eq!(actual, expected, "iter next value do not match");
                                }
                            }
                            Operation::MetaMessage(resp) => {
                                let op = resp.0.operation();
                                if let Some(info) = resp.0.info {
                                    replay.notify_hummock(info, op).await.unwrap();
                                }
                            }
                            Operation::UpdateVersion(_) => todo!(),
                            Operation::Finish => unreachable!(),
                            Operation::WaitEpoch(_) => {}
                            _ => {}
                        }
                    }
                }
                ReplayMessage::Fin => todo!(),
            }
        }
    }
}

async fn handle_record(r: Record, replay: Arc<Box<dyn Replayable>>) {
    let Record(_, _, op) = r;
    match op {
        Operation::Get(key, check_bloom_filter, epoch, table_id, retention_seconds) => {
            replay
                .get(key, check_bloom_filter, epoch, table_id, retention_seconds)
                .await;
        }
        Operation::Ingest(kv_pairs, epoch, table_id) => {
            let _ = replay.ingest(kv_pairs, epoch, table_id).await.unwrap();
        }
        Operation::Iter(
            prefix_hint,
            left_bound,
            right_bound,
            epoch,
            table_id,
            retention_seconds,
        ) => {
            let iter = replay
                .iter(
                    prefix_hint,
                    left_bound,
                    right_bound,
                    epoch,
                    table_id,
                    retention_seconds,
                )
                .await
                .unwrap();
        }
        Operation::Sync(epoch_id) => {
            replay.sync(epoch_id).await;
        }
        Operation::Seal(epoch_id, is_checkpoint) => {
            replay.seal_epoch(epoch_id, is_checkpoint).await;
        }
        Operation::IterNext(_, _) => {}
        Operation::MetaMessage(resp) => {
            let op = resp.0.operation();
            if let Some(info) = resp.0.info {
                replay.notify_hummock(info, op).await.unwrap();
            }
        }
        Operation::UpdateVersion(_) => todo!(),
        Operation::Finish => unreachable!(),
        Operation::WaitEpoch(_) => {
            // let f = replay.wait_epoch(epoch);
            // f.await.unwrap();
        }
        _ => {}
    }
}

type ReplayGroup = Vec<Record>;

enum ReplayMessage {
    Task(ReplayGroup),
    Fin,
}

// #[cfg(test)]
// mod tests {
//     use mockall::predicate;

//     use super::*;
//     use crate::MockTraceReader;

//     #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
//     async fn test_replay() {
//         let mut mock_reader = MockTraceReader::new();

//         let mut i = 0;

//         let f = move || {
//             let r = match i {
//                 0 => Ok(Record::new_local_none(
//                     0,
//                     Operation::Get(vec![0], true, 0, 0, None),
//                 )),
//                 1 => Ok(Record::new_local_none(
//                     1,
//                     Operation::Get(vec![1], true, 0, 0, None),
//                 )),
//                 2 => Ok(Record::new_local_none(
//                     2,
//                     Operation::Get(vec![0], true, 0, 0, None),
//                 )),
//                 3 => Ok(Record::new_local_none(2, Operation::Finish)),
//                 4 => Ok(Record::new_local_none(1, Operation::Finish)),
//                 5 => Ok(Record::new_local_none(0, Operation::Finish)),
//                 6 => Ok(Record::new_local_none(
//                     3,
//                     Operation::Ingest(vec![(vec![1], Some(vec![1]))], 0, 0),
//                 )),
//                 7 => Ok(Record::new_local_none(4, Operation::Sync(123))),
//                 8 => Ok(Record::new_local_none(5, Operation::Seal(321, true))),
//                 9 => Ok(Record::new_local_none(3, Operation::Finish)),
//                 10 => Ok(Record::new_local_none(4, Operation::Finish)),
//                 11 => Ok(Record::new_local_none(5, Operation::Finish)),
//                 _ => Err(crate::TraceError::FinRecord(5)), // intentional error
//             };
//             i += 1;
//             r
//         };

//         mock_reader.expect_read().times(13).returning(f);

//         let mut mock_replay = MockReplayable::new();

//         mock_replay.expect_get().times(3).return_const(vec![1]);
//         mock_replay
//             .expect_ingest()
//             .times(1)
//             .returning(|_, _, _| Ok(0));
//         mock_replay
//             .expect_sync()
//             .with(predicate::eq(123))
//             .times(1)
//             .return_const(());
//         mock_replay
//             .expect_seal_epoch()
//             .with(predicate::eq(321), predicate::eq(true))
//             .times(1)
//             .return_const(());

//         let (mut replay, join) = HummockReplay::new(mock_reader, Box::new(mock_replay));
//         replay.run().unwrap();

//         join.await.unwrap();
//     }
// }
