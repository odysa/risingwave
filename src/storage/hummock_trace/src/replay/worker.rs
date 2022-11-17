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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

use super::{ReplayRequest, WorkerId, WorkerResponse};
use crate::{
    LocalReplay, Operation, OperationResult, Record, RecordId, ReplayIter, Replayable, StorageType,
};

pub(crate) struct WorkerScheduler {
    workers: HashMap<WorkerId, WorkerHandler>,
    non_actor_task_ids: HashSet<RecordId>,
}

impl WorkerScheduler {
    pub(crate) fn new() -> Self {
        WorkerScheduler {
            workers: HashMap::new(),
            non_actor_task_ids: HashSet::new(),
        }
    }

    pub(crate) fn schedule(&mut self, record: Record, replay: Arc<Box<dyn Replayable>>) {
        let worker_id = self.allocate_worker_id(&record);
        println!("sc {:?},id {:?}", record, worker_id);

        let handler = self.workers.entry(worker_id.clone()).or_insert_with(|| {
            let (req_tx, req_rx) = unbounded_channel();
            let (resp_tx, resp_rx) = unbounded_channel();
            let (res_tx, res_rx) = unbounded_channel();
            let join = tokio::spawn(replay_worker(req_rx, res_rx, resp_tx, replay));
            WorkerHandler {
                req_tx,
                res_tx,
                resp_rx,
                join,
            }
        });

        handler.send_replay_req(ReplayRequest::Task(vec![record]));
    }

    pub(crate) fn send_result(&mut self, record: &Record, trace_result: OperationResult) {
        let worker_id = self.allocate_worker_id(record);
        if let Some(handler) = self.workers.get_mut(&worker_id) {
            handler.send_result(trace_result);
        }
    }

    pub(crate) async fn wait_finish(&mut self, record: &Record) {
        let worker_id = self.allocate_worker_id(record);
        if let Some(handler) = self.workers.get_mut(&worker_id) {
            handler.wait_resp().await;

            if let WorkerId::NonActor(_) = worker_id {
                handler.finish();
                self.workers.remove(&worker_id);
            }
        }
    }

    pub(crate) async fn shutdown(self) {
        for handler in self.workers.into_values() {
            handler.finish();
            handler.join().await;
        }
    }

    fn allocate_worker_id(&mut self, record: &Record) -> WorkerId {
        match record.storage_type() {
            StorageType::Local(id) => WorkerId::Local(*id),
            StorageType::Global => match record.op() {
                Operation::Sync(_) | Operation::Seal(_, _) | Operation::MetaMessage(_) => {
                    self.non_actor_task_ids.insert(record.record_id());
                    WorkerId::NonActor(record.record_id())
                }
                Operation::Result(_) => self.allocate_non_actor(&record),
                Operation::Finish => {
                    let id = self.allocate_non_actor(&record);
                    if let WorkerId::NonActor(_) = id {
                        self.non_actor_task_ids.remove(&record.record_id());
                    }
                    id
                }
                _ => WorkerId::Global,
            },
        }
    }

    fn allocate_non_actor(&self, record: &Record) -> WorkerId {
        if self.non_actor_task_ids.contains(&record.record_id()) {
            WorkerId::NonActor(record.record_id())
        } else {
            WorkerId::Global
        }
    }
}

struct WorkerHandler {
    req_tx: UnboundedSender<ReplayRequest>,
    res_tx: UnboundedSender<OperationResult>,
    resp_rx: UnboundedReceiver<WorkerResponse>,
    join: JoinHandle<()>,
}

impl WorkerHandler {
    async fn join(self) {
        self.join.await.expect("failed to stop worker");
    }

    fn finish(&self) {
        self.send_replay_req(ReplayRequest::Fin);
    }

    fn send_replay_req(&self, req: ReplayRequest) {
        self.req_tx
            .send(req)
            .expect("failed to send replay request");
    }

    fn send_result(&self, result: OperationResult) {
        self.res_tx.send(result).expect("failed to send result");
    }

    async fn wait_resp(&mut self) {
        self.resp_rx
            .recv()
            .await
            .expect("failed to wait worker resp");
    }
}

pub(crate) async fn replay_worker(
    mut rx: UnboundedReceiver<ReplayRequest>,
    mut res_rx: UnboundedReceiver<OperationResult>,
    tx: UnboundedSender<WorkerResponse>,
    replay: Arc<Box<dyn Replayable>>,
) {
    let mut iters_map = HashMap::new();
    let mut local_storages = HashMap::new();
    loop {
        if let Some(msg) = rx.recv().await {
            match msg {
                ReplayRequest::Task(record_group) => {
                    for record in record_group {
                        handle_record(
                            record,
                            &replay,
                            &mut res_rx,
                            &mut iters_map,
                            &mut local_storages,
                        )
                        .await;
                    }
                    tx.send(()).expect("failed to done task");
                }
                ReplayRequest::Fin => return,
            }
        }
    }
}

async fn handle_record(
    record: Record,
    replay: &Arc<Box<dyn Replayable>>,
    res_rx: &mut UnboundedReceiver<OperationResult>,
    iters_map: &mut HashMap<RecordId, Box<dyn ReplayIter>>,
    local_storages: &mut HashMap<u32, Box<dyn LocalReplay>>,
) {
    let Record(_, record_id, op) = record;
    match op {
        Operation::Get {
            key,
            check_bloom_filter,
            epoch,
            table_id,
            retention_seconds,
            prefix_hint,
        } => {
            let local_storage = {
                if let Entry::Vacant(e) = local_storages.entry(table_id) {
                    e.insert(replay.new_local(table_id).await)
                } else {
                    local_storages.get(&table_id).unwrap()
                }
            };

            let actual = local_storage
                .get(
                    key,
                    check_bloom_filter,
                    epoch,
                    prefix_hint,
                    table_id,
                    retention_seconds,
                )
                .await;
            let res = res_rx.recv().await.expect("recv result failed");
            if let OperationResult::Get(expected) = res {
                assert_eq!(actual.ok(), expected, "get result wrong");
            }
        }
        Operation::Ingest {
            kv_pairs,
            epoch,
            table_id,
            delete_ranges,
        } => {
            let local_storage = {
                if let Entry::Vacant(e) = local_storages.entry(table_id) {
                    e.insert(replay.new_local(table_id).await)
                } else {
                    local_storages.get(&table_id).unwrap()
                }
            };

            let actual = local_storage
                .ingest(kv_pairs, delete_ranges, epoch, table_id)
                .await;

            let res = res_rx.recv().await.expect("recv result failed");
            if let OperationResult::Ingest(expected) = res {
                assert_eq!(actual.ok(), expected, "ingest result wrong");
            }
        }
        Operation::Iter {
            prefix_hint,
            key_range,
            epoch,
            table_id,
            retention_seconds,
            check_bloom_filter,
        } => {
            let local_storage = {
                if let Entry::Vacant(e) = local_storages.entry(table_id) {
                    e.insert(replay.new_local(table_id).await)
                } else {
                    local_storages.get(&table_id).unwrap()
                }
            };

            let iter = local_storage
                .iter(
                    key_range,
                    epoch,
                    prefix_hint,
                    check_bloom_filter,
                    retention_seconds,
                    table_id,
                )
                .await;
            let res = res_rx.recv().await.expect("recv result failed");
            if let OperationResult::Iter(expected) = res {
                if expected.is_some() {
                    iters_map.insert(record_id, iter.unwrap());
                } else {
                    assert!(iter.is_err());
                }
            }
        }
        Operation::Sync(epoch_id) => {
            let sync_result = replay.sync(epoch_id).await.unwrap();
            let res = res_rx.recv().await.expect("recv result failed");
            if let OperationResult::Sync(expected) = res {
                let actual = Some(sync_result);
                assert_eq!(actual, expected, "sync failed");
            }
        }
        Operation::Seal(epoch_id, is_checkpoint) => {
            replay.seal_epoch(epoch_id, is_checkpoint).await;
        }
        Operation::IterNext(id) => {
            let iter = iters_map.get_mut(&id).expect("iter not in worker");
            let actual = iter.next().await;
            let res = res_rx.recv().await.expect("recv result failed");
            if let OperationResult::IterNext(expected) = res {
                assert_eq!(actual, expected, "iter_next result wrong");
            }
        }
        Operation::MetaMessage(resp) => {
            let op = resp.0.operation();
            if let Some(info) = resp.0.info {
                replay.notify_hummock(info, op).await.unwrap();
            }
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use mockall::predicate;
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;
    use crate::{MockLocalReplay, MockReplayIter, MockReplayable, StorageType};

    #[tokio::test]
    async fn test_handle_record() {
        let mut iters_map = HashMap::new();
        let mut local_storages = HashMap::new();
        let (res_tx, mut res_rx) = unbounded_channel();

        let op = Operation::Get {
            key: vec![123],
            epoch: 123,
            prefix_hint: None,
            check_bloom_filter: false,
            retention_seconds: Some(12),
            table_id: 12,
        };
        let record = Record::new(StorageType::Local(0), 0, op);
        let mut mock_replay = MockReplayable::new();

        mock_replay.expect_new_local().times(1).returning(|_| {
            let mut mock_local = MockLocalReplay::new();

            mock_local
                .expect_get()
                .with(
                    predicate::eq(vec![123]),
                    predicate::eq(false),
                    predicate::eq(123),
                    predicate::eq(None),
                    predicate::eq(12),
                    predicate::eq(Some(12)),
                )
                .returning(|_, _, _, _, _, _| Ok(Some(vec![120])));

            Box::new(mock_local)
        });

        mock_replay.expect_new_local().times(1).returning(|_| {
            let mut mock_local = MockLocalReplay::new();

            mock_local
                .expect_iter()
                .with(
                    predicate::eq((Bound::Unbounded, Bound::Unbounded)),
                    predicate::eq(45),
                    predicate::eq(None),
                    predicate::eq(false),
                    predicate::eq(Some(12)),
                    predicate::eq(500),
                )
                .returning(|_, _, _, _, _, _| {
                    let mut mock_iter = MockReplayIter::new();
                    mock_iter
                        .expect_next()
                        .times(1)
                        .returning(|| Some((vec![1], vec![0])));
                    Ok(Box::new(mock_iter))
                });

            Box::new(mock_local)
        });

        let replay: Arc<Box<dyn Replayable>> = Arc::new(Box::new(mock_replay));
        res_tx
            .send(OperationResult::Get(Some(Some(vec![120]))))
            .unwrap();
        handle_record(
            record,
            &replay,
            &mut res_rx,
            &mut iters_map,
            &mut local_storages,
        )
        .await;

        assert_eq!(local_storages.len(), 1);
        assert!(iters_map.is_empty());

        let op = Operation::Iter {
            key_range: (Bound::Unbounded, Bound::Unbounded),
            epoch: 45,
            prefix_hint: None,
            check_bloom_filter: false,
            retention_seconds: Some(12),
            table_id: 500,
        };
        let record = Record::new(StorageType::Local(0), 1, op);
        res_tx.send(OperationResult::Iter(Some(()))).unwrap();

        handle_record(
            record,
            &replay,
            &mut res_rx,
            &mut iters_map,
            &mut local_storages,
        )
        .await;

        assert_eq!(local_storages.len(), 2);
        assert_eq!(iters_map.len(), 1);

        let op = Operation::IterNext(1);
        let record = Record::new(StorageType::Local(0), 2, op);
        res_tx
            .send(OperationResult::IterNext(Some((vec![1], vec![0]))))
            .unwrap();

        handle_record(
            record,
            &replay,
            &mut res_rx,
            &mut iters_map,
            &mut local_storages,
        )
        .await;

        assert_eq!(local_storages.len(), 2);
        assert_eq!(iters_map.len(), 1);
    }
}
