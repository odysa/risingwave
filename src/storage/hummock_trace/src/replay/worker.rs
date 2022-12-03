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
use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

use super::{ReplayRequest, WorkerId, WorkerResponse};
use crate::{
    GlobalReplay, LocalReplay, Operation, OperationResult, Record, RecordId, ReplayIter,
    StorageType, TraceResult,
};

#[async_trait::async_trait]
pub trait ReplayWorkerScheduler {
    // schedule a replaying task for given record
    fn schedule(&mut self, record: Record);
    // send result of an operation for a worker
    fn send_result(&mut self, record: Record);
    // wait an operation finishes
    async fn wait_finish(&mut self, record: Record);
    // gracefully shutdown all workers
    async fn shutdown(self);
}

pub(crate) struct WorkerScheduler<G: GlobalReplay> {
    workers: HashMap<WorkerId, WorkerHandler>,
    replay: Arc<G>,
}

impl<G: GlobalReplay> WorkerScheduler<G> {
    pub(crate) fn new(replay: Arc<G>) -> Self {
        WorkerScheduler {
            workers: HashMap::new(),
            replay,
        }
    }

    fn allocate_worker_id(&mut self, record: &Record) -> WorkerId {
        match record.storage_type() {
            StorageType::Local(id) => WorkerId::Local(*id),
            StorageType::Global => WorkerId::OneShot(record.record_id()),
        }
    }
}

#[async_trait::async_trait]
impl<G: GlobalReplay + 'static> ReplayWorkerScheduler for WorkerScheduler<G> {
    fn schedule(&mut self, record: Record) {
        let worker_id = self.allocate_worker_id(&record);
        let handler = self
            .workers
            .entry(worker_id)
            .or_insert_with(|| ReplayWorker::spawn(self.replay.clone()));

        handler.replay(ReplayRequest::Task(record));
    }

    fn send_result(&mut self, record: Record) {
        let worker_id = self.allocate_worker_id(&record);
        if let Operation::Result(trace_result) = record.2 {
            if let Some(handler) = self.workers.get_mut(&worker_id) {
                handler.send_result(trace_result);
            }
        }
    }

    async fn wait_finish(&mut self, record: Record) {
        let worker_id = self.allocate_worker_id(&record);
        if let Some(handler) = self.workers.get_mut(&worker_id) {
            handler.wait().await;
            if let WorkerId::OneShot(_) = worker_id {
                let handler = self.workers.remove(&worker_id).unwrap();
                handler.finish();
            }
        }
    }

    async fn shutdown(self) {
        for handler in self.workers.into_values() {
            handler.finish();
            handler.join().await;
        }
    }
}

struct ReplayWorker {}

impl ReplayWorker {
    fn spawn(replay: Arc<impl GlobalReplay + 'static>) -> WorkerHandler {
        let (req_tx, req_rx) = unbounded_channel();
        let (resp_tx, resp_rx) = unbounded_channel();
        let (res_tx, res_rx) = unbounded_channel();

        let join = tokio::spawn(Self::run(req_rx, res_rx, resp_tx, replay));
        WorkerHandler {
            req_tx,
            res_tx,
            resp_rx,
            join,
            stacked_replay_count: 0,
        }
    }

    async fn run(
        mut req_rx: UnboundedReceiver<ReplayRequest>,
        mut res_rx: UnboundedReceiver<OperationResult>,
        resp_tx: UnboundedSender<WorkerResponse>,
        replay: Arc<impl GlobalReplay>,
    ) {
        let mut iters_map = HashMap::new();
        let mut local_storages = LocalStorages::new();
        while let Some(msg) = req_rx.recv().await {
            match msg {
                ReplayRequest::Task(record) => {
                    Self::handle_record(
                        record,
                        &replay,
                        &mut res_rx,
                        &mut iters_map,
                        &mut local_storages,
                    )
                    .await;
                    resp_tx.send(()).expect("failed to done task");
                }
                ReplayRequest::Fin => return,
            }
        }
    }

    async fn handle_record(
        record: Record,
        replay: &Arc<impl GlobalReplay>,
        res_rx: &mut UnboundedReceiver<OperationResult>,
        iters_map: &mut HashMap<RecordId, Box<dyn ReplayIter>>,
        local_storages: &mut LocalStorages,
    ) {
        let Record(storage_type, record_id, op) = record;
        match op {
            Operation::Get {
                key,
                epoch,
                read_options,
            } => {
                let actual = match storage_type {
                    StorageType::Global => replay.get(key, epoch, read_options).await,
                    StorageType::Local(_) => {
                        let s = local_storages
                            .get_or_insert(read_options.table_id, replay)
                            .await;
                        s.get(key, epoch, read_options).await
                    }
                };
                let res = res_rx.recv().await.expect("recv result failed");
                if let OperationResult::Get(expected) = res {
                    assert_eq!(TraceResult::from(actual), expected, "get result wrong");
                }
            }
            Operation::Ingest {
                kv_pairs,
                delete_ranges,
                write_options,
            } => {
                let s = local_storages
                    .get_or_insert(write_options.table_id, replay)
                    .await;
                let actual = s.ingest(kv_pairs, delete_ranges, write_options).await;

                let res = res_rx.recv().await.expect("recv result failed");
                if let OperationResult::Ingest(expected) = res {
                    assert_eq!(TraceResult::from(actual), expected, "ingest result wrong");
                }
            }
            Operation::Iter {
                key_range,
                epoch,
                read_options,
            } => {
                let iter = match storage_type {
                    StorageType::Global => replay.iter(key_range, epoch, read_options).await,
                    StorageType::Local(_) => {
                        let s = local_storages
                            .get_or_insert(read_options.table_id, replay)
                            .await;
                        s.iter(key_range, epoch, read_options).await
                    }
                };
                let res = res_rx.recv().await.expect("recv result failed");
                if let OperationResult::Iter(expected) = res {
                    if expected.is_ok() {
                        iters_map.insert(record_id, iter.unwrap());
                    } else {
                        assert!(iter.is_err());
                    }
                }
            }
            Operation::Sync(epoch_id) => {
                assert_eq!(storage_type, StorageType::Global);
                let sync_result = replay.sync(epoch_id).await.unwrap();
                let res = res_rx.recv().await.expect("recv result failed");
                if let OperationResult::Sync(expected) = res {
                    assert_eq!(TraceResult::Ok(sync_result), expected, "sync failed");
                }
            }
            Operation::Seal(epoch_id, is_checkpoint) => {
                assert_eq!(storage_type, StorageType::Global);
                replay.seal_epoch(epoch_id, is_checkpoint).await;
            }
            Operation::IterNext(id) => {
                let iter = iters_map.get_mut(&id).expect("iter not in worker");
                let actual = iter.next().await;
                let res = res_rx.recv().await.expect("recv result failed");
                if let OperationResult::IterNext(expected) = res {
                    assert_eq!(TraceResult::Ok(actual), expected, "iter_next result wrong");
                }
            }
            Operation::MetaMessage(resp) => {
                assert_eq!(storage_type, StorageType::Global);
                let op = resp.0.operation();
                if let Some(info) = resp.0.info {
                    replay.notify_hummock(info, op).await.unwrap();
                }
            }
            _ => unreachable!(),
        }
    }
}

struct WorkerHandler {
    req_tx: UnboundedSender<ReplayRequest>,
    res_tx: UnboundedSender<OperationResult>,
    resp_rx: UnboundedReceiver<WorkerResponse>,
    join: JoinHandle<()>,
    stacked_replay_count: u32,
}

impl WorkerHandler {
    async fn join(self) {
        self.join.await.expect("failed to stop worker");
    }

    fn finish(&self) {
        self.send_replay_req(ReplayRequest::Fin);
    }

    fn replay(&mut self, req: ReplayRequest) {
        self.stacked_replay_count += 1;
        self.send_replay_req(req);
    }

    async fn wait(&mut self) {
        while self.stacked_replay_count > 0 {
            self.resp_rx
                .recv()
                .await
                .expect("failed to wait worker resp");
            self.stacked_replay_count -= 1;
        }
    }

    fn send_replay_req(&self, req: ReplayRequest) {
        self.req_tx
            .send(req)
            .expect("failed to send replay request");
    }

    fn send_result(&self, result: OperationResult) {
        self.res_tx.send(result).expect("failed to send result");
    }
}

struct LocalStorages {
    storages: HashMap<u32, Box<dyn LocalReplay>>,
}
impl LocalStorages {
    fn new() -> Self {
        Self {
            storages: HashMap::new(),
        }
    }

    async fn get_or_insert(
        &mut self,
        table_id: u32,
        replay: &Arc<impl GlobalReplay>,
    ) -> &mut Box<dyn LocalReplay> {
        match self.storages.entry(table_id) {
            Entry::Occupied(s) => s.into_mut(),
            Entry::Vacant(e) => e.insert(replay.new_local(table_id).await),
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.storages.len()
    }
}

#[cfg(test)]
mod tests {

    use std::ops::Bound;

    use mockall::predicate;
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;
    use crate::{
        traced_bytes, MockGlobalReplayInterface, MockLocalReplayInterface, MockReplayIter,
        StorageType, TraceReadOptions,
    };

    #[tokio::test]
    async fn test_handle_record() {
        let mut iters_map = HashMap::new();
        let mut local_storages = LocalStorages::new();
        let (res_tx, mut res_rx) = unbounded_channel();

        let read_options = TraceReadOptions {
            prefix_hint: None,
            check_bloom_filter: false,
            retention_seconds: Some(12),
            table_id: 12,
            ignore_range_tombstone: false,
        };
        let iter_read_options = TraceReadOptions {
            prefix_hint: None,
            check_bloom_filter: true,
            retention_seconds: Some(124),
            table_id: 123,
            ignore_range_tombstone: true,
        };
        let op = Operation::Get {
            key: traced_bytes![123],
            epoch: 123,
            read_options: read_options.clone(),
        };

        let record = Record::new(StorageType::Local(0), 0, op);
        let mut mock_replay = MockGlobalReplayInterface::new();

        mock_replay.expect_new_local().times(1).returning(move |_| {
            let mut mock_local = MockLocalReplayInterface::new();

            mock_local
                .expect_get()
                .with(
                    predicate::eq(traced_bytes![123]),
                    predicate::eq(123),
                    predicate::always(),
                )
                .returning(|_, _, _| Ok(Some(traced_bytes![120])));

            Box::new(mock_local)
        });

        mock_replay.expect_new_local().times(1).returning(move |_| {
            let mut mock_local = MockLocalReplayInterface::new();

            mock_local
                .expect_iter()
                .with(
                    predicate::eq((Bound::Unbounded, Bound::Unbounded)),
                    predicate::eq(45),
                    predicate::always(),
                )
                .returning(|_, _, _| {
                    let mut mock_iter = MockReplayIter::new();
                    mock_iter
                        .expect_next()
                        .times(1)
                        .returning(|| Some((traced_bytes![1], traced_bytes![0])));
                    Ok(Box::new(mock_iter))
                });

            Box::new(mock_local)
        });

        let replay = Arc::new(mock_replay);
        res_tx
            .send(OperationResult::Get(TraceResult::Ok(Some(traced_bytes![
                120
            ]))))
            .unwrap();
        ReplayWorker::handle_record(
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
            read_options: iter_read_options,
        };
        let record = Record::new(StorageType::Local(0), 1, op);
        res_tx
            .send(OperationResult::Iter(TraceResult::Ok(())))
            .unwrap();

        ReplayWorker::handle_record(
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
            .send(OperationResult::IterNext(TraceResult::Ok(Some((
                traced_bytes![1],
                traced_bytes![0],
            )))))
            .unwrap();

        ReplayWorker::handle_record(
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
