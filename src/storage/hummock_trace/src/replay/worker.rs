use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use super::{ReplayRequest, WorkerResponse};
use crate::{LocalReplay, Operation, OperationResult, Record, RecordId, ReplayIter, Replayable};

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
    let Record(_, _, record_id, op) = record;
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
                // cannot use or_insert here because rust evaluates arguments even though
                // or_insert is never called
                if !local_storages.contains_key(&table_id) {
                    local_storages.insert(table_id, replay.new_local(table_id).await);
                }
                local_storages.get(&table_id).unwrap()
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
                if !local_storages.contains_key(&table_id) {
                    local_storages.insert(table_id, replay.new_local(table_id).await);
                }
                local_storages.get(&table_id).unwrap()
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
                if !local_storages.contains_key(&table_id) {
                    local_storages.insert(table_id, replay.new_local(table_id).await);
                }
                local_storages.get(&table_id).unwrap()
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
