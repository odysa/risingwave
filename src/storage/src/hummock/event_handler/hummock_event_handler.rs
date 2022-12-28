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

use std::collections::{BTreeMap, HashMap};
use std::ops::DerefMut;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use futures::future::{select, Either};
use futures::FutureExt;
use parking_lot::RwLock;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::{HummockEpoch, LocalSstableInfo};
use risingwave_pb::hummock::pin_version_response::Payload;
use tokio::spawn;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};

use super::{LocalInstanceGuard, LocalInstanceId, ReadVersionMappingType};
use crate::hummock::compactor::{compact, Context};
use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::event_handler::uploader::{
    HummockUploader, UploadTaskInfo, UploadTaskPayload, UploaderEvent,
};
use crate::hummock::event_handler::HummockEvent;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::shared_buffer::UncommittedData;
use crate::hummock::store::version::{
    HummockReadVersion, StagingData, StagingSstableInfo, VersionUpdate,
};
use crate::hummock::utils::validate_table_key_range;
use crate::hummock::{HummockError, HummockResult, MemoryLimiter, SstableIdManagerRef, TrackerId};
use crate::store::SyncResult;

#[derive(Clone)]
pub struct BufferTracker {
    flush_threshold: usize,
    global_buffer: Arc<MemoryLimiter>,
    global_upload_task_size: Arc<AtomicUsize>,
}

impl BufferTracker {
    pub fn from_storage_config(config: &StorageConfig) -> Self {
        let capacity = config.shared_buffer_capacity_mb as usize * (1 << 20);
        let flush_threshold = capacity * 4 / 5;
        Self::new(capacity, flush_threshold)
    }

    pub fn new(capacity: usize, flush_threshold: usize) -> Self {
        assert!(capacity >= flush_threshold);
        Self {
            flush_threshold,
            global_buffer: Arc::new(MemoryLimiter::new(capacity as u64)),
            global_upload_task_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn for_test() -> Self {
        Self::from_storage_config(&StorageConfig::default())
    }

    pub fn get_buffer_size(&self) -> usize {
        self.global_buffer.get_memory_usage() as usize
    }

    pub fn get_memory_limiter(&self) -> &Arc<MemoryLimiter> {
        &self.global_buffer
    }

    pub fn global_upload_task_size(&self) -> &Arc<AtomicUsize> {
        &self.global_upload_task_size
    }

    /// Return true when the buffer size minus current upload task size is still greater than the
    /// flush threshold.
    pub fn need_more_flush(&self) -> bool {
        self.get_buffer_size()
            > self.flush_threshold + self.global_upload_task_size.load(Ordering::Relaxed)
    }
}

pub struct HummockEventHandler {
    hummock_event_tx: mpsc::UnboundedSender<HummockEvent>,
    hummock_event_rx: mpsc::UnboundedReceiver<HummockEvent>,
    pending_sync_requests: BTreeMap<HummockEpoch, oneshot::Sender<HummockResult<SyncResult>>>,
    read_version_mapping: Arc<ReadVersionMappingType>,

    version_update_notifier_tx: Arc<tokio::sync::watch::Sender<HummockEpoch>>,
    seal_epoch: Arc<AtomicU64>,
    pinned_version: Arc<ArcSwap<PinnedVersion>>,
    write_conflict_detector: Option<Arc<ConflictDetector>>,

    uploader: HummockUploader,

    last_instance_id: LocalInstanceId,

    sstable_id_manager: SstableIdManagerRef,
}

async fn flush_imms(
    payload: UploadTaskPayload,
    task_info: UploadTaskInfo,
    compactor_context: Arc<crate::hummock::compactor::Context>,
) -> HummockResult<Vec<LocalSstableInfo>> {
    for epoch in &task_info.epochs {
        let _ = compactor_context
            .sstable_id_manager
            .add_watermark_sst_id(Some(*epoch))
            .await
            .inspect_err(|e| {
                error!("unable to set watermark sst id. epoch: {}, {:?}", epoch, e);
            });
    }
    compact(
        compactor_context,
        payload
            .into_iter()
            .map(|imm| vec![UncommittedData::Batch(imm)])
            .collect(),
        task_info.compaction_group_index,
    )
    .await
}

impl HummockEventHandler {
    pub fn new(
        hummock_event_tx: mpsc::UnboundedSender<HummockEvent>,
        hummock_event_rx: mpsc::UnboundedReceiver<HummockEvent>,
        pinned_version: PinnedVersion,
        compactor_context: Arc<Context>,
    ) -> Self {
        let seal_epoch = Arc::new(AtomicU64::new(pinned_version.max_committed_epoch()));
        let (version_update_notifier_tx, _) =
            tokio::sync::watch::channel(pinned_version.max_committed_epoch());
        let version_update_notifier_tx = Arc::new(version_update_notifier_tx);
        let read_version_mapping = Arc::new(RwLock::new(HashMap::default()));
        let buffer_tracker = BufferTracker::from_storage_config(&compactor_context.options);
        let write_conflict_detector = ConflictDetector::new_from_config(&compactor_context.options);
        let sstable_id_manager = compactor_context.sstable_id_manager.clone();
        let uploader = HummockUploader::new(
            pinned_version.clone(),
            Arc::new(move |payload, task_info| {
                spawn(flush_imms(payload, task_info, compactor_context.clone()))
            }),
            buffer_tracker,
        );

        Self {
            hummock_event_tx,
            hummock_event_rx,
            pending_sync_requests: Default::default(),
            version_update_notifier_tx,
            seal_epoch,
            pinned_version: Arc::new(ArcSwap::from_pointee(pinned_version)),
            write_conflict_detector,
            read_version_mapping,
            uploader,
            last_instance_id: 0,
            sstable_id_manager,
        }
    }

    pub fn sealed_epoch(&self) -> Arc<AtomicU64> {
        self.seal_epoch.clone()
    }

    pub fn version_update_notifier_tx(&self) -> Arc<tokio::sync::watch::Sender<HummockEpoch>> {
        self.version_update_notifier_tx.clone()
    }

    pub fn pinned_version(&self) -> Arc<ArcSwap<PinnedVersion>> {
        self.pinned_version.clone()
    }

    pub fn read_version_mapping(&self) -> Arc<ReadVersionMappingType> {
        self.read_version_mapping.clone()
    }

    pub fn buffer_tracker(&self) -> &BufferTracker {
        self.uploader.buffer_tracker()
    }
}

impl HummockEventHandler {
    async fn next_event(&mut self) -> Option<Either<UploaderEvent, HummockEvent>> {
        match select(
            self.uploader.next_event(),
            self.hummock_event_rx.recv().boxed(),
        )
        .await
        {
            Either::Left((event, _)) => Some(Either::Left(event)),
            Either::Right((event, _)) => event.map(Either::Right),
        }
    }
}

// Handler for different events
impl HummockEventHandler {
    fn handle_epoch_synced(
        &mut self,
        epoch: HummockEpoch,
        newly_uploaded_sstables: Vec<StagingSstableInfo>,
    ) {
        if !newly_uploaded_sstables.is_empty() {
            newly_uploaded_sstables
                .into_iter()
                // Take rev because newer data come first in `newly_uploaded_sstables` but we apply
                // older data first
                .rev()
                .for_each(|staging_sstable_info| {
                    Self::for_each_read_version(&self.read_version_mapping, |read_version| {
                        read_version.update(VersionUpdate::Staging(StagingData::Sst(
                            staging_sstable_info.clone(),
                        )))
                    });
                });
        }
        let result = self
            .uploader
            .get_synced_data(epoch)
            .expect("data just synced. must exist");
        // clear the pending sync epoch that is older than newly synced epoch
        while let Some((smallest_pending_sync_epoch, _)) =
            self.pending_sync_requests.first_key_value()
        {
            if *smallest_pending_sync_epoch > epoch {
                // The smallest pending sync epoch has not synced yet. Wait later
                break;
            }
            let (pending_sync_epoch, result_sender) =
                self.pending_sync_requests.pop_first().expect("must exist");
            if pending_sync_epoch == epoch {
                send_sync_result(result_sender, to_sync_result(result));
                break;
            } else {
                send_sync_result(
                    result_sender,
                    Err(HummockError::other(format!(
                        "epoch {} is not a checkpoint epoch",
                        pending_sync_epoch
                    ))),
                );
            }
        }
    }

    /// This function will be performed under the protection of the `read_version_mapping` read
    /// lock, and add write lock on each `read_version` operation
    fn for_each_read_version<F>(read_version: &Arc<ReadVersionMappingType>, mut f: F)
    where
        F: FnMut(&mut HummockReadVersion),
    {
        let read_version_mapping_guard = read_version.read();

        read_version_mapping_guard
            .values()
            .flat_map(HashMap::values)
            .for_each(|read_version| f(read_version.write().deref_mut()));
    }

    fn handle_data_spilled(&mut self, staging_sstable_info: StagingSstableInfo) {
        // todo: do some prune for version update
        Self::for_each_read_version(&self.read_version_mapping, |read_version| {
            read_version.update(VersionUpdate::Staging(StagingData::Sst(
                staging_sstable_info.clone(),
            )))
        })
    }

    fn handle_await_sync_epoch(
        &mut self,
        new_sync_epoch: HummockEpoch,
        sync_result_sender: oneshot::Sender<HummockResult<SyncResult>>,
    ) {
        // The epoch to sync has been committed already.
        if new_sync_epoch <= self.uploader.max_committed_epoch() {
            send_sync_result(
                sync_result_sender,
                Err(HummockError::other(format!(
                    "epoch {} has been committed. {}",
                    new_sync_epoch,
                    self.uploader.max_committed_epoch()
                ))),
            );
            return;
        }
        // The epoch has been synced
        if new_sync_epoch <= self.uploader.max_synced_epoch() {
            if let Some(result) = self.uploader.get_synced_data(new_sync_epoch) {
                let result = to_sync_result(result);
                send_sync_result(sync_result_sender, result);
            } else {
                send_sync_result(
                    sync_result_sender,
                    Err(HummockError::other(
                        "the requested sync epoch is not a checkpoint epoch",
                    )),
                );
            }
            return;
        }

        // If the epoch is not synced, we add to the `pending_sync_requests` anyway. If the epoch is
        // not a checkpoint epoch, it will be clear with the max synced epoch bumps up.
        if let Some(old_sync_result_sender) = self
            .pending_sync_requests
            .insert(new_sync_epoch, sync_result_sender)
        {
            let _ = old_sync_result_sender
                .send(Err(HummockError::other(
                    "the sync rx is overwritten by an new rx",
                )))
                .inspect_err(|e| {
                    error!(
                        "unable to send sync result: {}. Err: {:?}",
                        new_sync_epoch, e
                    );
                });
        }
    }

    fn handle_clear(&mut self, notifier: oneshot::Sender<()>) {
        self.uploader.clear();

        for (epoch, result_sender) in self.pending_sync_requests.drain_filter(|_, _| true) {
            send_sync_result(
                result_sender,
                Err(HummockError::other(format!(
                    "the sync epoch {} has been cleared",
                    epoch
                ))),
            );
        }

        {
            Self::for_each_read_version(&self.read_version_mapping, |read_version| {
                read_version.clear_uncommitted()
            });
        }

        self.sstable_id_manager
            .remove_watermark_sst_id(TrackerId::Epoch(HummockEpoch::MAX));

        // Notify completion of the Clear event.
        let _ = notifier.send(()).inspect_err(|e| {
            error!("failed to notify completion of clear event: {:?}", e);
        });
    }

    fn handle_version_update(&mut self, version_payload: Payload) {
        let pinned_version = self.pinned_version.load();

        let prev_max_committed_epoch = pinned_version.max_committed_epoch();
        let newly_pinned_version = match version_payload {
            Payload::VersionDeltas(version_deltas) => {
                let mut version_to_apply = pinned_version.version();
                for version_delta in &version_deltas.version_deltas {
                    assert_eq!(version_to_apply.id, version_delta.prev_id);
                    version_to_apply.apply_version_delta(version_delta);
                }
                version_to_apply
            }
            Payload::PinnedVersion(version) => version,
        };

        validate_table_key_range(&newly_pinned_version);

        let new_pinned_version = pinned_version.new_pin_version(newly_pinned_version);
        self.pinned_version
            .store(Arc::new(new_pinned_version.clone()));

        {
            Self::for_each_read_version(&self.read_version_mapping, |read_version| {
                read_version.update(VersionUpdate::CommittedSnapshot(new_pinned_version.clone()))
            });
        }

        let max_committed_epoch = new_pinned_version.max_committed_epoch();

        // only notify local_version_manager when MCE change
        self.version_update_notifier_tx.send_if_modified(|state| {
            assert_eq!(prev_max_committed_epoch, *state);
            if max_committed_epoch > *state {
                *state = max_committed_epoch;
                true
            } else {
                false
            }
        });

        if let Some(conflict_detector) = self.write_conflict_detector.as_ref() {
            conflict_detector.set_watermark(max_committed_epoch);
        }
        self.sstable_id_manager
            .remove_watermark_sst_id(TrackerId::Epoch(
                self.pinned_version.load().max_committed_epoch(),
            ));

        self.uploader.update_pinned_version(new_pinned_version);
    }
}

impl HummockEventHandler {
    pub async fn start_hummock_event_handler_worker(mut self) {
        while let Some(event) = self.next_event().await {
            match event {
                Either::Left(event) => match event {
                    UploaderEvent::SyncFinish(epoch, newly_uploaded_sstables) => {
                        self.handle_epoch_synced(epoch, newly_uploaded_sstables);
                    }

                    UploaderEvent::DataSpilled(staging_sstable_info) => {
                        self.handle_data_spilled(staging_sstable_info);
                    }
                },
                Either::Right(event) => {
                    match event {
                        HummockEvent::BufferMayFlush => {
                            self.uploader.may_flush();
                        }
                        HummockEvent::AwaitSyncEpoch {
                            new_sync_epoch,
                            sync_result_sender,
                        } => {
                            self.handle_await_sync_epoch(new_sync_epoch, sync_result_sender);
                        }
                        HummockEvent::Clear(notifier) => {
                            self.handle_clear(notifier);
                        }
                        HummockEvent::Shutdown => {
                            info!("buffer tracker shutdown");
                            break;
                        }

                        HummockEvent::VersionUpdate(version_payload) => {
                            self.handle_version_update(version_payload);
                        }

                        HummockEvent::ImmToUploader(imm) => {
                            self.uploader.add_imm(imm);
                            self.uploader.may_flush();
                        }

                        HummockEvent::SealEpoch {
                            epoch,
                            is_checkpoint,
                        } => {
                            self.uploader.seal_epoch(epoch);
                            if is_checkpoint {
                                self.uploader.start_sync_epoch(epoch);
                            }
                            self.seal_epoch.store(epoch, Ordering::SeqCst);
                        }
                        #[cfg(any(test, feature = "test"))]
                        HummockEvent::FlushEvent(sender) => {
                            let _ = sender.send(()).inspect_err(|e| {
                                error!("unable to send flush result: {:?}", e);
                            });
                        }

                        HummockEvent::RegisterReadVersion {
                            table_id,
                            new_read_version_sender,
                        } => {
                            let pinned_version = self.pinned_version.load();
                            let basic_read_version = Arc::new(RwLock::new(
                                HummockReadVersion::new((**pinned_version).clone()),
                            ));

                            let instance_id = self.generate_instance_id();

                            {
                                let mut read_version_mapping_guard =
                                    self.read_version_mapping.write();

                                read_version_mapping_guard
                                    .entry(table_id)
                                    .or_default()
                                    .insert(instance_id, basic_read_version.clone());
                            }

                            match new_read_version_sender.send((
                                basic_read_version,
                                LocalInstanceGuard {
                                    table_id,
                                    instance_id,
                                    event_sender: self.hummock_event_tx.clone(),
                                },
                            )) {
                                Ok(_) => {}
                                Err(_) => {
                                    panic!("RegisterReadVersion send fail table_id {:?} instance_is {:?}", table_id, instance_id)
                                }
                            }
                        }

                        HummockEvent::DestroyReadVersion {
                            table_id,
                            instance_id,
                        } => {
                            let mut read_version_mapping_guard = self.read_version_mapping.write();
                            read_version_mapping_guard
                                .get_mut(&table_id)
                                .unwrap_or_else(|| {
                                    panic!(
                                        "DestroyHummockInstance table_id {} instance_id {} fail",
                                        table_id, instance_id
                                    )
                                })
                                .remove(&instance_id).unwrap_or_else(|| panic!("DestroyHummockInstance inexist instance table_id {} instance_id {}",  table_id, instance_id));
                        }
                    }
                }
            };
        }
    }

    fn generate_instance_id(&mut self) -> LocalInstanceId {
        self.last_instance_id += 1;
        self.last_instance_id
    }
}

fn send_sync_result(
    sender: oneshot::Sender<HummockResult<SyncResult>>,
    result: HummockResult<SyncResult>,
) {
    let _ = sender.send(result).inspect_err(|e| {
        error!("unable to send sync result. Err: {:?}", e);
    });
}

fn to_sync_result(
    staging_sstable_infos: &HummockResult<Vec<StagingSstableInfo>>,
) -> HummockResult<SyncResult> {
    match staging_sstable_infos {
        Ok(staging_sstable_infos) => {
            let sync_size = staging_sstable_infos
                .iter()
                .map(StagingSstableInfo::imm_size)
                .sum();
            Ok(SyncResult {
                sync_size,
                uncommitted_ssts: staging_sstable_infos
                    .iter()
                    .flat_map(|staging_sstable_info| staging_sstable_info.sstable_infos().clone())
                    .collect(),
            })
        }
        Err(e) => Err(HummockError::other(format!("sync task failed for {:?}", e))),
    }
}
