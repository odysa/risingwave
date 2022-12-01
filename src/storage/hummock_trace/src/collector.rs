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

use std::env;
use std::fs::{create_dir_all, OpenOptions};
use std::io::BufWriter;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::LazyLock;

use bincode::{Decode, Encode};
use parking_lot::Mutex;
use tokio::sync::mpsc::{
    unbounded_channel as channel, UnboundedReceiver as Receiver, UnboundedSender as Sender,
};
use tokio::task_local;

use crate::write::{TraceWriter, TraceWriterImpl};
use crate::{
    ConcurrentIdGenerator, Operation, OperationResult, Record, RecordId, RecordIdGenerator,
    UniqueIdGenerator,
};

static GLOBAL_COLLECTOR: LazyLock<GlobalCollector> = LazyLock::new(GlobalCollector::new);
static GLOBAL_RECORD_ID: LazyLock<RecordIdGenerator> =
    LazyLock::new(|| UniqueIdGenerator::new(AtomicU64::new(0)));
static SHOULD_USE_TRACE: LazyLock<bool> = LazyLock::new(set_use_trace);
pub static CONCURRENT_ID: LazyLock<ConcurrentIdGenerator> =
    LazyLock::new(|| UniqueIdGenerator::new(AtomicU64::new(0)));

const USE_TRACE: &str = "USE_HM_TRACE";
const LOG_PATH: &str = "HM_TRACE_PATH";
const DEFAULT_PATH: &str = ".trace/hummock.ht";
const WRITER_BUFFER_SIZE: usize = 1024;

pub fn should_use_trace() -> bool {
    *SHOULD_USE_TRACE
}

fn set_use_trace() -> bool {
    if let Ok(v) = std::env::var(USE_TRACE) {
        v.parse().unwrap()
    } else {
        false
    }
}

/// Initialize the `GLOBAL_COLLECTOR` with configured log file
pub fn init_collector() {
    tokio::spawn(async move {
        let path = match env::var(LOG_PATH) {
            Ok(p) => p,
            Err(_) => DEFAULT_PATH.to_string(),
        };
        let path = Path::new(&path);

        if let Some(parent) = path.parent() {
            if !parent.exists() {
                create_dir_all(parent).unwrap();
            }
        }
        let f = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)
            // .await
            .expect("failed to open log file");
        let writer = BufWriter::with_capacity(WRITER_BUFFER_SIZE, f);
        let writer = TraceWriterImpl::new_bincode(writer).unwrap();
        GlobalCollector::run(writer);
    });
}

/// `GlobalCollector` collects traced hummock operations.
/// It starts a collector thread and writer thread.
struct GlobalCollector {
    tx: Sender<RecordMsg>,
    rx: Mutex<Option<Receiver<RecordMsg>>>,
}

impl GlobalCollector {
    fn new() -> Self {
        let (tx, rx) = channel();
        Self {
            tx,
            rx: Mutex::new(Some(rx)),
        }
    }

    fn run(mut writer: impl TraceWriter + Send + 'static) -> tokio::task::JoinHandle<()> {
        tokio::task::spawn_blocking(move || {
            let mut rx = GLOBAL_COLLECTOR.rx.lock().take().unwrap();
            while let Some(r) = rx.blocking_recv() {
                match r {
                    Some(r) => {
                        writer.write(r).expect("failed to write hummock trace");
                    }
                    None => {
                        writer.flush().expect("failed to flush hummock trace");
                        break;
                    }
                }
            }
        })
    }

    fn finish(&self) {
        self.tx.send(None).expect("failed to finish worker");
    }

    fn tx(&self) -> Sender<RecordMsg> {
        self.tx.clone()
    }
}

impl Drop for GlobalCollector {
    fn drop(&mut self) {
        // only send when channel is not closed
        if !self.tx.is_closed() {
            self.finish();
        }
    }
}

/// `TraceSpan` traces hummock operations. It marks the beginning of an operation and
/// the end when the span is dropped. So, please make sure the span live long enough.
/// Underscore binding like `let _ = span` will drop the span immediately.
#[must_use = "TraceSpan Lifetime is important"]
#[derive(Clone)]
pub struct TraceSpan {
    tx: Sender<RecordMsg>,
    id: RecordId,
    storage_type: StorageType,
}

impl TraceSpan {
    pub fn new(tx: Sender<RecordMsg>, id: RecordId, storage_type: StorageType) -> Self {
        Self {
            tx,
            id,
            storage_type,
        }
    }

    pub fn send(&self, op: Operation) {
        self.tx
            .send(Some(Record::new(self.storage_type(), self.id(), op)))
            .expect("failed to log record");
    }

    pub fn send_result(&self, res: OperationResult) {
        self.send(Operation::Result(res));
    }

    pub fn finish(&self) {
        self.send(Operation::Finish);
    }

    pub fn id(&self) -> RecordId {
        self.id
    }

    fn storage_type(&self) -> StorageType {
        self.storage_type
    }

    /// Create a span and send operation to the `GLOBAL_COLLECTOR`
    pub fn new_to_global(op: Operation, storage_type: StorageType) -> Self {
        let span = TraceSpan::new(GLOBAL_COLLECTOR.tx(), GLOBAL_RECORD_ID.next(), storage_type);
        span.send(op);
        span
    }

    #[cfg(test)]
    pub fn new_op(
        tx: Sender<RecordMsg>,
        id: RecordId,
        op: Operation,
        storage_type: StorageType,
    ) -> Self {
        let span = TraceSpan::new(tx, id, storage_type);
        span.send(op);
        span
    }
}

impl Drop for TraceSpan {
    fn drop(&mut self) {
        self.finish();
    }
}

pub type RecordMsg = Option<Record>;
pub type ConcurrentId = u64;

#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
pub enum StorageType {
    Global,
    Local(ConcurrentId),
}

task_local! {
    // This is why we need to ignore this rule
    // https://github.com/rust-lang/rust-clippy/issues/9224
    #[allow(clippy::declare_interior_mutable_const)]
    pub static LOCAL_ID: ConcurrentId;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{MockTraceWriter, TracedBytes};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_new_spans_concurrent() {
        let count = 200;

        let collector = Arc::new(GlobalCollector::new());
        let generator = Arc::new(UniqueIdGenerator::new(AtomicU64::new(0)));
        let mut handles = Vec::with_capacity(count);

        for i in 0..count {
            let collector = collector.clone();
            let generator = generator.clone();
            let handle = tokio::spawn(async move {
                let op = Operation::get(
                    TracedBytes::from(vec![i as u8]),
                    123,
                    None,
                    true,
                    Some(12),
                    123,
                    false,
                );
                let _span =
                    TraceSpan::new_op(collector.tx(), generator.next(), op, StorageType::Global);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let mut rx = collector.rx.lock().take().unwrap();
        let mut rx_count = 0;
        rx.close();
        while rx.recv().await.is_some() {
            rx_count += 1;
        }
        assert_eq!(count * 2, rx_count);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_collector_run() {
        let count = 5000;
        let generator = Arc::new(UniqueIdGenerator::new(AtomicU64::new(0)));

        let op = Operation::get(
            TracedBytes::from(vec![74, 56, 43, 67]),
            256,
            None,
            true,
            Some(242),
            167,
            false,
        );
        let mut mock_writer = MockTraceWriter::new();

        mock_writer
            .expect_write()
            .times(count * 2)
            .returning(|_| Ok(0));
        mock_writer.expect_flush().times(1).returning(|| Ok(()));

        let runner_handle = GlobalCollector::run(mock_writer);

        let mut handles = Vec::with_capacity(count);

        for _ in 0..count {
            let op = op.clone();
            let tx = GLOBAL_COLLECTOR.tx();
            let generator = generator.clone();
            let handle = tokio::spawn(async move {
                let _span = TraceSpan::new_op(tx, generator.next(), op, StorageType::Local(0));
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        GLOBAL_COLLECTOR.finish();

        runner_handle.await.unwrap();
    }
}
