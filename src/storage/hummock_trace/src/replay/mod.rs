use std::ops::Bound;

use crate::Record;

mod replay;
mod worker;

#[cfg(test)]
use mockall::automock;
pub use replay::*;
use risingwave_pb::meta::subscribe_response::{Info, Operation as RespOperation};
pub(crate) use worker::*;

use crate::error::Result;

type ReplayGroup = Vec<Record>;

type WorkerResponse = ();

#[derive(Debug)]
pub(crate) enum ReplayRequest {
    Task(ReplayGroup),
    Fin,
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
enum WorkerId {
    Actor(u64),
    Executor(u64),
    None(u64),
}

#[cfg_attr(test, automock)]
#[async_trait::async_trait]
pub trait Replayable: Send + Sync {
    async fn sync(&self, id: u64) -> Result<usize>;
    async fn seal_epoch(&self, epoch_id: u64, is_checkpoint: bool);
    async fn notify_hummock(&self, info: Info, op: RespOperation) -> Result<u64>;
    async fn new_local(&self, table_id: u32) -> Box<dyn LocalReplay>;
}

#[cfg_attr(test, automock)]
#[async_trait::async_trait]
pub trait LocalReplay: Send + Sync {
    async fn get(
        &self,
        key: Vec<u8>,
        check_bloom_filter: bool,
        epoch: u64,
        prefix_hint: Option<Vec<u8>>,
        table_id: u32,
        retention_seconds: Option<u32>,
    ) -> Result<Option<Vec<u8>>>;
    async fn ingest(
        &self,
        kv_pairs: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        delete_ranges: Vec<(Vec<u8>, Vec<u8>)>,
        epoch: u64,
        table_id: u32,
    ) -> Result<usize>;
    async fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        prefix_hint: Option<Vec<u8>>,
        check_bloom_filter: bool,
        retention_seconds: Option<u32>,
        table_id: u32,
    ) -> Result<Box<dyn ReplayIter>>;
}

#[async_trait::async_trait]
pub trait ReplayIter: Send + Sync {
    async fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)>;
}
