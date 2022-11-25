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

mod runner;
mod worker;

use std::ops::Bound;

#[cfg(test)]
use mockall::{automock, mock};
use risingwave_pb::meta::subscribe_response::{Info, Operation as RespOperation};
pub use runner::*;
pub(crate) use worker::*;

use crate::error::Result;
use crate::{Record, TraceReadOptions, TraceWriteOptions, TracedBytes};

type ReplayGroup = Record;

type WorkerResponse = ();

#[derive(Debug)]
pub(crate) enum ReplayRequest {
    Task(ReplayGroup),
    Fin,
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub(crate) enum WorkerId {
    Local(u64),
    OneShot(u64),
}
pub trait LocalReplay: ReplayRead + ReplayWrite + Send + Sync {}
pub trait GlobalReplay: ReplayRead + ReplayStateStore + Send + Sync {}

#[cfg_attr(test, automock)]
#[async_trait::async_trait]
pub trait ReplayRead {
    async fn iter(
        &self,
        key_range: (Bound<TracedBytes>, Bound<TracedBytes>),
        epoch: u64,
        read_options: TraceReadOptions,
    ) -> Result<Box<dyn ReplayIter>>;
    async fn get(
        &self,
        key: TracedBytes,
        epoch: u64,
        read_options: TraceReadOptions,
    ) -> Result<Option<TracedBytes>>;
}

#[cfg_attr(test, automock)]
#[async_trait::async_trait]
pub trait ReplayWrite {
    async fn ingest(
        &self,
        kv_pairs: Vec<(TracedBytes, Option<TracedBytes>)>,
        delete_ranges: Vec<(TracedBytes, TracedBytes)>,
        write_options: TraceWriteOptions,
    ) -> Result<usize>;
}

#[cfg_attr(test, automock)]
#[async_trait::async_trait]
pub trait ReplayStateStore {
    async fn sync(&self, id: u64) -> Result<usize>;
    async fn seal_epoch(&self, epoch_id: u64, is_checkpoint: bool);
    async fn notify_hummock(&self, info: Info, op: RespOperation) -> Result<u64>;
    async fn new_local(&self, table_id: u32) -> Box<dyn LocalReplay>;
}

#[cfg_attr(test, automock)]
#[async_trait::async_trait]
pub trait ReplayIter: Send + Sync {
    async fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)>;
}

// define mock trait for replay interfaces
#[cfg(test)]
mock! {
    pub GlobalReplayInterface{}
    #[async_trait::async_trait]
    impl ReplayRead for GlobalReplayInterface{
        async fn iter(
            &self,
            key_range: (Bound<TracedBytes>, Bound<TracedBytes>),
            epoch: u64,
            read_options: TraceReadOptions,
        ) -> Result<Box<dyn ReplayIter>>;
        async fn get(
            &self,
            key: TracedBytes,
            epoch: u64,
            read_options: TraceReadOptions,
        ) -> Result<Option<TracedBytes>>;
    }
    #[async_trait::async_trait]
    impl ReplayWrite for GlobalReplayInterface{
        async fn ingest(
            &self,
            kv_pairs: Vec<(TracedBytes, Option<TracedBytes>)>,
            delete_ranges: Vec<(TracedBytes, TracedBytes)>,
            write_options: TraceWriteOptions,
        ) -> Result<usize>;
    }
    #[async_trait::async_trait]
    impl ReplayStateStore for GlobalReplayInterface{
        async fn sync(&self, id: u64) -> Result<usize>;
        async fn seal_epoch(&self, epoch_id: u64, is_checkpoint: bool);
        async fn notify_hummock(&self, info: Info, op: RespOperation) -> Result<u64>;
        async fn new_local(&self, table_id: u32) -> Box<dyn LocalReplay>;
    }
    impl GlobalReplay for GlobalReplayInterface{}
}

#[cfg(test)]
mock! {
    pub LocalReplayInterface{}
    #[async_trait::async_trait]
    impl ReplayRead for LocalReplayInterface{
        async fn iter(
            &self,
            key_range: (Bound<TracedBytes>, Bound<TracedBytes>),
            epoch: u64,
            read_options: TraceReadOptions,
        ) -> Result<Box<dyn ReplayIter>>;
        async fn get(
            &self,
            key: TracedBytes,
            epoch: u64,
            read_options: TraceReadOptions,
        ) -> Result<Option<TracedBytes>>;
    }
    #[async_trait::async_trait]
    impl ReplayWrite for LocalReplayInterface{
        async fn ingest(
            &self,
            kv_pairs: Vec<(TracedBytes, Option<TracedBytes>)>,
            delete_ranges: Vec<(TracedBytes, TracedBytes)>,
            write_options: TraceWriteOptions,
        ) -> Result<usize>;
    }
    impl LocalReplay for LocalReplayInterface{}
}
