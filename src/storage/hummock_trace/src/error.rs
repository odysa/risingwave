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

use bincode::error::{DecodeError, EncodeError};
use thiserror::Error;

use crate::RecordId;

pub type Result<T> = std::result::Result<T, TraceError>;

#[derive(Error, Debug)]
pub enum TraceError {
    #[error("failed to encode, {0}")]
    Encode(EncodeError),

    #[error("failed to decode, {0}")]
    Decode(DecodeError),

    #[error("failed to read or write {0}")]
    Io(std::io::Error),

    #[error("invalid magic bytes, expected {expected:?}, found {found:?}")]
    MagicBytes { expected: u32, found: u32 },

    #[error("try to close a non-existing record {0}")]
    FinRecord(RecordId),

    #[error("failed to create a iter")]
    IterFailed,

    #[error("failed to get key")]
    GetFailed,

    #[error("failed to ingest")]
    IngestFailed,

    #[error("failed to sync")]
    SyncFailed,
}

impl From<EncodeError> for TraceError {
    fn from(err: EncodeError) -> Self {
        TraceError::Encode(err)
    }
}

impl From<DecodeError> for TraceError {
    fn from(err: DecodeError) -> Self {
        TraceError::Decode(err)
    }
}

impl From<std::io::Error> for TraceError {
    fn from(err: std::io::Error) -> Self {
        TraceError::Io(err)
    }
}
