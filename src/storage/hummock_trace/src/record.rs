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
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};

use bincode::{BorrowDecode, Decode, Encode};
use risingwave_common::hm_trace::TraceLocalId;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::meta::SubscribeResponse;

pub type RecordId = u64;

pub(crate) struct RecordIdGenerator {
    record_id: AtomicU64,
}

impl RecordIdGenerator {
    pub(crate) fn new() -> Self {
        Self {
            record_id: AtomicU64::new(0),
        }
    }

    pub(crate) fn next(&self) -> RecordId {
        self.record_id.fetch_add(1, Ordering::Relaxed)
    }
}

#[derive(Encode, Decode, Debug, PartialEq, Clone)]
pub struct Record(pub TraceLocalId, pub RecordId, pub Operation);

impl Record {
    pub(crate) fn new(local_id: TraceLocalId, record_id: RecordId, op: Operation) -> Self {
        Self(local_id, record_id, op)
    }

    pub(crate) fn new_local_none(record_id: RecordId, op: Operation) -> Self {
        Self::new(TraceLocalId::None, record_id, op)
    }

    pub(crate) fn local_id(&self) -> TraceLocalId {
        self.0
    }

    pub(crate) fn record_id(&self) -> RecordId {
        self.1
    }

    pub(crate) fn op(&self) -> &Operation {
        &self.2
    }
}

/// Operations represents Hummock operations
#[derive(Encode, Decode, PartialEq, Debug, Clone)]
pub enum Operation {
    /// Get operation of Hummock.
    /// (key, check_bloom_filter, epoch, table_id, retention_seconds)
    Get(Vec<u8>, bool, u64, u32, Option<u32>),

    /// Ingest operation of Hummock.
    /// (kv_pairs, epoch, table_id)
    Ingest(Vec<(Vec<u8>, Option<Vec<u8>>)>, u64, u32),

    /// Iter operation of Hummock
    /// (prefix_hint, left_bound, right_bound, epoch, table_id, retention_seconds)
    Iter(
        Option<Vec<u8>>,
        Bound<Vec<u8>>,
        Bound<Vec<u8>>,
        u64,
        u32,
        Option<u32>,
    ),

    /// Iter.next operation
    /// (record_id, kv_pair)
    IterNext(RecordId, Option<(Vec<u8>, Vec<u8>)>),

    /// Sync operation
    /// (epoch)
    Sync(u64),

    /// Seal operation
    /// (epoch, is_checkpoint)
    Seal(u64, bool),

    /// Update local_version
    UpdateVersion(u64),

    WaitEpoch(ReadEpochStatus),

    /// The end of an operation
    Finish,

    /// SubscribeResponse implements Serde's Serialize and Deserialize, so use serde
    MetaMessage(TraceSubResp),

    Result(TraceOpResult),
}

type TraceResult<T> = Option<T>;

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub enum TraceOpResult {
    Get(TraceResult<Option<Vec<u8>>>),
    Ingest(TraceResult<usize>),
    Iter(TraceResult<()>),
    Sync(TraceResult<()>),
    Seal(TraceResult<()>),
    WaitEpoch(TraceResult<()>),
    NotifyHummock(TraceResult<()>),
}

/// We must derive serialization trait for this
/// so create a dummy enum here
#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub enum ReadEpochStatus {
    Committed(u64),
    Current(u64),
    NoWait(u64),
}

impl From<HummockReadEpoch> for ReadEpochStatus {
    fn from(epoch: HummockReadEpoch) -> Self {
        match epoch {
            HummockReadEpoch::Committed(id) => ReadEpochStatus::Committed(id),
            HummockReadEpoch::Current(id) => ReadEpochStatus::Current(id),
            HummockReadEpoch::NoWait(id) => ReadEpochStatus::NoWait(id),
        }
    }
}

impl Into<HummockReadEpoch> for ReadEpochStatus {
    fn into(self) -> HummockReadEpoch {
        match self {
            ReadEpochStatus::Committed(id) => HummockReadEpoch::Committed(id),
            ReadEpochStatus::Current(id) => HummockReadEpoch::Current(id),
            ReadEpochStatus::NoWait(id) => HummockReadEpoch::NoWait(id),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct TraceSubResp(pub SubscribeResponse);

impl Encode for TraceSubResp {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        let encoded = ron::to_string(&self.0).unwrap();
        Encode::encode(&encoded, encoder)
    }
}

impl Decode for TraceSubResp {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let s: String = Decode::decode(decoder)?;
        let resp: SubscribeResponse = ron::from_str(&s).unwrap();
        Ok(Self(resp))
    }
}

impl<'de> bincode::BorrowDecode<'de> for TraceSubResp {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> core::result::Result<Self, bincode::error::DecodeError> {
        let s: String = BorrowDecode::borrow_decode(decoder)?;
        let resp: SubscribeResponse = ron::from_str(&s).unwrap();
        Ok(Self(resp))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use parking_lot::Mutex;

    use crate::RecordIdGenerator;

    // test atomic id
    #[tokio::test(flavor = "multi_thread")]
    async fn atomic_span_id() {
        // reset record id to be 0
        let gen = Arc::new(RecordIdGenerator::new());
        let mut handles = Vec::new();
        let ids_lock = Arc::new(Mutex::new(HashSet::new()));
        let count: u64 = 10;

        for _ in 0..count {
            let ids = ids_lock.clone();
            let gen = gen.clone();
            handles.push(tokio::spawn(async move {
                let id = gen.next();
                ids.lock().insert(id);
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let ids = ids_lock.lock();

        for i in 0..count {
            assert!(ids.contains(&i));
        }
    }
}
