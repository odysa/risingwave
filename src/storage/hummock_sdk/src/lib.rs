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

#![feature(async_closure)]
#![feature(drain_filter)]
#![feature(hash_drain_filter)]
#![feature(lint_reasons)]
#![feature(map_many_mut)]
#![feature(bound_map)]

mod key_cmp;

#[macro_use]
extern crate num_derive;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::Deref;

pub use key_cmp::*;
use risingwave_pb::hummock::SstableInfo;

use crate::compaction_group::StaticCompactionGroupId;
use crate::key::user_key;
use crate::table_stats::TableStats;

pub mod compact;
pub mod compaction_group;
pub mod filter_key_extractor;
pub mod key;
pub mod key_range;
pub mod prost_key_range;
pub mod table_stats;

pub type HummockSstableId = u64;
pub type HummockRefCount = u64;
pub type HummockVersionId = u64;
pub type HummockContextId = u32;
pub type HummockEpoch = u64;
pub type HummockCompactionTaskId = u64;
pub type CompactionGroupId = u64;
pub const INVALID_VERSION_ID: HummockVersionId = 0;
pub const FIRST_VERSION_ID: HummockVersionId = 1;

pub const LOCAL_SST_ID_MASK: HummockSstableId = 1 << (HummockSstableId::BITS - 1);
pub const REMOTE_SST_ID_MASK: HummockSstableId = !LOCAL_SST_ID_MASK;

#[derive(Debug, Clone)]
pub struct LocalSstableInfo {
    pub compaction_group_id: CompactionGroupId,
    pub sst_info: SstableInfo,
    pub table_stats: HashMap<u32, TableStats>,
}

impl LocalSstableInfo {
    pub fn new(compaction_group_id: CompactionGroupId, sstable_info: SstableInfo) -> Self {
        Self {
            compaction_group_id,
            sst_info: sstable_info,
            table_stats: Default::default(),
        }
    }

    pub fn with_stats(sstable_info: SstableInfo, table_stats: HashMap<u32, TableStats>) -> Self {
        Self {
            compaction_group_id: StaticCompactionGroupId::StateDefault as CompactionGroupId,
            sst_info: sstable_info,
            table_stats,
        }
    }

    pub fn file_size(&self) -> u64 {
        self.sst_info.file_size
    }
}

impl PartialEq for LocalSstableInfo {
    fn eq(&self, other: &Self) -> bool {
        self.compaction_group_id == other.compaction_group_id && self.sst_info == other.sst_info
    }
}

pub fn get_remote_sst_id(id: HummockSstableId) -> HummockSstableId {
    id & REMOTE_SST_ID_MASK
}

pub fn get_local_sst_id(id: HummockSstableId) -> HummockSstableId {
    id | LOCAL_SST_ID_MASK
}

pub fn is_remote_sst_id(id: HummockSstableId) -> bool {
    id & LOCAL_SST_ID_MASK == 0
}

/// Package read epoch of hummock, it be used for `wait_epoch`
#[derive(Debug, Clone)]
pub enum HummockReadEpoch {
    /// We need to wait the `max_committed_epoch`
    Committed(HummockEpoch),
    /// We need to wait the `max_current_epoch`
    Current(HummockEpoch),
    /// We don't need to wait epoch, we usually do stream reading with it.
    NoWait(HummockEpoch),
}

impl HummockReadEpoch {
    pub fn get_epoch(&self) -> HummockEpoch {
        *match self {
            HummockReadEpoch::Committed(epoch) => epoch,
            HummockReadEpoch::Current(epoch) => epoch,
            HummockReadEpoch::NoWait(epoch) => epoch,
        }
    }
}
pub struct SstIdRange {
    // inclusive
    pub start_id: HummockSstableId,
    // exclusive
    pub end_id: HummockSstableId,
}

impl SstIdRange {
    pub fn new(start_id: HummockSstableId, end_id: HummockSstableId) -> Self {
        Self { start_id, end_id }
    }

    pub fn peek_next_sst_id(&self) -> Option<HummockSstableId> {
        if self.start_id < self.end_id {
            return Some(self.start_id);
        }
        None
    }

    /// Pops and returns next SST id.
    pub fn get_next_sst_id(&mut self) -> Option<HummockSstableId> {
        let next_id = self.peek_next_sst_id();
        self.start_id += 1;
        next_id
    }
}

pub fn can_concat(ssts: &[impl Deref<Target = SstableInfo>]) -> bool {
    let len = ssts.len();
    for i in 0..len - 1 {
        if user_key(&ssts[i].get_key_range().as_ref().unwrap().right).cmp(user_key(
            &ssts[i + 1].get_key_range().as_ref().unwrap().left,
        )) != Ordering::Less
        {
            return false;
        }
    }
    true
}
