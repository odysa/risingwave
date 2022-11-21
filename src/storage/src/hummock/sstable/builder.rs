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

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use bytes::BytesMut;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::filter_key_extractor::{
    FilterKeyExtractorImpl, FullKeyFilterKeyExtractor,
};
use risingwave_hummock_sdk::key::{key_with_epoch, user_key, FullKey};
use risingwave_hummock_sdk::table_stats::TableStats;
use risingwave_hummock_sdk::{HummockEpoch, LocalSstableInfo};
use risingwave_pb::hummock::SstableInfo;

use super::bloom::Bloom;
use super::utils::CompressionAlgorithm;
use super::{
    BlockBuilder, BlockBuilderOptions, BlockMeta, SstableMeta, SstableWriter, DEFAULT_BLOCK_SIZE,
    DEFAULT_ENTRY_SIZE, DEFAULT_RESTART_INTERVAL, VERSION,
};
use crate::hummock::value::HummockValue;
use crate::hummock::{DeleteRangeTombstone, HummockResult};

pub const DEFAULT_SSTABLE_SIZE: usize = 4 * 1024 * 1024;
pub const DEFAULT_BLOOM_FALSE_POSITIVE: f64 = 0.1;
#[derive(Clone, Debug)]
pub struct SstableBuilderOptions {
    /// Approximate sstable capacity.
    pub capacity: usize,
    /// Approximate block capacity.
    pub block_capacity: usize,
    /// Restart point interval.
    pub restart_interval: usize,
    /// False positive probability of bloom filter.
    pub bloom_false_positive: f64,
    /// Compression algorithm.
    pub compression_algorithm: CompressionAlgorithm,
}

impl From<&StorageConfig> for SstableBuilderOptions {
    fn from(options: &StorageConfig) -> SstableBuilderOptions {
        let capacity = (options.sstable_size_mb as usize) * (1 << 20);
        SstableBuilderOptions {
            capacity,
            block_capacity: (options.block_size_kb as usize) * (1 << 10),
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: options.bloom_false_positive,
            compression_algorithm: CompressionAlgorithm::None,
        }
    }
}

impl Default for SstableBuilderOptions {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_SSTABLE_SIZE,
            block_capacity: DEFAULT_BLOCK_SIZE,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: DEFAULT_BLOOM_FALSE_POSITIVE,
            compression_algorithm: CompressionAlgorithm::None,
        }
    }
}

pub struct SstableBuilderOutput<WO> {
    pub sst_info: LocalSstableInfo,
    pub bloom_filter_size: usize,
    pub writer_output: WO,
    pub avg_key_size: usize,
    pub avg_value_size: usize,
}

pub struct SstableBuilder<W: SstableWriter> {
    /// Options.
    options: SstableBuilderOptions,
    /// Data writer.
    writer: W,
    /// Current block builder.
    block_builder: BlockBuilder,
    filter_key_extractor: Arc<FilterKeyExtractorImpl>,
    /// Block metadata vec.
    block_metas: Vec<BlockMeta>,
    /// store operation of delete-range
    range_tombstones: Vec<DeleteRangeTombstone>,
    /// `table_id` of added keys.
    table_ids: BTreeSet<u32>,
    /// Hashes of user keys.
    user_key_hashes: Vec<u32>,
    last_full_key: Vec<u8>,
    /// Buffer for encoded key and value to avoid allocation.
    raw_key: BytesMut,
    raw_value: BytesMut,
    last_table_id: u32,
    sstable_id: u64,

    last_bloom_filter_key_length: usize,

    /// `stale_key_count` counts range_tombstones as well.
    stale_key_count: u64,
    /// `total_key_count` counts range_tombstones as well.
    total_key_count: u64,
    /// Per table stats.
    table_stats: HashMap<u32, TableStats>,
    /// `last_table_stats` accumulates stats for `last_table_id` and finalizes it in `table_stats`
    /// by `finalize_last_table_stats`
    last_table_stats: TableStats,
}

impl<W: SstableWriter> SstableBuilder<W> {
    pub fn for_test(sstable_id: u64, writer: W, options: SstableBuilderOptions) -> Self {
        Self::new(
            sstable_id,
            writer,
            options,
            Arc::new(FilterKeyExtractorImpl::FullKey(
                FullKeyFilterKeyExtractor::default(),
            )),
        )
    }

    pub fn new(
        sstable_id: u64,
        writer: W,
        options: SstableBuilderOptions,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
    ) -> Self {
        Self {
            options: options.clone(),
            writer,
            block_builder: BlockBuilder::new(BlockBuilderOptions {
                capacity: options.block_capacity,
                restart_interval: options.restart_interval,
                compression_algorithm: options.compression_algorithm,
            }),
            block_metas: Vec::with_capacity(options.capacity / options.block_capacity + 1),
            table_ids: BTreeSet::new(),
            user_key_hashes: Vec::with_capacity(options.capacity / DEFAULT_ENTRY_SIZE + 1),
            last_table_id: 0,
            raw_key: BytesMut::new(),
            raw_value: BytesMut::new(),
            last_full_key: vec![],
            range_tombstones: vec![],
            sstable_id,
            filter_key_extractor,
            last_bloom_filter_key_length: 0,
            stale_key_count: 0,
            total_key_count: 0,
            table_stats: Default::default(),
            last_table_stats: Default::default(),
        }
    }

    /// Add kv pair to sstable.
    pub fn add_delete_range(&mut self, tombstone: DeleteRangeTombstone) {
        self.range_tombstones.push(tombstone);
    }

    /// Add kv pair to sstable.
    pub async fn add(
        &mut self,
        full_key: &FullKey<impl AsRef<[u8]>>,
        value: HummockValue<&[u8]>,
        is_new_user_key: bool,
    ) -> HummockResult<()> {
        // Rotate block builder if the previous one has been built.
        if self.block_builder.is_empty() {
            self.block_metas.push(BlockMeta {
                offset: self.writer.data_len() as u32,
                len: 0,
                smallest_key: full_key.encode(),
                uncompressed_size: 0,
            })
        }

        // TODO: refine me
        full_key.encode_into(&mut self.raw_key);
        value.encode(&mut self.raw_value);
        if is_new_user_key {
            let table_id = full_key.user_key.table_id.table_id();
            if self.last_table_id != table_id {
                self.table_ids.insert(table_id);
                self.finalize_last_table_stats();
                self.last_table_id = table_id;
            }
            let mut extract_key = user_key(&self.raw_key);
            extract_key = self.filter_key_extractor.extract(extract_key);

            // add bloom_filter check
            // 1. not empty_key
            // 2. extract_key key is not duplicate
            if !extract_key.is_empty()
                && (extract_key != &self.last_full_key[0..self.last_bloom_filter_key_length])
            {
                // avoid duplicate add to bloom filter
                self.user_key_hashes
                    .push(farmhash::fingerprint32(extract_key));
                self.last_bloom_filter_key_length = extract_key.len();
            }
        } else {
            self.stale_key_count += 1;
            self.last_table_stats.stale_key_count += 1;
        }
        self.total_key_count += 1;
        self.last_table_stats.total_key_count += 1;

        self.block_builder
            .add(self.raw_key.as_ref(), self.raw_value.as_ref());
        self.last_table_stats.total_key_size += self.raw_key.len();
        self.last_table_stats.total_value_size += self.raw_value.len();

        self.last_full_key.clear();
        self.last_full_key.extend_from_slice(&self.raw_key);

        self.raw_key.clear();
        self.raw_value.clear();

        if self.block_builder.approximate_len() >= self.options.block_capacity {
            self.build_block().await?;
        }

        Ok(())
    }

    /// Finish building sst.
    ///
    /// Unlike most LSM-Tree implementations, sstable meta and data are encoded separately.
    /// Both meta and data has its own object (file).
    ///
    /// # Format
    ///
    /// data:
    ///
    /// ```plain
    /// | Block 0 | ... | Block N-1 | N (4B) |
    /// ```
    pub async fn finish(mut self) -> HummockResult<SstableBuilderOutput<W::Output>> {
        let mut smallest_key = if self.block_metas.is_empty() {
            vec![]
        } else {
            self.block_metas[0].smallest_key.clone()
        };
        let mut largest_key = self.last_full_key.clone();
        self.finalize_last_table_stats();

        self.build_block().await?;
        let meta_offset = self.writer.data_len() as u64;
        for tombstone in &self.range_tombstones {
            assert!(!tombstone.end_user_key.is_empty());
            if largest_key.is_empty()
                || user_key(&largest_key).lt(tombstone.end_user_key.as_slice())
            {
                // use MAX as epoch because `end_user_key` of the range-tombstone is exclusive, so
                // we can not include any version of this key.
                largest_key = key_with_epoch(tombstone.end_user_key.clone(), HummockEpoch::MAX);
            }
            if smallest_key.is_empty()
                || user_key(&smallest_key).gt(tombstone.start_user_key.as_slice())
            {
                smallest_key = key_with_epoch(tombstone.start_user_key.clone(), tombstone.sequence);
            }
        }
        self.total_key_count += self.range_tombstones.len() as u64;
        self.stale_key_count += self.range_tombstones.len() as u64;

        let mut meta = SstableMeta {
            block_metas: self.block_metas,
            bloom_filter: if self.options.bloom_false_positive > 0.0 {
                let bits_per_key = Bloom::bloom_bits_per_key(
                    self.user_key_hashes.len(),
                    self.options.bloom_false_positive,
                );
                Bloom::build_from_key_hashes(&self.user_key_hashes, bits_per_key)
            } else {
                vec![]
            },
            estimated_size: 0,
            key_count: self.total_key_count as u32,
            smallest_key,
            largest_key,
            version: VERSION,
            meta_offset,
            range_tombstone_list: self.range_tombstones,
        };
        meta.estimated_size = meta.encoded_size() as u32 + meta_offset as u32;
        let sst_info = SstableInfo {
            id: self.sstable_id,
            key_range: Some(risingwave_pb::hummock::KeyRange {
                left: meta.smallest_key.clone(),
                right: meta.largest_key.clone(),
            }),
            file_size: meta.estimated_size as u64,
            table_ids: self.table_ids.into_iter().collect(),
            meta_offset: meta.meta_offset,
            stale_key_count: self.stale_key_count,
            total_key_count: self.total_key_count,
            divide_version: 0,
        };
        tracing::trace!(
            "meta_size {} bloom_filter_size {}  add_key_counts {} ",
            meta.encoded_size(),
            meta.bloom_filter.len(),
            self.total_key_count,
        );
        let bloom_filter_size = meta.bloom_filter.len();
        let (avg_key_size, avg_value_size) = if self.table_stats.is_empty() {
            (0, 0)
        } else {
            let avg_key_size = self
                .table_stats
                .values()
                .map(|s| s.total_key_size)
                .sum::<usize>()
                / self.table_stats.len();
            let avg_value_size = self
                .table_stats
                .values()
                .map(|s| s.total_value_size)
                .sum::<usize>()
                / self.table_stats.len();
            (avg_key_size, avg_value_size)
        };
        let writer_output = self.writer.finish(meta).await?;
        Ok(SstableBuilderOutput::<W::Output> {
            sst_info: LocalSstableInfo::with_stats(sst_info, self.table_stats),
            bloom_filter_size,
            writer_output,
            avg_key_size,
            avg_value_size,
        })
    }

    pub fn approximate_len(&self) -> usize {
        self.writer.data_len()
            + self.block_builder.approximate_len()
            + self.user_key_hashes.len() * 4
    }

    async fn build_block(&mut self) -> HummockResult<()> {
        // Skip empty block.
        if self.block_builder.is_empty() {
            return Ok(());
        }

        let mut block_meta = self.block_metas.last_mut().unwrap();
        block_meta.uncompressed_size = self.block_builder.uncompressed_block_size() as u32;
        let block = self.block_builder.build();
        self.writer.write_block(block, block_meta).await?;
        block_meta.len = self.writer.data_len() as u32 - block_meta.offset;
        self.block_builder.clear();
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.user_key_hashes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.user_key_hashes.is_empty()
    }

    /// Returns true if we roughly reached capacity
    pub fn reach_capacity(&self) -> bool {
        self.approximate_len() >= self.options.capacity
    }

    fn finalize_last_table_stats(&mut self) {
        if self.table_ids.is_empty() {
            return;
        }
        self.table_stats.insert(
            self.last_table_id,
            std::mem::take(&mut self.last_table_stats),
        );
    }
}

#[cfg(test)]
pub(super) mod tests {
    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::key::UserKey;

    use super::*;
    use crate::assert_bytes_eq;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_default_test_sstable, mock_sst_writer, test_key_of,
        test_value_of, TEST_KEYS_COUNT,
    };

    #[tokio::test]
    async fn test_empty() {
        let opt = SstableBuilderOptions {
            capacity: 0,
            block_capacity: 4096,
            restart_interval: 16,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorithm::None,
        };

        let b = SstableBuilder::for_test(0, mock_sst_writer(&opt), opt);

        b.finish().await.unwrap();
    }

    #[tokio::test]
    async fn test_empty_with_delete_range() {
        let opt = SstableBuilderOptions {
            capacity: 0,
            block_capacity: 4096,
            restart_interval: 16,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorithm::None,
        };
        let table_id = TableId::default();
        let mut b = SstableBuilder::for_test(0, mock_sst_writer(&opt), opt);
        b.add_delete_range(DeleteRangeTombstone::new(
            table_id,
            b"abcd".to_vec(),
            b"eeee".to_vec(),
            0,
        ));
        let s = b.finish().await.unwrap().sst_info;
        let key_range = s.sst_info.key_range.unwrap();
        assert_eq!(
            user_key(&key_range.left),
            UserKey::for_test(TableId::default(), b"abcd").encode()
        );
        assert_eq!(
            user_key(&key_range.right),
            UserKey::for_test(TableId::default(), b"eeee").encode()
        );
    }

    #[tokio::test]
    async fn test_basic() {
        let opt = default_builder_opt_for_test();
        let mut b = SstableBuilder::for_test(0, mock_sst_writer(&opt), opt);

        for i in 0..TEST_KEYS_COUNT {
            b.add(&test_key_of(i), HummockValue::put(&test_value_of(i)), true)
                .await
                .unwrap();
        }

        let output = b.finish().await.unwrap();
        let info = output.sst_info.sst_info;

        assert_bytes_eq!(
            test_key_of(0).encode(),
            info.key_range.as_ref().unwrap().left
        );
        assert_bytes_eq!(
            test_key_of(TEST_KEYS_COUNT - 1).encode(),
            info.key_range.as_ref().unwrap().right
        );
        let (data, meta) = output.writer_output;
        assert_eq!(info.file_size, meta.estimated_size as u64);
        let offset = info.meta_offset as usize;
        let meta2 = SstableMeta::decode(&mut &data[offset..]).unwrap();
        assert_eq!(meta2, meta);
    }

    async fn test_with_bloom_filter(with_blooms: bool) {
        let key_count = 1000;

        let opts = SstableBuilderOptions {
            capacity: 0,
            block_capacity: 4096,
            restart_interval: 16,
            bloom_false_positive: if with_blooms { 0.01 } else { 0.0 },
            compression_algorithm: CompressionAlgorithm::None,
        };

        // build remote table
        let sstable_store = mock_sstable_store();
        let table = gen_default_test_sstable(opts, 0, sstable_store).await;

        assert_eq!(table.has_bloom_filter(), with_blooms);
        for i in 0..key_count {
            let full_key = test_key_of(i);
            assert!(!table.surely_not_have_user_key(full_key.user_key.encode().as_slice()));
        }
    }

    #[tokio::test]
    async fn test_bloom_filter() {
        test_with_bloom_filter(false).await;
        test_with_bloom_filter(true).await;
    }
}
