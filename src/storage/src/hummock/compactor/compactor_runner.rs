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

use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_hummock_sdk::can_concat;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorImpl;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::{KeyRange, KeyRangeCommon};
use risingwave_pb::hummock::{CompactTask, LevelType};

use super::task_progress::TaskProgress;
use crate::hummock::compactor::iterator::ConcatSstableIterator;
use crate::hummock::compactor::{
    CompactOutput, CompactionFilter, Compactor, CompactorContext, CompactorSstableStoreRef,
};
use crate::hummock::iterator::{Forward, HummockIterator, UnorderedMergeIteratorInner};
use crate::hummock::sstable::DeleteRangeAggregatorBuilder;
use crate::hummock::{
    CachePolicy, CompressionAlgorithm, HummockResult, RangeTombstonesCollector,
    SstableBuilderOptions,
};
use crate::monitor::StoreLocalStatistic;

pub struct CompactorRunner {
    compact_task: CompactTask,
    compactor: Compactor,
    sstable_store: CompactorSstableStoreRef,
    key_range: KeyRange,
    split_index: usize,
}

impl CompactorRunner {
    pub fn new(split_index: usize, context: &CompactorContext, task: CompactTask) -> Self {
        let max_target_file_size = context.context.options.sstable_size_mb as usize * (1 << 20);
        let total_file_size = task
            .input_ssts
            .iter()
            .flat_map(|level| level.table_infos.iter())
            .map(|table| table.file_size)
            .sum::<u64>();
        let mut options: SstableBuilderOptions = context.context.options.as_ref().into();
        options.capacity = std::cmp::min(task.target_file_size as usize, max_target_file_size);
        options.compression_algorithm = match task.compression_algorithm {
            0 => CompressionAlgorithm::None,
            1 => CompressionAlgorithm::Lz4,
            _ => CompressionAlgorithm::Zstd,
        };
        let total_file_size = (total_file_size as f64 * 1.2).round() as usize;
        if options.compression_algorithm == CompressionAlgorithm::None {
            options.capacity = std::cmp::min(options.capacity, total_file_size);
        }
        let key_range = KeyRange {
            left: Bytes::copy_from_slice(task.splits[split_index].get_left()),
            right: Bytes::copy_from_slice(task.splits[split_index].get_right()),
            right_exclusive: true,
        };
        let stats_target_table_ids = task
            .input_ssts
            .iter()
            .flat_map(|i| {
                i.table_infos
                    .iter()
                    .flat_map(|t| t.table_ids.clone())
                    .collect_vec()
            })
            .collect();
        let compactor = Compactor::new(
            context.context.clone(),
            options,
            key_range.clone(),
            CachePolicy::NotFill,
            task.gc_delete_keys,
            task.watermark,
            Some(stats_target_table_ids),
        );

        Self {
            compactor,
            compact_task: task,
            sstable_store: context.sstable_store.clone(),
            key_range,
            split_index,
        }
    }

    pub async fn run(
        &self,
        compaction_filter: impl CompactionFilter,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
        del_agg: Arc<RangeTombstonesCollector>,
        task_progress: Arc<TaskProgress>,
    ) -> HummockResult<CompactOutput> {
        let iter = self.build_sst_iter()?;
        let (ssts, table_stats_map) = self
            .compactor
            .compact_key_range(
                iter,
                compaction_filter,
                del_agg,
                filter_key_extractor,
                Some(task_progress),
            )
            .await?;
        Ok((self.split_index, ssts, table_stats_map))
    }

    pub async fn build_delete_range_iter<F: CompactionFilter>(
        compact_task: &CompactTask,
        sstable_store: &CompactorSstableStoreRef,
        filter: &mut F,
    ) -> HummockResult<Arc<RangeTombstonesCollector>> {
        let mut builder = DeleteRangeAggregatorBuilder::default();
        let mut local_stats = StoreLocalStatistic::default();
        for level in &compact_task.input_ssts {
            if level.table_infos.is_empty() {
                continue;
            }

            for table_info in &level.table_infos {
                let table = sstable_store.sstable(table_info, &mut local_stats).await?;
                let range_tombstone_list = table
                    .value()
                    .meta
                    .range_tombstone_list
                    .iter()
                    .filter(|tombstone| {
                        !filter.should_delete(FullKey::from_user_key(
                            tombstone.start_user_key.as_ref(),
                            tombstone.sequence,
                        ))
                    })
                    .cloned()
                    .collect_vec();
                builder.add_tombstone(range_tombstone_list);
            }
        }
        let aggregator = builder.build(compact_task.watermark, compact_task.gc_delete_keys);
        Ok(aggregator)
    }

    /// Build the merge iterator based on the given input ssts.
    fn build_sst_iter(&self) -> HummockResult<impl HummockIterator<Direction = Forward>> {
        let mut table_iters = Vec::new();

        for level in &self.compact_task.input_ssts {
            if level.table_infos.is_empty() {
                continue;
            }

            // Do not need to filter the table because manager has done it.
            if level.level_type == LevelType::Nonoverlapping as i32 {
                debug_assert!(can_concat(&level.table_infos));
                let tables = level
                    .table_infos
                    .iter()
                    .filter(|info| {
                        let key_range = KeyRange::from(info.key_range.as_ref().unwrap());
                        self.key_range.full_key_overlap(&key_range)
                    })
                    .cloned()
                    .collect_vec();
                table_iters.push(ConcatSstableIterator::new(
                    tables,
                    self.compactor.task_config.key_range.clone(),
                    self.sstable_store.clone(),
                ));
            } else {
                for table_info in &level.table_infos {
                    let key_range = KeyRange::from(table_info.key_range.as_ref().unwrap());
                    if !self.key_range.full_key_overlap(&key_range) {
                        continue;
                    }
                    table_iters.push(ConcatSstableIterator::new(
                        vec![table_info.clone()],
                        self.compactor.task_config.key_range.clone(),
                        self.sstable_store.clone(),
                    ));
                }
            }
        }
        Ok(UnorderedMergeIteratorInner::for_compactor(table_iters))
    }
}
#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::key::UserKey;
    use risingwave_pb::hummock::InputLevel;

    use super::*;
    use crate::hummock::compactor::StateCleanUpCompactionFilter;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_test_sstable_with_range_tombstone,
    };
    use crate::hummock::{CompactorSstableStore, DeleteRangeTombstone, MemoryLimiter};

    #[tokio::test]
    async fn test_delete_range_aggregator_with_filter() {
        let sstable_store = mock_sstable_store();
        let kv_pairs = vec![];
        let range_tombstones = vec![
            DeleteRangeTombstone::new(TableId::new(1), b"abc".to_vec(), b"cde".to_vec(), 1),
            DeleteRangeTombstone::new(TableId::new(2), b"abc".to_vec(), b"def".to_vec(), 1),
        ];
        let sstable_info = gen_test_sstable_with_range_tombstone(
            default_builder_opt_for_test(),
            1,
            kv_pairs.into_iter(),
            range_tombstones.clone(),
            sstable_store.clone(),
        )
        .await
        .get_sstable_info();
        let compact_store = Arc::new(CompactorSstableStore::new(
            sstable_store,
            MemoryLimiter::unlimit(),
        ));
        let compact_task = CompactTask {
            input_ssts: vec![InputLevel {
                level_idx: 0,
                level_type: 0,
                table_infos: vec![sstable_info],
            }],
            existing_table_ids: vec![2],
            ..Default::default()
        };
        let mut state_clean_up_filter = StateCleanUpCompactionFilter::new(HashSet::from_iter(
            compact_task.existing_table_ids.clone(),
        ));
        let collector = CompactorRunner::build_delete_range_iter(
            &compact_task,
            &compact_store,
            &mut state_clean_up_filter,
        )
        .await
        .unwrap();
        let ret = collector
            .get_tombstone_between(&UserKey::default().as_ref(), &UserKey::default().as_ref());
        assert_eq!(ret.len(), 1);
        assert_eq!(ret[0], range_tombstones[1]);
    }
}
