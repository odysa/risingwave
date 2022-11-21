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

use futures::{pin_mut, StreamExt};
use risingwave_common::array::Row;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::ordered::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_connector::source::DataType;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use crate::common::iter_state_table;
use crate::executor::error::StreamExecutorResult;
use crate::executor::top_n::{serialize_pk_to_cache_key, CacheKey, TopNCache};

/// * For TopN, the storage key is: `[ order_by + remaining columns of pk ]`
/// * For group TopN, the storage key is: `[ group_key + order_by + remaining columns of pk ]`
///
/// The key in [`TopNCache`] is [`CacheKey`], which is `[ order_by|remaining columns of pk ]`, and
/// `group_key` is not included.
pub struct ManagedTopNState<S: StateStore> {
    /// Relational table.
    pub(crate) state_table: StateTable<S>,

    /// Used for serializing pk into CacheKey.
    cache_key_serde: (OrderedRowSerde, OrderedRowSerde),
}

#[derive(Clone, PartialEq, Debug)]
pub struct TopNStateRow {
    // (order_key|input_pk)
    pub cache_key: CacheKey,
    pub row: Row,
}

impl TopNStateRow {
    pub fn new(cache_key: CacheKey, row: Row) -> Self {
        Self { cache_key, row }
    }
}

impl<S: StateStore> ManagedTopNState<S> {
    pub fn new(
        state_table: StateTable<S>,
        pk_data_types: &[DataType],
        pk_order_types: &[OrderType],
        order_by_len: usize,
    ) -> Self {
        let (first_key_data_types, second_key_data_types) = pk_data_types.split_at(order_by_len);
        let (first_key_order_types, second_key_order_types) = pk_order_types.split_at(order_by_len);
        let first_key_serde = OrderedRowSerde::new(
            first_key_data_types.to_vec(),
            first_key_order_types.to_vec(),
        );
        let second_key_serde = OrderedRowSerde::new(
            second_key_data_types.to_vec(),
            second_key_order_types.to_vec(),
        );
        let cache_key_serde = (first_key_serde, second_key_serde);
        Self {
            state_table,
            cache_key_serde,
        }
    }

    pub fn insert(&mut self, value: Row) {
        self.state_table.insert(value);
    }

    pub fn delete(&mut self, value: Row) {
        self.state_table.delete(value);
    }

    fn get_topn_row(&self, row: Row, group_key_len: usize, order_by_len: usize) -> TopNStateRow {
        let datums = self
            .state_table
            .pk_indices()
            .iter()
            .skip(group_key_len)
            .map(|pk_index| row[*pk_index].clone())
            .collect();
        let pk = Row::new(datums);
        let cache_key = serialize_pk_to_cache_key(pk, order_by_len, &self.cache_key_serde);

        TopNStateRow::new(cache_key, row)
    }

    /// This function will return the rows in the range of [`offset`, `offset` + `limit`).
    ///
    /// If `group_key` is None, it will scan rows from the very beginning.
    /// Otherwise it will scan rows with prefix `group_key`.
    #[cfg(test)]
    pub async fn find_range(
        &self,
        group_key: Option<&Row>,
        offset: usize,
        limit: Option<usize>,
        order_by_len: usize,
    ) -> StreamExecutorResult<Vec<TopNStateRow>> {
        let state_table_iter = iter_state_table(&self.state_table, group_key).await?;
        pin_mut!(state_table_iter);

        // here we don't expect users to have large OFFSET.
        let (mut rows, mut stream) = if let Some(limit) = limit {
            (
                Vec::with_capacity(limit.min(1024)),
                state_table_iter.skip(offset).take(limit),
            )
        } else {
            (
                Vec::with_capacity(1024),
                state_table_iter.skip(offset).take(1024),
            )
        };
        while let Some(item) = stream.next().await {
            rows.push(self.get_topn_row(
                item?.into_owned(),
                group_key.map(|p| p.size()).unwrap_or(0),
                order_by_len,
            ));
        }
        Ok(rows)
    }

    /// # Arguments
    ///
    /// * `group_key` - Used as the prefix of the key to scan. Only for group TopN.
    /// * `start_key` - The start point of the key to scan. It should be the last key of the middle
    ///   cache. It doesn't contain the group key.
    pub async fn fill_high_cache<const WITH_TIES: bool>(
        &self,
        group_key: Option<&Row>,
        topn_cache: &mut TopNCache<WITH_TIES>,
        start_key: CacheKey,
        cache_size_limit: usize,
        order_by_len: usize,
    ) -> StreamExecutorResult<()> {
        let cache = &mut topn_cache.high;
        let state_table_iter = iter_state_table(&self.state_table, group_key).await?;
        pin_mut!(state_table_iter);
        while let Some(item) = state_table_iter.next().await {
            // Note(bugen): should first compare with start key before constructing TopNStateRow.
            let topn_row = self.get_topn_row(
                item?.into_owned(),
                group_key.map(|p| p.size()).unwrap_or(0),
                order_by_len,
            );
            if topn_row.cache_key <= start_key {
                continue;
            }
            // let row= &topn_row.row;
            cache.insert(topn_row.cache_key, (&topn_row.row).into());
            if cache.len() == cache_size_limit {
                break;
            }
        }
        if WITH_TIES && topn_cache.is_high_cache_full() {
            let high_last_sort_key = topn_cache.high.last_key_value().unwrap().0 .0.clone();
            while let Some(item) = state_table_iter.next().await {
                let topn_row = self.get_topn_row(
                    item?.into_owned(),
                    group_key.map(|p| p.size()).unwrap_or(0),
                    order_by_len,
                );
                if topn_row.cache_key.0 == high_last_sort_key {
                    topn_cache
                        .high
                        .insert(topn_row.cache_key, (&topn_row.row).into());
                } else {
                    break;
                }
            }
        }

        Ok(())
    }

    pub async fn init_topn_cache<const WITH_TIES: bool>(
        &self,
        group_key: Option<&Row>,
        topn_cache: &mut TopNCache<WITH_TIES>,
        order_by_len: usize,
    ) -> StreamExecutorResult<()> {
        assert!(topn_cache.low.is_empty());
        assert!(topn_cache.middle.is_empty());
        assert!(topn_cache.high.is_empty());

        let state_table_iter = iter_state_table(&self.state_table, group_key).await?;
        pin_mut!(state_table_iter);
        if topn_cache.offset > 0 {
            while let Some(item) = state_table_iter.next().await {
                let topn_row = self.get_topn_row(
                    item?.into_owned(),
                    group_key.map(|p| p.size()).unwrap_or(0),
                    order_by_len,
                );
                topn_cache
                    .low
                    .insert(topn_row.cache_key, (&topn_row.row).into());
                if topn_cache.low.len() == topn_cache.offset {
                    break;
                }
            }
        }

        assert!(topn_cache.limit > 0, "topn cache limit should always > 0");
        while let Some(item) = state_table_iter.next().await {
            let topn_row = self.get_topn_row(
                item?.into_owned(),
                group_key.map(|p| p.size()).unwrap_or(0),
                order_by_len,
            );
            topn_cache
                .middle
                .insert(topn_row.cache_key, (&topn_row.row).into());
            if topn_cache.middle.len() == topn_cache.limit {
                break;
            }
        }
        if WITH_TIES && topn_cache.is_middle_cache_full() {
            let middle_last_sort_key = topn_cache.middle.last_key_value().unwrap().0 .0.clone();
            while let Some(item) = state_table_iter.next().await {
                let topn_row = self.get_topn_row(
                    item?.into_owned(),
                    group_key.map(|p| p.size()).unwrap_or(0),
                    order_by_len,
                );
                if topn_row.cache_key.0 == middle_last_sort_key {
                    topn_cache
                        .middle
                        .insert(topn_row.cache_key, (&topn_row.row).into());
                } else {
                    topn_cache
                        .high
                        .insert(topn_row.cache_key, (&topn_row.row).into());
                    break;
                }
            }
        }

        assert!(
            topn_cache.high_capacity > 0,
            "topn cache high_capacity should always > 0"
        );
        while !topn_cache.is_high_cache_full() && let Some(item) = state_table_iter.next().await {
            let topn_row = self.get_topn_row(item?.into_owned(), group_key.map(|p|p.size()).unwrap_or(0), order_by_len);
            topn_cache.high.insert(topn_row.cache_key, (&topn_row.row).into());
        }
        if WITH_TIES && topn_cache.is_high_cache_full() {
            let high_last_sort_key = topn_cache.high.last_key_value().unwrap().0 .0.clone();
            while let Some(item) = state_table_iter.next().await {
                let topn_row = self.get_topn_row(
                    item?.into_owned(),
                    group_key.map(|p| p.size()).unwrap_or(0),
                    order_by_len,
                );
                if topn_row.cache_key.0 == high_last_sort_key {
                    topn_cache
                        .high
                        .insert(topn_row.cache_key, (&topn_row.row).into());
                } else {
                    break;
                }
            }
        }

        Ok(())
    }

    pub async fn flush(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_table.commit(epoch).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::DataType;
    use risingwave_common::util::ordered::OrderedRowSerde;
    use risingwave_common::util::sort_util::OrderType;

    // use std::collections::BTreeMap;
    use super::*;
    use crate::executor::test_utils::top_n_executor::create_in_memory_state_table;
    use crate::row_nonnull;

    pub fn serialize_row_to_cache_key(
        pk: Row,
        order_by_len: usize,
        cache_key_serde: &(OrderedRowSerde, OrderedRowSerde),
    ) -> CacheKey {
        let (cache_key_first, cache_key_second) = pk.0.split_at(order_by_len);
        let mut cache_key_first_bytes = vec![];
        let mut cache_key_second_bytes = vec![];
        cache_key_serde.0.serialize(
            &Row::new(cache_key_first.to_vec()),
            &mut cache_key_first_bytes,
        );
        cache_key_serde.1.serialize(
            &Row::new(cache_key_second.to_vec()),
            &mut cache_key_second_bytes,
        );
        (cache_key_first_bytes, cache_key_second_bytes)
    }

    #[tokio::test]
    async fn test_managed_top_n_state() {
        let data_types = vec![DataType::Varchar, DataType::Int64];
        let order_types = vec![OrderType::Ascending, OrderType::Ascending];
        let state_table = {
            let mut tb = create_in_memory_state_table(
                &[DataType::Varchar, DataType::Int64],
                &[OrderType::Ascending, OrderType::Ascending],
                &[0, 1],
            )
            .await;
            tb.init_epoch(EpochPair::new_test_epoch(1));
            tb
        };

        let (first_key_data_types, second_key_data_types) = data_types.split_at(1);
        let (first_key_order_types, second_key_order_types) = order_types.split_at(1);
        let first_key_serde = OrderedRowSerde::new(
            first_key_data_types.to_vec(),
            first_key_order_types.to_vec(),
        );
        let second_key_serde = OrderedRowSerde::new(
            second_key_data_types.to_vec(),
            second_key_order_types.to_vec(),
        );
        let cache_key_serde = (first_key_serde, second_key_serde);
        let mut managed_state = ManagedTopNState::new(state_table, &data_types, &order_types, 1);

        let row1 = row_nonnull!["abc".to_string(), 2i64];
        let row2 = row_nonnull!["abc".to_string(), 3i64];
        let row3 = row_nonnull!["abd".to_string(), 3i64];
        let row4 = row_nonnull!["ab".to_string(), 4i64];

        let row1_bytes = serialize_row_to_cache_key(row1.clone(), 1, &cache_key_serde);
        let row2_bytes = serialize_row_to_cache_key(row2.clone(), 1, &cache_key_serde);
        let row3_bytes = serialize_row_to_cache_key(row3.clone(), 1, &cache_key_serde);
        let row4_bytes = serialize_row_to_cache_key(row4.clone(), 1, &cache_key_serde);
        let rows = vec![row1, row2, row3, row4];
        let ordered_rows = vec![row1_bytes, row2_bytes, row3_bytes, row4_bytes];

        managed_state.insert(rows[3].clone());

        // now ("ab", 4)
        let valid_rows = managed_state.find_range(None, 0, Some(1), 1).await.unwrap();

        assert_eq!(valid_rows.len(), 1);
        assert_eq!(valid_rows[0].cache_key, ordered_rows[3].clone());

        managed_state.insert(rows[2].clone());
        let valid_rows = managed_state.find_range(None, 1, Some(1), 1).await.unwrap();
        assert_eq!(valid_rows.len(), 1);
        assert_eq!(valid_rows[0].cache_key, ordered_rows[2].clone());

        managed_state.insert(rows[1].clone());

        let valid_rows = managed_state.find_range(None, 1, Some(2), 1).await.unwrap();
        assert_eq!(valid_rows.len(), 2);
        assert_eq!(
            valid_rows.first().unwrap().cache_key,
            ordered_rows[1].clone()
        );
        assert_eq!(
            valid_rows.last().unwrap().cache_key,
            ordered_rows[2].clone()
        );

        // delete ("abc", 3)
        managed_state.delete(rows[1].clone());

        // insert ("abc", 2)
        managed_state.insert(rows[0].clone());

        let valid_rows = managed_state.find_range(None, 0, Some(3), 1).await.unwrap();

        assert_eq!(valid_rows.len(), 3);
        assert_eq!(valid_rows[0].cache_key, ordered_rows[3].clone());
        assert_eq!(valid_rows[1].cache_key, ordered_rows[0].clone());
        assert_eq!(valid_rows[2].cache_key, ordered_rows[2].clone());
    }

    #[tokio::test]
    async fn test_managed_top_n_state_fill_cache() {
        let data_types = vec![DataType::Varchar, DataType::Int64];
        let order_types = vec![OrderType::Ascending, OrderType::Ascending];
        let state_table = {
            let mut tb = create_in_memory_state_table(
                &[DataType::Varchar, DataType::Int64],
                &[OrderType::Ascending, OrderType::Ascending],
                &[0, 1],
            )
            .await;
            tb.init_epoch(EpochPair::new_test_epoch(1));
            tb
        };

        let (first_key_data_types, second_key_data_types) = data_types.split_at(1);
        let (first_key_order_types, second_key_order_types) = order_types.split_at(1);
        let first_key_serde = OrderedRowSerde::new(
            first_key_data_types.to_vec(),
            first_key_order_types.to_vec(),
        );
        let second_key_serde = OrderedRowSerde::new(
            second_key_data_types.to_vec(),
            second_key_order_types.to_vec(),
        );

        let cache_key_serde = (first_key_serde, second_key_serde);
        let mut managed_state = ManagedTopNState::new(state_table, &data_types, &order_types, 1);

        let row1 = row_nonnull!["abc".to_string(), 2i64];
        let row2 = row_nonnull!["abc".to_string(), 3i64];
        let row3 = row_nonnull!["abd".to_string(), 3i64];
        let row4 = row_nonnull!["ab".to_string(), 4i64];
        let row5 = row_nonnull!["abcd".to_string(), 5i64];

        let row1_bytes = serialize_row_to_cache_key(row1.clone(), 1, &cache_key_serde);
        let row2_bytes = serialize_row_to_cache_key(row2.clone(), 1, &cache_key_serde);
        let row3_bytes = serialize_row_to_cache_key(row3.clone(), 1, &cache_key_serde);
        let row4_bytes = serialize_row_to_cache_key(row4.clone(), 1, &cache_key_serde);
        let row5_bytes = serialize_row_to_cache_key(row5.clone(), 1, &cache_key_serde);
        let rows = vec![row1, row2, row3, row4, row5];
        let ordered_rows = vec![row1_bytes, row2_bytes, row3_bytes, row4_bytes, row5_bytes];

        let mut cache = TopNCache::<false>::new(1, 1, 1);

        managed_state.insert(rows[3].clone());
        managed_state.insert(rows[1].clone());
        managed_state.insert(rows[2].clone());
        managed_state.insert(rows[4].clone());

        managed_state
            .fill_high_cache(None, &mut cache, ordered_rows[3].clone(), 2, 1)
            .await
            .unwrap();
        assert_eq!(cache.high.len(), 2);
        assert_eq!(cache.high.first_key_value().unwrap().0, &ordered_rows[1]);
        assert_eq!(cache.high.last_key_value().unwrap().0, &ordered_rows[4]);
    }
}
