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

use std::marker::PhantomData;
use std::mem::swap;

use itertools::Itertools;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId, TableOption};
use risingwave_common::error::{internal_error, Result};
use risingwave_common::hash::{HashKey, HashKeyDispatcher};
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::scan_range::ScanRange;
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::expr::expr_unary::new_unary_expr;
use risingwave_expr::expr::{build_from_prost, BoxedExpression, LiteralExpression};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::expr::expr_node::Type;
use risingwave_pb::plan_common::OrderType as ProstOrderType;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::Distribution;
use risingwave_storage::{dispatch_state_store, StateStore};

use crate::executor::join::JoinType;
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, BufferChunkExecutor, Executor,
    ExecutorBuilder, LookupExecutorBuilder, LookupJoinBase,
};
use crate::task::BatchTaskContext;

/// Distributed Lookup Join Executor.
/// High level Execution flow:
/// Repeat 1-3:
///   1. Read N rows from outer side input and send keys to inner side builder after deduplication.
///   2. Inner side input lookups inner side table with keys and builds hash map.
///   3. Outer side rows join each inner side rows by probing the hash map.
///
/// Distributed lookup join already scheduled to its inner side corresponding compute node, so that
/// it can just lookup the compute node locally without sending RPCs to other compute nodes.
pub struct DistributedLookupJoinExecutor<K> {
    base: LookupJoinBase<K>,
    _phantom: PhantomData<K>,
}

impl<K: HashKey> Executor for DistributedLookupJoinExecutor<K> {
    fn schema(&self) -> &Schema {
        &self.base.schema
    }

    fn identity(&self) -> &str {
        &self.base.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        Box::new(self.base).do_execute()
    }
}

impl<K> DistributedLookupJoinExecutor<K> {
    pub fn new(base: LookupJoinBase<K>) -> Self {
        Self {
            base,
            _phantom: PhantomData,
        }
    }
}

pub struct DistributedLookupJoinExecutorBuilder {}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for DistributedLookupJoinExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [outer_side_input]: [_; 1] = inputs.try_into().unwrap();

        let distributed_lookup_join_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::DistributedLookupJoin
        )?;

        let join_type = JoinType::from_prost(distributed_lookup_join_node.get_join_type()?);
        let condition = match distributed_lookup_join_node.get_condition() {
            Ok(cond_prost) => Some(build_from_prost(cond_prost)?),
            Err(_) => None,
        };

        let output_indices: Vec<usize> = distributed_lookup_join_node
            .get_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect();

        let outer_side_data_types = outer_side_input.schema().data_types();

        let table_desc = distributed_lookup_join_node.get_inner_side_table_desc()?;
        let inner_side_column_ids = distributed_lookup_join_node
            .get_inner_side_column_ids()
            .to_vec();

        let inner_side_schema = Schema {
            fields: inner_side_column_ids
                .iter()
                .map(|&id| {
                    let column = table_desc
                        .columns
                        .iter()
                        .find(|c| c.column_id == id)
                        .unwrap();
                    Field::from(&ColumnDesc::from(column))
                })
                .collect_vec(),
        };

        let fields = if join_type == JoinType::LeftSemi || join_type == JoinType::LeftAnti {
            outer_side_input.schema().fields.clone()
        } else {
            [
                outer_side_input.schema().fields.clone(),
                inner_side_schema.fields.clone(),
            ]
            .concat()
        };

        let original_schema = Schema { fields };
        let actual_schema = output_indices
            .iter()
            .map(|&idx| original_schema[idx].clone())
            .collect();

        let mut outer_side_key_idxs = vec![];
        for outer_side_key in distributed_lookup_join_node.get_outer_side_key() {
            outer_side_key_idxs.push(*outer_side_key as usize)
        }

        let outer_side_key_types: Vec<DataType> = outer_side_key_idxs
            .iter()
            .map(|&i| outer_side_data_types[i].clone())
            .collect_vec();

        let mut inner_side_key_idxs = vec![];
        for pk in &table_desc.pk {
            let key_idx = inner_side_column_ids
                .iter()
                .position(|&i| table_desc.columns[pk.index as usize].column_id == i)
                .ok_or_else(|| {
                    internal_error("Inner side key is not part of its output columns")
                })?;
            inner_side_key_idxs.push(key_idx);
        }

        let inner_side_key_types = inner_side_key_idxs
            .iter()
            .map(|&i| inner_side_schema.fields[i].data_type.clone())
            .collect_vec();

        let null_safe = distributed_lookup_join_node.get_null_safe().to_vec();

        let chunk_size = source.context.get_config().developer.batch_chunk_size;

        let table_id = TableId {
            table_id: table_desc.table_id,
        };
        let column_descs = table_desc
            .columns
            .iter()
            .map(ColumnDesc::from)
            .collect_vec();
        let column_ids = inner_side_column_ids
            .iter()
            .copied()
            .map(ColumnId::from)
            .collect();

        let order_types: Vec<OrderType> = table_desc
            .pk
            .iter()
            .map(|order| {
                OrderType::from_prost(&ProstOrderType::from_i32(order.order_type).unwrap())
            })
            .collect();

        let pk_indices = table_desc.pk.iter().map(|k| k.index as usize).collect_vec();

        let dist_key_indices = table_desc
            .dist_key_indices
            .iter()
            .map(|&k| k as usize)
            .collect_vec();
        // Lookup Join always contains distribution key, so we don't need vnode bitmap
        let distribution = Distribution::all_vnodes(dist_key_indices);
        let table_option = TableOption {
            retention_seconds: if table_desc.retention_seconds > 0 {
                Some(table_desc.retention_seconds)
            } else {
                None
            },
        };
        let value_indices = table_desc
            .get_value_indices()
            .iter()
            .map(|&k| k as usize)
            .collect_vec();

        dispatch_state_store!(source.context().state_store(), state_store, {
            let table = StorageTable::new_partial(
                state_store,
                table_id,
                column_descs,
                column_ids,
                order_types,
                pk_indices,
                distribution,
                table_option,
                value_indices,
            );

            let inner_side_builder = InnerSideExecutorBuilder::new(
                outer_side_key_types,
                inner_side_key_types.clone(),
                source.epoch(),
                vec![],
                table,
                chunk_size,
            );

            Ok(DistributedLookupJoinExecutorArgs {
                join_type,
                condition,
                outer_side_input,
                outer_side_data_types,
                outer_side_key_idxs,
                inner_side_builder: Box::new(inner_side_builder),
                inner_side_key_types,
                inner_side_key_idxs,
                null_safe,
                chunk_builder: DataChunkBuilder::new(original_schema.data_types(), chunk_size),
                schema: actual_schema,
                output_indices,
                chunk_size,
                identity: source.plan_node().get_identity().clone(),
            }
            .dispatch())
        })
    }
}

struct DistributedLookupJoinExecutorArgs {
    join_type: JoinType,
    condition: Option<BoxedExpression>,
    outer_side_input: BoxedExecutor,
    outer_side_data_types: Vec<DataType>,
    outer_side_key_idxs: Vec<usize>,
    inner_side_builder: Box<dyn LookupExecutorBuilder>,
    inner_side_key_types: Vec<DataType>,
    inner_side_key_idxs: Vec<usize>,
    null_safe: Vec<bool>,
    chunk_builder: DataChunkBuilder,
    schema: Schema,
    output_indices: Vec<usize>,
    chunk_size: usize,
    identity: String,
}

impl HashKeyDispatcher for DistributedLookupJoinExecutorArgs {
    type Output = BoxedExecutor;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        Box::new(DistributedLookupJoinExecutor::<K>::new(
            LookupJoinBase::<K> {
                join_type: self.join_type,
                condition: self.condition,
                outer_side_input: self.outer_side_input,
                outer_side_data_types: self.outer_side_data_types,
                outer_side_key_idxs: self.outer_side_key_idxs,
                inner_side_builder: self.inner_side_builder,
                inner_side_key_types: self.inner_side_key_types,
                inner_side_key_idxs: self.inner_side_key_idxs,
                null_safe: self.null_safe,
                chunk_builder: self.chunk_builder,
                schema: self.schema,
                output_indices: self.output_indices,
                chunk_size: self.chunk_size,
                identity: self.identity,
                _phantom: PhantomData,
            },
        ))
    }

    fn data_types(&self) -> &[DataType] {
        &self.inner_side_key_types
    }
}

/// Inner side executor builder for the `DistributedLookupJoinExecutor`
struct InnerSideExecutorBuilder<S: StateStore> {
    outer_side_key_types: Vec<DataType>,
    inner_side_key_types: Vec<DataType>,
    epoch: u64,
    row_list: Vec<Row>,
    table: StorageTable<S>,
    chunk_size: usize,
}

impl<S: StateStore> InnerSideExecutorBuilder<S> {
    fn new(
        outer_side_key_types: Vec<DataType>,
        inner_side_key_types: Vec<DataType>,
        epoch: u64,
        row_list: Vec<Row>,
        table: StorageTable<S>,
        chunk_size: usize,
    ) -> Self {
        Self {
            outer_side_key_types,
            inner_side_key_types,
            epoch,
            row_list,
            table,
            chunk_size,
        }
    }
}

#[async_trait::async_trait]
impl<S: StateStore> LookupExecutorBuilder for InnerSideExecutorBuilder<S> {
    fn reset(&mut self) {
        // PASS
    }

    /// Fetch row from inner side table by the scan range added.
    async fn add_scan_range(&mut self, key_datums: Vec<Datum>) -> Result<()> {
        let mut scan_range = ScanRange::full_table_scan();

        for ((datum, outer_type), inner_type) in key_datums
            .into_iter()
            .zip_eq(self.outer_side_key_types.iter())
            .zip_eq(self.inner_side_key_types.iter())
        {
            let datum = if inner_type == outer_type {
                datum
            } else {
                let cast_expr = new_unary_expr(
                    Type::Cast,
                    inner_type.clone(),
                    Box::new(LiteralExpression::new(outer_type.clone(), datum.clone())),
                )?;

                cast_expr.eval_row(Row::empty())?
            };

            scan_range.eq_conds.push(datum);
        }

        let pk_prefix = Row::new(scan_range.eq_conds);
        let row = self
            .table
            .get_row(&pk_prefix, HummockReadEpoch::Committed(self.epoch))
            .await?;

        if let Some(row) = row {
            self.row_list.push(row);
        }

        Ok(())
    }

    /// Build a `BufferChunkExecutor` to return all its rows fetched by `add_scan_range` before.
    async fn build_executor(&mut self) -> Result<BoxedExecutor> {
        let mut data_chunk_builder =
            DataChunkBuilder::new(self.table.schema().data_types(), self.chunk_size);
        let mut chunk_list = Vec::new();

        let mut new_row_list = vec![];
        swap(&mut new_row_list, &mut self.row_list);

        for row in new_row_list {
            if let Some(chunk) = data_chunk_builder.append_one_row_from_datums(row.values()) {
                chunk_list.push(chunk);
            }
        }
        if let Some(chunk) = data_chunk_builder.consume_all() {
            chunk_list.push(chunk);
        }

        Ok(Box::new(BufferChunkExecutor::new(
            self.table.schema().clone(),
            chunk_list,
        )))
    }
}
