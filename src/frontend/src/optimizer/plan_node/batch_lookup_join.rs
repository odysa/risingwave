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

use std::fmt;

use risingwave_common::catalog::{ColumnId, Schema, TableDesc};
use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{DistributedLookupJoinNode, LocalLookupJoinNode};

use crate::expr::Expr;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{
    EqJoinPredicate, EqJoinPredicateDisplay, LogicalJoin, PlanBase, PlanTreeNodeBinary,
    PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch, ToLocalBatch,
};
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::optimizer::PlanRef;

#[derive(Debug, Clone)]
pub struct BatchLookupJoin {
    pub base: PlanBase,
    logical: LogicalJoin,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    eq_join_predicate: EqJoinPredicate,

    /// Table description of the right side table
    right_table_desc: TableDesc,

    /// Output column ids of the right side table
    right_output_column_ids: Vec<ColumnId>,

    /// The prefix length of the order key of right side table.
    lookup_prefix_len: usize,

    /// If `distributed_lookup` is true, it will generate `DistributedLookupJoinNode` for
    /// `ToBatchProst`. Otherwise, it will generate `LookupJoinNode`.
    distributed_lookup: bool,
}

impl BatchLookupJoin {
    pub fn new(
        logical: LogicalJoin,
        eq_join_predicate: EqJoinPredicate,
        right_table_desc: TableDesc,
        right_output_column_ids: Vec<ColumnId>,
        lookup_prefix_len: usize,
        distributed_lookup: bool,
    ) -> Self {
        // We cannot create a `BatchLookupJoin` without any eq keys. We require eq keys to do the
        // lookup.
        assert!(eq_join_predicate.has_eq());
        let ctx = logical.base.ctx.clone();
        let dist = Self::derive_dist(logical.left().distribution(), &logical);
        let base = PlanBase::new_batch(ctx, logical.schema().clone(), dist, Order::any());
        Self {
            base,
            logical,
            eq_join_predicate,
            right_table_desc,
            right_output_column_ids,
            lookup_prefix_len,
            distributed_lookup,
        }
    }

    fn derive_dist(left: &Distribution, logical: &LogicalJoin) -> Distribution {
        match left {
            Distribution::Single => Distribution::Single,
            Distribution::HashShard(_) | Distribution::UpstreamHashShard(_, _) => {
                let l2o = logical
                    .l2i_col_mapping()
                    .composite(&logical.i2o_col_mapping());
                l2o.rewrite_provided_distribution(left)
            }
            _ => unreachable!(),
        }
    }

    fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }

    pub fn right_table_desc(&self) -> &TableDesc {
        &self.right_table_desc
    }

    fn clone_with_distributed_lookup(&self, input: PlanRef, distributed_lookup: bool) -> Self {
        let mut batch_lookup_join = self.clone_with_input(input);
        batch_lookup_join.distributed_lookup = distributed_lookup;
        batch_lookup_join
    }

    pub fn lookup_prefix_len(&self) -> usize {
        self.lookup_prefix_len
    }
}

impl fmt::Display for BatchLookupJoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let verbose = self.base.ctx.is_explain_verbose();
        let mut builder = f.debug_struct("BatchLookupJoin");
        builder.field("type", &self.logical.join_type());

        let mut concat_schema = self.logical.left().schema().fields.clone();
        concat_schema.extend(self.logical.right().schema().fields.clone());
        let concat_schema = Schema::new(concat_schema);
        builder.field(
            "predicate",
            &EqJoinPredicateDisplay {
                eq_join_predicate: self.eq_join_predicate(),
                input_schema: &concat_schema,
            },
        );

        if verbose {
            if self
                .logical
                .output_indices()
                .iter()
                .copied()
                .eq(0..self.logical.internal_column_num())
            {
                builder.field("output", &format_args!("all"));
            } else {
                builder.field(
                    "output",
                    &IndicesDisplay {
                        indices: self.logical.output_indices(),
                        input_schema: &concat_schema,
                    },
                );
            }
        }

        builder.finish()
    }
}

impl PlanTreeNodeUnary for BatchLookupJoin {
    fn input(&self) -> PlanRef {
        self.logical.left()
    }

    // Only change left side
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(
            self.logical
                .clone_with_left_right(input, self.logical.right()),
            self.eq_join_predicate.clone(),
            self.right_table_desc.clone(),
            self.right_output_column_ids.clone(),
            self.lookup_prefix_len,
            self.distributed_lookup,
        )
    }
}

impl_plan_tree_node_for_unary! { BatchLookupJoin }

impl ToDistributedBatch for BatchLookupJoin {
    fn to_distributed(&self) -> Result<PlanRef> {
        let input = self.input().to_distributed_with_required(
            &Order::any(),
            &RequiredDist::PhysicalDist(Distribution::UpstreamHashShard(
                self.eq_join_predicate.left_eq_indexes(),
                self.right_table_desc.table_id,
            )),
        )?;
        Ok(self.clone_with_distributed_lookup(input, true).into())
    }
}

impl ToBatchProst for BatchLookupJoin {
    fn to_batch_prost_body(&self) -> NodeBody {
        if self.distributed_lookup {
            NodeBody::DistributedLookupJoin(DistributedLookupJoinNode {
                join_type: self.logical.join_type() as i32,
                condition: self
                    .eq_join_predicate
                    .other_cond()
                    .as_expr_unless_true()
                    .map(|x| x.to_expr_proto()),
                outer_side_key: self
                    .eq_join_predicate
                    .left_eq_indexes()
                    .into_iter()
                    .map(|a| a as _)
                    .collect(),
                inner_side_key: self
                    .eq_join_predicate
                    .right_eq_indexes()
                    .into_iter()
                    .map(|a| a as _)
                    .collect(),
                inner_side_table_desc: Some(self.right_table_desc.to_protobuf()),
                inner_side_column_ids: self
                    .right_output_column_ids
                    .iter()
                    .map(ColumnId::get_id)
                    .collect(),
                output_indices: self
                    .logical
                    .output_indices()
                    .iter()
                    .map(|&x| x as u32)
                    .collect(),
                null_safe: self.eq_join_predicate.null_safes(),
                lookup_prefix_len: self.lookup_prefix_len as u32,
            })
        } else {
            NodeBody::LocalLookupJoin(LocalLookupJoinNode {
                join_type: self.logical.join_type() as i32,
                condition: self
                    .eq_join_predicate
                    .other_cond()
                    .as_expr_unless_true()
                    .map(|x| x.to_expr_proto()),
                outer_side_key: self
                    .eq_join_predicate
                    .left_eq_indexes()
                    .into_iter()
                    .map(|a| a as _)
                    .collect(),
                inner_side_key: self
                    .eq_join_predicate
                    .right_eq_indexes()
                    .into_iter()
                    .map(|a| a as _)
                    .collect(),
                inner_side_table_desc: Some(self.right_table_desc.to_protobuf()),
                inner_side_vnode_mapping: vec![], // To be filled in at local.rs
                inner_side_column_ids: self
                    .right_output_column_ids
                    .iter()
                    .map(ColumnId::get_id)
                    .collect(),
                output_indices: self
                    .logical
                    .output_indices()
                    .iter()
                    .map(|&x| x as u32)
                    .collect(),
                worker_nodes: vec![], // To be filled in at local.rs
                null_safe: self.eq_join_predicate.null_safes(),
                lookup_prefix_len: self.lookup_prefix_len as u32,
            })
        }
    }
}

impl ToLocalBatch for BatchLookupJoin {
    fn to_local(&self) -> Result<PlanRef> {
        let input = RequiredDist::single()
            .enforce_if_not_satisfies(self.input().to_local()?, &Order::any())?;

        Ok(self.clone_with_distributed_lookup(input, false).into())
    }
}
