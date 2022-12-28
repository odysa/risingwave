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

//! Define all [`Rule`]

use super::PlanRef;

/// A one-to-one transform for the [`PlanNode`](super::plan_node::PlanNode), every [`Rule`] should
/// downcast and check if the node matches the rule.
pub trait Rule: Send + Sync + Description {
    /// return err(()) if not match
    fn apply(&self, plan: PlanRef) -> Option<PlanRef>;
}

pub trait Description {
    fn description(&self) -> &str;
}

pub(super) type BoxedRule = Box<dyn Rule>;

mod project_join_merge_rule;
pub use project_join_merge_rule::*;
mod project_eliminate_rule;
pub use project_eliminate_rule::*;
mod project_merge_rule;
pub use project_merge_rule::*;
mod pull_up_correlated_predicate_rule;
pub use pull_up_correlated_predicate_rule::*;
mod index_delta_join_rule;
pub use index_delta_join_rule::*;
mod reorder_multijoin_rule;
pub use reorder_multijoin_rule::*;
mod apply_agg_transpose_rule;
pub use apply_agg_transpose_rule::*;
mod apply_filter_transpose_rule;
pub use apply_filter_transpose_rule::*;
mod apply_project_transpose_rule;
pub use apply_project_transpose_rule::*;
mod apply_scan_rule;
pub use apply_scan_rule::*;
mod translate_apply_rule;
pub use translate_apply_rule::*;
mod merge_multijoin_rule;
pub use merge_multijoin_rule::*;
mod max_one_row_eliminate_rule;
pub use max_one_row_eliminate_rule::*;
mod apply_join_transpose_rule;
pub use apply_join_transpose_rule::*;
mod apply_to_join_rule;
pub use apply_to_join_rule::*;
mod distinct_agg_rule;
pub use distinct_agg_rule::*;
mod index_selection_rule;
pub use index_selection_rule::*;
mod push_calculation_of_join_rule;
pub use push_calculation_of_join_rule::*;
mod join_commute_rule;
mod over_agg_to_topn_rule;
pub use join_commute_rule::*;
pub use over_agg_to_topn_rule::*;
mod union_to_distinct_rule;
pub use union_to_distinct_rule::*;
mod agg_project_merge_rule;
pub use agg_project_merge_rule::*;
mod union_merge_rule;
pub use union_merge_rule::*;
mod dag_to_tree_rule;
pub use dag_to_tree_rule::*;
mod apply_share_eliminate_rule;
pub use apply_share_eliminate_rule::*;

#[macro_export]
macro_rules! for_all_rules {
    ($macro:ident) => {
        $macro! {
             {ApplyAggTransposeRule}
            ,{ApplyFilterTransposeRule}
            ,{ApplyProjectTransposeRule}
            ,{ApplyScanRule}
            ,{ApplyJoinTransposeRule}
            ,{ApplyShareEliminateRule}
            ,{ApplyToJoinRule}
            ,{MaxOneRowEliminateRule}
            ,{DistinctAggRule}
            ,{IndexDeltaJoinRule}
            ,{MergeMultiJoinRule}
            ,{ProjectEliminateRule}
            ,{ProjectJoinMergeRule}
            ,{ProjectMergeRule}
            ,{PullUpCorrelatedPredicateRule}
            ,{ReorderMultiJoinRule}
            ,{TranslateApplyRule}
            ,{PushCalculationOfJoinRule}
            ,{IndexSelectionRule}
            ,{OverAggToTopNRule}
            ,{JoinCommuteRule}
            ,{UnionToDistinctRule}
            ,{AggProjectMergeRule}
            ,{UnionMergeRule}
            ,{DagToTreeRule}
        }
    };
}

macro_rules! impl_description {
    ($( { $name:ident }),*) => {
        paste::paste!{
            $(impl Description for [<$name>] {
                fn description(&self) -> &str {
                    stringify!([<$name>])
                }
            })*
        }
    }
}

for_all_rules! {impl_description}
