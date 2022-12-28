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

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::error::Result;

use super::generic::{self, GenericPlanNode, Project};
use super::{
    gen_filter_and_pushdown, BatchProject, ColPrunable, PlanBase, PlanRef, PlanTreeNodeUnary,
    PredicatePushdown, StreamProject, ToBatch, ToStream,
};
use crate::expr::{ExprImpl, ExprRewriter, ExprVisitor, InputRef};
use crate::optimizer::plan_node::{
    CollectInputRef, ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext,
    ToStreamContext,
};
use crate::optimizer::property::{Distribution, FunctionalDependencySet, Order, RequiredDist};
use crate::utils::{ColIndexMapping, Condition, Substitute};

/// `LogicalProject` computes a set of expressions from its input relation.
#[derive(Debug, Clone)]
pub struct LogicalProject {
    pub base: PlanBase,
    core: generic::Project<PlanRef>,
}

impl LogicalProject {
    pub fn create(input: PlanRef, exprs: Vec<ExprImpl>) -> PlanRef {
        Self::new(input, exprs).into()
    }

    pub fn new(input: PlanRef, exprs: Vec<ExprImpl>) -> Self {
        let core = generic::Project::new(exprs, input.clone());
        Self::with_core(core)
    }

    pub fn with_core(core: generic::Project<PlanRef>) -> Self {
        let ctx = core.input.ctx();

        let schema = core.schema();
        let pk_indices = core.logical_pk();
        let functional_dependency = Self::derive_fd(&core, core.input.functional_dependency());

        let base = PlanBase::new_logical(
            ctx,
            schema,
            pk_indices.unwrap_or_default(),
            functional_dependency,
        );
        LogicalProject { base, core }
    }

    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        self.core.o2i_col_mapping()
    }

    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        self.core.i2o_col_mapping()
    }

    /// Creates a `LogicalProject` which select some columns from the input.
    ///
    /// `mapping` should maps from `(0..input_fields.len())` to a consecutive range starting from 0.
    ///
    /// This is useful in column pruning when we want to add a project to ensure the output schema
    /// is correct.
    pub fn with_mapping(input: PlanRef, mapping: ColIndexMapping) -> Self {
        Self::with_core(generic::Project::with_mapping(input, mapping))
    }

    /// Creates a `LogicalProject` which select some columns from the input.
    pub fn with_out_fields(input: PlanRef, out_fields: &FixedBitSet) -> Self {
        Self::with_core(generic::Project::with_out_fields(input, out_fields))
    }

    /// Creates a `LogicalProject` which select some columns from the input.
    pub fn with_out_col_idx(input: PlanRef, out_fields: impl Iterator<Item = usize>) -> Self {
        Self::with_core(generic::Project::with_out_col_idx(input, out_fields))
    }

    fn derive_fd(
        core: &Project<PlanRef>,
        input_fd_set: &FunctionalDependencySet,
    ) -> FunctionalDependencySet {
        let i2o = core.i2o_col_mapping();
        let mut fd_set = FunctionalDependencySet::new(core.exprs.len());
        for fd in input_fd_set.as_dependencies() {
            if let Some(fd) = i2o.rewrite_functional_dependency(fd) {
                fd_set.add_functional_dependency(fd);
            }
        }
        fd_set
    }

    pub fn exprs(&self) -> &Vec<ExprImpl> {
        &self.core.exprs
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        self.core.fmt_with_name(f, name)
    }

    pub fn is_identity(&self) -> bool {
        self.core.is_identity()
    }

    pub fn try_as_projection(&self) -> Option<Vec<usize>> {
        self.core.try_as_projection()
    }

    pub fn decompose(self) -> (Vec<ExprImpl>, PlanRef) {
        self.core.decompose()
    }

    pub fn is_all_inputref(&self) -> bool {
        self.core.is_all_inputref()
    }
}

impl PlanTreeNodeUnary for LogicalProject {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.exprs().clone())
    }

    fn rewrite_with_input(
        &self,
        input: PlanRef,
        mut input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let exprs = self
            .exprs()
            .clone()
            .into_iter()
            .map(|expr| input_col_change.rewrite_expr(expr))
            .collect();
        let proj = Self::new(input, exprs);
        // change the input columns index will not change the output column index
        let out_col_change = ColIndexMapping::identity(self.schema().len());
        (proj, out_col_change)
    }
}

impl_plan_tree_node_for_unary! {LogicalProject}

impl fmt::Display for LogicalProject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with_name(f, "LogicalProject")
    }
}

impl ColPrunable for LogicalProject {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let input_col_num = self.input().schema().len();
        let mut input_required_appeared = FixedBitSet::with_capacity(input_col_num);

        // Record each InputRef's index.
        let mut input_ref_collector = CollectInputRef::with_capacity(input_col_num);
        required_cols.iter().for_each(|i| {
            if let ExprImpl::InputRef(ref input_ref) = self.exprs()[*i] {
                let input_idx = input_ref.index;
                input_required_appeared.put(input_idx);
            } else {
                input_ref_collector.visit_expr(&self.exprs()[*i]);
            }
        });
        let input_required_cols = {
            let mut tmp = FixedBitSet::from(input_ref_collector);
            tmp.union_with(&input_required_appeared);
            tmp
        };

        let input_required_cols = input_required_cols.ones().collect_vec();
        let new_input = self.input().prune_col(&input_required_cols, ctx);
        let mut mapping = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input().schema().len(),
        );
        // Rewrite each InputRef with new index.
        let exprs = required_cols
            .iter()
            .map(|&id| mapping.rewrite_expr(self.exprs()[id].clone()))
            .collect();

        // Reconstruct the LogicalProject.
        LogicalProject::new(new_input, exprs).into()
    }
}

impl PredicatePushdown for LogicalProject {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // convert the predicate to one that references the child of the project
        let mut subst = Substitute {
            mapping: self.exprs().clone(),
        };
        let predicate = predicate.rewrite_expr(&mut subst);

        gen_filter_and_pushdown(self, Condition::true_cond(), predicate, ctx)
    }
}

impl ToBatch for LogicalProject {
    fn to_batch(&self) -> Result<PlanRef> {
        self.to_batch_with_order_required(&Order::any())
    }

    fn to_batch_with_order_required(&self, required_order: &Order) -> Result<PlanRef> {
        let input_order = self
            .o2i_col_mapping()
            .rewrite_provided_order(required_order);
        let new_input = self.input().to_batch_with_order_required(&input_order)?;
        let new_logical = self.clone_with_input(new_input.clone());
        let batch_project = if let Some(input_proj) = new_input.as_batch_project() {
            let outer_project = new_logical;
            let inner_project = input_proj.as_logical();
            let mut subst = Substitute {
                mapping: inner_project.exprs().clone(),
            };
            let exprs = outer_project
                .exprs()
                .iter()
                .cloned()
                .map(|expr| subst.rewrite_expr(expr))
                .collect();
            BatchProject::new(LogicalProject::new(inner_project.input(), exprs))
        } else {
            BatchProject::new(new_logical)
        };
        required_order.enforce_if_not_satisfies(batch_project.into())
    }
}

impl ToStream for LogicalProject {
    fn to_stream_with_dist_required(
        &self,
        required_dist: &RequiredDist,
        ctx: &mut ToStreamContext,
    ) -> Result<PlanRef> {
        let input_required = if required_dist.satisfies(&RequiredDist::AnyShard) {
            RequiredDist::Any
        } else {
            let input_required = self
                .o2i_col_mapping()
                .rewrite_required_distribution(required_dist);
            match input_required {
                RequiredDist::PhysicalDist(dist) => match dist {
                    Distribution::Single => RequiredDist::Any,
                    _ => RequiredDist::PhysicalDist(dist),
                },
                _ => input_required,
            }
        };
        let new_input = self
            .input()
            .to_stream_with_dist_required(&input_required, ctx)?;
        let new_logical = self.clone_with_input(new_input.clone());
        let stream_plan = if let Some(input_proj) = new_input.as_stream_project() {
            let outer_project = new_logical;
            let inner_project = input_proj.as_logical();
            let mut subst = Substitute {
                mapping: inner_project.exprs().clone(),
            };
            let exprs = outer_project
                .exprs()
                .iter()
                .cloned()
                .map(|expr| subst.rewrite_expr(expr))
                .collect();
            StreamProject::new(LogicalProject::new(inner_project.input(), exprs))
        } else {
            StreamProject::new(new_logical)
        };
        required_dist.enforce_if_not_satisfies(stream_plan.into(), &Order::any())
    }

    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        self.to_stream_with_dist_required(&RequiredDist::Any, ctx)
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input().logical_rewrite_for_stream(ctx)?;
        let (proj, out_col_change) = self.rewrite_with_input(input.clone(), input_col_change);

        // Add missing columns of input_pk into the select list.
        let input_pk = input.logical_pk();
        let i2o = proj.i2o_col_mapping();
        let col_need_to_add = input_pk
            .iter()
            .cloned()
            .filter(|i| i2o.try_map(*i).is_none());
        let input_schema = input.schema();
        let exprs =
            proj.exprs()
                .iter()
                .cloned()
                .chain(col_need_to_add.map(|idx| {
                    InputRef::new(idx, input_schema.fields[idx].data_type.clone()).into()
                }))
                .collect();
        let proj = Self::new(input, exprs);
        // The added columns is at the end, so it will not change existing column indices.
        // But the target size of `out_col_change` should be the same as the length of the new
        // schema.
        let (map, _) = out_col_change.into_parts();
        let out_col_change = ColIndexMapping::with_target_size(map, proj.base.schema.len());
        Ok((proj.into(), out_col_change))
    }
}
#[cfg(test)]
mod tests {

    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_pb::expr::expr_node::Type;

    use super::*;
    use crate::expr::{assert_eq_input_ref, FunctionCall, InputRef, Literal};
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::LogicalValues;

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Project(1, input_ref(2), input_ref(0)<5)
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns `[1, 2]` will result in
    /// ```text
    /// Project(input_ref(1), input_ref(0)<5)
    ///   TableScan(v1, v3)
    /// ```
    async fn test_prune_project() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty.clone(), "v3"),
        ];
        let values = LogicalValues::new(
            vec![],
            Schema {
                fields: fields.clone(),
            },
            ctx,
        );
        let project: PlanRef = LogicalProject::new(
            values.into(),
            vec![
                ExprImpl::Literal(Box::new(Literal::new(None, ty.clone()))),
                InputRef::new(2, ty.clone()).into(),
                ExprImpl::FunctionCall(Box::new(
                    FunctionCall::new(
                        Type::LessThan,
                        vec![
                            ExprImpl::InputRef(Box::new(InputRef::new(0, ty.clone()))),
                            ExprImpl::Literal(Box::new(Literal::new(None, ty))),
                        ],
                    )
                    .unwrap(),
                )),
            ],
        )
        .into();

        // Perform the prune
        let required_cols = vec![1, 2];
        let plan = project.prune_col(
            &required_cols,
            &mut ColumnPruningContext::new(project.clone()),
        );

        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 2);
        assert_eq_input_ref!(&project.exprs()[0], 1);

        let expr = project.exprs()[1].clone();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);

        let values = project.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields().len(), 2);
        assert_eq!(values.schema().fields()[0], fields[0]);
        assert_eq!(values.schema().fields()[1], fields[2]);
    }
}
