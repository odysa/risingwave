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

use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;
use risingwave_pb::stream_plan::UnionNode;

use super::PlanRef;
use crate::optimizer::plan_node::stream::StreamPlanRef;
use crate::optimizer::plan_node::{LogicalUnion, PlanBase, PlanTreeNode, StreamNode};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamUnion` implements [`super::LogicalUnion`]
#[derive(Debug, Clone)]
pub struct StreamUnion {
    pub base: PlanBase,
    logical: LogicalUnion,
}

impl StreamUnion {
    pub fn new(logical: LogicalUnion) -> Self {
        let ctx = logical.base.ctx.clone();
        let pk_indices = logical.base.logical_pk.to_vec();
        let inputs = logical.inputs();
        let dist = inputs[0].distribution().clone();
        assert!(logical
            .inputs()
            .iter()
            .all(|input| *input.distribution() == dist));

        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            pk_indices,
            logical.functional_dependency().clone(),
            dist,
            logical.inputs().iter().all(|x| x.append_only()),
        );
        StreamUnion { base, logical }
    }
}

impl fmt::Display for StreamUnion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(f, "StreamUnion")
    }
}

impl PlanTreeNode for StreamUnion {
    fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
        let mut vec = smallvec::SmallVec::new();
        vec.extend(self.logical.inputs().into_iter());
        vec
    }

    fn clone_with_inputs(&self, inputs: &[crate::optimizer::PlanRef]) -> PlanRef {
        Self::new(LogicalUnion::new_with_source_col(
            self.logical.all(),
            inputs.to_owned(),
            self.logical.source_col(),
        ))
        .into()
    }
}

impl StreamNode for StreamUnion {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        ProstStreamNode::Union(UnionNode {})
    }
}
