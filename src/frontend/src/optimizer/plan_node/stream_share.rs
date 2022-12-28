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

use itertools::Itertools;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;
use risingwave_pb::stream_plan::{
    DispatchStrategy, DispatcherType, ExchangeNode, StreamNode as ProstStreamPlan,
};

use super::{PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::optimizer::plan_node::{LogicalShare, PlanBase, PlanTreeNode};
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone)]
pub struct StreamShare {
    pub base: PlanBase,
    logical: LogicalShare,
}

impl StreamShare {
    pub fn new(logical: LogicalShare) -> Self {
        let ctx = logical.base.ctx.clone();
        let input = logical.input();
        let pk_indices = logical.base.logical_pk.to_vec();
        let dist = input.distribution().clone();
        // Filter executor won't change the append-only behavior of the stream.
        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            pk_indices,
            logical.functional_dependency().clone(),
            dist,
            logical.input().append_only(),
        );
        StreamShare { base, logical }
    }
}

impl fmt::Display for StreamShare {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(f, "StreamShare")
    }
}

impl PlanTreeNodeUnary for StreamShare {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { StreamShare }

impl StreamNode for StreamShare {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        unreachable!("stream scan cannot be converted into a prost body -- call `adhoc_to_stream_prost` instead.")
    }
}

impl StreamShare {
    pub fn adhoc_to_stream_prost(&self, state: &mut BuildFragmentGraphState) -> ProstStreamPlan {
        let operator_id = self.base.id.0 as u32;
        match state.get_share_stream_node(operator_id) {
            None => {
                let dispatch_strategy = match &self.base.dist {
                    Distribution::HashShard(keys) | Distribution::UpstreamHashShard(keys, _) => {
                        DispatchStrategy {
                            r#type: DispatcherType::Hash as i32,
                            column_indices: keys.iter().map(|x| *x as u32).collect_vec(),
                        }
                    }
                    Distribution::Single => DispatchStrategy {
                        r#type: DispatcherType::Simple as i32,
                        column_indices: vec![],
                    },
                    Distribution::Broadcast => DispatchStrategy {
                        r#type: DispatcherType::Broadcast as i32,
                        column_indices: vec![],
                    },
                    Distribution::SomeShard => {
                        // FIXME: use another DispatcherType?
                        DispatchStrategy {
                            r#type: DispatcherType::Hash as i32,
                            column_indices: self
                                .base
                                .logical_pk
                                .iter()
                                .map(|x| *x as u32)
                                .collect_vec(),
                        }
                    }
                };

                let node_body = Some(ProstStreamNode::Exchange(ExchangeNode {
                    strategy: Some(dispatch_strategy),
                }));
                let input = self
                    .inputs()
                    .into_iter()
                    .map(|plan| plan.to_stream_prost(state))
                    .collect();

                let stream_node = ProstStreamPlan {
                    input,
                    identity: format!("{}", self),
                    node_body,
                    operator_id: self.id().0 as _,
                    stream_key: self.logical_pk().iter().map(|x| *x as u32).collect(),
                    fields: self.schema().to_prost(),
                    append_only: self.append_only(),
                };

                state.add_share_stream_node(operator_id, stream_node.clone());
                stream_node
            }

            Some(stream_node) => stream_node.clone(),
        }
    }
}
