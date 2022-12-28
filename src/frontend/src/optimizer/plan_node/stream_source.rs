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
use risingwave_pb::catalog::ColumnIndex;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;
use risingwave_pb::stream_plan::{SourceNode, StreamSource as ProstStreamSource};

use super::{LogicalSource, PlanBase, StreamNode};
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// [`StreamSource`] represents a table/connector source at the very beginning of the graph.
#[derive(Debug, Clone)]
pub struct StreamSource {
    pub base: PlanBase,
    logical: LogicalSource,
}

impl StreamSource {
    pub fn new(logical: LogicalSource) -> Self {
        let base = PlanBase::new_stream(
            logical.ctx(),
            logical.schema().clone(),
            logical.logical_pk().to_vec(),
            logical.functional_dependency().clone(),
            Distribution::SomeShard,
            logical
                .core
                .catalog
                .as_ref()
                .map_or(true, |s| s.append_only),
        );
        Self { base, logical }
    }

    pub fn column_names(&self) -> Vec<String> {
        self.schema()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect()
    }
}

impl_plan_tree_node_for_leaf! { StreamSource }

impl fmt::Display for StreamSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("StreamSource");
        if let Some(catalog) = self.logical.source_catalog() {
            builder
                .field("source", &catalog.name)
                .field("columns", &self.column_names());
        }
        builder.finish()
    }
}

impl StreamNode for StreamSource {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        let source_catalog = self.logical.source_catalog();
        let source_inner = source_catalog.map(|source_catalog| ProstStreamSource {
            source_id: source_catalog.id,
            source_name: source_catalog.name.clone(),
            state_table: Some(
                self.logical
                    .infer_internal_table_catalog()
                    .with_id(state.gen_table_id_wrapped())
                    .to_internal_table_prost(),
            ),
            info: Some(source_catalog.info.clone()),
            row_id_index: source_catalog
                .row_id_index
                .map(|index| ColumnIndex { index: index as _ }),
            columns: source_catalog
                .columns
                .iter()
                .map(|c| c.to_protobuf())
                .collect_vec(),
            pk_column_ids: source_catalog
                .pk_col_ids
                .iter()
                .map(Into::into)
                .collect_vec(),
            properties: source_catalog.properties.clone(),
        });
        ProstStreamNode::Source(SourceNode { source_inner })
    }
}
