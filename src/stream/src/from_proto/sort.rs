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

use risingwave_storage::table::streaming_table::state_table::StateTable;

use super::*;
use crate::executor::SortExecutor;

pub struct SortExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for SortExecutorBuilder {
    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::Sort)?;
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let vnodes = Arc::new(params.vnode_bitmap.expect("vnodes not set for sort"));
        let state_table =
            StateTable::from_table_catalog(node.get_state_table()?, store, Some(vnodes)).await;
        Ok(Box::new(SortExecutor::new(
            params.actor_context,
            input,
            params.pk_indices,
            params.executor_id,
            state_table,
            params.env.config().developer.stream_chunk_size,
            node.sort_column_index as _,
        )))
    }
}
