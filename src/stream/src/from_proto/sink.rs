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

use risingwave_common::catalog::Field;
use risingwave_connector::sink::SinkConfig;
use risingwave_pb::stream_plan::SinkNode;

use super::*;
use crate::executor::{SinkExecutor, StreamExecutorError};

pub struct SinkExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for SinkExecutorBuilder {
    type Node = SinkNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let [materialize_executor]: [_; 1] = params.input.try_into().unwrap();

        let mut properties = node.get_properties().clone();
        let pk_indices = node
            .sink_pk
            .iter()
            .map(|idx| *idx as usize)
            .collect::<Vec<_>>();
        let schema = node.fields.iter().map(Field::from).collect();
        // This field can be used to distinguish a specific actor in parallelism to prevent
        // transaction execution errors
        properties.insert(
            "identifier".to_string(),
            format!("sink-{:?}", params.executor_id),
        );
        let config = SinkConfig::from_hashmap(properties).map_err(StreamExecutorError::from)?;

        Ok(Box::new(SinkExecutor::new(
            materialize_executor,
            stream.streaming_metrics.clone(),
            config,
            params.executor_id,
            params.env.connector_params(),
            schema,
            pk_indices,
        )))
    }
}
