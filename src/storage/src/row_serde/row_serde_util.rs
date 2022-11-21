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

use risingwave_common::array::Row;
use risingwave_common::error::Result;
use risingwave_common::types::{VirtualNode, VIRTUAL_NODE_SIZE};
use risingwave_common::util::ordered::OrderedRowSerde;

pub fn serialize_pk(pk: &Row, serializer: &OrderedRowSerde) -> Vec<u8> {
    let mut result = vec![];
    serializer.serialize(pk, &mut result);
    result
}

pub fn serialize_pk_with_vnode(
    pk: &Row,
    serializer: &OrderedRowSerde,
    vnode: VirtualNode,
) -> Vec<u8> {
    let mut result = vnode.to_be_bytes().to_vec();
    serializer.serialize(pk, &mut result);
    result
}

// NOTE: Only for debug purpose now
pub fn deserialize_pk_with_vnode(
    key: &[u8],
    deserializer: &OrderedRowSerde,
) -> Result<(VirtualNode, Row)> {
    let vnode = VirtualNode::from_be_bytes(key[0..VIRTUAL_NODE_SIZE].try_into().unwrap());
    let pk = deserializer.deserialize(&key[VIRTUAL_NODE_SIZE..])?;
    Ok((vnode, pk))
}

pub fn parse_raw_key_to_vnode_and_key(raw_key: &[u8]) -> (VirtualNode, &[u8]) {
    let (vnode_bytes, key_bytes) = raw_key.split_at(VIRTUAL_NODE_SIZE);
    let vnode = VirtualNode::from_be_bytes(vnode_bytes.try_into().unwrap());
    (vnode, key_bytes)
}
