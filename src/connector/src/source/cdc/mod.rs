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

pub mod enumerator;
pub mod source;
pub mod split;

use std::collections::HashMap;

use anyhow::anyhow;
pub use enumerator::*;
use risingwave_pb::connector_service::{SourceType, TableSchema};
use serde::Deserialize;
pub use source::*;
pub use split::*;

pub const MYSQL_CDC_CONNECTOR: &str = "mysql-cdc";
pub const POSTGRES_CDC_CONNECTOR: &str = "postgres-cdc";

#[derive(Clone, Debug, Deserialize, Default)]
pub struct CdcProperties {
    /// Set by `ConnectorSource`
    pub connector_node_addr: String,
    /// Set by `SourceManager` when creating the source, used by `DebeziumSplitEnumerator`
    pub source_id: u32,
    /// Type of the cdc source, e.g. mysql, postgres
    pub source_type: String,
    /// Properties specified in the WITH clause by user
    pub props: HashMap<String, String>,

    /// Schema of the source specified by users
    pub table_schema: Option<TableSchema>,
}

impl CdcProperties {
    pub fn source_type_enum(&self) -> anyhow::Result<SourceType> {
        match self.source_type.as_str() {
            "mysql" => Ok(SourceType::Mysql),
            "postgres" => Ok(SourceType::Postgres),
            _ => Err(anyhow!("unknown source type")),
        }
    }
}
