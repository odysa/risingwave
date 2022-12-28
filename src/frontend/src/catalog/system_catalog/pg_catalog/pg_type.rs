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

use std::sync::LazyLock;

use itertools::Itertools;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::SystemCatalogColumnsDef;

/// The catalog `pg_type` stores information about data types.
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-type.html`]
pub const PG_TYPE_TABLE_NAME: &str = "pg_type";
pub const PG_TYPE_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "oid"),
    (DataType::Varchar, "typname"),
    // 0
    (DataType::Int32, "typelem"),
    // false
    (DataType::Boolean, "typnotnull"),
    // 0
    (DataType::Int32, "typbasetype"),
    // -1
    (DataType::Int32, "typtypmod"),
    // 0
    (DataType::Int32, "typcollation"),
    // 0
    (DataType::Int32, "typlen"),
    // 0
    (DataType::Int32, "typnamespace"),
    // 'b'
    (DataType::Varchar, "typtype"),
    // 0
    (DataType::Int32, "typrelid"),
    // None
    (DataType::Varchar, "typdefault"),
    // None
    (DataType::Varchar, "typcategory"),
];

// TODO: uniform the default data with `TypeOid` under `pg_field_descriptor`.
pub const PG_TYPE_DATA: &[(i32, &str)] = &[
    (16, "bool"),
    (20, "int8"),
    (21, "int2"),
    (23, "int4"),
    (700, "float4"),
    (701, "float8"),
    (1043, "varchar"),
    (1082, "date"),
    (1083, "time"),
    (1114, "timestamp"),
    (1184, "timestamptz"),
    (1186, "interval"),
    (1700, "numeric"),
];

pub static PG_TYPE_DATA_ROWS: LazyLock<Vec<OwnedRow>> = LazyLock::new(|| {
    PG_TYPE_DATA
        .iter()
        .map(|(oid, name)| {
            OwnedRow::new(vec![
                Some(ScalarImpl::Int32(*oid)),
                Some(ScalarImpl::Utf8((*name).into())),
                Some(ScalarImpl::Int32(0)),
                Some(ScalarImpl::Bool(false)),
                Some(ScalarImpl::Int32(0)),
                Some(ScalarImpl::Int32(-1)),
                Some(ScalarImpl::Int32(0)),
                Some(ScalarImpl::Int32(0)),
                Some(ScalarImpl::Int32(0)),
                Some(ScalarImpl::Utf8("b".into())),
                Some(ScalarImpl::Int32(0)),
                None,
                None,
            ])
        })
        .collect_vec()
});
