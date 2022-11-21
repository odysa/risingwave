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

pub mod information_schema;
pub mod pg_catalog;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use paste::paste;
use risingwave_common::array::Row;
use risingwave_common::catalog::{
    ColumnDesc, SysCatalogReader, TableDesc, TableId, DEFAULT_SUPER_USER_ID,
    INFORMATION_SCHEMA_SCHEMA_NAME, PG_CATALOG_SCHEMA_NAME,
};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;

use crate::catalog::catalog_service::CatalogReader;
use crate::catalog::column_catalog::ColumnCatalog;
use crate::catalog::system_catalog::information_schema::*;
use crate::catalog::system_catalog::pg_catalog::*;
use crate::meta_client::FrontendMetaClient;
use crate::scheduler::worker_node_manager::WorkerNodeManagerRef;
use crate::session::AuthContext;
use crate::user::user_service::UserInfoReader;

#[derive(Clone, Debug, PartialEq)]
pub struct SystemCatalog {
    pub id: TableId,

    pub name: String,

    // All columns in this table.
    pub columns: Vec<ColumnCatalog>,

    /// Primary key columns indices.
    pub pk: Vec<usize>,

    // owner of table, should always be default super user, keep it for compatibility.
    pub owner: u32,
}

impl SystemCatalog {
    /// Get a reference to the system catalog's table id.
    pub fn id(&self) -> TableId {
        self.id
    }

    /// Get a reference to the system catalog's columns.
    pub fn columns(&self) -> &[ColumnCatalog] {
        &self.columns
    }

    /// Get a [`TableDesc`] of the system table.
    pub fn table_desc(&self) -> TableDesc {
        TableDesc {
            table_id: self.id,
            columns: self.columns.iter().map(|c| c.column_desc.clone()).collect(),
            stream_key: self.pk.clone(),
            ..Default::default()
        }
    }

    /// Get a reference to the system catalog's name.
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }
}

#[expect(dead_code)]
pub struct SysCatalogReaderImpl {
    // Read catalog info: database/schema/source/table.
    catalog_reader: CatalogReader,
    // Read user info.
    user_info_reader: UserInfoReader,
    // Read cluster info.
    worker_node_manager: WorkerNodeManagerRef,
    // Read from meta.
    meta_client: Arc<dyn FrontendMetaClient>,
    auth_context: Arc<AuthContext>,
}

impl SysCatalogReaderImpl {
    pub fn new(
        catalog_reader: CatalogReader,
        user_info_reader: UserInfoReader,
        worker_node_manager: WorkerNodeManagerRef,
        meta_client: Arc<dyn FrontendMetaClient>,
        auth_context: Arc<AuthContext>,
    ) -> Self {
        Self {
            catalog_reader,
            user_info_reader,
            worker_node_manager,
            meta_client,
            auth_context,
        }
    }
}

// TODO: support struct column and type name when necessary.
pub(super) type SystemCatalogColumnsDef<'a> = (DataType, &'a str);

/// `def_sys_catalog` defines a table with given id, name and columns.
macro_rules! def_sys_catalog {
    ($id:expr, $name:ident, $columns:expr, $pk:expr) => {
        SystemCatalog {
            id: TableId::new($id),
            name: $name.to_string(),
            columns: $columns
                .iter()
                .enumerate()
                .map(|(idx, col)| ColumnCatalog {
                    column_desc: ColumnDesc {
                        column_id: (idx as i32).into(),
                        data_type: col.0.clone(),
                        name: col.1.to_string(),
                        field_descs: vec![],
                        type_name: "".to_string(),
                    },
                    is_hidden: false,
                })
                .collect::<Vec<_>>(),
            pk: $pk, // change this when multi-column pk is needed in some system table.
            owner: DEFAULT_SUPER_USER_ID,
        }
    };
}

pub fn get_sys_catalogs_in_schema(schema_name: &str) -> Option<Vec<SystemCatalog>> {
    SYS_CATALOG_MAP.get(schema_name).map(Clone::clone)
}

macro_rules! prepare_sys_catalog {
    ($( { $schema_name:expr, $catalog_name:ident, $pk:expr, $func:tt $($await:tt)? } ),* $(,)?) => {
        /// `SYS_CATALOG_MAP` includes all system catalogs.
        pub(crate) static SYS_CATALOG_MAP: LazyLock<HashMap<&str, Vec<SystemCatalog>>> = LazyLock::new(|| {
            let mut hash_map: HashMap<&str, Vec<SystemCatalog>> = HashMap::new();
            $(
                paste!{
                    let sys_catalog = def_sys_catalog!(${index()} + 1, [<$catalog_name _TABLE_NAME>], [<$catalog_name _COLUMNS>], $pk);
                    hash_map.entry([<$schema_name _SCHEMA_NAME>]).or_insert(vec![]).push(sys_catalog);
                }
            )*
            hash_map
        });

        #[async_trait]
        impl SysCatalogReader for SysCatalogReaderImpl {
            async fn read_table(&self, table_id: &TableId) -> Result<Vec<Row>> {
                match table_id.table_id - 1 {
                    $(
                        ${index()} => {
                            let rows = self.$func();
                            $(let rows = rows.$await;)?
                            rows
                        },
                    )*
                    _ => unreachable!(),
                }
            }
        }
    };
}

// If you added a new system catalog, be sure to add a corresponding entry here.
prepare_sys_catalog! {
    { PG_CATALOG, PG_TYPE, vec![0], read_types },
    { PG_CATALOG, PG_NAMESPACE, vec![0], read_namespace },
    { PG_CATALOG, PG_CAST, vec![0], read_cast },
    { PG_CATALOG, PG_MATVIEWS_INFO, vec![0], read_mviews_info await },
    { PG_CATALOG, PG_USER, vec![0], read_user_info },
    { PG_CATALOG, PG_CLASS, vec![0], read_class_info },
    { PG_CATALOG, PG_INDEX, vec![0], read_index_info },
    { PG_CATALOG, PG_OPCLASS, vec![0], read_opclass_info },
    { PG_CATALOG, PG_COLLATION, vec![0], read_collation_info },
    { PG_CATALOG, PG_AM, vec![0], read_am_info },
    { PG_CATALOG, PG_OPERATOR, vec![0], read_operator_info },
    { PG_CATALOG, PG_VIEWS, vec![], read_views_info },
    { INFORMATION_SCHEMA, COLUMNS, vec![], read_columns_info },
    { INFORMATION_SCHEMA, TABLES, vec![], read_tables_info },
}
