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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::ErrorCode::PermissionDenied;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_sqlparser::ast::ObjectName;

use super::privilege::check_super_user;
use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::TableType;
use crate::handler::HandlerArgs;

pub async fn handle_drop_table(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    if_exists: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, table_name) = Binder::resolve_schema_qualified_name(db_name, table_name)?;
    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let (source_id, table_id) = {
        let reader = session.env().catalog_reader().read_guard();
        let (table, schema_name) = match reader.get_table_by_name(db_name, schema_path, &table_name)
        {
            Ok((t, s)) => (t, s),
            Err(e) => {
                return if if_exists {
                    Ok(RwPgResponse::empty_result_with_notice(
                        StatementType::DROP_TABLE,
                        format!("table \"{}\" does not exist, skipping", table_name),
                    ))
                } else {
                    Err(e.into())
                }
            }
        };

        let schema_catalog = reader
            .get_schema_by_name(session.database(), schema_name)
            .unwrap();
        let schema_owner = schema_catalog.owner();
        if session.user_id() != table.owner
            && session.user_id() != schema_owner
            && !check_super_user(&session)
        {
            return Err(PermissionDenied("Do not have the privilege".to_string()).into());
        }

        match table.table_type() {
            TableType::MaterializedView => {
                return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                    "Use `DROP MATERIALIZED VIEW` to drop a materialized view.".to_owned(),
                )));
            }
            TableType::Index => {
                return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                    "Use `DROP INDEX` to drop an index.".to_owned(),
                )));
            }
            TableType::Internal => {
                return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                    "Internal tables cannot be dropped.".to_owned(),
                )));
            }
            TableType::Table => {
                // TODO(Yuanxin): Remove this after unsupporting `CREATE MATERIALIZED SOURCE`.
                if table.associated_source_id().is_some() {
                    return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                        "Use `DROP SOURCE` to drop a source.".to_owned(),
                    )));
                }
            }
        }

        (table.associated_source_id(), table.id())
    };

    let catalog_writer = session.env().catalog_writer();
    catalog_writer
        .drop_table(source_id.map(|id| id.table_id), table_id)
        .await?;

    Ok(PgResponse::empty_result(StatementType::DROP_TABLE))
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_drop_table_handler() {
        let sql_create_table = "create table t (v1 smallint);";
        let sql_drop_table = "drop table t;";
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql_create_table).await.unwrap();
        frontend.run_sql(sql_drop_table).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        let source = catalog_reader.get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t");
        assert!(source.is_err());

        let table = catalog_reader.get_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "t");
        assert!(table.is_err());
    }
}
