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
use std::time::Instant;

use futures::StreamExt;
use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use postgres_types::FromSql;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::session_config::QueryMode;
use risingwave_sqlparser::ast::Statement;

use super::{PgResponseStream, RwPgResponse};
use crate::binder::{Binder, BoundSetExpr, BoundStatement};
use crate::handler::privilege::{check_privileges, resolve_privileges};
use crate::handler::util::{to_pg_field, DataChunkToRowSetAdapter};
use crate::handler::HandlerArgs;
use crate::optimizer::{OptimizerContext, OptimizerContextRef};
use crate::planner::Planner;
use crate::scheduler::plan_fragmenter::Query;
use crate::scheduler::{
    BatchPlanFragmenter, DistributedQueryStream, ExecutionContext, ExecutionContextRef,
    LocalQueryExecution, LocalQueryStream, PinnedHummockSnapshot,
};
use crate::session::SessionImpl;
use crate::PlanRef;

pub fn gen_batch_query_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    stmt: Statement,
) -> Result<(PlanRef, QueryMode, Schema)> {
    let stmt_type = to_statement_type(&stmt)?;

    let bound = {
        let mut binder = Binder::new(session);
        binder.bind(stmt)?
    };

    let check_items = resolve_privileges(&bound);
    check_privileges(session, &check_items)?;

    let mut planner = Planner::new(context);

    let mut must_local = false;
    if let BoundStatement::Query(query) = &bound {
        if let BoundSetExpr::Select(select) = &query.body
            && let Some(relation) = &select.from
            && relation.contains_sys_table() {
                must_local = true;
        }
    }
    let must_dist = stmt_type.is_dml();

    let query_mode = match (must_dist, must_local) {
        (true, true) => {
            return Err(ErrorCode::InternalError(
                "the query is forced to both local and distributed mode by optimizer".to_owned(),
            )
            .into())
        }
        (true, false) => QueryMode::Distributed,
        (false, true) => QueryMode::Local,
        (false, false) => session.config().get_query_mode(),
    };

    let mut logical = planner.plan(bound)?;
    let schema = logical.schema().clone();

    let physical = match query_mode {
        QueryMode::Local => logical.gen_batch_local_plan()?,
        QueryMode::Distributed => logical.gen_batch_distributed_plan()?,
    };
    Ok((physical, query_mode, schema))
}

pub async fn handle_query(
    handler_args: HandlerArgs,
    stmt: Statement,
    format: bool,
) -> Result<RwPgResponse> {
    let stmt_type = to_statement_type(&stmt)?;
    let session = handler_args.session.clone();
    let query_start_time = Instant::now();

    // Subblock to make sure PlanRef (an Rc) is dropped before `await` below.
    let (query, query_mode, output_schema) = {
        let context = OptimizerContext::new_with_handler_args(handler_args);
        let (plan, query_mode, schema) = gen_batch_query_plan(&session, context.into(), stmt)?;

        tracing::trace!(
            "Generated query plan: {:?}, query_mode:{:?}",
            plan.explain_to_string()?,
            query_mode
        );
        let plan_fragmenter = BatchPlanFragmenter::new(
            session.env().worker_node_manager_ref(),
            session.env().catalog_reader().clone(),
        );
        (plan_fragmenter.split(plan)?, query_mode, schema)
    };
    tracing::trace!("Generated query after plan fragmenter: {:?}", &query);

    let pg_descs = output_schema
        .fields()
        .iter()
        .map(to_pg_field)
        .collect::<Vec<PgFieldDescriptor>>();
    let column_types = output_schema
        .fields()
        .iter()
        .map(|f| f.data_type())
        .collect_vec();

    let mut row_stream = {
        let query_epoch = session.config().get_query_epoch();
        let query_snapshot = if let Some(query_epoch) = query_epoch {
            PinnedHummockSnapshot::Other(query_epoch)
        } else {
            // Acquire hummock snapshot for execution.
            // TODO: if there's no table scan, we don't need to acquire snapshot.
            let hummock_snapshot_manager = session.env().hummock_snapshot_manager();
            let query_id = query.query_id().clone();
            let pinned_snapshot = hummock_snapshot_manager.acquire(&query_id).await?;
            PinnedHummockSnapshot::FrontendPinned(pinned_snapshot)
        };
        match query_mode {
            QueryMode::Local => PgResponseStream::LocalQuery(DataChunkToRowSetAdapter::new(
                local_execute(session.clone(), query, query_snapshot).await?,
                column_types,
                format,
            )),
            // Local mode do not support cancel tasks.
            QueryMode::Distributed => {
                PgResponseStream::DistributedQuery(DataChunkToRowSetAdapter::new(
                    distribute_execute(session.clone(), query, query_snapshot).await?,
                    column_types,
                    format,
                ))
            }
        }
    };

    let rows_count = match stmt_type {
        StatementType::SELECT => None,
        StatementType::INSERT | StatementType::DELETE | StatementType::UPDATE => {
            // Get the row from the row_stream.
            let first_row_set = row_stream
                .next()
                .await
                .expect("compute node should return affected rows in output")
                .map_err(|err| RwError::from(ErrorCode::InternalError(format!("{}", err))))?;
            let affected_rows_str = first_row_set[0].values()[0]
                .as_ref()
                .expect("compute node should return affected rows in output");
            if format {
                Some(
                    i64::from_sql(&postgres_types::Type::INT8, affected_rows_str)
                        .unwrap()
                        .try_into()
                        .expect("affected rows count large than i32"),
                )
            } else {
                Some(
                    String::from_utf8(affected_rows_str.to_vec())
                        .unwrap()
                        .parse()
                        .unwrap_or_default(),
                )
            }
        }
        _ => unreachable!(),
    };

    // Implicitly flush the writes.
    if session.config().get_implicit_flush() {
        flush_for_write(&session, stmt_type).await?;
    }

    // update some metrics
    if query_mode == QueryMode::Local {
        session
            .env()
            .frontend_metrics
            .latency_local_execution
            .observe(query_start_time.elapsed().as_secs_f64());

        session
            .env()
            .frontend_metrics
            .query_counter_local_execution
            .inc();
    }

    Ok(PgResponse::new_for_stream(
        stmt_type, rows_count, row_stream, pg_descs,
    ))
}

fn to_statement_type(stmt: &Statement) -> Result<StatementType> {
    use StatementType::*;

    match stmt {
        Statement::Query(_) => Ok(SELECT),
        Statement::Insert { .. } => Ok(INSERT),
        Statement::Delete { .. } => Ok(DELETE),
        Statement::Update { .. } => Ok(UPDATE),
        _ => Err(RwError::from(ErrorCode::InvalidInputSyntax(
            "unsupported statement type".to_string(),
        ))),
    }
}

pub async fn distribute_execute(
    session: Arc<SessionImpl>,
    query: Query,
    pinned_snapshot: PinnedHummockSnapshot,
) -> Result<DistributedQueryStream> {
    let execution_context: ExecutionContextRef = ExecutionContext::new(session.clone()).into();
    let query_manager = session.env().query_manager().clone();
    query_manager
        .schedule(execution_context, query, pinned_snapshot)
        .await
        .map_err(|err| err.into())
}

#[expect(clippy::unused_async)]
pub async fn local_execute(
    session: Arc<SessionImpl>,
    query: Query,
    pinned_snapshot: PinnedHummockSnapshot,
) -> Result<LocalQueryStream> {
    let front_env = session.env();

    // TODO: Passing sql here
    let execution = LocalQueryExecution::new(
        query,
        front_env.clone(),
        "",
        pinned_snapshot,
        session.auth_context(),
    );

    Ok(execution.stream_rows())
}

pub async fn flush_for_write(session: &SessionImpl, stmt_type: StatementType) -> Result<()> {
    match stmt_type {
        StatementType::INSERT | StatementType::DELETE | StatementType::UPDATE => {
            let client = session.env().meta_client();
            let snapshot = client.flush(true).await?;
            session
                .env()
                .hummock_snapshot_manager()
                .update_epoch(snapshot);
        }
        _ => {}
    }
    Ok(())
}
