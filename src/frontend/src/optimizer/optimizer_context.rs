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

use core::convert::Into;
use core::fmt::Formatter;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use risingwave_sqlparser::ast::ExplainOptions;

use crate::expr::CorrelatedId;
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::PlanNodeId;
use crate::session::SessionImpl;
use crate::WithOptions;

pub struct OptimizerContext {
    session_ctx: Arc<SessionImpl>,
    /// Store plan node id
    next_plan_node_id: RefCell<i32>,
    /// For debugging purposes, store the SQL string in Context
    sql: Arc<str>,
    /// Explain options
    explain_options: ExplainOptions,
    /// Store the trace of optimizer
    optimizer_trace: RefCell<Vec<String>>,
    /// Store correlated id
    next_correlated_id: RefCell<u32>,
    /// Store options or properties from the `with` clause
    with_options: WithOptions,
}
pub type OptimizerContextRef = Rc<OptimizerContext>;

impl OptimizerContext {
    pub fn new_with_handler_args(handler_args: HandlerArgs) -> Self {
        Self::new(
            handler_args.session,
            handler_args.sql,
            handler_args.with_options,
            ExplainOptions::default(),
        )
    }

    pub fn new(
        session_ctx: Arc<SessionImpl>,
        sql: Arc<str>,
        with_options: WithOptions,
        explain_options: ExplainOptions,
    ) -> Self {
        Self {
            session_ctx,
            next_plan_node_id: RefCell::new(0),
            sql,
            explain_options,
            optimizer_trace: RefCell::new(vec![]),
            next_correlated_id: RefCell::new(0),
            with_options,
        }
    }

    // TODO(TaoWu): Remove the async.
    #[cfg(test)]
    #[expect(clippy::unused_async)]
    pub async fn mock() -> OptimizerContextRef {
        Self {
            session_ctx: Arc::new(SessionImpl::mock()),
            next_plan_node_id: RefCell::new(0),
            sql: Arc::from(""),
            explain_options: ExplainOptions::default(),
            optimizer_trace: RefCell::new(vec![]),
            next_correlated_id: RefCell::new(0),
            with_options: Default::default(),
        }
        .into()
    }

    pub fn next_plan_node_id(&self) -> PlanNodeId {
        *self.next_plan_node_id.borrow_mut() += 1;
        PlanNodeId(*self.next_plan_node_id.borrow())
    }

    pub fn next_correlated_id(&self) -> CorrelatedId {
        *self.next_correlated_id.borrow_mut() += 1;
        *self.next_correlated_id.borrow()
    }

    pub fn is_explain_verbose(&self) -> bool {
        self.explain_options.verbose
    }

    pub fn is_explain_trace(&self) -> bool {
        self.explain_options.trace
    }

    pub fn trace(&self, str: impl Into<String>) {
        let mut optimizer_trace = self.optimizer_trace.borrow_mut();
        optimizer_trace.push(str.into());
        optimizer_trace.push("\n".to_string());
    }

    pub fn take_trace(&self) -> Vec<String> {
        self.optimizer_trace.borrow_mut().drain(..).collect()
    }

    pub fn with_options(&self) -> &WithOptions {
        &self.with_options
    }

    pub fn session_ctx(&self) -> &Arc<SessionImpl> {
        &self.session_ctx
    }
}

impl std::fmt::Debug for OptimizerContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QueryContext {{ next_plan_node_id = {}, sql = {}, explain_options = {}, next_correlated_id = {}, with_options = {:?} }}",
            self.next_plan_node_id.borrow(),
            self.sql,
            self.explain_options,
            self.next_correlated_id.borrow(),
            &self.with_options
        )
    }
}
