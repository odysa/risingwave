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

use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Row;
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Ident, SetVariableValue};

use super::RwPgResponse;
use crate::handler::HandlerArgs;

pub fn handle_set(
    handler_args: HandlerArgs,
    name: Ident,
    value: Vec<SetVariableValue>,
) -> Result<RwPgResponse> {
    let string_vals = value.into_iter().map(|v| v.to_string()).collect_vec();

    // Currently store the config variable simply as String -> ConfigEntry(String).
    // In future we can add converter/parser to make the API more robust.
    // We remark that the name of session parameter is always case-insensitive.
    handler_args
        .session
        .set_config(&name.real_value().to_lowercase(), string_vals)?;

    Ok(PgResponse::empty_result(StatementType::SET_OPTION))
}

pub(super) fn handle_show(handler_args: HandlerArgs, variable: Vec<Ident>) -> Result<RwPgResponse> {
    let config_reader = handler_args.session.config();
    // TODO: Verify that the name used in `show` command is indeed always case-insensitive.
    let name = variable.iter().map(|e| e.real_value()).join(" ");
    if name.eq_ignore_ascii_case("ALL") {
        return handle_show_all(handler_args.clone());
    }
    let row = Row::new(vec![Some(config_reader.get(&name)?.into())]);

    Ok(PgResponse::new_for_stream(
        StatementType::SHOW_COMMAND,
        Some(1),
        vec![row].into(),
        vec![PgFieldDescriptor::new(
            name.to_ascii_lowercase(),
            DataType::VARCHAR.to_oid(),
            DataType::VARCHAR.type_len(),
        )],
    ))
}

pub(super) fn handle_show_all(handler_args: HandlerArgs) -> Result<RwPgResponse> {
    let config_reader = handler_args.session.config();

    let all_variables = config_reader.get_all();

    let rows = all_variables
        .iter()
        .map(|info| {
            Row::new(vec![
                Some(info.name.clone().into()),
                Some(info.setting.clone().into()),
                Some(info.description.clone().into()),
            ])
        })
        .collect_vec();

    Ok(RwPgResponse::new_for_stream(
        StatementType::SHOW_COMMAND,
        Some(all_variables.len() as i32),
        rows.into(),
        vec![
            PgFieldDescriptor::new(
                "Name".to_string(),
                DataType::VARCHAR.to_oid(),
                DataType::VARCHAR.type_len(),
            ),
            PgFieldDescriptor::new(
                "Setting".to_string(),
                DataType::VARCHAR.to_oid(),
                DataType::VARCHAR.type_len(),
            ),
            PgFieldDescriptor::new(
                "Description".to_string(),
                DataType::VARCHAR.to_oid(),
                DataType::VARCHAR.type_len(),
            ),
        ],
    ))
}
