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

use std::time::Duration;

/// A RisingWave client.
pub struct RisingWave {
    client: tokio_postgres::Client,
    task: tokio::task::JoinHandle<()>,
    host: String,
    dbname: String,
}

impl RisingWave {
    pub async fn connect(
        host: String,
        dbname: String,
    ) -> Result<Self, tokio_postgres::error::Error> {
        let (client, connection) = tokio_postgres::Config::new()
            .host(&host)
            .port(4566)
            .dbname(&dbname)
            .user("root")
            .connect_timeout(Duration::from_secs(5))
            .connect(tokio_postgres::NoTls)
            .await?;
        let task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("postgres connection error: {e}");
            }
        });
        // for recovery
        client
            .simple_query("SET RW_IMPLICIT_FLUSH TO true;")
            .await?;
        client
            .simple_query("SET CREATE_COMPACTION_GROUP_FOR_MV TO true;")
            .await?;
        Ok(RisingWave {
            client,
            task,
            host,
            dbname,
        })
    }

    /// Returns a reference of the inner Postgres client.
    pub fn pg_client(&self) -> &tokio_postgres::Client {
        &self.client
    }
}

impl Drop for RisingWave {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for RisingWave {
    type Error = tokio_postgres::error::Error;

    async fn run(&mut self, sql: &str) -> Result<sqllogictest::DBOutput, Self::Error> {
        use sqllogictest::{ColumnType, DBOutput};

        if self.client.is_closed() {
            // connection error, reset the client
            *self = Self::connect(self.host.clone(), self.dbname.clone()).await?;
        }

        let mut output = vec![];

        let is_query_sql = {
            let lower_sql = sql.trim_start().to_ascii_lowercase();
            lower_sql.starts_with("select")
                || lower_sql.starts_with("values")
                || lower_sql.starts_with("show")
                || lower_sql.starts_with("with")
                || lower_sql.starts_with("describe")
        };

        let rows = self.client.simple_query(sql).await?;
        for row in rows {
            let mut row_vec = vec![];

            match row {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    for i in 0..row.len() {
                        match row.get(i) {
                            Some(v) if v.is_empty() => row_vec.push("(empty)".to_string()),
                            Some(v) => row_vec.push(v.to_string()),
                            None => row_vec.push("NULL".to_string()),
                        }
                    }
                }
                tokio_postgres::SimpleQueryMessage::CommandComplete(cnt) => {
                    if is_query_sql {
                        break;
                    } else {
                        return Ok(DBOutput::StatementComplete(cnt));
                    }
                }
                _ => unreachable!(),
            }
            output.push(row_vec);
        }

        if output.is_empty() {
            let stmt = self.client.prepare(sql).await?;
            Ok(DBOutput::Rows {
                types: vec![ColumnType::Any; stmt.columns().len()],
                rows: vec![],
            })
        } else {
            Ok(DBOutput::Rows {
                types: vec![ColumnType::Any; output[0].len()],
                rows: output,
            })
        }
    }

    fn engine_name(&self) -> &str {
        "risingwave"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }
}
