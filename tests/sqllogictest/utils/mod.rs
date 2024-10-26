// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::execute_stream;
// use datafusion::sql::parser::DFParser;
use datafusion_functions_extra::register_all_extra_functions;
use log::{debug, info};
use sqllogictest::{DBOutput, TestError};
use sqlparser::parser::ParserError;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

/// DataFusion sql-logicaltest error
#[derive(Debug, Error)]
pub enum DFSqlLogicTestError {
    /// Error from sqllogictest-rs
    #[error("SqlLogicTest error(from sqllogictest-rs crate): {0}")]
    SqlLogicTest(#[from] TestError),
    /// Error from datafusion
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] DataFusionError),
    /// Error returned when SQL is syntactically incorrect.
    #[error("SQL Parser error: {0}")]
    Sql(#[from] ParserError),
    /// Error from arrow-rs
    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),
    /// Generic error
    #[error("Other Error: {0}")]
    Other(String),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DFColumnType {
    Boolean,
    DateTime,
    Integer,
    Float,
    Text,
    Timestamp,
    Another,
}

pub struct TestExecution {
    ctx: SessionContext,
    relative_path: PathBuf,
}

impl TestExecution {
    pub async fn new(mut ctx: SessionContext, relative_path: PathBuf) -> Self {
        register_all_extra_functions(&mut ctx).unwrap();
        Self { ctx, relative_path }
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for TestExecution {
    type Error = DFSqlLogicTestError;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<DFColumnType>> {
        info!("[{}] Running query: \"{}\"", self.relative_path.display(), sql);
        run_query(&self.ctx, sql).await
    }

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        "TestExecution"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

fn format_results(results: &[RecordBatch]) -> Vec<String> {
    let formatted = pretty_format_batches(results).unwrap().to_string();

    formatted.lines().map(|s| s.to_string()).collect()
}

pub async fn run_query(ctx: &SessionContext, sql: &str) -> Result<DBOutput<DFColumnType>> {
    debug!("Running query: {sql}");
    let df = ctx.sql(sql).await?;

    let task_ctx = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await?;

    // let stream = execute_stream(plan, task_ctx)?;
    // let types = normalize::convert_schema_to_types(stream.schema().fields());
    // let results: Vec<RecordBatch> = collect(stream).await?;
    // let rows = normalize::convert_batches(results)?;
    //
    // if rows.is_empty() && types.is_empty() {
    //     Ok(DBOutput::StatementComplete(0))
    // } else {
    //     Ok(DBOutput::Rows { types, rows })
    // }
    let stream = execute_stream(plan, task_ctx)?;
    let results: Vec<RecordBatch> = collect(stream).await?;

    if results.is_empty() {
        Ok(DBOutput::StatementComplete(0))
    } else {
        let formatted_results = pretty_format_batches(&results).unwrap().to_string();
        let rows: Vec<Vec<String>> = formatted_results
            .lines()
            .map(|line| line.split_whitespace().map(|s| s.to_string()).collect())
            .collect();

        let types = Vec::new();
        Ok(DBOutput::Rows { types, rows })
    }
}
