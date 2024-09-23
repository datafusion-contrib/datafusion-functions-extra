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

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use datafusion::sql::parser::DFParser;
use datafusion_functions_extra::register_all_extra_functions;
use log::debug;

pub struct TestExecution {
    ctx: SessionContext,
}

impl TestExecution {
    pub async fn new() -> Result<Self> {
        let config = SessionConfig::new();
        let mut ctx = SessionContext::new_with_config(config);
        register_all_extra_functions(&mut ctx)?;
        Ok(Self { ctx })
    }

    pub async fn with_setup(self, sql: &str) -> Self {
        debug!("Running setup query: {sql}");
        let statements = DFParser::parse_sql(sql).expect("Error parsing setup query");
        for statement in statements {
            debug!("Running setup statement: {statement}");
            let statement_sql = statement.to_string();
            self.ctx
                .sql(&statement_sql)
                .await
                .expect("Error planning setup failed")
                .collect()
                .await
                .expect("Error executing setup query");
        }
        self
    }

    pub async fn run(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        debug!("Running query: {sql}");
        self.ctx.sql(sql).await?.collect().await
    }

    pub async fn run_and_format(&mut self, sql: &str) -> Vec<String> {
        let results = self.run(sql).await.expect("Error running query");
        format_results(&results)
    }
}

fn format_results(results: &[RecordBatch]) -> Vec<String> {
    let formatted = pretty_format_batches(results).unwrap().to_string();

    formatted.lines().map(|s| s.to_string()).collect()
}
