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

use std::sync::Arc;

use arrow::array::{Date64Array, Float64Array, Time64MicrosecondArray};
use arrow::datatypes::TimeUnit;
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::arrow::{array::StringArray, record_batch::RecordBatch};
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use datafusion_functions_extra::register_all_extra_functions;

// TODO: It would be great to release `datafusion-sqllogictest` as a crate.
// This would allow easy integration and testing of SQL queries across projects.
pub async fn create_context() -> Result<SessionContext> {
    let config = SessionConfig::new();
    let mut ctx = SessionContext::new_with_config(config);
    register_all_extra_functions(&mut ctx)?;
    Ok(ctx)
}

async fn create_test_table() -> Result<SessionContext> {
    let ctx = create_context().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("utf8_col", DataType::Utf8, true),
        Field::new("int64_col", DataType::Int64, true),
        Field::new("float64_col", DataType::Float64, true),
        Field::new("date64_col", DataType::Date64, true),
        Field::new("time64_col", DataType::Time64(TimeUnit::Microsecond), true),
    ]));

    let utf8_values: ArrayRef = Arc::new(StringArray::from(vec![
        Some("apple"),
        Some("banana"),
        Some("apple"),
        Some("orange"),
        Some("banana"),
        Some("apple"),
        None,
    ]));

    let int64_values: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(1),
        Some(2),
        Some(2),
        Some(3),
        Some(3),
        Some(3),
        None,
    ]));

    let float64_values: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(1.0),
        Some(2.0),
        Some(2.0),
        Some(3.0),
        Some(3.0),
        Some(3.0),
        None,
    ]));

    let date64_values: ArrayRef = Arc::new(Date64Array::from(vec![
        Some(1609459200000),
        Some(1609545600000),
        Some(1609545600000),
        Some(1609632000000),
        Some(1609632000000),
        Some(1609632000000),
        None,
    ]));

    let time64_values: ArrayRef = Arc::new(Time64MicrosecondArray::from(vec![
        Some(3600000000),
        Some(7200000000),
        Some(7200000000),
        Some(10800000000),
        Some(10800000000),
        Some(10800000000),
        None,
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![utf8_values, int64_values, float64_values, date64_values, time64_values],
    )?;

    ctx.register_batch("test_table", batch)?;

    Ok(ctx)
}

pub async fn run_query(sql: &str) -> Result<Vec<RecordBatch>> {
    let ctx = create_test_table().await?;
    ctx.sql(sql).await?.collect().await
}

pub async fn display_val(batch: Vec<RecordBatch>) -> (DataType, String) {
    assert_eq!(batch.len(), 1);
    let batch = batch.first().unwrap();
    assert_eq!(batch.num_rows(), 1);
    let schema = batch.schema();
    let schema_col = schema.field(0);
    let c = batch.column(0);
    let options = FormatOptions::default().with_display_error(true);
    let f = ArrayFormatter::try_new(c.as_ref(), &options).unwrap();
    let repr = f.value(0).try_to_string().unwrap();
    (schema_col.data_type().clone(), repr)
}
