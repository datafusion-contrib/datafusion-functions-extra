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

use arrow::datatypes::{DataType, TimeUnit};
use utils::{display_val, run_query};

mod utils;

#[tokio::test]
async fn test_mode_utf8() {
    let sql = "SELECT MODE(utf8_col) FROM test_table";
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Utf8, "apple".to_string()));
}

#[tokio::test]
async fn test_mode_int64() {
    let sql = "SELECT MODE(int64_col) FROM test_table";
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Int64, "3".to_string()));
}

#[tokio::test]
async fn test_mode_float64() {
    let sql = "SELECT MODE(float64_col) FROM test_table";
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Float64, "3.0".to_string()));
}

#[tokio::test]
async fn test_mode_date64() {
    let sql = "SELECT MODE(date64_col) FROM test_table";
    let batches = run_query(sql).await.unwrap();
    assert_eq!(
        display_val(batches).await,
        (DataType::Date64, "2021-01-03T00:00:00".to_string())
    );
}

#[tokio::test]
async fn test_mode_time64() {
    let sql = "SELECT MODE(time64_col) FROM test_table";
    let batches = run_query(sql).await.unwrap();
    assert_eq!(
        display_val(batches).await,
        (DataType::Time64(TimeUnit::Microsecond), "03:00:00".to_string())
    );
}
