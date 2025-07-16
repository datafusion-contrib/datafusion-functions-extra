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

#![deny(warnings)]

use log::debug;
use mode::mode_udaf;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::AggregateUDF;

#[macro_use]
pub mod macros;
pub mod common;
pub mod kurtosis;
pub mod kurtosis_pop;
pub mod max_min_by;
pub mod mode;
pub mod skewness;
pub mod expr_extra_fn {
    pub use super::kurtosis::kurtosis;
    pub use super::kurtosis_pop::kurtosis_pop;
    pub use super::max_min_by::max_by;
    pub use super::max_min_by::min_by;
    pub use super::mode::mode;
    pub use super::skewness::skewness;
}

pub fn all_extra_aggregate_functions() -> Vec<Arc<AggregateUDF>> {
    vec![
        mode_udaf(),
        max_min_by::max_by_udaf(),
        max_min_by::min_by_udaf(),
        kurtosis::kurtosis_udaf(),
        skewness::skewness_udaf(),
        kurtosis_pop::kurtosis_pop_udaf(),
    ]
}

/// Registers all enabled packages with a [`FunctionRegistry`]
pub fn register_all_extra_functions(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let functions: Vec<Arc<AggregateUDF>> = all_extra_aggregate_functions();

    functions.into_iter().try_for_each(|udf| {
        let existing_udaf = registry.register_udaf(udf)?;
        if let Some(existing_udaf) = existing_udaf {
            debug!("Overwrite existing UDAF: {}", existing_udaf.name());
        }
        Ok(()) as Result<()>
    })?;

    Ok(())
}
