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

use arrow::util::bench_util::create_primitive_array;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion::{
    arrow::{
        self,
        array::ArrayRef,
        datatypes::{DataType, Int32Type},
    },
    logical_expr::Accumulator,
};
use datafusion_functions_extra::common::mode::PrimitiveModeAccumulator;

fn prepare_mode_accumulator() -> Box<dyn Accumulator> {
    Box::new(PrimitiveModeAccumulator::<Int32Type>::new(&DataType::Int32))
}

fn mode_bench(c: &mut Criterion, name: &str, values: ArrayRef) {
    let mut accumulator = prepare_mode_accumulator();
    c.bench_function(name, |b| {
        b.iter(|| {
            accumulator.update_batch(&[values.clone()]).unwrap();
            black_box(accumulator.evaluate().unwrap());
        });
    });
}

fn mode_benchmark(c: &mut Criterion) {
    // Case: No nulls
    let values = Arc::new(create_primitive_array::<Int32Type>(8192, 0.0)) as ArrayRef;
    mode_bench(c, "mode benchmark no nulls", values);

    // Case: 30% nulls
    let values = Arc::new(create_primitive_array::<Int32Type>(8192, 0.3)) as ArrayRef;
    mode_bench(c, "mode benchmark 30% nulls", values);

    // Case: 70% nulls
    let values = Arc::new(create_primitive_array::<Int32Type>(8192, 0.7)) as ArrayRef;
    mode_bench(c, "mode benchmark 70% nulls", values);
}

criterion_group!(benches, mode_benchmark);
criterion_main!(benches);
