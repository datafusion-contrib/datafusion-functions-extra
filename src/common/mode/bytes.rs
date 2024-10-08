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

use arrow::array::ArrayAccessor;
use arrow::array::ArrayIter;
use arrow::array::ArrayRef;
use arrow::array::AsArray;
use arrow::datatypes::DataType;
use datafusion::arrow;
use datafusion::common::cast::as_primitive_array;
use datafusion::common::cast::as_string_array;
use datafusion::error::Result;
use datafusion::logical_expr::Accumulator;
use datafusion::scalar::ScalarValue;
use std::collections::HashMap;

#[derive(Debug)]
pub struct BytesModeAccumulator {
    value_counts: HashMap<String, i64>,
    data_type: DataType,
}

impl BytesModeAccumulator {
    pub fn new(data_type: &DataType) -> Self {
        Self {
            value_counts: HashMap::new(),
            data_type: data_type.clone(),
        }
    }

    fn update_counts<'a, V>(&mut self, array: V)
    where
        V: ArrayAccessor<Item = &'a str>,
    {
        for value in ArrayIter::new(array).flatten() {
            let key = value;
            if let Some(count) = self.value_counts.get_mut(key) {
                *count += 1;
            } else {
                self.value_counts.insert(key.to_string(), 1);
            }
        }
    }
}

impl Accumulator for BytesModeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        match &self.data_type {
            DataType::Utf8View => {
                let array = values[0].as_string_view();
                self.update_counts(array);
            }
            _ => {
                let array = values[0].as_string::<i32>();
                self.update_counts(array);
            }
        };

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values: Vec<ScalarValue> = self
            .value_counts
            .keys()
            .map(|key| ScalarValue::Utf8(Some(key.to_string())))
            .collect();

        let frequencies: Vec<ScalarValue> = self
            .value_counts
            .values()
            .map(|&count| ScalarValue::Int64(Some(count)))
            .collect();

        let values_scalar = ScalarValue::new_list_nullable(&values, &DataType::Utf8);
        let frequencies_scalar = ScalarValue::new_list_nullable(&frequencies, &DataType::Int64);

        Ok(vec![
            ScalarValue::List(values_scalar),
            ScalarValue::List(frequencies_scalar),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let values_array = as_string_array(&states[0])?;
        let counts_array = as_primitive_array::<arrow::datatypes::Int64Type>(&states[1])?;

        for (i, value_option) in values_array.iter().enumerate() {
            if let Some(value) = value_option {
                let count = counts_array.value(i);
                let entry = self.value_counts.entry(value.to_string()).or_insert(0);
                *entry += count;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.value_counts.is_empty() {
            return match &self.data_type {
                DataType::Utf8View => Ok(ScalarValue::Utf8View(None)),
                _ => Ok(ScalarValue::Utf8(None)),
            };
        }

        let mode = self
            .value_counts
            .iter()
            .max_by(|a, b| {
                // First compare counts
                a.1.cmp(b.1)
                    // If counts are equal, compare keys in reverse order to get the maximum string
                    .then_with(|| b.0.cmp(a.0))
            })
            .map(|(value, _)| value.to_string());

        match mode {
            Some(result) => match &self.data_type {
                DataType::Utf8View => Ok(ScalarValue::Utf8View(Some(result))),
                _ => Ok(ScalarValue::Utf8(Some(result))),
            },
            None => match &self.data_type {
                DataType::Utf8View => Ok(ScalarValue::Utf8View(None)),
                _ => Ok(ScalarValue::Utf8(None)),
            },
        }
    }

    fn size(&self) -> usize {
        self.value_counts.capacity() * std::mem::size_of::<(String, i64)>() + std::mem::size_of_val(&self.data_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, GenericByteViewArray, StringArray};
    use std::sync::Arc;

    #[test]
    fn test_mode_accumulator_single_mode_utf8() -> Result<()> {
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8);
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("apple"),
            Some("banana"),
            Some("apple"),
            Some("orange"),
            Some("banana"),
            Some("apple"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8(Some("apple".to_string())));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_tie_utf8() -> Result<()> {
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8);
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("apple"),
            Some("banana"),
            Some("apple"),
            Some("orange"),
            Some("banana"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8(Some("apple".to_string())));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_all_nulls_utf8() -> Result<()> {
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8);
        let values: ArrayRef = Arc::new(StringArray::from(vec![None as Option<&str>, None, None]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8(None));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_with_nulls_utf8() -> Result<()> {
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8);
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("apple"),
            None,
            Some("banana"),
            Some("apple"),
            None,
            None,
            None,
            Some("banana"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8(Some("apple".to_string())));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_single_mode_utf8view() -> Result<()> {
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8View);
        let values: ArrayRef = Arc::new(GenericByteViewArray::from(vec![
            Some("apple"),
            Some("banana"),
            Some("apple"),
            Some("orange"),
            Some("banana"),
            Some("apple"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8View(Some("apple".to_string())));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_tie_utf8view() -> Result<()> {
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8View);
        let values: ArrayRef = Arc::new(GenericByteViewArray::from(vec![
            Some("apple"),
            Some("banana"),
            Some("apple"),
            Some("orange"),
            Some("banana"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8View(Some("apple".to_string())));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_all_nulls_utf8view() -> Result<()> {
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8View);
        let values: ArrayRef = Arc::new(GenericByteViewArray::from(vec![None as Option<&str>, None, None]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8View(None));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_with_nulls_utf8view() -> Result<()> {
        let mut acc = BytesModeAccumulator::new(&DataType::Utf8View);
        let values: ArrayRef = Arc::new(GenericByteViewArray::from(vec![
            Some("apple"),
            None,
            Some("banana"),
            Some("apple"),
            None,
            None,
            None,
            Some("banana"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8View(Some("apple".to_string())));
        Ok(())
    }
}
