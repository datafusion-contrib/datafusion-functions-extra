use datafusion::logical_expr::AggregateUDFImpl;
use datafusion::{arrow, common, error, functions_aggregate, logical_expr};
use std::ops::Deref;
use std::{any, fmt};

make_udaf_expr_and_func!(
    MaxByFunction,
    max_by,
    x y,
    "Returns the value of the first column corresponding to the maximum value in the second column.",
    max_by_udaf
);

#[derive(Eq, Hash, PartialEq)]
pub struct MaxByFunction {
    signature: logical_expr::Signature,
}

impl fmt::Debug for MaxByFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("MaxBy")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}
impl Default for MaxByFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl MaxByFunction {
    pub fn new() -> Self {
        Self {
            signature: logical_expr::Signature::user_defined(logical_expr::Volatility::Immutable),
        }
    }
}

fn get_min_max_by_result_type(
    input_types: &[arrow::datatypes::DataType],
) -> error::Result<Vec<arrow::datatypes::DataType>> {
    match &input_types[0] {
        arrow::datatypes::DataType::Dictionary(_, dict_value_type) => {
            // x add checker, if the value type is complex data type
            let mut result = vec![dict_value_type.deref().clone()];
            result.extend_from_slice(&input_types[1..]); // Preserve all other argument types
            Ok(result)
        }
        _ => Ok(input_types.to_vec()),
    }
}

impl logical_expr::AggregateUDFImpl for MaxByFunction {
    fn as_any(&self) -> &dyn any::Any {
        self
    }

    fn name(&self) -> &str {
        "max_by"
    }

    fn signature(&self) -> &logical_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> error::Result<arrow::datatypes::DataType> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(
        &self,
        _acc_args: logical_expr::function::AccumulatorArgs,
    ) -> error::Result<Box<dyn logical_expr::Accumulator>> {
        common::exec_err!("should not reach here")
    }
    fn coerce_types(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> error::Result<Vec<arrow::datatypes::DataType>> {
        get_min_max_by_result_type(arg_types)
    }

    fn simplify(&self) -> Option<logical_expr::function::AggregateFunctionSimplification> {
        let simplify = |mut aggr_func: logical_expr::expr::AggregateFunction,
                        _: &dyn logical_expr::simplify::SimplifyInfo| {
            let mut order_by = aggr_func.params.order_by;
            let (second_arg, first_arg) = (
                aggr_func.params.args.remove(1),
                aggr_func.params.args.remove(0),
            );
            let sort = logical_expr::expr::Sort::new(second_arg, true, false);
            order_by.push(sort);
            let func = logical_expr::expr::Expr::AggregateFunction(
                logical_expr::expr::AggregateFunction::new_udf(
                    functions_aggregate::first_last::last_value_udaf(),
                    vec![first_arg],
                    aggr_func.params.distinct,
                    aggr_func.params.filter,
                    order_by,
                    aggr_func.params.null_treatment,
                ),
            );
            Ok(func)
        };
        Some(Box::new(simplify))
    }
}

make_udaf_expr_and_func!(
    MinByFunction,
    min_by,
    x y,
    "Returns the value of the first column corresponding to the minimum value in the second column.",
    min_by_udaf
);

#[derive(Eq, Hash, PartialEq)]
pub struct MinByFunction {
    signature: logical_expr::Signature,
}

impl fmt::Debug for MinByFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("MinBy")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}

impl Default for MinByFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl MinByFunction {
    pub fn new() -> Self {
        Self {
            signature: logical_expr::Signature::user_defined(logical_expr::Volatility::Immutable),
        }
    }
}

impl logical_expr::AggregateUDFImpl for MinByFunction {
    fn as_any(&self) -> &dyn any::Any {
        self
    }

    fn name(&self) -> &str {
        "min_by"
    }

    fn signature(&self) -> &logical_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> error::Result<arrow::datatypes::DataType> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(
        &self,
        _acc_args: logical_expr::function::AccumulatorArgs,
    ) -> error::Result<Box<dyn logical_expr::Accumulator>> {
        common::exec_err!("should not reach here")
    }

    fn coerce_types(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> error::Result<Vec<arrow::datatypes::DataType>> {
        get_min_max_by_result_type(arg_types)
    }

    fn simplify(&self) -> Option<logical_expr::function::AggregateFunctionSimplification> {
        let simplify = |mut aggr_func: logical_expr::expr::AggregateFunction,
                        _: &dyn logical_expr::simplify::SimplifyInfo| {
            let mut order_by = aggr_func.params.order_by;
            let (second_arg, first_arg) = (
                aggr_func.params.args.remove(1),
                aggr_func.params.args.remove(0),
            );

            let sort = logical_expr::expr::Sort::new(second_arg, false, false);
            order_by.push(sort); // false for ascending sort
            let func = logical_expr::expr::Expr::AggregateFunction(
                logical_expr::expr::AggregateFunction::new_udf(
                    functions_aggregate::first_last::last_value_udaf(),
                    vec![first_arg],
                    aggr_func.params.distinct,
                    aggr_func.params.filter,
                    order_by,
                    aggr_func.params.null_treatment,
                ),
            );
            Ok(func)
        };
        Some(Box::new(simplify))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{
        ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use std::any::Any;
    use std::sync::Arc;

    #[cfg(test)]
    mod tests_max_by {
        use crate::max_min_by::max_by_udaf;
        use crate::max_min_by::tests::{
            extract_single_float64, extract_single_int64, extract_single_string, test_ctx,
        };
        use datafusion::error::Result;
        use datafusion::prelude::SessionContext;

        #[tokio::test]
        async fn test_max_by_string_int() -> Result<()> {
            let df = ctx()?
                .sql("SELECT max_by(string, int64) FROM types")
                .await?;
            assert_eq!(extract_single_string(df.collect().await?), "h");
            Ok(())
        }

        #[tokio::test]
        async fn test_max_by_string_float() -> Result<()> {
            let df = ctx()?
                .sql("SELECT max_by(string, float64) FROM types")
                .await?;
            assert_eq!(extract_single_string(df.collect().await?), "h");
            Ok(())
        }

        #[tokio::test]
        async fn test_max_by_float_string() -> Result<()> {
            let df = ctx()?
                .sql("SELECT max_by(float64, string) FROM types")
                .await?;
            assert_eq!(extract_single_float64(df.collect().await?), 8.0);
            Ok(())
        }

        #[tokio::test]
        async fn test_max_by_int_string() -> Result<()> {
            let df = ctx()?
                .sql("SELECT max_by(int64, string) FROM types")
                .await?;
            assert_eq!(extract_single_int64(df.collect().await?), 8);
            Ok(())
        }

        #[tokio::test]
        async fn test_max_by_dictionary_int() -> Result<()> {
            let df = ctx()?
                .sql("SELECT max_by(dict_string, int64) FROM types")
                .await?;
            assert_eq!(extract_single_string(df.collect().await?), "h");
            Ok(())
        }

        fn ctx() -> Result<SessionContext> {
            let ctx = test_ctx()?;
            ctx.register_udaf(max_by_udaf().as_ref().clone());
            Ok(ctx)
        }
    }

    #[cfg(test)]
    mod test_min_by {
        use crate::max_min_by::min_by_udaf;
        use crate::max_min_by::tests::{
            extract_single_float64, extract_single_int64, extract_single_string, test_ctx,
        };
        use datafusion::error::Result;
        use datafusion::prelude::SessionContext;

        #[tokio::test]
        async fn test_min_by_string_int() -> Result<()> {
            let df = ctx()?
                .sql("SELECT min_by(string, int64) FROM types")
                .await?;
            assert_eq!(extract_single_string(df.collect().await?), "a");
            Ok(())
        }

        #[tokio::test]
        async fn test_min_by_string_float() -> Result<()> {
            let df = ctx()?
                .sql("SELECT min_by(string, float64) FROM types")
                .await?;
            assert_eq!(extract_single_string(df.collect().await?), "a");
            Ok(())
        }

        #[tokio::test]
        async fn test_min_by_float_string() -> Result<()> {
            let df = ctx()?
                .sql("SELECT min_by(float64, string) FROM types")
                .await?;
            assert_eq!(extract_single_float64(df.collect().await?), 0.5);
            Ok(())
        }

        #[tokio::test]
        async fn test_min_by_int_string() -> Result<()> {
            let df = ctx()?
                .sql("SELECT min_by(int64, string) FROM types")
                .await?;
            assert_eq!(extract_single_int64(df.collect().await?), 1);
            Ok(())
        }

        #[tokio::test]
        async fn test_min_by_dictionary_int() -> Result<()> {
            let df = ctx()?
                .sql("SELECT min_by(dict_string, int64) FROM types")
                .await?;
            assert_eq!(extract_single_string(df.collect().await?), "a");
            Ok(())
        }

        fn ctx() -> Result<SessionContext> {
            let ctx = test_ctx()?;
            ctx.register_udaf(min_by_udaf().as_ref().clone());
            Ok(ctx)
        }
    }

    pub(super) fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("string", DataType::Utf8, false),
            Field::new_dictionary("dict_string", DataType::Int32, DataType::Utf8, false),
            Field::new("int64", DataType::Int64, false),
            Field::new("uint64", DataType::UInt64, false),
            Field::new("float64", DataType::Float64, false),
        ]))
    }

    pub(super) fn test_data(schema: Arc<Schema>) -> Vec<RecordBatch> {
        use datafusion::arrow::array::DictionaryArray;
        use datafusion::arrow::datatypes::Int32Type;

        vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                    Arc::new(
                        vec![Some("a"), Some("b"), Some("c"), Some("d")]
                            .into_iter()
                            .collect::<DictionaryArray<Int32Type>>(),
                    ),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                    Arc::new(UInt64Array::from(vec![1, 2, 3, 4])),
                    Arc::new(Float64Array::from(vec![0.5, 2.0, 3.0, 4.0])),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["e", "f", "g", "h"])),
                    Arc::new(
                        vec![Some("e"), Some("f"), Some("g"), Some("h")]
                            .into_iter()
                            .collect::<DictionaryArray<Int32Type>>(),
                    ),
                    Arc::new(Int64Array::from(vec![5, 6, 7, 8])),
                    Arc::new(UInt64Array::from(vec![5, 6, 7, 8])),
                    Arc::new(Float64Array::from(vec![5.0, 6.0, 7.0, 8.0])),
                ],
            )
            .unwrap(),
        ]
    }

    pub(crate) fn test_ctx() -> datafusion::common::Result<SessionContext> {
        let schema = test_schema();
        let table = MemTable::try_new(schema.clone(), vec![test_data(schema)])?;
        let ctx = SessionContext::new();
        ctx.register_table("types", Arc::new(table))?;
        Ok(ctx)
    }

    fn downcast<T: Any>(col: &ArrayRef) -> &T {
        col.as_any().downcast_ref::<T>().unwrap()
    }

    pub(crate) fn extract_single_string(results: Vec<RecordBatch>) -> String {
        let v1 = downcast::<StringArray>(results[0].column(0));
        v1.value(0).to_string()
    }

    pub(crate) fn extract_single_int64(results: Vec<RecordBatch>) -> i64 {
        let v1 = downcast::<Int64Array>(results[0].column(0));
        v1.value(0)
    }

    pub(crate) fn extract_single_float64(results: Vec<RecordBatch>) -> f64 {
        let v1 = downcast::<Float64Array>(results[0].column(0));
        v1.value(0)
    }
}
