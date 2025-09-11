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
            // TODO add checker, if the value type is complex data type
            Ok(vec![dict_value_type.deref().clone()])
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
