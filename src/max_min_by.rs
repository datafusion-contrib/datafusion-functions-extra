use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::functions_aggregate::first_last::last_value_udaf;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::simplify::SimplifyInfo;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, expr, function};
use datafusion::prelude::Expr;
use datafusion::{
    common::exec_err,
    logical_expr::{Signature, Volatility, function::AccumulatorArgs},
};
use std::any::Any;
use std::fmt::Debug;
use std::ops::Deref;

make_udaf_expr_and_func!(
    MaxByFunction,
    max_by,
    x y,
    "Returns the value of the first column corresponding to the maximum value in the second column.",
    max_by_udaf
);

pub struct MaxByFunction {
    signature: Signature,
}

impl Debug for MaxByFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

fn get_min_max_by_result_type(input_types: &[DataType]) -> Result<Vec<DataType>, DataFusionError> {
    match &input_types[0] {
        DataType::Dictionary(_, dict_value_type) => {
            // TODO add checker, if the value type is complex data type
            Ok(vec![dict_value_type.deref().clone()])
        }
        _ => Ok(input_types.to_vec()),
    }
}

impl AggregateUDFImpl for MaxByFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "max_by"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>, DataFusionError> {
        exec_err!("should not reach here")
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>, DataFusionError> {
        get_min_max_by_result_type(arg_types)
    }

    fn simplify(&self) -> Option<function::AggregateFunctionSimplification> {
        let simplify = |mut aggr_func: expr::AggregateFunction, _: &dyn SimplifyInfo| {
            let mut order_by = aggr_func.order_by.unwrap_or_default();
            let (second_arg, first_arg) = (aggr_func.args.remove(1), aggr_func.args.remove(0));

            order_by.push(Sort::new(second_arg, true, false));

            Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                last_value_udaf(),
                vec![first_arg],
                aggr_func.distinct,
                aggr_func.filter,
                Some(order_by),
                aggr_func.null_treatment,
            )))
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
    signature: Signature,
}

impl Debug for MinByFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for MinByFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "min_by"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>, DataFusionError> {
        exec_err!("should not reach here")
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>, DataFusionError> {
        get_min_max_by_result_type(arg_types)
    }

    fn simplify(&self) -> Option<function::AggregateFunctionSimplification> {
        let simplify = |mut aggr_func: expr::AggregateFunction, _: &dyn SimplifyInfo| {
            let mut order_by = aggr_func.order_by.unwrap_or_default();
            let (second_arg, first_arg) = (aggr_func.args.remove(1), aggr_func.args.remove(0));

            order_by.push(Sort::new(second_arg, false, false)); // false for ascending sort

            Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                last_value_udaf(),
                vec![first_arg],
                aggr_func.distinct,
                aggr_func.filter,
                Some(order_by),
                aggr_func.null_treatment,
            )))
        };
        Some(Box::new(simplify))
    }
}
