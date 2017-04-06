use super::{Function, FunctionError, FunctionResult, ScalarFunction};

use ::expressions::{Environment, Expression, Literal};
use ::relations::{ColumnType, Schema};
use ::queries::Planner;

impl_scalar_func!(Sin, |env, args, planner| {
    let first_arg = args[0].clone();
    return_arithmetic_eval!(first_arg, env, planner, (|n| (n as f64).sin(), |n| n.sin()))
});

impl_scalar_func!(Cos, |env, args, planner| {
    let first_arg = args[0].clone();
    return_arithmetic_eval!(first_arg, env, planner, (|n| (n as f64).cos(), |n| n.cos()))
});

impl_scalar_func!(Tan, |env, args, planner| {
    let first_arg = args[0].clone();
    return_arithmetic_eval!(first_arg, env, planner, (|n| (n as f64).tan(), |n| n.tan()))
});

impl_scalar_func!(ASin, |env, args, planner| {
    let first_arg = args[0].clone();
    return_arithmetic_eval!(first_arg, env, planner, (|n| (n as f64).asin(), |n| n.asin()))
});

impl_scalar_func!(ACos, |env, args, planner| {
    let first_arg = args[0].clone();
    return_arithmetic_eval!(first_arg, env, planner, (|n| (n as f64).acos(), |n| n.acos()))
});

impl_scalar_func!(ATan, |env, args, planner| {
    let first_arg = args[0].clone();
    return_arithmetic_eval!(first_arg, env, planner, (|n| (n as f64).atan(), |n| n.atan()))
});

impl_scalar_func!(ATan2, |env, args, planner| {
    if args.len() < 2 {
        return Err(FunctionError::NeedsMoreArguments("$func_name".to_string().to_uppercase(), 2, args.len()));
    }
    let y = args[0].clone();
    let x = args[1].clone();
    match (y.evaluate(env, planner), x.evaluate(env, planner)) {
        (Err(e), _) => {
            Err(FunctionError::CouldNotEvaluateExpression(y.clone(), Box::new(e)))
        },
        (_, Err(e)) => {
            Err(FunctionError::CouldNotEvaluateExpression(x.clone(), Box::new(e)))
        },
        (Ok(y_value), Ok(x_value)) => {
            let x_num = as_double!(x_value, x);
            let y_num = as_double!(y_value, y);
            Ok(y_num.atan2(x_num).into())
        }
    }
});
