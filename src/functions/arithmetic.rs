use super::{Function, FunctionError, FunctionResult, ScalarFunction};

use ::expressions::{Environment, Expression, Literal};
use ::relations::{ColumnType, Schema};
use ::queries::Planner;

impl_scalar_func!(Abs, |env, args, planner| {
    let first_arg = args[0].clone();
    return_arithmetic_eval!(first_arg, env, planner, |n| n.abs())
});

impl_scalar_func!(Ceil, |env, args, planner| {
    let first_arg = args[0].clone();
    return_arithmetic_eval!(first_arg, env, planner, (|n| n, |n| n.ceil()))
});

impl_scalar_func!(Floor, |env, args, planner| {
    let first_arg = args[0].clone();
    return_arithmetic_eval!(first_arg, env, planner, (|n| n, |n| n.floor()))
});

impl_scalar_func!(Exp, |env, args, planner| {
    let first_arg = args[0].clone();
    return_arithmetic_eval!(first_arg, env, planner, (|n| (n as f64).exp(), |n| n.exp()))
});

impl_scalar_func!(Ln, |env, args, planner| {
    let first_arg = args[0].clone();
    return_arithmetic_eval!(first_arg, env, planner, (|n| (n as f64).ln(), |n| n.ln()))
});

impl_scalar_func!(Sqrt, |env, args, planner| {
    let first_arg = args[0].clone();
    return_arithmetic_eval!(first_arg, env, planner, (|n| (n as f64).sqrt(), |n| n.sqrt()))
});

impl_scalar_func!(Power, |env, args, planner| {
    if args.len() < 2 {
        return Err(FunctionError::NeedsMoreArguments("$func_name".to_string().to_uppercase(), 2, args.len()));
    }
    let base = args[0].clone();
    let exp = args[1].clone();
    match (base.evaluate(env, planner), exp.evaluate(env, planner)) {
        (Err(e), _) => {
            Err(FunctionError::CouldNotEvaluateExpression(base.clone(), Box::new(e)))
        },
        (_, Err(e)) => {
            Err(FunctionError::CouldNotEvaluateExpression(exp.clone(), Box::new(e)))
        },
        (Ok(base_value), Ok(exp_value)) => {
            let base_num = as_double!(base_value, base);
            if !exp_value.is_numeric() {
                return Err(FunctionError::ExpressionNotNumeric(exp.clone()));
            }
            match exp_value {
                Literal::Int(i) => Ok(base_num.powi(i).into()),
                Literal::Long(l) => Ok(base_num.powi(l as i32).into()),
                Literal::Float(f) => Ok(base_num.powf(f as f64).into()),
                Literal::Double(d) => Ok(base_num.powf(d).into()),
                _ => unreachable!()
            }
        }
    }
});

// TODO: MOD, SGN