use super::{Function, FunctionError, FunctionResult, ScalarFunction};

use ::expressions::{Environment, Expression, Literal};
use ::relations::{ColumnType, Schema};

macro_rules! return_arithmetic_eval {
    ($to_eval:ident, $env:ident, (|$int_name:ident| $int_expr:expr, |$dec_name:ident| $dec_expr:expr)) => (
        match $to_eval.evaluate($env) {
            Ok(value) => {
                match value {
                    Literal::Int($int_name) => Ok($int_expr.into()),
                    Literal::Long($int_name) => Ok($int_expr.into()),
                    Literal::Double($dec_name) => Ok($dec_expr.into()),
                    Literal::Float($dec_name) => Ok($dec_expr.into()),
                    _ => Err(FunctionError::ExpressionNotNumeric($to_eval.clone())),
                }
            }
            Err(e) => {
                Err(FunctionError::CouldNotEvaluateExpression($to_eval.clone(), Box::new(e)))
            }
        }
    );
    ($to_eval:ident, $env:ident, | $var_name:ident | $expr:expr) => (
        match $to_eval.evaluate($env) {
            Ok(value) => {
                match value {
                    Literal::Int($var_name) => Ok($expr.into()),
                    Literal::Long($var_name) => Ok($expr.into()),
                    Literal::Double($var_name) => Ok($expr.into()),
                    Literal::Float($var_name) => Ok($expr.into()),
                    _ => Err(FunctionError::ExpressionNotNumeric($to_eval.clone())),
                }
            }
            Err(e) => {
                Err(FunctionError::CouldNotEvaluateExpression($to_eval.clone(), Box::new(e)))
            }
        }
    );
}

macro_rules! check_has_args {
    ($args:ident, $func_name:ident) => {
        if $args.len() < 1 {
            return Err(FunctionError::NeedsArguments("$func_name".to_string().to_uppercase()));
        }
    }
}

macro_rules! impl_scalar_func {
    ($name:ident, | $env:ident, $args:ident | $eval:block) => {
        pub struct $name;

        impl $name {
            pub fn new() -> Box<Function> {
                Box::new($name)
            }
        }

        impl Function for $name {
            fn evaluate(&self, $env: &mut Option<&mut Environment>, $args: Vec<Expression>) -> FunctionResult {
                check_has_args!($args, $name);

                $eval
            }

            fn get_as_scalar(&self) -> Option<Box<ScalarFunction>> {
                Some(Box::new($name))
            }
        }

        impl ScalarFunction for $name {
            fn get_return_type(&self, args: Vec<Expression>, schema: &Schema) -> Result<ColumnType, FunctionError> {
                check_has_args!(args, $name);

                let first_expr = args[0].clone();
                let arg_type = try!(first_expr.get_column_type(schema).map_err(|e| {
                    FunctionError::CouldNotRetrieveExpressionColumnType(first_expr.clone(), Box::new(e))
                }));

                Ok(if arg_type.is_numeric() {
                    arg_type
                } else {
                    ColumnType::Double
                })
            }
        }
    }
}

impl_scalar_func!(Abs, |env, args| {
    let first_arg = args[0].clone();
    return_arithmetic_eval!(first_arg, env, |n| n.abs())
});

impl_scalar_func!(Ceil, |env, args| {
    let first_arg = args[0].clone();
    return_arithmetic_eval!(first_arg, env, (|n| n, |n| n.ceil()))
});

impl_scalar_func!(Floor, |env, args| {
    let first_arg = args[0].clone();
    return_arithmetic_eval!(first_arg, env, (|n| n, |n| n.floor()))
});

impl_scalar_func!(Exp, |env, args| {
    let first_arg = args[0].clone();
    return_arithmetic_eval!(first_arg, env, (|n| (n as f64).exp(), |n| n.exp()))
});

impl_scalar_func!(Ln, |env, args| {
    let first_arg = args[0].clone();
    return_arithmetic_eval!(first_arg, env, (|n| (n as f64).ln(), |n| n.ln()))
});

impl_scalar_func!(Sqrt, |env, args| {
    let first_arg = args[0].clone();
    return_arithmetic_eval!(first_arg, env, (|n| (n as f64).sqrt(), |n| n.sqrt()))
});

impl_scalar_func!(Power, |env, args| {
    if args.len() < 2 {
        return Err(FunctionError::NeedsMoreArguments("$func_name".to_string().to_uppercase(), 2, args.len()));
    }
    let base = args[0].clone();
    let exp = args[1].clone();
    match (base.evaluate(env), exp.evaluate(env)) {
        (Err(e), _) => {
            Err(FunctionError::CouldNotEvaluateExpression(base.clone(), Box::new(e)))
        },
        (_, Err(e)) => {
            Err(FunctionError::CouldNotEvaluateExpression(exp.clone(), Box::new(e)))
        },
        (Ok(base_value), Ok(exp_value)) => {
            if !base_value.is_numeric() {
                return Err(FunctionError::ExpressionNotNumeric(base.clone()));
            }
            if !exp_value.is_numeric() {
                return Err(FunctionError::ExpressionNotNumeric(exp.clone()));
            }
            let base_num: f64 = match base_value {
                Literal::Int(i) => i as f64,
                Literal::Long(l) => l as f64,
                Literal::Float(f) => f as f64,
                Literal::Double(d) => d,
                _ => unreachable!()
            };
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