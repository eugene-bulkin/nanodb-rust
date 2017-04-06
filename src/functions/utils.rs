macro_rules! return_arithmetic_eval {
    ($to_eval:ident, $env:ident, $planner:ident, (|$int_name:ident| $int_expr:expr, |$dec_name:ident| $dec_expr:expr)) => (
        match $to_eval.evaluate($env, $planner) {
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
    ($to_eval:ident, $env:ident, $planner:ident, | $var_name:ident | $expr:expr) => (
        match $to_eval.evaluate($env, $planner) {
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

macro_rules! as_double {
    ($num:ident, $expr:ident) => {{
        if !$num.is_numeric() {
                return Err(FunctionError::ExpressionNotNumeric($expr.clone()));
        }
        as_double!($num)
    }};
    ($num:ident) => {{
        match $num {
            Literal::Int(i) => i as f64,
            Literal::Long(l) => l as f64,
            Literal::Float(f) => f as f64,
            Literal::Double(d) => d,
            _ => unreachable!()
        }
    }};
}

macro_rules! check_has_args {
    ($args:ident, $func_name:ident) => {
        if $args.len() < 1 {
            return Err(FunctionError::NeedsArguments("$func_name".to_string().to_uppercase()));
        }
    }
}

macro_rules! impl_scalar_func {
    ($name:ident, | $env:ident, $args:ident, $planner:ident | $eval:block) => {
        pub struct $name;

        impl $name {
            pub fn new() -> Box<Function> {
                Box::new($name)
            }
        }

        impl Function for $name {
            fn evaluate(&self, $env: &mut Option<&mut Environment>, $args: Vec<Expression>, $planner: &Option<&Planner>) -> FunctionResult {
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