use super::{Function, FunctionError, FunctionResult};

use ::expressions::{Environment, Expression, Literal};

pub struct Coalesce {}

impl Coalesce {
    pub fn new() -> Box<Function> {
        Box::new(Coalesce {})
    }
}

impl Function for Coalesce {
    fn evaluate(&self, mut env: &mut Option<&mut Environment>, args: Vec<Expression>) -> FunctionResult {
        if args.is_empty() {
            return Err(FunctionError::NeedsArguments("COALESCE".into()));
        }

        for arg in args.iter() {
            if let Ok(value) = arg.evaluate(env, &mut None) {
                if value != Literal::Null {
                    return Ok(value);
                }
            }
        }

        Ok(Literal::Null)
    }
}