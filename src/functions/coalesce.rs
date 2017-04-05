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
            if let Ok(value) = arg.evaluate(env) {
                if value != Literal::Null {
                    return Ok(value);
                }
            }
        }

        Ok(Literal::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ::expressions::{Expression, Environment, Literal};
    use ::functions::FunctionError;
    use ::relations::{ColumnInfo, ColumnType, Schema};
    use ::storage::TupleLiteral;

    #[test]
    fn test_coalesce() {
        let schema1 = Schema::with_columns(vec![ColumnInfo::with_name(ColumnType::Integer, "A")]).unwrap();
        let schema2 = Schema::with_columns(vec![ColumnInfo::with_name(ColumnType::Double, "B")]).unwrap();

        let mut env1 = {
            let mut env = Environment::new();
            env.add_tuple(schema1.clone(), TupleLiteral::from_iter(vec![47.into()]));
            env
        };
        let mut env2 = {
            let mut env = Environment::new();
            env.add_tuple(schema2.clone(), TupleLiteral::from_iter(vec![3.0.into()]));
            env
        };

        let func = Coalesce::new();

        let e1 = Expression::ColumnValue((None, Some("A".into())));
        let e2 = Expression::Int(5);
        let e3 = Expression::Null;
        let e4 = Expression::Double(9.0);

        assert_eq!(Ok(Literal::Null), func.evaluate(&mut None, vec![e1.clone()]));
        assert_eq!(Ok(Literal::Int(5)), func.evaluate(&mut None, vec![e2.clone()]));
        assert_eq!(Ok(Literal::Null), func.evaluate(&mut None, vec![e3.clone()]));

        assert_eq!(Ok(Literal::Int(5)), func.evaluate(&mut None, vec![e1.clone(), e2.clone()]));
        assert_eq!(Ok(Literal::Int(5)), func.evaluate(&mut None, vec![e3.clone(), e2.clone()]));
        assert_eq!(Ok(Literal::Double(9.0)), func.evaluate(&mut None, vec![e4.clone(), e2.clone()]));

        assert_eq!(Ok(Literal::Int(47)), func.evaluate(&mut Some(&mut env1), vec![e1.clone()]));
        assert_eq!(Ok(Literal::Int(5)), func.evaluate(&mut Some(&mut env1), vec![e2.clone()]));
        assert_eq!(Ok(Literal::Null), func.evaluate(&mut Some(&mut env1), vec![e3.clone()]));

        assert_eq!(Ok(Literal::Int(47)), func.evaluate(&mut Some(&mut env1), vec![e1.clone(), e2.clone()]));
        assert_eq!(Ok(Literal::Int(5)), func.evaluate(&mut Some(&mut env1), vec![e3.clone(), e2.clone()]));
        assert_eq!(Ok(Literal::Double(9.0)), func.evaluate(&mut Some(&mut env1), vec![e4.clone(), e2.clone()]));

        assert_eq!(Ok(Literal::Null), func.evaluate(&mut Some(&mut env2), vec![e1.clone()]));
        assert_eq!(Ok(Literal::Int(5)), func.evaluate(&mut Some(&mut env2), vec![e2.clone()]));
        assert_eq!(Ok(Literal::Null), func.evaluate(&mut Some(&mut env2), vec![e3.clone()]));

        assert_eq!(Ok(Literal::Int(5)), func.evaluate(&mut Some(&mut env2), vec![e1.clone(), e2.clone()]));
        assert_eq!(Ok(Literal::Int(5)), func.evaluate(&mut Some(&mut env2), vec![e3.clone(), e2.clone()]));
        assert_eq!(Ok(Literal::Double(9.0)), func.evaluate(&mut Some(&mut env2), vec![e4.clone(), e2.clone()]));

        assert_eq!(Err(FunctionError::NeedsArguments("COALESCE".into())), func.evaluate(&mut None, vec![]));
    }
}