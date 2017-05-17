use std::collections::HashSet;
use std::default::Default;

use ::expressions::{Environment, Expression, Literal};
use ::functions::{AggregateFunction, Function, FunctionError, FunctionResult, ScalarFunction};
use ::queries::Planner;
use ::relations::{ColumnType, Schema};

#[derive(Debug, Clone)]
pub struct CountStar {
    count: i32,
}

impl Default for CountStar {
    fn default() -> CountStar {
        CountStar {
            count: 0,
        }
    }
}

impl CountStar {
    /// Creates a new count function.
    pub fn count() -> Box<Function> {
        Box::new(CountStar::default())
    }
}

impl Function for CountStar {
    fn evaluate(&self, _env: &mut Option<&mut Environment>, _args: Vec<Expression>, _planner: &Option<&Planner>) -> FunctionResult {
        Ok(self.get_result())
    }

    fn get_as_scalar(&self) -> Option<Box<ScalarFunction>> {
        Some(Box::new(CountStar {
            count: self.count
        }))
    }

    fn get_as_aggregate(&self) -> Option<Box<AggregateFunction>> {
        Some(Box::new(CountStar {
            count: self.count
        }))
    }

    fn is_scalar(&self) -> bool { true }

    fn is_aggregate(&self) -> bool { true }

    fn clone(&self) -> Self where Self: Sized {
        Clone::clone(&self)
    }
}

impl ScalarFunction for CountStar {
    fn get_return_type(&self, args: Vec<Expression>, _schema: &Schema) -> Result<ColumnType, FunctionError> {
        if args.len() != 1 {
            Err(FunctionError::TakesArguments("COUNT".into(), 1, args.len()))
        } else {
            Ok(ColumnType::Integer)
        }
    }
}

impl AggregateFunction for CountStar {
    fn supports_distinct(&self) -> bool { false }

    fn clear_result(&mut self) {
        self.count = 0;
    }

    fn add_value(&mut self, _value: Literal) {
        self.count += 1;
    }

    fn get_result(&self) -> Literal { Literal::Int(self.count) }
}

#[derive(Debug, Clone)]
pub struct CountAggregate {
    count: Option<i32>,
    values_seen: HashSet<Literal>,
    last_value_seen: Option<Literal>,
    distinct: bool,
    sorted_inputs: bool,
}

impl Default for CountAggregate {
    fn default() -> CountAggregate {
        CountAggregate {
            distinct: false,
            count: None,
            values_seen: HashSet::new(),
            last_value_seen: None,
            sorted_inputs: false,
        }
    }
}

impl CountAggregate {
    /// Creates a new count function.
    pub fn count() -> Box<Function> {
        Box::new(CountAggregate {
            distinct: false,
            ..Default::default()
        })
    }
}

impl Function for CountAggregate {
    fn evaluate(&self, _env: &mut Option<&mut Environment>, _args: Vec<Expression>, _planner: &Option<&Planner>) -> FunctionResult {
        Ok(self.get_result())
    }

    fn get_as_scalar(&self) -> Option<Box<ScalarFunction>> {
        Some(Box::new(CountAggregate {
            distinct: self.distinct,
            ..Default::default()
        }))
    }

    fn get_as_aggregate(&self) -> Option<Box<AggregateFunction>> {
        Some(Box::new(CountAggregate {
            distinct: self.distinct,
            ..Default::default()
        }))
    }

    fn is_scalar(&self) -> bool { true }

    fn is_aggregate(&self) -> bool { true }

    fn clone(&self) -> Self where Self: Sized {
        Clone::clone(&self)
    }
}

impl ScalarFunction for CountAggregate {
    fn get_return_type(&self, args: Vec<Expression>, _schema: &Schema) -> Result<ColumnType, FunctionError> {
        if args.len() != 1 {
            Err(FunctionError::TakesArguments("COUNT".into(), 1, args.len()))
        } else {
            Ok(ColumnType::Integer)
        }
    }
}

impl AggregateFunction for CountAggregate {
    fn supports_distinct(&self) -> bool { true }

    fn clear_result(&mut self) {
        self.count = None;

        if self.distinct {
            if self.sorted_inputs {
                self.last_value_seen = None;
            } else {
                self.values_seen.clear();
            }
        }
    }

    fn add_value(&mut self, value: Literal) {
        if value == Literal::Null {
            return;
        }

        if self.count.is_none() {
            self.count = Some(0);
        }

        if self.distinct {
            // TODO
        } else {
            // Non-distinct count.  Just increment on any non-null value.
            self.count = self.count.map(|n| n + 1);
        }
    }

    fn get_result(&self) -> Literal {
        match self.count {
            Some(count) => Literal::Int(count),
            None => Literal::Int(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use std::collections::HashSet;
    use std::iter::FromIterator;

    use ::expressions::Literal;
    use ::parser::statements;
    use ::server::Server;
    use ::storage::TupleLiteral;

    #[test]
    fn test_count() {
        let dir = TempDir::new("test_dbfiles").unwrap();
        let mut server = Server::with_data_path(dir.path());

        let stmts = statements(b"CREATE TABLE foo (a integer, b integer, c varchar(20));\
                                 INSERT INTO foo VALUES (3, 6, 'bar');\
                                 INSERT INTO foo VALUES (3, 7, 'baz');\
                                 INSERT INTO foo VALUES (2, 10, 'baz');\
                                 INSERT INTO foo VALUES (1, 9, 'foo');\
                                 INSERT INTO foo VALUES (1, 13, 'foo');\
                                 INSERT INTO foo VALUES (NULL, NULL, NULL);\
        ").unwrap().1;
        for stmt in stmts {
            server.handle_command(stmt);
        }

        let ref mut select_command = statements(b"SELECT COUNT(B) FROM foo;").unwrap().1[0];
        assert_eq!(Ok(Some(vec![TupleLiteral::from_iter(vec![5.into()])])),
        select_command.execute(&mut server, &mut ::std::io::sink()));

        let ref mut select_command = statements(b"SELECT COUNT(*) FROM foo;").unwrap().1[0];
        assert_eq!(Ok(Some(vec![TupleLiteral::from_iter(vec![6.into()])])),
        select_command.execute(&mut server, &mut ::std::io::sink()));

        let ref mut select_command = statements(b"SELECT A, COUNT(B) FROM foo GROUP BY A;").unwrap().1[0];

        let result: Vec<TupleLiteral> = select_command.execute(&mut server, &mut ::std::io::sink()).unwrap().unwrap();
        let expected: Vec<TupleLiteral> = vec![
            TupleLiteral::from_iter(vec![3.into(), 2.into()]),
            TupleLiteral::from_iter(vec![2.into(), 1.into()]),
            TupleLiteral::from_iter(vec![1.into(), 2.into()]),
            TupleLiteral::from_iter(vec![Literal::Null, 0.into()]),
        ];

        let expected_set: HashSet<TupleLiteral> = HashSet::from_iter(expected);
        let result_set: HashSet<TupleLiteral> = HashSet::from_iter(result);
        assert_eq!(expected_set, result_set);
    }
}
