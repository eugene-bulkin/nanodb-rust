use std::default::Default;

use ::expressions::{ArithmeticType, Environment, Expression, Literal};
use ::expressions::expression::literal_arithmetic;
use ::functions::{AggregateFunction, Function, FunctionError, FunctionResult, ScalarFunction};
use ::queries::Planner;
use ::relations::{ColumnType, Schema};

#[derive(Debug, Clone)]
pub struct SumAverage {
    sum: Literal,
    count: i32,
    compute_average: bool,
    distinct: bool,
}

impl Default for SumAverage {
    fn default() -> SumAverage {
        SumAverage {
            sum: Literal::Int(0),
            count: 0,
            compute_average: false,
            distinct: false,
        }
    }
}

impl SumAverage {
    pub fn sum() -> Box<Function> {
        Box::new(SumAverage {
            compute_average: false,
            ..Default::default()
        })
    }

    pub fn average() -> Box<Function> {
        Box::new(SumAverage {
            compute_average: true,
            ..Default::default()
        })
    }
}

impl Function for SumAverage {
    fn evaluate(&self, _env: &mut Option<&mut Environment>, _args: Vec<Expression>, _planner: &Option<&Planner>) -> FunctionResult {
        Ok(self.get_result())
    }

    fn get_as_scalar(&self) -> Option<Box<ScalarFunction>> {
        Some(Box::new(SumAverage {
            sum: self.sum.clone(),
            count: self.count,
            compute_average: self.compute_average,
            distinct: self.distinct,
        }))
    }

    fn get_as_aggregate(&self) -> Option<Box<AggregateFunction>> {
        Some(Box::new(SumAverage {
            sum: self.sum.clone(),
            count: self.count,
            compute_average: self.compute_average,
            distinct: self.distinct,
        }))
    }

    fn is_scalar(&self) -> bool { true }

    fn is_aggregate(&self) -> bool { true }

    fn clone(&self) -> Self where Self: Sized {
        Clone::clone(&self)
    }
}

impl ScalarFunction for SumAverage {
    fn get_return_type(&self, args: Vec<Expression>, schema: &Schema) -> Result<ColumnType, FunctionError> {
        let func_name = (if self.compute_average { "AVERAGE" } else { "SUM" }).into();
        if args.len() != 1 {
            Err(FunctionError::TakesArguments(func_name, 1, args.len()))
        } else {
            if self.compute_average {
                Ok(ColumnType::Double)
            } else {
                if let Expression::ColumnValue(ref col_name) = args[0] {
                    let col_infos = schema.find_columns(col_name);
                    if col_infos.is_empty() {
                        Err(FunctionError::ColumnValueNotInSchema(args[0].clone()))
                    } else if col_infos.len() > 1 {
                        Err(FunctionError::ColumnValueAmbiguous(args[0].clone()))
                    } else {
                        Ok(col_infos[0].1.column_type)
                    }
                } else {
                    Err(FunctionError::ColumnValueArgumentNeeded(func_name, args[0].clone()))
                }
            }
        }
    }
}

impl AggregateFunction for SumAverage {
    fn supports_distinct(&self) -> bool {
        self.distinct
    }

    fn clear_result(&mut self) {
        self.sum = Literal::Int(0);
        self.count = 0;
    }

    fn add_value(&mut self, value: Literal) {
        if value == Literal::Null {
            return;
        }

        if self.compute_average {
            self.count += 1;
        }

        if self.distinct {
            // TODO
        } else {
            // We assume this can't fail because we are using numeric literals throughout.
            self.sum = literal_arithmetic(self.sum.clone(), value, ArithmeticType::Plus).unwrap();
        }
    }

    fn get_result(&self) -> Literal {
        if self.compute_average {
            if self.count == 0 {
                Literal::Float(::std::f32::NAN)
            } else {
                // See above for why we can unwrap.
                literal_arithmetic(self.sum.clone(), Literal::Double(self.count.into()), ArithmeticType::Divide).unwrap()
            }
        } else {
            self.sum.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use std::collections::HashSet;
    use std::iter::FromIterator;

    use ::parser::statements;
    use ::server::Server;
    use ::storage::TupleLiteral;

    #[test]
    fn test_sum() {
        let dir = TempDir::new("test_dbfiles").unwrap();
        let mut server = Server::with_data_path(dir.path());
        {
            let stmts = statements(b"CREATE TABLE foo (a integer, b integer);\
                                 INSERT INTO foo VALUES (3, 1);\
                                 INSERT INTO foo VALUES (4, 1);\
                                 INSERT INTO foo VALUES (5, 1);\
                                 INSERT INTO foo VALUES (7, 2);\
                                 INSERT INTO foo VALUES (12, 2);\
                                 INSERT INTO foo VALUES (32, 2);\
        ").unwrap().1;
            for stmt in stmts {
                server.handle_command(stmt);
            }

            let ref mut select_command = statements(b"SELECT SUM(A) FROM foo;").unwrap().1[0];
            assert_eq!(Ok(Some(vec![TupleLiteral::from_iter(vec![63.into()])])),
            select_command.execute(&mut server, &mut ::std::io::sink()));


            let ref mut select_command = statements(b"SELECT SUM(A) FROM foo GROUP BY B;").unwrap().1[0];
            let result: Vec<TupleLiteral> = select_command.execute(&mut server, &mut ::std::io::sink()).unwrap().unwrap();
            let expected: Vec<TupleLiteral> = vec![TupleLiteral::from_iter(vec![12.into()]),
                                                   TupleLiteral::from_iter(vec![51.into()])];

            let expected_set: HashSet<TupleLiteral> = HashSet::from_iter(expected);
            let result_set: HashSet<TupleLiteral> = HashSet::from_iter(result);
            assert_eq!(expected_set, result_set);
        }

        {
            let stmts = statements(b"CREATE TABLE bar (a float);\
                                 INSERT INTO bar VALUES (1);\
                                 INSERT INTO bar VALUES (2);\
            ").unwrap().1;
            for stmt in stmts {
                server.handle_command(stmt);
            }

            let ref mut select_command = statements(b"SELECT SUM(A) FROM bar;").unwrap().1[0];
            assert_eq!(Ok(Some(vec![TupleLiteral::from_iter(vec![3.0f32.into()])])),
            select_command.execute(&mut server, &mut ::std::io::sink()));
        }
    }

    #[test]
    fn test_average() {
        let dir = TempDir::new("test_dbfiles").unwrap();
        let mut server = Server::with_data_path(dir.path());

        let stmts = statements(b"CREATE TABLE foo (a integer, b integer);\
                                 INSERT INTO foo VALUES (3, 1);\
                                 INSERT INTO foo VALUES (4, 1);\
                                 INSERT INTO foo VALUES (5, 1);\
                                 INSERT INTO foo VALUES (7, 2);\
                                 INSERT INTO foo VALUES (12, 2);\
                                 INSERT INTO foo VALUES (32, 2);\
        ").unwrap().1;
        for stmt in stmts {
            server.handle_command(stmt);
        }

        let ref mut select_command = statements(b"SELECT AVERAGE(A) FROM foo;").unwrap().1[0];
        assert_eq!(Ok(Some(vec![TupleLiteral::from_iter(vec![10.5f64.into()])])),
        select_command.execute(&mut server, &mut ::std::io::sink()));

        let ref mut select_command = statements(b"SELECT AVG(A) FROM foo;").unwrap().1[0];
        assert_eq!(Ok(Some(vec![TupleLiteral::from_iter(vec![10.5f64.into()])])),
        select_command.execute(&mut server, &mut ::std::io::sink()));

        let ref mut select_command = statements(b"SELECT MEAN(A) FROM foo;").unwrap().1[0];
        assert_eq!(Ok(Some(vec![TupleLiteral::from_iter(vec![10.5f64.into()])])),
        select_command.execute(&mut server, &mut ::std::io::sink()));

        let ref mut select_command = statements(b"SELECT AVERAGE(A) FROM foo GROUP BY B;").unwrap().1[0];
        let result: Vec<TupleLiteral> = select_command.execute(&mut server, &mut ::std::io::sink()).unwrap().unwrap();
        let expected: Vec<TupleLiteral> = vec![TupleLiteral::from_iter(vec![4.0f64.into()]),
                                               TupleLiteral::from_iter(vec![17.0f64.into()])];

        let expected_set: HashSet<TupleLiteral> = HashSet::from_iter(expected);
        let result_set: HashSet<TupleLiteral> = HashSet::from_iter(result);
        assert_eq!(expected_set, result_set);
    }
}
