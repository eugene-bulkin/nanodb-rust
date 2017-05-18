use std::collections::HashSet;
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
    values_seen: HashSet<Literal>,
}

impl Default for SumAverage {
    fn default() -> SumAverage {
        SumAverage {
            sum: Literal::Int(0),
            count: 0,
            compute_average: false,
            distinct: false,
            values_seen: Default::default(),
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
    pub fn sum_distinct() -> Box<Function> {
        Box::new(SumAverage {
            compute_average: false,
            distinct: true,
            ..Default::default()
        })
    }

    pub fn average_distinct() -> Box<Function> {
        Box::new(SumAverage {
            compute_average: true,
            distinct: true,
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
            values_seen: self.values_seen.clone(),
        }))
    }

    fn get_as_aggregate(&self) -> Option<Box<AggregateFunction>> {
        Some(Box::new(SumAverage {
            sum: self.sum.clone(),
            count: self.count,
            compute_average: self.compute_average,
            distinct: self.distinct,
            values_seen: self.values_seen.clone(),
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
        self.values_seen.clear();
    }

    fn add_value(&mut self, value: Literal) {
        if value == Literal::Null {
            return;
        }

        // If we are not looking at distinct values, or the value has not been seen yet, add it to
        // the sum.
        if !self.distinct || (self.distinct && self.values_seen.insert(value.clone())) {
            // We assume this can't fail because we are using numeric literals throughout.
            self.sum = literal_arithmetic(&self.sum, &value, ArithmeticType::Plus).unwrap();

            if self.compute_average {
                self.count += 1;
            }
        }
    }

    fn get_result(&self) -> Literal {
        if self.compute_average {
            if self.count == 0 {
                Literal::Float(::std::f32::NAN)
            } else {
                // See above for why we can unwrap.
                literal_arithmetic(&self.sum, &Literal::Double(self.count.into()), ArithmeticType::Divide).unwrap()
            }
        } else {
            self.sum.clone()
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum MinMaxType {
    Min,
    Max
}

impl ::std::fmt::Display for MinMaxType {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            MinMaxType::Min => write!(f, "MIN"),
            MinMaxType::Max => write!(f, "MAX"),
        }
    }
}

impl From<MinMaxType> for ArithmeticType {
    fn from(func_type: MinMaxType) -> ArithmeticType {
        match func_type {
            MinMaxType::Min => ArithmeticType::Min,
            MinMaxType::Max => ArithmeticType::Max,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MinMax {
    value: Literal,
    func_type: MinMaxType,
}

impl MinMax {
    pub fn min() -> Box<Function> {
        Box::new(MinMax {
            value: Literal::Null,
            func_type: MinMaxType::Min,
        })
    }

    pub fn max() -> Box<Function> {
        Box::new(MinMax {
            value: Literal::Null,
            func_type: MinMaxType::Max,
        })
    }
}

impl Function for MinMax {
    fn evaluate(&self, _env: &mut Option<&mut Environment>, _args: Vec<Expression>, _planner: &Option<&Planner>) -> FunctionResult {
        Ok(self.get_result())
    }

    fn get_as_scalar(&self) -> Option<Box<ScalarFunction>> {
        Some(Box::new(MinMax {
            value: self.value.clone(),
            func_type: self.func_type
        }))
    }

    fn get_as_aggregate(&self) -> Option<Box<AggregateFunction>> {
        Some(Box::new(MinMax {
            value: self.value.clone(),
            func_type: self.func_type
        }))
    }

    fn is_scalar(&self) -> bool { true }

    fn is_aggregate(&self) -> bool { true }

    fn clone(&self) -> Self where Self: Sized {
        Clone::clone(&self)
    }
}

impl ScalarFunction for MinMax {
    fn get_return_type(&self, args: Vec<Expression>, schema: &Schema) -> Result<ColumnType, FunctionError> {
        let func_name = format!("{}", self.func_type);
        if args.len() != 1 {
            Err(FunctionError::TakesArguments(func_name, 1, args.len()))
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

impl AggregateFunction for MinMax {
    fn supports_distinct(&self) -> bool { false }

    fn clear_result(&mut self) {
        self.value = Literal::Null;
    }

    fn add_value(&mut self, value: Literal) {
        if self.value == Literal::Null {
            self.value = value;
        } else {
            self.value = literal_arithmetic(&self.value, &value, self.func_type.into()).unwrap();
        }
    }

    fn get_result(&self) -> Literal {
        self.value.clone()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StdDevVarType {
    StdDev,
    Variance,
    StdDevPopulation,
    VariancePopulation,
}

impl ::std::fmt::Display for StdDevVarType {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            StdDevVarType::StdDev => write!(f, "STDDEV"),
            StdDevVarType::Variance => write!(f, "VARIANCE"),
            StdDevVarType::StdDevPopulation => write!(f, "STDDEVP"),
            StdDevVarType::VariancePopulation => write!(f, "VARIANCEP"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StdDevVariance {
    count: u32,
    mean: f64,
    m2: f64,
    func_type: StdDevVarType,
}

impl StdDevVariance {
    pub fn std_dev() -> Box<Function> {
        Box::new(StdDevVariance {
            func_type: StdDevVarType::StdDev,
            ..Default::default()
        })
    }

    pub fn std_dev_population() -> Box<Function> {
        Box::new(StdDevVariance {
            func_type: StdDevVarType::StdDevPopulation,
            ..Default::default()
        })
    }
    pub fn variance() -> Box<Function> {
        Box::new(StdDevVariance {
            func_type: StdDevVarType::Variance,
            ..Default::default()
        })
    }

    pub fn variance_population() -> Box<Function> {
        Box::new(StdDevVariance {
            func_type: StdDevVarType::VariancePopulation,
            ..Default::default()
        })
    }
}

impl Default for StdDevVariance {
    fn default() -> StdDevVariance {
        StdDevVariance {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            func_type: StdDevVarType::StdDev,
        }
    }
}

impl Function for StdDevVariance {
    fn evaluate(&self, _env: &mut Option<&mut Environment>, _args: Vec<Expression>, _planner: &Option<&Planner>) -> FunctionResult {
        Ok(self.get_result())
    }

    fn get_as_scalar(&self) -> Option<Box<ScalarFunction>> {
        Some(Box::new(StdDevVariance {
            count: self.count,
            mean: self.mean,
            m2: self.m2,
            func_type: self.func_type,
        }))
    }

    fn get_as_aggregate(&self) -> Option<Box<AggregateFunction>> {
        Some(Box::new(StdDevVariance {
            count: self.count,
            mean: self.mean,
            m2: self.m2,
            func_type: self.func_type,
        }))
    }

    fn is_scalar(&self) -> bool { true }

    fn is_aggregate(&self) -> bool { true }

    fn clone(&self) -> Self where Self: Sized {
        Clone::clone(&self)
    }
}

impl ScalarFunction for StdDevVariance {
    fn get_return_type(&self, args: Vec<Expression>, _schema: &Schema) -> Result<ColumnType, FunctionError> {
        let func_name = format!("{}", self.func_type);
        if args.len() != 1 {
            Err(FunctionError::TakesArguments(func_name, 1, args.len()))
        } else {
            Ok(ColumnType::Double)
        }
    }
}

impl AggregateFunction for StdDevVariance {
    fn supports_distinct(&self) -> bool {
        // TODO: We can support this if we want.
        false
    }

    fn clear_result(&mut self) {
        self.count = 0;
        self.mean = 0.0;
        self.m2 = 0.0;
    }

    fn add_value(&mut self, value: Literal) {
        if value == Literal::Null {
            return;
        }

        // This algorithm is due to Welford, 1962.
        self.count += 1;
        let delta = literal_arithmetic(&value, &Literal::Double(self.mean), ArithmeticType::Minus).unwrap();
        if let Literal::Double(delta) = delta {
            self.mean += delta / (self.count as f64);
            let delta2 = literal_arithmetic(&value, &Literal::Double(self.mean), ArithmeticType::Minus).unwrap();
            if let Literal::Double(delta2) = delta2 {
                self.m2 += delta * delta2;
            } else {
                // See below.
                unreachable!()
            }
        } else {
            // Because the mean is a double, delta will always coerce to a double.
            unreachable!()
        }
    }

    fn get_result(&self) -> Literal {
        if self.count < 2 {
            return Literal::Double(::std::f64::NAN);
        }
        let count: f64 = self.count as f64;
        match self.func_type {
            StdDevVarType::StdDev => {
                Literal::Double((self.m2 / (count - 1.0)).sqrt())
            },
            StdDevVarType::Variance => {
                Literal::Double(self.m2 / (count - 1.0))
            },
            StdDevVarType::StdDevPopulation => {
                Literal::Double((self.m2 / count).sqrt())
            },
            StdDevVarType::VariancePopulation => {
                Literal::Double(self.m2 / count)
            },
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

    #[test]
    fn test_sum_avg_distinct() {
        let dir = TempDir::new("test_dbfiles").unwrap();
        let mut server = Server::with_data_path(dir.path());

        let stmts = statements(b"CREATE TABLE foo (a integer, b integer, c integer);\
                                 INSERT INTO foo VALUES (3, 1, 1);\
                                 INSERT INTO foo VALUES (3, 1, 2);\
                                 INSERT INTO foo VALUES (5, 1, 3);\
                                 INSERT INTO foo VALUES (5, 2, 4);\
                                 INSERT INTO foo VALUES (7, 2, 5);\
                                 INSERT INTO foo VALUES (12, 2, 6);\
        ").unwrap().1;
        for stmt in stmts {
            server.handle_command(stmt);
        }

        let ref mut select_command = statements(b"SELECT SUM(DISTINCT A) FROM foo;").unwrap().1[0];
        assert_eq!(Ok(Some(vec![TupleLiteral::from_iter(vec![27.into()])])),
        select_command.execute(&mut server, &mut ::std::io::sink()));

        let ref mut select_command = statements(b"SELECT AVERAGE(DISTINCT A) FROM foo;").unwrap().1[0];
        assert_eq!(Ok(Some(vec![TupleLiteral::from_iter(vec![6.75f64.into()])])),
        select_command.execute(&mut server, &mut ::std::io::sink()));

        let ref mut select_command = statements(b"SELECT SUM(DISTINCT A), B FROM foo GROUP BY B;").unwrap().1[0];
        let result: Vec<TupleLiteral> = select_command.execute(&mut server, &mut ::std::io::sink()).unwrap().unwrap();
        let expected: Vec<TupleLiteral> = vec![TupleLiteral::from_iter(vec![8.into(), 1.into()]),
                                               TupleLiteral::from_iter(vec![24.into(), 2.into()])];

        let expected_set: HashSet<TupleLiteral> = HashSet::from_iter(expected);
        let result_set: HashSet<TupleLiteral> = HashSet::from_iter(result);
        assert_eq!(expected_set, result_set);

        let ref mut select_command = statements(b"SELECT AVERAGE(DISTINCT A), B FROM foo GROUP BY B;").unwrap().1[0];
        let result: Vec<TupleLiteral> = select_command.execute(&mut server, &mut ::std::io::sink()).unwrap().unwrap();
        let expected: Vec<TupleLiteral> = vec![TupleLiteral::from_iter(vec![4.0f64.into(), 1.into()]),
                                               TupleLiteral::from_iter(vec![8.0f64.into(), 2.into()])];

        let expected_set: HashSet<TupleLiteral> = HashSet::from_iter(expected);
        let result_set: HashSet<TupleLiteral> = HashSet::from_iter(result);
        assert_eq!(expected_set, result_set);
    }

    #[test]
    fn test_min_max() {
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

        // MIN
        {
            let ref mut select_command = statements(b"SELECT MIN(A) FROM foo;").unwrap().1[0];
            assert_eq!(Ok(Some(vec![TupleLiteral::from_iter(vec![3.into()])])),
            select_command.execute(&mut server, &mut ::std::io::sink()));

            let ref mut select_command = statements(b"SELECT MIN(A) FROM foo GROUP BY B;").unwrap().1[0];
            let result: Vec<TupleLiteral> = select_command.execute(&mut server, &mut ::std::io::sink()).unwrap().unwrap();
            let expected: Vec<TupleLiteral> = vec![TupleLiteral::from_iter(vec![3.into()]),
                                                   TupleLiteral::from_iter(vec![7.into()])];

            let expected_set: HashSet<TupleLiteral> = HashSet::from_iter(expected);
            let result_set: HashSet<TupleLiteral> = HashSet::from_iter(result);
            assert_eq!(expected_set, result_set);
        }

        // MAX
        {
            let ref mut select_command = statements(b"SELECT MAX(A) FROM foo;").unwrap().1[0];
            assert_eq!(Ok(Some(vec![TupleLiteral::from_iter(vec![32.into()])])),
            select_command.execute(&mut server, &mut ::std::io::sink()));

            let ref mut select_command = statements(b"SELECT MAX(A) FROM foo GROUP BY B;").unwrap().1[0];
            let result: Vec<TupleLiteral> = select_command.execute(&mut server, &mut ::std::io::sink()).unwrap().unwrap();
            let expected: Vec<TupleLiteral> = vec![TupleLiteral::from_iter(vec![5.into()]),
                                                   TupleLiteral::from_iter(vec![32.into()])];

            let expected_set: HashSet<TupleLiteral> = HashSet::from_iter(expected);
            let result_set: HashSet<TupleLiteral> = HashSet::from_iter(result);
            assert_eq!(expected_set, result_set);
        }
    }

    fn assert_double_sets_equal(left: HashSet<TupleLiteral>, right: HashSet<TupleLiteral>) {
        const EPSILON: f64 = 1.0e-6;

        let mut left_set: Vec<f64> = Vec::new();
        let mut right_set: Vec<f64> = Vec::new();

        for row in left.iter() {
            if let Literal::Double(value) = row[0] {
                left_set.push(value);
            }
        }

        for row in right.iter() {
            if let Literal::Double(value) = row[0] {
                right_set.push(value);
            }
        }

        assert_eq!(left_set.len(), right_set.len());

        for value in left_set.iter() {
            assert!(right_set.iter().any(|rval| (rval - value).abs() < EPSILON));
        }
    }

    #[test]
    fn test_stddev_variance() {

        let dir = TempDir::new("test_dbfiles").unwrap();
        let mut server = Server::with_data_path(dir.path());

        {
            let stmts = statements(b"CREATE TABLE small (a integer);\
                                     INSERT INTO small VALUES (1);\
            ").unwrap().1;
            for stmt in stmts {
                server.handle_command(stmt);
            }

            let ref mut select_command = statements(b"SELECT VARIANCE(A) FROM small;").unwrap().1[0];
            let result = select_command.execute(&mut server, &mut ::std::io::sink()).unwrap().unwrap();
            let ref row = result[0];
            if let Literal::Double(value) = row[0] {
                assert!(value.is_nan(), "expected NaN, got {}", value);
            } else {
                panic!("expected double, got {}", row[0])
            }
        }

        {
            let stmts = statements(b"CREATE TABLE foo (a integer, b integer);\
                                     INSERT INTO foo VALUES (1, 1);\
                                     INSERT INTO foo VALUES (2, 1);\
                                     INSERT INTO foo VALUES (3, 1);\
                                     INSERT INTO foo VALUES (4, 1);\
                                     INSERT INTO foo VALUES (6, 2);\
                                     INSERT INTO foo VALUES (4, 2);\
                                     INSERT INTO foo VALUES (1, 2);\
                                     INSERT INTO foo VALUES (2, 2);\
            ").unwrap().1;
            for stmt in stmts {
                server.handle_command(stmt);
            }

            let ref mut select_command = statements(b"SELECT VARIANCE(A) FROM foo;").unwrap().1[0];
            assert_eq!(Ok(Some(vec![TupleLiteral::from_iter(vec![(167f64 / 56f64).into()])])),
            select_command.execute(&mut server, &mut ::std::io::sink()));

            let ref mut select_command = statements(b"SELECT VARIANCE(A) FROM foo GROUP BY B;").unwrap().1[0];
            let result: Vec<TupleLiteral> = select_command.execute(&mut server, &mut ::std::io::sink()).unwrap().unwrap();
            let expected: Vec<TupleLiteral> = vec![TupleLiteral::from_iter(vec![(5f64 / 3f64).into()]),
                                                   TupleLiteral::from_iter(vec![(59f64 / 12f64).into()])];

            let expected_set: HashSet<TupleLiteral> = HashSet::from_iter(expected);
            let result_set: HashSet<TupleLiteral> = HashSet::from_iter(result);
            assert_double_sets_equal(expected_set, result_set);

            let ref mut select_command = statements(b"SELECT STDDEV(A) FROM foo;").unwrap().1[0];
            assert_eq!(Ok(Some(vec![TupleLiteral::from_iter(vec![(167f64 / 56f64).sqrt().into()])])),
            select_command.execute(&mut server, &mut ::std::io::sink()));

            let ref mut select_command = statements(b"SELECT STDDEV(A) FROM foo GROUP BY B;").unwrap().1[0];
            let result: Vec<TupleLiteral> = select_command.execute(&mut server, &mut ::std::io::sink()).unwrap().unwrap();
            let expected: Vec<TupleLiteral> = vec![TupleLiteral::from_iter(vec![(5f64 / 3f64).sqrt().into()]),
                                                   TupleLiteral::from_iter(vec![(59f64 / 12f64).sqrt().into()])];

            let expected_set: HashSet<TupleLiteral> = HashSet::from_iter(expected);
            let result_set: HashSet<TupleLiteral> = HashSet::from_iter(result);
            assert_double_sets_equal(expected_set, result_set);

            let ref mut select_command = statements(b"SELECT VARIANCEP(A) FROM foo;").unwrap().1[0];
            assert_eq!(Ok(Some(vec![TupleLiteral::from_iter(vec![(167f64 / 64f64).into()])])),
            select_command.execute(&mut server, &mut ::std::io::sink()));

            let ref mut select_command = statements(b"SELECT VARIANCEP(A) FROM foo GROUP BY B;").unwrap().1[0];
            let result: Vec<TupleLiteral> = select_command.execute(&mut server, &mut ::std::io::sink()).unwrap().unwrap();
            let expected: Vec<TupleLiteral> = vec![TupleLiteral::from_iter(vec![(5f64 / 4f64).into()]),
                                                   TupleLiteral::from_iter(vec![(59f64 / 16f64).into()])];

            let expected_set: HashSet<TupleLiteral> = HashSet::from_iter(expected);
            let result_set: HashSet<TupleLiteral> = HashSet::from_iter(result);
            assert_double_sets_equal(expected_set, result_set);

            let ref mut select_command = statements(b"SELECT STDDEVP(A) FROM foo;").unwrap().1[0];
            assert_eq!(Ok(Some(vec![TupleLiteral::from_iter(vec![(167f64 / 64f64).sqrt().into()])])),
            select_command.execute(&mut server, &mut ::std::io::sink()));

            let ref mut select_command = statements(b"SELECT STDDEVP(A) FROM foo GROUP BY B;").unwrap().1[0];
            let result: Vec<TupleLiteral> = select_command.execute(&mut server, &mut ::std::io::sink()).unwrap().unwrap();
            let expected: Vec<TupleLiteral> = vec![TupleLiteral::from_iter(vec![(5f64 / 4f64).sqrt().into()]),
                                                   TupleLiteral::from_iter(vec![(59f64 / 16f64).sqrt().into()])];

            let expected_set: HashSet<TupleLiteral> = HashSet::from_iter(expected);
            let result_set: HashSet<TupleLiteral> = HashSet::from_iter(result);
            assert_double_sets_equal(expected_set, result_set);
        }
    }
}
