//! This module contains utilities for storing a function directory.

use super::coalesce::Coalesce;
use super::arithmetic::*;
use super::count::*;
use super::stats::*;
use super::trig::*;
use super::{Function, FunctionError};

use std::collections::HashMap;

/// This class is a directory of all functions recognized within NanoDB, including both simple and
/// aggregate functions.
pub struct Directory {
    map: HashMap<String, Box<Fn() -> Box<Function> + Sync>>,
}

impl Directory {
    /// Creates a new function directory.
    pub fn new() -> Directory {
        let mut result = Directory {
            map: HashMap::new()
        };
        result.init_builtin_functions();
        result
    }

    fn init_builtin_functions(&mut self) {
        self.add_function("COALESCE", Box::new(Coalesce::new));

        self.add_function("ABS", Box::new(Abs::new));
        self.add_function("CEIL", Box::new(Ceil::new));
        self.add_function("EXP", Box::new(Exp::new));
        self.add_function("FLOOR", Box::new(Floor::new));
        self.add_function("LN", Box::new(Ln::new));
        self.add_function("SQRT", Box::new(Sqrt::new));
        self.add_function("POWER", Box::new(Power::new));

        self.add_function("SIN", Box::new(Sin::new));
        self.add_function("COS", Box::new(Cos::new));
        self.add_function("TAN", Box::new(Tan::new));
        self.add_function("ASIN", Box::new(ASin::new));
        self.add_function("ACOS", Box::new(ACos::new));
        self.add_function("ATAN", Box::new(ATan::new));
        self.add_function("ATAN2", Box::new(ATan2::new));

        self.add_function("COUNT", Box::new(CountAggregate::count));
        self.add_function("COUNT#DISTINCT", Box::new(CountAggregate::distinct));
        self.add_function("COUNT#STAR", Box::new(CountStar::count));
        self.add_function("AVERAGE", Box::new(SumAverage::average));
        self.add_function("AVG", Box::new(SumAverage::average));
        self.add_function("MEAN", Box::new(SumAverage::average));
        self.add_function("AVERAGE#DISTINCT", Box::new(SumAverage::average_distinct));
        self.add_function("AVG#DISTINCT", Box::new(SumAverage::average_distinct));
        self.add_function("MEAN#DISTINCT", Box::new(SumAverage::average_distinct));
        self.add_function("SUM", Box::new(SumAverage::sum));
        self.add_function("SUM#DISTINCT", Box::new(SumAverage::sum_distinct));
        self.add_function("MIN", Box::new(MinMax::min));
        self.add_function("MAX", Box::new(MinMax::max));
        self.add_function("STDDEV", Box::new(StdDevVariance::std_dev));
        self.add_function("VARIANCE", Box::new(StdDevVariance::variance));
        self.add_function("STDDEVP", Box::new(StdDevVariance::std_dev_population));
        self.add_function("VARIANCEP", Box::new(StdDevVariance::variance_population));
    }

    fn add_function<S: Into<String>, F: Fn() -> Box<Function> + 'static + Sync>(&mut self, name: S, f: Box<F>) {
        // Probably, function names will come in cleaned up, but this will make doubly sure!
        let name: String = name.into().as_str().to_uppercase();
        self.map.insert(name, f);
    }

    /// Retrieves a function from the directory by name, if it exists.
    pub fn get<S: Into<String>>(&self, name: S) -> Result<Box<Function>, FunctionError> {
        let name: String = name.into().as_str().to_uppercase();
        match self.map.get(&name) {
            Some(ref constructor) => {
                Ok(constructor())
            },
            None => {
                Err(FunctionError::DoesNotExist(name.clone()))
            }
        }
    }
}