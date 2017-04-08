use std::collections::HashSet;
use std::default::Default;

use ::expressions::{Environment, Expression, Literal};
use ::functions::{AggregateFunction, Function, FunctionError, FunctionResult, ScalarFunction};
use ::queries::Planner;
use ::relations::{ColumnType, Schema};

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
            None => Literal::Null,
        }
    }
}
