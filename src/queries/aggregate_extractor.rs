use std::collections::HashMap;

use ::expressions::{Expression, ExpressionError, ExpressionProcessor};
use ::functions::Directory;

lazy_static! {
    static ref DIRECTORY: Directory = Directory::new();
}

/// This expression-processor implementation looks for aggregate function calls within an
/// expression, extracts them and gives them a name, then replaces the aggregate calls with
/// column-lookups using the generated names.
// TODO: This class needs to be updated to combine duplicated aggregate expressions, so that they
// are only computed once and then reused.
pub struct AggregateFunctionExtractor {
    aggregate_calls: HashMap<String, Expression>,
    current_aggregate: Option<Expression>,
    found: bool,
}

impl AggregateFunctionExtractor {
    /// Create a new aggregate function extractor.
    pub fn new() -> AggregateFunctionExtractor {
        AggregateFunctionExtractor {
            aggregate_calls: HashMap::new(),
            current_aggregate: None,
            found: false
        }
    }

    /// Whether aggregates were found or not.
    pub fn found_aggregates(&self) -> bool { self.found }

    /// Clears the found flag.
    pub fn clear_found_flag(&mut self) {
        self.found = false;
    }

    /// Returns a map of the aggregate calls.
    pub fn get_aggregate_calls(&self) -> Vec<(String, Expression)> {
        self.aggregate_calls.clone().into_iter().collect()
    }
}

impl ExpressionProcessor for AggregateFunctionExtractor {
    fn enter(&mut self, node: &Expression) -> Result<(), ExpressionError> {
        if let Expression::Function { ref name, .. } = *node {
            let func = try!(DIRECTORY.get(name.as_ref()));
            if func.is_aggregate() {
                if let Some(ref aggregate) = self.current_aggregate {
                    return Err(ExpressionError::NestedAggregateCall {
                        parent: aggregate.clone(),
                        nested: node.clone()
                    });
                } else {
                    self.current_aggregate = Some(node.clone());
                    self.found = true;
                }
            }
        }
        Ok(())
    }

    fn leave(&mut self, node: &Expression) -> Result<Expression, ExpressionError> {
        if let Expression::Function { ref name, .. } = *node {
            let func = try!(DIRECTORY.get(name.as_ref()));
            if func.is_aggregate() {
                if self.current_aggregate != Some(node.clone()) {
                    // This would be a bug.
                    return Err(ExpressionError::UnexpectedAggregate {
                        expected: self.current_aggregate.clone().unwrap_or(Expression::Null),
                        received: node.clone()
                    });
                }

                // We will compute the aggregate separately, so replace the aggregate call with a
                // placeholder column name.
                let name = format!("#AGG{}", self.aggregate_calls.len() + 1);
                self.aggregate_calls.insert(name.clone(), self.current_aggregate.clone().unwrap());
                self.current_aggregate = None;


                // This is the replacement.
                return Ok(Expression::ColumnValue((None, Some(name))));
            }
        }
        Ok(node.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ::expressions::Expression::*;
    use ::expressions::{ExpressionError};
    use ::functions::FunctionError;

    #[test]
    fn test_processor() {
        let mut extractor = AggregateFunctionExtractor::new();

        let mut expr1 = Function {
            name: "COUNT".into(),
            distinct: false,
            args: vec![ColumnValue((None, Some("A".into())))]
        };
        let mut expr2 = Int(4);
        let mut expr3 = Function {
            name: "BAR".into(),
            distinct: false,
            args: vec![ColumnValue((None, Some("A".into())))]
        };
        let mut expr4 = Function {
            name: "COUNT".into(),
            distinct: false,
            args: vec![expr1.clone()]
        };

        assert_eq!(Ok(ColumnValue((None, Some("#AGG1".into())))), expr1.traverse(&mut extractor));
        assert_eq!(Ok(Int(4)), expr2.traverse(&mut extractor));
        assert_eq!(Err(FunctionError::DoesNotExist("BAR".into()).into()), expr3.traverse(&mut extractor));
        assert_eq!(Err(ExpressionError::NestedAggregateCall {
            parent: expr4.clone(),
            nested: expr1.clone(),
        }), expr4.traverse(&mut extractor));
    }
}