//! This module provides the literal plan node. This node is currently only used for testing.

use ::Schema;
use ::queries::plan_nodes::PlanNode;
use ::queries::planning::{PlanError, PlanResult};
use ::storage::{Tuple, TupleLiteral};

/// A plan node that is created with a set of tuple literals and just generates those.
#[derive(Clone, Debug)]
pub struct LiteralNode {
    data: Vec<TupleLiteral>,
    index: usize,
    length: usize,
    schema: Schema,
}

impl LiteralNode {
    /// Create a new literal node from an iterator of TupleLiterals and a given schema.
    pub fn from_iter<I: Iterator<Item=TupleLiteral>>(iter: I, schema: Schema) -> PlanResult<LiteralNode> {
        let data: Vec<TupleLiteral> = iter.collect();
        let length = data.len();

        let schema_size = schema.num_columns();
        for t in data.iter() {
            let tup_size = t.len();
            if tup_size != schema_size {
                return Err(PlanError::WrongArity(tup_size, schema_size));
            }
        }

        // TODO: Should check that tuples match schema too.

        Ok(LiteralNode {
            data: data,
            index: 0,
            length: length,
            schema: schema,
        })
    }
}

impl PlanNode for LiteralNode {
    fn get_schema(&self) -> Schema {
        self.schema.clone()
    }

    fn get_next_tuple(&mut self) -> PlanResult<Option<&mut Tuple>> {
        Ok(if self.index >= self.length {
            None
        } else {
            let result = &mut self.data[self.index];
            self.index += 1;
            Some(result)
        })
    }

    fn prepare(&mut self) -> PlanResult<()> {
        self.initialize();

        Ok(())
    }

    fn initialize(&mut self) {
        self.index = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::expressions::Literal;
    use ::queries::PlanNode;
    use ::relations::{ColumnInfo, ColumnType};
    use ::storage::TupleLiteral;
    use ::Schema;

    #[test]
    fn test_wrong_arity() {
        let empty_schema = Schema::new();
        let schema = Schema::with_columns(vec![ColumnInfo::with_name(ColumnType::Integer, "FOO")]).unwrap();

        let no_tuples = vec![];
        let right_size = vec![TupleLiteral::from_iter(vec![Literal::Int(3)])];
        let wrong_size = vec![TupleLiteral::from_iter(vec![Literal::Int(3), Literal::Int(4)])];
        let wrong_size2 = vec![TupleLiteral::from_iter(vec![])];

        // An empty set will work with any schema.
        assert!(LiteralNode::from_iter(no_tuples.clone().into_iter(), empty_schema.clone()).is_ok());
        assert!(LiteralNode::from_iter(no_tuples.clone().into_iter(), schema.clone()).is_ok());

        assert!(LiteralNode::from_iter(right_size.clone().into_iter(), empty_schema.clone()).is_err());
        assert!(LiteralNode::from_iter(right_size.clone().into_iter(), schema.clone()).is_ok());

        assert!(LiteralNode::from_iter(wrong_size.clone().into_iter(), empty_schema.clone()).is_err());
        assert!(LiteralNode::from_iter(wrong_size.clone().into_iter(), schema.clone()).is_err());

        assert!(LiteralNode::from_iter(wrong_size2.clone().into_iter(), empty_schema.clone()).is_ok());
        assert!(LiteralNode::from_iter(wrong_size2.clone().into_iter(), schema.clone()).is_err());
    }

    #[test]
    fn test_node() {
        let schema = Schema::with_columns(vec![ColumnInfo::with_name(ColumnType::Integer, "FOO"),
                                               ColumnInfo::with_name(ColumnType::Integer, "BAR")])
            .unwrap();
        let tuples = vec![
            TupleLiteral::from_iter(vec![Literal::Int(3), Literal::Int(4)]),
            TupleLiteral::from_iter(vec![Literal::Int(4), Literal::Int(5)])
        ];
        let mut node = LiteralNode::from_iter(tuples.clone().into_iter(), schema).unwrap();

        let mut result: Vec<TupleLiteral> = Vec::new();
        while let Some(tuple) = node.get_next_tuple().unwrap() {
            result.push(TupleLiteral::from_tuple(tuple));
        }

        assert_eq!(tuples, result);

        // Can run it multiple times.
        node.initialize();
        result.clear();
        while let Some(tuple) = node.get_next_tuple().unwrap() {
            result.push(TupleLiteral::from_tuple(tuple));
        }

        assert_eq!(tuples, result);
    }
}