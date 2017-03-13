//! This module provides the nested-loops join plan node.

use ::Schema;
use ::expressions::{Environment, Expression, ExpressionError, JoinConditionType, JoinType, Literal};
use ::queries::{PlanError, PlanNode, PlanResult};
use ::storage::{Pinnable, Tuple, TupleLiteral};

/// This plan node implements a nested-loops join operation, which can support arbitrary join
/// conditions but is also the slowest join implementation.
pub struct NestedLoopJoinNode<'a> {
    /// The left child of the join node.
    left: Box<PlanNode + 'a>,
    /// The right child of the join node.
    right: Box<PlanNode + 'a>,
    /// The type of join being performed.
    join_type: JoinType,
    /// The condition type of the join being performed.
    condition_type: JoinConditionType,
    /// Whether there are more tuples to process or not.
    done: bool,
    /// Whether the schema is swapped (e.g. in a right outer join).
    schema_swapped: bool,
    /// The current left tuple (if it exists).
    left_tuple: Option<TupleLiteral>,
    /// The current right tuple (if it exists).
    right_tuple: Option<TupleLiteral>,
    /// The current joined tuple (if it exists).
    current_tuple: Option<Box<Tuple>>,
    /// The output schema for use by outside sources.
    output_schema: Option<Schema>,
    /// The predicate for testing the join.
    predicate: Option<Expression>,
}

impl<'a> NestedLoopJoinNode<'a> {
    /// Instantiate a new nested-loops join node.
    pub fn new(left: Box<PlanNode + 'a>,
               right: Box<PlanNode + 'a>,
               join_type: JoinType,
               condition_type: JoinConditionType,
               predicate: Option<Expression>)
               -> NestedLoopJoinNode<'a> {
        NestedLoopJoinNode {
            left: left,
            right: right,
            join_type: join_type,
            condition_type: condition_type,
            done: false,
            schema_swapped: false,
            left_tuple: None,
            right_tuple: None,
            current_tuple: None,
            output_schema: None,
            predicate: predicate,
        }
    }

    fn get_tuples_to_join(&mut self) -> PlanResult<bool> {
        self.current_tuple = None;
        if self.left_tuple.is_none() {
            self.left_tuple = try!(self.left.get_next_tuple()).map(|t| TupleLiteral::from_tuple(t));
            if self.left_tuple.is_none() {
                self.done = true;
                return Ok(false);
            }
        }

        if let Some(right) = self.right_tuple.as_mut() {
            try!(right.unpin());
        }
        self.right_tuple = try!(self.right.get_next_tuple()).map(|t| TupleLiteral::from_tuple(t));

        // If inner table is exhausted, move back to start
        // and increment outer table.
        if self.right_tuple.is_none() {
            if let Some(left) = self.left_tuple.as_mut() {
                try!(left.unpin());
            }
            self.left_tuple = try!(self.left.get_next_tuple()).map(|t| TupleLiteral::from_tuple(t));
            //            self.matched = true;

            if self.left_tuple.is_none() {
                self.done = true;
                return Ok(false);
            }

            self.right.initialize();

            // Increment the inner tuple.
            self.right_tuple = try!(self.right.get_next_tuple()).map(|t| TupleLiteral::from_tuple(t));
        }

        Ok(!self.done)
    }

    fn can_join_tuples(&mut self) -> PlanResult<bool> {
        if self.predicate.is_none() {
            return Ok(true);
        }
        let predicate = self.predicate.clone().unwrap();
        let mut env = Environment::new();

        assert!(self.left_tuple.is_some());
        assert!(self.right_tuple.is_some());

        if let Some(mut boxed) = self.left_tuple.as_mut() {
            env.add_tuple_ref(self.left.get_schema(), &mut *boxed);
        }
        if let Some(mut boxed) = self.right_tuple.as_mut() {
            env.add_tuple_ref(self.right.get_schema(), &mut *boxed);
        }
        let result = predicate.evaluate(&mut Some(&mut env));
        match result {
            Ok(l) => {
                match l {
                    Literal::True => Ok(true),
                    Literal::False => Ok(false),
                    _ => Err(PlanError::CouldNotApplyPredicate(ExpressionError::NotBoolean(l))),
                }
            }
            Err(e) => Err(PlanError::CouldNotApplyPredicate(e)),
        }
    }

    fn join_tuples<T1: Tuple + ?Sized, T2: Tuple + ?Sized>(&mut self, left: &mut T1, right: &mut T2) {
        let mut result = TupleLiteral::new();

        if !self.schema_swapped {
            result.append_tuple(left);
            result.append_tuple(right);
        } else {
            result.append_tuple(right);
            result.append_tuple(left);
        }

        self.current_tuple = Some(Box::new(result));
    }
}


impl<'a> PlanNode for NestedLoopJoinNode<'a> {
    fn get_schema(&self) -> Schema {
        self.output_schema.clone().unwrap_or(Schema::new())
    }

    fn get_next_tuple(&mut self) -> PlanResult<Option<&mut Tuple>> {
        if self.output_schema.is_none() {
            return Err(PlanError::NodeNotPrepared);
        }

        if self.done {
            return Ok(None);
        }

        while try!(self.get_tuples_to_join()) {
            if try!(self.can_join_tuples()) {
                // This step won't occur unless the left and right tuple are set
                let mut left = self.left_tuple.clone().unwrap();
                let mut right = self.right_tuple.clone().unwrap();
                self.join_tuples(&mut left, &mut right);
                break;
            }
        }

        Ok(match self.current_tuple.as_mut() {
            Some(mut boxed_tuple) => Some(&mut **boxed_tuple),
            _ => None,
        })
    }

    fn prepare(&mut self) -> PlanResult<()> {
        try!(self.left.prepare());
        try!(self.right.prepare());

        let (left_schema, right_schema) = (self.left.get_schema(), self.right.get_schema());

        let mut schema = Schema::new();

        if !self.schema_swapped {
            try!(schema.add_columns(left_schema));
            try!(schema.add_columns(right_schema));
        } else {
            try!(schema.add_columns(right_schema));
            try!(schema.add_columns(left_schema));
        }

        self.output_schema = Some(schema);

        Ok(())
    }
}
