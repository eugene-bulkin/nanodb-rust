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
    /// Whether a tuple has been matched (for outer joins).
    matched: bool,
    /// Whether the left child is empty (only for full outer joins).
    left_empty: bool,
}

impl<'a> NestedLoopJoinNode<'a> {
    /// Instantiate a new nested-loops join node.
    pub fn new(left: Box<PlanNode + 'a>,
               right: Box<PlanNode + 'a>,
               join_type: JoinType,
               condition_type: JoinConditionType,
               predicate: Option<Expression>)
               -> NestedLoopJoinNode<'a> {
        match join_type {
            JoinType::RightOuter => {
                // We can't naturally do a RIGHT OUTER join with a nested-loop join node, but we can get
                // around that limitation by emulating one using a schema swap.
                NestedLoopJoinNode {
                    // Note the swap here!
                    left: right,
                    right: left,
                    // Now this is a LEFT OUTER join with swapped schemas.
                    join_type: JoinType::LeftOuter,
                    condition_type: condition_type,
                    done: false,
                    schema_swapped: true,
                    left_tuple: None,
                    right_tuple: None,
                    current_tuple: None,
                    output_schema: None,
                    predicate: predicate,
                    matched: false,
                    left_empty: false,
                }
            },
            _ => {
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
                    matched: false,
                    left_empty: false,
                }
            }
        }
    }

    fn get_tuples_to_join(&mut self) -> PlanResult<bool> {
        // Reset current tuple so that we ensure we get a fresh one if it exists.
        self.current_tuple = None;

        if self.right_tuple.is_none() {
            self.left_tuple = try!(self.left.get_next_tuple()).map(|t| TupleLiteral::from_tuple(t));
            self.matched = false;

            if self.left_tuple.is_none() {
                return Ok(false);
            }
            self.right.initialize();
        }
        self.right_tuple = try!(self.right.get_next_tuple()).map(|t| TupleLiteral::from_tuple(t));

        Ok(true)
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

    fn join_tuples<T1: Tuple + ? Sized, T2: Tuple + ? Sized>(&mut self, left: &mut T1, right: &mut T2) {
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

        match self.join_type {
            JoinType::Inner | JoinType::Cross => {
                while try!(self.get_tuples_to_join()) {
                    if try!(self.can_join_tuples()) {
                        // This step won't occur unless the left and right tuple are set
                        let mut left = self.left_tuple.clone().unwrap();
                        let mut right = self.right_tuple.clone().unwrap();
                        self.join_tuples(&mut left, &mut right);
                        break;
                    }
                }
            },
            JoinType::LeftOuter => {
                while try!(self.get_tuples_to_join()) {
                    if self.right_tuple.is_some() && try!(self.can_join_tuples()) {
                        self.matched = true;
                        // If a match is found, return joined pair,
                        // switched if needed.
                        let mut left = self.left_tuple.clone().unwrap();
                        let mut right = self.right_tuple.clone().unwrap();
                        self.join_tuples(&mut left, &mut right);
                        break;
                    } else if !self.matched && self.right_tuple.is_none() {
                        // For left outer join, include the left tuple if it
                        // hasn't been matched yet.
                        let right_schema: Schema = self.right.get_schema();
                        let mut left = self.left_tuple.clone().unwrap();
                        let mut right = TupleLiteral::null(right_schema.num_columns());
                        self.join_tuples(&mut left, &mut right);
                        break;
                    }
                }
            },
            JoinType::FullOuter => {
                // TODO: This one is pretty tricky.
                // One way to do this is to leverage file pointers to keep track of the tuples we've
                // already seen, while storing the literal itself in an unused list.
                // There may be other ways, though.
                return Err(PlanError::Unimplemented);
            },
            JoinType::RightOuter => {
                // This shouldn't happen since we do a swap!
                return Err(PlanError::Unimplemented);
            }
            _ => {
                // TODO: implement antijoin and semijoin.
                return Err(PlanError::Unimplemented);
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

    fn initialize(&mut self) {
        self.done = false;
        self.left_tuple = None;
        self.right_tuple = None;
    }
}
