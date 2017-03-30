//! This module provides the nested-loops join plan node.

use std::collections::{HashMap, HashSet};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use ::Schema;
use ::expressions::{Environment, Expression, ExpressionError, JoinType, Literal};
use ::queries::{PlanError, PlanNode, PlanResult};
use ::storage::{Tuple, TupleLiteral};

/// A struct containing information about the current join status of the node.
pub struct JoinData {
    /// Whether a tuple has been matched (for outer joins).
    matched: bool,
    /// Whether the left child is empty (only for full outer joins).
    left_empty: bool,
    /// Associates a tuple with whether it's been joined. The keys are the hashes.
    used_tuples: HashMap<u64, bool>,
    unused_tuples: HashSet<TupleLiteral>,
    unused_tuple_iterator: Option<Box<Iterator<Item=TupleLiteral>>>,
}

impl Default for JoinData {
    fn default() -> Self {
        JoinData {
            matched: false,
            left_empty: true,
            used_tuples: HashMap::new(),
            unused_tuples: HashSet::new(),
            unused_tuple_iterator: None,
        }
    }
}

/// This plan node implements a nested-loops join operation, which can support arbitrary join
/// conditions but is also the slowest join implementation.
pub struct NestedLoopJoinNode<'a> {
    /// The left child of the join node.
    left: Box<PlanNode + 'a>,
    /// The right child of the join node.
    right: Box<PlanNode + 'a>,
    /// The type of join being performed.
    join_type: JoinType,
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
    /// Information used to join nodes.
    join_data: JoinData,
}

impl<'a> ::std::ops::Deref for NestedLoopJoinNode<'a> {
    type Target = JoinData;
    fn deref(&self) -> &Self::Target {
        &self.join_data
    }
}

impl<'a> ::std::ops::DerefMut for NestedLoopJoinNode<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.join_data
    }
}

impl<'a> NestedLoopJoinNode<'a> {
    /// Instantiate a new nested-loops join node.
    pub fn new(left: Box<PlanNode + 'a>,
               right: Box<PlanNode + 'a>,
               join_type: JoinType,
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
                    done: false,
                    schema_swapped: true,
                    left_tuple: None,
                    right_tuple: None,
                    current_tuple: None,
                    output_schema: None,
                    predicate: predicate,
                    join_data: Default::default(),
                }
            },
            _ => {
                NestedLoopJoinNode {
                    left: left,
                    right: right,
                    join_type: join_type,
                    done: false,
                    schema_swapped: false,
                    left_tuple: None,
                    right_tuple: None,
                    current_tuple: None,
                    output_schema: None,
                    predicate: predicate,
                    join_data: Default::default(),
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

    fn get_remaining_tuples(&mut self) -> PlanResult<bool> {
        if self.left_tuple.is_some() {
            return Ok(false);
        }

        if self.unused_tuple_iterator.is_none() {
            self.unused_tuple_iterator = Some(Box::new(self.unused_tuples.clone().into_iter()));
        }

        let mut iter = self.unused_tuple_iterator.take().unwrap();
        let result = match iter.next() {
            Some(next) => {
                self.right_tuple = Some(next);
                Ok(true)
            },
            None => Ok(false)
        };
        self.unused_tuple_iterator = Some(iter);
        result
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
                    if self.right_tuple.is_some() && try!(self.can_join_tuples()) {
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
                while try!(self.get_tuples_to_join()) {
                    self.left_empty = false;
                    if self.right_tuple.is_some() {
                        let literal = TupleLiteral::from_tuple(self.right_tuple.as_mut().unwrap());
                        let hash = {
                            let mut hasher = DefaultHasher::new();
                            literal.hash(&mut hasher);
                            hasher.finish()
                        };
                        if !self.used_tuples.contains_key(&hash) {
                            self.unused_tuples.insert(literal.clone());
                            self.used_tuples.insert(hash, false);
                        }
                        if self.left_tuple.is_some() && try!(self.can_join_tuples()) {
                            self.matched = true;
                            self.used_tuples.insert(hash, true);

                            let mut right = literal.clone();
                            self.unused_tuples.remove(&right);

                            let mut left = self.left_tuple.clone().unwrap();

                            self.join_tuples(&mut left, &mut right);
                            break;
                        }
                    } else if !self.matched && self.right_tuple.is_none() && self.left_tuple.is_some() {
                        let right_schema: Schema = self.right.get_schema();
                        let mut left = self.left_tuple.clone().unwrap();
                        let mut right = TupleLiteral::null(right_schema.num_columns());
                        self.join_tuples(&mut left, &mut right);
                        break;
                    }
                }

                if self.left_empty {
                    self.right_tuple = try!(self.right.get_next_tuple()).map(|t| TupleLiteral::from_tuple(t));

                    if self.right_tuple.is_none() {
                        return Ok(None);
                    }
                    let left_schema: Schema = self.left.get_schema();
                    let mut left = TupleLiteral::null(left_schema.num_columns());
                    let mut right = self.right_tuple.clone().unwrap();
                    self.join_tuples(&mut left, &mut right);
                } else {
                    while try!(self.get_remaining_tuples()) {
                        let left_schema: Schema = self.left.get_schema();
                        let mut left = TupleLiteral::null(left_schema.num_columns());
                        let mut right = self.right_tuple.clone().unwrap();
                        self.join_tuples(&mut left, &mut right);
                    }
                }
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

        try!(schema.add_columns(left_schema));
        try!(schema.add_columns(right_schema));

        self.output_schema = Some(schema);

        Ok(())
    }

    fn initialize(&mut self) {
        self.done = false;
        self.left_tuple = None;
        self.right_tuple = None;
    }
}


#[cfg(test)]
mod tests {
    use std::iter::empty;

    use super::*;
    use ::Schema;
    use ::expressions::{CompareType, JoinType, Expression, Literal};
    use ::expressions::Expression::*;
    use ::queries::plan_nodes::LiteralNode;
    use ::relations::{ColumnInfo, ColumnType};
    use ::storage::TupleLiteral;

    lazy_static! {
        static ref LEFT_A: ColumnInfo = ColumnInfo::with_table_name(ColumnType::Integer, "A", "LEFT");
        static ref LEFT_B: ColumnInfo = ColumnInfo::with_table_name(ColumnType::Integer, "B", "LEFT");
        static ref RIGHT_A: ColumnInfo = ColumnInfo::with_table_name(ColumnType::Integer, "A", "RIGHT");
        static ref RIGHT_C: ColumnInfo = ColumnInfo::with_table_name(ColumnType::Integer, "C", "RIGHT");

        static ref PREDICATE: Expression = {
            let left_name = (Some("LEFT".into()), Some("A".into()));
            let right_name = (Some("RIGHT".into()), Some("A".into()));
            Compare(Box::new(ColumnValue(left_name)), CompareType::Equals, Box::new(ColumnValue(right_name)))
        };

        static ref LEFT_SCHEMA: Schema = Schema::with_columns(vec ! [LEFT_A.clone(), LEFT_B.clone()]).unwrap();
        static ref RIGHT_SCHEMA: Schema = Schema::with_columns(vec ! [RIGHT_A.clone(), RIGHT_C.clone()]).unwrap();

        static ref LEFT_TUPLES: Vec<TupleLiteral> = vec![
            TupleLiteral::from_iter(vec![1i32.into(), 2i32.into()]),
            TupleLiteral::from_iter(vec![3i32.into(), 4i32.into()]),
        ];
        static ref RIGHT_TUPLES: Vec<TupleLiteral> = vec![
            TupleLiteral::from_iter(vec![3i32.into(), 6i32.into()]),
            TupleLiteral::from_iter(vec![7i32.into(), 8i32.into()]),
        ];

        static ref EMPTY: Vec<TupleLiteral> = vec![];
    }

    fn generate_node_results(left: Box<PlanNode>, right: Box<PlanNode>, join_type: JoinType, predicate: Option<Expression>) -> (Schema, Vec<TupleLiteral>) {
        let mut node = NestedLoopJoinNode::new(left, right, join_type, predicate);
        node.prepare().unwrap();

        let schema = node.get_schema();

        let mut result: Vec<TupleLiteral> = Vec::new();
        while let Some(tuple) = node.get_next_tuple().unwrap() {
            result.push(TupleLiteral::from_tuple(tuple));
        }
        (schema, result)
    }

    #[test]
    fn test_cross_join() {
        let result_schema = LEFT_SCHEMA.clone() + RIGHT_SCHEMA.clone();

        // Test empty joins
        {
            let left_node = LiteralNode::from_iter(LEFT_TUPLES.clone().into_iter(), LEFT_SCHEMA.clone()).unwrap();
            let right_node = LiteralNode::from_iter(RIGHT_TUPLES.clone().into_iter(), RIGHT_SCHEMA.clone()).unwrap();

            {
                let empty_node = LiteralNode::from_iter(empty::<TupleLiteral>(), RIGHT_SCHEMA.clone()).unwrap();
                let (schema, result) = generate_node_results(Box::new(left_node), Box::new(empty_node), JoinType::Cross, None);

                assert_eq!(schema, result_schema);
                assert_eq!(EMPTY.clone(), result);
            }

            {
                let empty_node = LiteralNode::from_iter(empty::<TupleLiteral>(), LEFT_SCHEMA.clone()).unwrap();
                let (schema, result) = generate_node_results(Box::new(empty_node), Box::new(right_node), JoinType::Cross, None);

                assert_eq!(schema, result_schema);
                assert_eq!(EMPTY.clone(), result);
            }
        }

        // Test non-empty joins
        {
            let left_node = LiteralNode::from_iter(LEFT_TUPLES.clone().into_iter(), LEFT_SCHEMA.clone()).unwrap();
            let right_node = LiteralNode::from_iter(RIGHT_TUPLES.clone().into_iter(), RIGHT_SCHEMA.clone()).unwrap();

            let (schema, result) = generate_node_results(Box::new(left_node), Box::new(right_node), JoinType::Cross, None);

            assert_eq!(schema, result_schema);
            assert_eq!(vec![
                TupleLiteral::from_iter(vec![1i32.into(), 2i32.into(), 3i32.into(), 6i32.into()]),
                TupleLiteral::from_iter(vec![1i32.into(), 2i32.into(), 7i32.into(), 8i32.into()]),
                TupleLiteral::from_iter(vec![3i32.into(), 4i32.into(), 3i32.into(), 6i32.into()]),
                TupleLiteral::from_iter(vec![3i32.into(), 4i32.into(), 7i32.into(), 8i32.into()]),
            ], result);
        }
    }

    #[test]
    fn test_inner_join() {
        let result_schema = LEFT_SCHEMA.clone() + RIGHT_SCHEMA.clone();

        // Test empty joins
        {
            let left_node = LiteralNode::from_iter(LEFT_TUPLES.clone().into_iter(), LEFT_SCHEMA.clone()).unwrap();
            let right_node = LiteralNode::from_iter(RIGHT_TUPLES.clone().into_iter(), RIGHT_SCHEMA.clone()).unwrap();

            {
                let empty_node = LiteralNode::from_iter(empty::<TupleLiteral>(), RIGHT_SCHEMA.clone()).unwrap();
                let (schema, result) = generate_node_results(Box::new(left_node), Box::new(empty_node), JoinType::Inner, None);

                assert_eq!(schema, result_schema);
                assert_eq!(EMPTY.clone(), result);
            }

            {
                let empty_node = LiteralNode::from_iter(empty::<TupleLiteral>(), LEFT_SCHEMA.clone()).unwrap();
                let (schema, result) = generate_node_results(Box::new(empty_node), Box::new(right_node), JoinType::Inner, None);

                assert_eq!(schema, result_schema);
                assert_eq!(EMPTY.clone(), result);
            }
        }

        // Test INNER JOIN without predicate.
        {
            let left_node = LiteralNode::from_iter(LEFT_TUPLES.clone().into_iter(), LEFT_SCHEMA.clone()).unwrap();
            let right_node = LiteralNode::from_iter(RIGHT_TUPLES.clone().into_iter(), RIGHT_SCHEMA.clone()).unwrap();
            let (schema, result) = generate_node_results(Box::new(left_node), Box::new(right_node), JoinType::Inner, None);

            assert_eq!(schema, result_schema);
            assert_eq!(vec![
                TupleLiteral::from_iter(vec![1i32.into(), 2i32.into(), 3i32.into(), 6i32.into()]),
                TupleLiteral::from_iter(vec![1i32.into(), 2i32.into(), 7i32.into(), 8i32.into()]),
                TupleLiteral::from_iter(vec![3i32.into(), 4i32.into(), 3i32.into(), 6i32.into()]),
                TupleLiteral::from_iter(vec![3i32.into(), 4i32.into(), 7i32.into(), 8i32.into()]),
            ], result);
        }

        // Test INNER JOIN with predicate.
        {
            let left_node = LiteralNode::from_iter(LEFT_TUPLES.clone().into_iter(), LEFT_SCHEMA.clone()).unwrap();
            let right_node = LiteralNode::from_iter(RIGHT_TUPLES.clone().into_iter(), RIGHT_SCHEMA.clone()).unwrap();
            let (schema, result) = generate_node_results(Box::new(left_node), Box::new(right_node),
                                                         JoinType::Inner,
                                                         Some(PREDICATE.clone()));

            assert_eq!(schema, result_schema);
            assert_eq!(vec![
                TupleLiteral::from_iter(vec![3i32.into(), 4i32.into(), 3i32.into(), 6i32.into()]),
            ], result);
        }
    }

    #[test]
    fn test_left_outer_join() {
        let result_schema = LEFT_SCHEMA.clone() + RIGHT_SCHEMA.clone();

        // Test empty joins
        {
            let left_node = LiteralNode::from_iter(LEFT_TUPLES.clone().into_iter(), LEFT_SCHEMA.clone()).unwrap();
            let right_node = LiteralNode::from_iter(RIGHT_TUPLES.clone().into_iter(), RIGHT_SCHEMA.clone()).unwrap();

            {
                let empty_node = LiteralNode::from_iter(empty::<TupleLiteral>(), RIGHT_SCHEMA.clone()).unwrap();
                let (schema, result) = generate_node_results(Box::new(left_node), Box::new(empty_node), JoinType::LeftOuter, Some(PREDICATE.clone()));

                assert_eq!(schema, result_schema);
                assert_eq!(vec![
                    TupleLiteral::from_iter(vec![1i32.into(), 2i32.into(), Literal::Null, Literal::Null]),
                    TupleLiteral::from_iter(vec![3i32.into(), 4i32.into(), Literal::Null, Literal::Null]),
                ], result);
            }

            {
                let empty_node = LiteralNode::from_iter(empty::<TupleLiteral>(), LEFT_SCHEMA.clone()).unwrap();
                let (schema, result) = generate_node_results(Box::new(empty_node), Box::new(right_node), JoinType::LeftOuter, Some(PREDICATE.clone()));

                assert_eq!(schema, result_schema);
                assert_eq!(EMPTY.clone(), result);
            }
        }

        // Test non-empty joins
        {
            let left_node = LiteralNode::from_iter(LEFT_TUPLES.clone().into_iter(), LEFT_SCHEMA.clone()).unwrap();
            let right_node = LiteralNode::from_iter(RIGHT_TUPLES.clone().into_iter(), RIGHT_SCHEMA.clone()).unwrap();

            let (schema, result) = generate_node_results(Box::new(left_node), Box::new(right_node),
                                                         JoinType::LeftOuter,
                                                         Some(PREDICATE.clone()));

            assert_eq!(schema, result_schema);
            assert_eq!(vec![
                TupleLiteral::from_iter(vec![1i32.into(), 2i32.into(), Literal::Null, Literal::Null]),
                TupleLiteral::from_iter(vec![3i32.into(), 4i32.into(), 3i32.into(), 6i32.into()]),
            ], result);
        }
    }

    #[test]
    fn test_right_outer_join() {
        let result_schema = RIGHT_SCHEMA.clone() + LEFT_SCHEMA.clone();

        // Test empty joins
        {
            let left_node = LiteralNode::from_iter(LEFT_TUPLES.clone().into_iter(), LEFT_SCHEMA.clone()).unwrap();
            let right_node = LiteralNode::from_iter(RIGHT_TUPLES.clone().into_iter(), RIGHT_SCHEMA.clone()).unwrap();

            {
                let empty_node = LiteralNode::from_iter(empty::<TupleLiteral>(), RIGHT_SCHEMA.clone()).unwrap();
                let (schema, result) = generate_node_results(Box::new(left_node), Box::new(empty_node), JoinType::RightOuter, Some(PREDICATE.clone()));

                assert_eq!(schema, result_schema);
                assert_eq!(EMPTY.clone(), result);
            }

            {
                let empty_node = LiteralNode::from_iter(empty::<TupleLiteral>(), LEFT_SCHEMA.clone()).unwrap();
                let (schema, result) = generate_node_results(Box::new(empty_node), Box::new(right_node), JoinType::RightOuter, Some(PREDICATE.clone()));

                assert_eq!(schema, result_schema);
                assert_eq!(vec![
                    TupleLiteral::from_iter(vec![Literal::Null, Literal::Null, 3i32.into(), 6i32.into()]),
                    TupleLiteral::from_iter(vec![Literal::Null, Literal::Null, 7i32.into(), 8i32.into()]),
                ], result);
            }
        }

        // Test non-empty joins
        {
            let left_node = LiteralNode::from_iter(LEFT_TUPLES.clone().into_iter(), LEFT_SCHEMA.clone()).unwrap();
            let right_node = LiteralNode::from_iter(RIGHT_TUPLES.clone().into_iter(), RIGHT_SCHEMA.clone()).unwrap();

            let (schema, result) = generate_node_results(Box::new(left_node), Box::new(right_node),
                                                         JoinType::RightOuter,
                                                         Some(PREDICATE.clone()));

            assert_eq!(schema, result_schema);
            assert_eq!(vec![
                TupleLiteral::from_iter(vec![3i32.into(), 4i32.into(), 3i32.into(), 6i32.into()]),
                TupleLiteral::from_iter(vec![Literal::Null, Literal::Null, 7i32.into(), 8i32.into()]),
            ], result);
        }
    }

    #[test]
    fn test_full_outer_join() {
        let result_schema = LEFT_SCHEMA.clone() + RIGHT_SCHEMA.clone();

        // Test empty joins
        {
            let left_node = LiteralNode::from_iter(LEFT_TUPLES.clone().into_iter(), LEFT_SCHEMA.clone()).unwrap();
            let right_node = LiteralNode::from_iter(RIGHT_TUPLES.clone().into_iter(), RIGHT_SCHEMA.clone()).unwrap();

            {
                let empty_node = LiteralNode::from_iter(empty::<TupleLiteral>(), RIGHT_SCHEMA.clone()).unwrap();
                let (schema, result) = generate_node_results(Box::new(left_node), Box::new(empty_node), JoinType::FullOuter, Some(PREDICATE.clone()));

                assert_eq!(schema, result_schema);
                assert_eq!(vec![
                    TupleLiteral::from_iter(vec![1i32.into(), 2i32.into(), Literal::Null, Literal::Null]),
                    TupleLiteral::from_iter(vec![3i32.into(), 4i32.into(), Literal::Null, Literal::Null]),
                ], result);
            }

            {
                let empty_node = LiteralNode::from_iter(empty::<TupleLiteral>(), LEFT_SCHEMA.clone()).unwrap();
                let (schema, result) = generate_node_results(Box::new(empty_node), Box::new(right_node), JoinType::FullOuter, Some(PREDICATE.clone()));

                assert_eq!(schema, result_schema);
                assert_eq!(vec![
                    TupleLiteral::from_iter(vec![Literal::Null, Literal::Null, 3i32.into(), 6i32.into()]),
                    TupleLiteral::from_iter(vec![Literal::Null, Literal::Null, 7i32.into(), 8i32.into()]),
                ], result);
            }
        }

        // Test non-empty joins
        {
            let left_node = LiteralNode::from_iter(LEFT_TUPLES.clone().into_iter(), LEFT_SCHEMA.clone()).unwrap();
            let right_node = LiteralNode::from_iter(RIGHT_TUPLES.clone().into_iter(), RIGHT_SCHEMA.clone()).unwrap();

            let (schema, result) = generate_node_results(Box::new(left_node), Box::new(right_node),
                                                         JoinType::FullOuter,
                                                         Some(PREDICATE.clone()));

            assert_eq!(schema, result_schema);
            assert_eq!(vec![
                TupleLiteral::from_iter(vec![1i32.into(), 2i32.into(), Literal::Null, Literal::Null]),
                TupleLiteral::from_iter(vec![3i32.into(), 4i32.into(), 3i32.into(), 6i32.into()]),
                TupleLiteral::from_iter(vec![Literal::Null, Literal::Null, 7i32.into(), 8i32.into()]),
            ], result);
        }
    }
}