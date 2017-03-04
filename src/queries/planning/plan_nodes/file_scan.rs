//! This module provides the file scan plan node.

use std::default::Default;

use super::super::{PlanError, PlanResult};
use super::PlanNode;
use super::super::super::super::Schema;
use super::super::super::super::expressions::{Expression, Environment, Literal};
use super::super::super::super::storage::tuple_files::HeapFilePageTuple;
use super::super::super::super::storage::table_manager::Table;
use super::super::super::super::storage::{Pinnable, Tuple};

/// Checks whether the tuple fits the predicate.
    ///
    /// # Arguments
    /// * tuple - The tuple to verify.
fn is_tuple_selected(predicate: Option<&Expression>, schema: Schema, tuple: &mut HeapFilePageTuple) -> PlanResult<bool> {
    match predicate {
        Some(ref expr) => {
            let mut env: Environment = Default::default();
            env.add_tuple(schema, tuple);
            match expr.evaluate(&mut Some(&mut env)) {
                Ok(Literal::True) => Ok(true),
                Ok(Literal::False) => Ok(false),
                Ok(_) => Err(PlanError::InvalidPredicate),
                Err(e) => Err(PlanError::CouldNotApplyPredicate(e)),
            }
        },
        None => Ok(true)
    }
}

/// A select plan-node that scans a tuple file, checking the optional predicate against each tuple
/// in the file. Note that there are no optimizations used if the tuple file is a sequential tuple
/// file or a hashed tuple file.
///
/// This plan node can also be used with indexes, when a "file-scan" is to be performed over all of
/// the index's tuples, in whatever order the index will produce the tuples. If the planner wishes
/// to take advantage of an index's ability to look up tuples based on various values, the
/// `IndexScanNode` should be used instead.
pub struct FileScanNode<'a> {
    table: &'a mut Table,
    jump_to_marked: bool,
    done: bool,
    predicate: Option<Expression>,
    current_tuple: Option<Box<HeapFilePageTuple>>,
}

impl<'a> FileScanNode<'a> {
    /// Instantiate a new file scan node.
    ///
    /// # Arguments
    /// * table - The table to scan.
    /// * predicate - The predicate to filter on if it exists.
    pub fn new(table: &'a mut Table, predicate: Option<Expression>) -> FileScanNode<'a> {
        FileScanNode {
            table: table,
            jump_to_marked: false,
            done: false,
            predicate: predicate,
            current_tuple: None,
        }
    }

    fn advance_current_tuple(&mut self) -> PlanResult<()> {
        if self.jump_to_marked {
            debug!("Resuming at previously marked tuple.");
            unimplemented!()
        } else {
            self.current_tuple = (match self.current_tuple {
                Some(ref tuple) => {
                    try!(self.table.get_next_tuple(tuple))
                },
                None => {
                    try!(self.table.get_first_tuple())
                }
            }).map(Box::new);
        }
        Ok(())
    }

    fn get_next_tuple_helper(&mut self) -> PlanResult<()> {
        if self.done {
            return Ok(());
        }

        // Continue to advance the current tuple until it is selected by the
        // predicate.
        loop {
            try!(self.advance_current_tuple());

            if self.current_tuple.is_none() {
                self.done = true;
                return Ok(());
            }

            let mut boxed_tuple = self.current_tuple.as_mut().unwrap();
            let is_selected = is_tuple_selected(self.predicate.as_ref(), self.table.schema.clone(), &mut *boxed_tuple);
            // If we found a tuple that satisfies the predicate, break out of the loop!
            if try!(is_selected) {
                return Ok(());
            }

            try!(boxed_tuple.unpin());
        }
    }
}

impl<'a> PlanNode for FileScanNode<'a> {
    fn get_schema(&self) -> Schema {
        self.table.schema.clone()
    }

    fn prepare(&mut self) -> PlanResult<()> {
        // TODO
        Ok(())
    }

    fn get_next_tuple(&mut self) -> PlanResult<Option<&mut Tuple>> {
        try!(self.get_next_tuple_helper());

        Ok(match self.current_tuple.as_mut() {
            Some(mut boxed_tuple) => {
                Some(&mut **boxed_tuple)
            },
            _ => { None }
        })
    }
}