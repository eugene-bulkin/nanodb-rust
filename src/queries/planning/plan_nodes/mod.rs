//! This module contains all plan nodes.

mod file_scan;
mod project;
mod nested_loop_join;

pub use self::project::ProjectNode;
pub use self::file_scan::FileScanNode;
pub use self::nested_loop_join::NestedLoopJoinNode;

use super::super::super::storage::{Tuple};
use super::super::super::Schema;
use super::PlanResult;
use ::expressions::Expression;

/// Represents a query plan node in its most abstract form.
pub trait PlanNode {
    /// Retrieves the current plan's schema.
    fn get_schema(&self) -> Schema;

    /// Retrieves the next tuple in the plan.
    fn get_next_tuple(&mut self) -> PlanResult<Option<&mut Tuple>>;

    /// Prepare the plan node for execution.
    fn prepare(&mut self) -> PlanResult<()>;

    /// Initialize the node.
    fn initialize(&mut self) {
        // Do nothing by default.
    }

    /// Check whether the node has a predicate wrapping it. This is basically a static method for a
    /// class, defaulting to false.
    #[inline]
    fn has_predicate(&self) -> bool { false }

    /// Retrieve the node's wrapping predicate. Defaults to None.
    #[inline]
    fn get_predicate(&self) -> Option<Expression> { None }

    /// Set the wrapping predicate if there is one.
    #[inline]
    fn set_predicate(&mut self, _predicate: Expression) -> PlanResult<()> {
        Ok(())
    }
}