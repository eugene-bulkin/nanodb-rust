//! This module contains all plan nodes.

mod file_scan;
mod hashed_group_aggregate;
mod project;
#[cfg(test)]
mod literal;
mod nested_loop_join;
mod rename;

pub use self::file_scan::FileScanNode;
pub use self::hashed_group_aggregate::HashedGroupAggregateNode;
#[cfg(test)]
pub use self::literal::LiteralNode;
pub use self::nested_loop_join::NestedLoopJoinNode;
pub use self::project::{ProjectNode, ProjectError};
pub use self::rename::RenameNode;

use ::Schema;
use ::expressions::Expression;
use ::queries::planning::{PlanResult, PlanError};
use ::storage::Tuple;

/// A result that returns a plan node.
pub type NodeResult<'a> = Result<Box<PlanNode + 'a>, PlanError>;

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
    fn has_predicate(&self) -> bool {
        false
    }

    /// Retrieve the node's wrapping predicate. Defaults to None.
    #[inline]
    fn get_predicate(&self) -> Option<Expression> {
        None
    }

    /// Set the wrapping predicate if there is one.
    #[inline]
    fn set_predicate(&mut self, _predicate: Expression) -> PlanResult<()> {
        Ok(())
    }
}
