//! This module contains all plan nodes.

mod file_scan;
mod project;

pub use self::project::ProjectNode;
pub use self::file_scan::FileScanNode;

use super::super::super::storage::{Tuple};
use super::super::super::Schema;
use super::PlanResult;

/// Represents a query plan node in its most abstract form.
pub trait PlanNode {
    /// Retrieves the current plan's schema.
    fn get_schema(&self) -> Schema;

    /// Retrieves the next tuple in the plan.
    fn get_next_tuple(&mut self) -> PlanResult<Option<&mut Tuple>>;

    /// Prepare the plan node for execution.
    fn prepare(&mut self) -> PlanResult<()>;
}