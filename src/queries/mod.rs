//! This package contains modules that handle all query processing, including planning and
//! evaluation.

mod aggregate_extractor;
mod plan_nodes;
mod planning;

pub use self::plan_nodes::{NodeResult, FileScanNode, HashedGroupAggregateNode, NestedLoopJoinNode,
                           PlanNode, ProjectNode, RenameNode};
pub use self::planning::{PlanError, PlanResult, Planner, SimplePlanner, make_simple_select,
                         get_plan_results};
pub use self::aggregate_extractor::AggregateFunctionExtractor;