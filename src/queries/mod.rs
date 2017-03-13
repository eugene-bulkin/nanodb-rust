//! This package contains modules that handle all query processing, including planning and
//! evaluation.

mod evaluation;
mod planning;

pub use self::planning::{NodeResult, PlanError, PlanNode, PlanResult, Planner, SimplePlanner};
