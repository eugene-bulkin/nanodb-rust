//! This module contains the classes and functions needed for a simple query planner.

use ::expressions::{FromClause, FromClauseType, SelectClause};
use ::queries::{NestedLoopJoinNode, NodeResult, PlanNode, Planner, PlanError, ProjectNode,
                make_simple_select, RenameNode};
use ::storage::{FileManager, TableManager};

/// This class generates execution plannodes for performing SQL queries. The primary responsibility
/// is to generate plannodes for SQL `SELECT` statements, but `UPDATE` and `DELETE` expressions will
/// also use this class to generate simple plannodes to identify the tuples to update or delete.
pub struct SimplePlanner<'a> {
    file_manager: &'a FileManager,
    table_manager: &'a TableManager,
}

impl<'a> SimplePlanner<'a> {
    /// Instantiates a new SimplePlanner.
    pub fn new(file_manager: &'a FileManager, table_manager: &'a TableManager) -> SimplePlanner<'a> {
        SimplePlanner {
            file_manager: file_manager,
            table_manager: table_manager,
        }
    }

    fn make_join_tree(&self, clause: FromClause) -> NodeResult {
        match *clause {
            FromClauseType::BaseTable { ref table, ref alias } => {
                let mut cur_node = try!(make_simple_select(self.file_manager, self.table_manager, table.clone(), None));
                if let Some(ref name) = *alias {
                    cur_node = Box::new(RenameNode::new(cur_node, name.as_ref()));
                }
                Ok(cur_node)
            }
            FromClauseType::JoinExpression { ref left, ref right, ref join_type, .. } => {
                let left_child = try!(self.make_join_tree(*left.clone()));
                let right_child = try!(self.make_join_tree(*right.clone()));

                let mut cur_node: Box<PlanNode> = Box::new(NestedLoopJoinNode::new(left_child,
                                                                                   right_child,
                                                                                   join_type.clone(),
                                                                                   clause.get_computed_join_expr()));
                try!(cur_node.prepare());

                if let Some(values) = clause.get_computed_select_values() {
                    cur_node = Box::new(ProjectNode::new(cur_node, values));
                    try!(cur_node.prepare());
                }

                Ok(cur_node)
            }
        }
    }
}

impl<'a> Planner for SimplePlanner<'a> {
    fn make_plan(&mut self, clause: SelectClause) -> NodeResult {
        match clause.from_clause {
            Some(ref from_clause) => {
                let mut cur_node = try!(self.make_join_tree(from_clause.clone()));
                try!(cur_node.prepare());

                if cur_node.has_predicate() {
                    if let Some(ref expr) = clause.where_expr {
                        try!(cur_node.as_mut().set_predicate(expr.clone()));
                    }
                }

                if !clause.is_trivial_project() {
                    cur_node = Box::new(ProjectNode::new(cur_node, clause.values));
                    try!(cur_node.prepare());
                }

                Ok(cur_node)
            },
            None => {
                // TODO
                Err(PlanError::Unimplemented)
            }
        }
    }
}
