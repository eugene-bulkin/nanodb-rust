//! This module contains the classes and functions needed for a simple query planner.

use super::{Planner, NodeResult, make_simple_select, ProjectNode, NestedLoopJoinNode};
use super::super::super::expressions::{FromClause, FromClauseType, JoinType, SelectClause};
use super::super::super::parser::select::Value;
use super::super::super::storage::{FileManager, TableManager};

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
                let cur_node = make_simple_select(self.file_manager, self.table_manager, table.clone(), None);
                if let Some(name) = alias.clone() {
                    // TODO: RenameNode
                }
                cur_node
            },
            FromClauseType::JoinExpression { ref left, ref right, ref join_type, ref condition_type } => {
                let left_child = try!(self.make_join_tree(*left.clone()));
                let right_child = try!(self.make_join_tree(*right.clone()));

                let cur_node = NestedLoopJoinNode::new(left_child, right_child, join_type.clone(), condition_type.clone(), clause.get_computed_join_expr());

                // TODO: Project

                Ok(Box::new(cur_node))
            }
        }
    }
}

impl<'a> Planner for SimplePlanner<'a> {
    fn make_plan(&mut self, clause: SelectClause) -> NodeResult {
        let mut cur_node = try!(self.make_join_tree(clause.from_clause));
        try!(cur_node.prepare());

        if cur_node.has_predicate() {
            if let Some(expr) = clause.where_expr {
                try!(cur_node.as_mut().set_predicate(expr));
            }
        }

        if let Value::Values(values) = clause.value {
            cur_node = Box::new(ProjectNode::new(cur_node, values));
            try!(cur_node.prepare());
        }

        Ok(cur_node)
    }
}