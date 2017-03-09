//! This module contains the classes and functions needed for a simple query planner.

use super::{Planner, NodeResult, make_simple_select, ProjectNode};
use super::super::super::expressions::{FromClause, JoinType, SelectClause};
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
        match clause {
            FromClause::BaseTable { table, alias } => {
                let cur_node = make_simple_select(self.file_manager, self.table_manager, table, None);
                if let Some(name) = alias {
                    // TODO: RenameNode
                }
                cur_node
            },
            FromClause::JoinExpression { left, right, join_type } => {
                unimplemented!()
            }
        }
    }
}

impl<'a> Planner for SimplePlanner<'a> {
    fn make_plan(&mut self, clause: SelectClause) -> NodeResult {
        let mut cur_node = try!(self.make_join_tree(clause.from_clause));
//        let mut cur_node = try!(make_simple_select(self.file_manager, self.table_manager, clause.table.clone(), clause.where_expr.clone()));
//        try!(cur_node.prepare());
//
        if let Value::Values(values) = clause.value {
            cur_node = Box::new(ProjectNode::new(cur_node, values));
            try!(cur_node.prepare());
        }

        Ok(cur_node)
    }
}