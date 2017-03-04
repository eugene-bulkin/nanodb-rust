//! This module contains the classes and functions needed for a simple query planner.

use super::super::super::expressions::SelectClause;
use super::{Planner, NodeResult, make_simple_select};
use super::super::super::storage::{FileManager, TableManager};

/// This class generates execution plannodes for performing SQL queries. The primary responsibility
/// is to generate plannodes for SQL `SELECT` statements, but `UPDATE` and `DELETE` expressions will
/// also use this class to generate simple plannodes to identify the tuples to update or delete.
pub struct SimplePlanner<'a> {
    file_manager: &'a FileManager,
    table_manager: &'a mut TableManager,
}

impl<'a> SimplePlanner<'a> {
    /// Instantiates a new SimplePlanner.
    pub fn new(file_manager: &'a FileManager, table_manager: &'a mut TableManager) -> SimplePlanner<'a> {
        SimplePlanner {
            file_manager: file_manager,
            table_manager: table_manager,
        }
    }
}

impl<'a> Planner for SimplePlanner<'a> {
    fn make_plan(&mut self, clause: SelectClause) -> NodeResult {
        make_simple_select(self.file_manager, self.table_manager, clause.table, clause.where_expr)
    }
}