use std::error::Error;

use ::Server;
use ::commands::{Command, CommandResult, ExecutionError};
use ::commands::utils::print_table;
use ::expressions::SelectClause;
use ::queries::{Planner, SimplePlanner};
use ::relations::column_name_to_string;
use ::storage::TupleLiteral;

#[derive(Debug, Clone, PartialEq)]
/// A command for selecting rows from a table.
///
/// TODO: This should really be a wrapper for a select clause to handle nested queries.
pub struct SelectCommand {
    clause: SelectClause,
}

impl SelectCommand {
    /// Creates a new select command.
    ///
    /// # Arguments
    /// * table - The name of the table. TODO: This should be an arbitrary `FROM` clause.
    /// * distinct - Whether the values should be distinct.
    /// * value - The select values or wildcard being selected.
    /// * limit - Optionally, how many rows to return.
    /// * offset - Optionally, the index at which to start returning rows.
    pub fn new(select_clause: SelectClause) -> SelectCommand {
        SelectCommand { clause: select_clause }
    }
}

impl Command for SelectCommand {
    fn execute(&mut self, server: &mut Server, out: &mut ::std::io::Write) -> CommandResult {
        let result_schema = try!(self.clause.compute_schema(&server.file_manager, &server.table_manager));
        debug!("Prepared SelectClause:\n{}", self.clause);
        debug!("Result schema: {}", result_schema);

        let mut planner = SimplePlanner::new(&server.file_manager, &mut server.table_manager);
        let mut plan = try!(planner.make_plan(self.clause.clone()).map_err(ExecutionError::CouldNotExecutePlan));

        let col_names: Vec<String> = plan.get_schema().iter().map(|col_info| column_name_to_string(&col_info.get_column_name())).collect();
        let mut tuples: Vec<Vec<String>> = Vec::new();

        while let Some(mut boxed_tuple) = try!(plan.get_next_tuple().map_err(ExecutionError::CouldNotGetNextTuple)) {
            let literal = TupleLiteral::from_tuple(&mut *boxed_tuple);
            tuples.push(literal.into());
        }
        if tuples.is_empty() {
            println!("No rows are in the table.");
            return Ok(None);
        }

        match print_table(out, col_names, tuples) {
            Ok(_) => {
                // TODO
                Ok(None)
            },
            Err(e) => Err(ExecutionError::PrintError(e.description().into()))
        }
    }

    fn as_any(&self) -> &::std::any::Any {
        self
    }
}
