use super::{Command, ExecutionError};
use super::super::Server;
use super::super::expressions::SelectClause;
use super::super::queries::{Planner, SimplePlanner};
use super::super::storage::TupleLiteral;
use super::utils::print_table;

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
    fn execute(&mut self, server: &mut Server) -> Result<(), ExecutionError> {
        let result_schema = try!(self.clause.compute_schema(&server.file_manager, &server.table_manager));
        debug!("Prepared SelectClause:\n{}", self.clause);
        debug!("Result schema: {}", result_schema);

        let mut planner = SimplePlanner::new(&server.file_manager, &mut server.table_manager);
        let mut plan = try!(planner.make_plan(self.clause.clone()).map_err(ExecutionError::CouldNotExecutePlan));

        let col_names: Vec<String> = plan.get_schema().iter().map(|col_info| col_info.name.clone().unwrap()).collect();
        let mut tuples: Vec<Vec<String>> = Vec::new();

        while let Some(mut boxed_tuple) = try!(plan.get_next_tuple().map_err(ExecutionError::CouldNotGetNextTuple)) {
            let literal = TupleLiteral::from_tuple(&mut *boxed_tuple);
            tuples.push(literal.into());
        }
        if tuples.is_empty() {
            println!("No rows are in the table.");
            return Ok(());
        }

        if let Err(_) = print_table(&mut ::std::io::stdout(), col_names, tuples) {
            // TODO
            Err(ExecutionError::Unimplemented)
        } else {
            Ok(())
        }
    }

    fn as_any(&self) -> &::std::any::Any {
        self
    }
}
