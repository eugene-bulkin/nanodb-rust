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

        let planner = SimplePlanner::new(&server.file_manager, &mut server.table_manager);
        let mut plan = try!(planner.make_plan(self.clause.clone()).map_err(ExecutionError::CouldNotExecutePlan));

        let col_names: Vec<String> = plan.get_schema().iter().map(|col_info| column_name_to_string(&col_info.get_column_name())).collect();
        let mut tuples: Vec<TupleLiteral> = Vec::new();

        while let Some(mut boxed_tuple) = try!(plan.get_next_tuple().map_err(ExecutionError::CouldNotGetNextTuple)) {
            let literal = TupleLiteral::from_tuple(&mut *boxed_tuple);
            tuples.push(literal);
        }
        if tuples.is_empty() {
            println!("No rows are in the table.");
            return Ok(None);
        }

        match print_table(out, col_names, tuples.clone().into_iter().map(Into::into)) {
            Ok(_) => {
                Ok(Some(tuples))
            },
            Err(e) => Err(ExecutionError::PrintError(e.description().into()))
        }
    }

    fn as_any(&self) -> &::std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;

    use ::commands::ExecutionError;
    use ::parser::statements;
    use ::storage::TupleLiteral;

    #[test]
    fn test_select() {
        let dir = TempDir::new("test_dbfiles").unwrap();
        let mut server = Server::with_data_path(dir.path());

        let stmts = statements(b"CREATE TABLE foo (a integer); INSERT INTO foo VALUES (3);").unwrap().1;
        for stmt in stmts {
            server.handle_command(stmt);
        }

        let ref mut select_command = statements(b"SELECT * FROM foo;").unwrap().1[0];
        assert_eq!(Ok(Some(vec![TupleLiteral::from_iter(vec![3.into()])])),
        select_command.execute(&mut server, &mut ::std::io::sink()));

        let ref mut select_command = statements(b"SELECT a FROM foo;").unwrap().1[0];
        assert_eq!(Ok(Some(vec![TupleLiteral::from_iter(vec![3.into()])])),
        select_command.execute(&mut server, &mut ::std::io::sink()));

        let ref mut select_command = statements(b"SELECT * FROM bar;").unwrap().1[0];
        assert_eq!(Err(ExecutionError::TableDoesNotExist("BAR".into())),
        select_command.execute(&mut server, &mut ::std::io::sink()));
    }
}
