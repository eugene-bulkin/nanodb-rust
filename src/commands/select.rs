use super::{Command, ExecutionError};
use super::utils::print_table;
use super::super::Server;
use super::super::parser::select;
use super::super::storage::{TupleLiteral, Pinnable};

#[derive(Debug, Clone, PartialEq)]
/// A command for selecting rows from a table.
///
/// TODO: This should really be a wrapper for a select clause to handle nested queries.
pub struct SelectCommand {
    /// The name of the table.
    ///
    /// TODO: Support general `FROM` expressions.
    table: String,
    /// Whether the row values must be distinct.
    distinct: bool,
    /// What select values are desired.
    value: select::Value,
    /// An optional limit on the number of rows.
    limit: Option<u32>,
    /// An optional starting point at which to start returning rows.
    offset: Option<u32>,
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
    pub fn new<S: Into<String>>(table: S,
                                distinct: bool,
                                value: select::Value,
                                limit: Option<u32>,
                                offset: Option<u32>)
                                -> SelectCommand {
        SelectCommand {
            table: table.into(),
            distinct: distinct,
            value: value,
            limit: limit,
            offset: offset,
        }
    }
}

impl Command for SelectCommand {
    fn execute(&mut self, server: &mut Server) -> Result<(), ExecutionError> {
        let table = try!(server.table_manager.get_table(&server.file_manager, self.table.as_ref()).map_err(|e| ExecutionError::CouldNotOpenTable(self.table.clone(), e)));
        let col_names: Vec<String> = table.schema.iter().map(|col_info| col_info.name.clone().unwrap()).collect();
        let mut tuples: Vec<Vec<String>> = Vec::new();
        let mut cur_tuple = try!(table.get_first_tuple().map_err(|_| ExecutionError::Unimplemented));
        if cur_tuple.is_none() {
            println!("No rows are in the table.");
            return Ok(());
        }
        while cur_tuple.is_some() {
            let mut tuple = cur_tuple.unwrap();
            tuples.push(TupleLiteral::from_tuple(&mut tuple).into());
            try!(tuple.unpin());

            let tuple = tuple;
            cur_tuple = try!(table.get_next_tuple(&tuple).map_err(|_| ExecutionError::Unimplemented));
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
