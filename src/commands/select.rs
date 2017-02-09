use super::{Command, ExecutionError};
use super::super::Server;
use super::super::parser::select;

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
        println!("{:?}", self);
        Err(ExecutionError::Unimplemented)
    }

    fn as_any(&self) -> &::std::any::Any {
        self
    }
}
