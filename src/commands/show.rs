use super::{Command, ExecutionError};
use super::super::Server;

#[derive(Debug, Clone, PartialEq)]
/// A command for showing database information.
pub enum ShowCommand {
    /// List the tables in the database.
    Tables,
    /// List the variables that are set in the database and their values.
    Variables,
}

impl Command for ShowCommand {
    fn execute(&mut self, server: &mut Server) -> Result<(), ExecutionError> {
        println!("{:?}", self);
        Err(ExecutionError::Unimplemented)
    }

    fn as_any(&self) -> &::std::any::Any {
        self
    }
}
