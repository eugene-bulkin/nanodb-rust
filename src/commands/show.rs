use std::error::Error;

use ::Server;
use ::commands::{Command, CommandResult, ExecutionError};
use ::commands::utils::print_table;

#[derive(Debug, Clone, PartialEq)]
/// A command for showing database information.
pub enum ShowCommand {
    /// List the tables in the database.
    Tables,
    /// List the variables that are set in the database and their values.
    Variables,
}

impl Command for ShowCommand {
    fn execute(&mut self, server: &mut Server) -> CommandResult {
        match *self {
            ShowCommand::Tables => {
                match server.file_manager.get_file_paths() {
                    Ok(paths) => {
                        let header = vec!["TABLE NAME"];
                        let table_names: Vec<Vec<String>> = paths.iter()
                            .map(|p| vec![p.as_path().file_stem().unwrap().to_str().unwrap().into()])
                            .collect();
                        print_table(&mut ::std::io::stdout(), header, table_names).map_err(|e| {
                            ExecutionError::PrintError(e.description().into())
                        })
                    }
                    Err(e) => Err(ExecutionError::CouldNotListTables(e)),
                }
            }
            ShowCommand::Variables => Err(ExecutionError::Unimplemented),
        }
    }

    fn as_any(&self) -> &::std::any::Any {
        self
    }
}
