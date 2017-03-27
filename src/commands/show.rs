use std::error::Error;

use ::Server;
use ::commands::{Command, CommandResult, ExecutionError};
use ::commands::utils::print_table;
use ::expressions::Literal;
use ::storage::TupleLiteral;

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
                        let mut table_names: Vec<Vec<String>> = Vec::new();
                        let mut tuple_results: Vec<TupleLiteral> = Vec::new();
                        for path in &paths {
                            let name: String = path.as_path().file_stem().unwrap().to_str().unwrap().into();
                            table_names.push(vec![name.clone()]);
                            tuple_results.push(TupleLiteral::from_iter(vec![Literal::String(name.clone())]));
                        }
                        match print_table(&mut ::std::io::stdout(), header, table_names) {
                            Ok(_) => Ok(Some(tuple_results)),
                            Err(e) => Err(ExecutionError::PrintError(e.description().into()))
                        }
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

#[cfg(test)]
mod tests {
    use std::fs::File;

    use tempdir::TempDir;

    use super::*;
    use ::Server;
    use ::expressions::Literal;
    use ::storage::TupleLiteral;

    #[test]
    fn test_show_tables_command() {
        let dir = TempDir::new("test_dbfiles").unwrap();

        File::create(&dir.path().join("FOO.tbl")).unwrap();
        File::create(&dir.path().join("BAR.tbl")).unwrap();

        let mut server = Server::with_data_path(dir.path());

        let mut cmd = ShowCommand::Tables;

        let foo_tup = TupleLiteral::from_iter(vec![Literal::String("FOO".into())]);
        let bar_tup = TupleLiteral::from_iter(vec![Literal::String("BAR".into())]);

        assert_eq!(Ok(Some(vec![bar_tup, foo_tup])), cmd.execute(&mut server));
    }
}