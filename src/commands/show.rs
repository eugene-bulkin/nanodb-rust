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
    fn execute(&mut self, server: &mut Server, out: &mut ::std::io::Write) -> CommandResult {
        match *self {
            ShowCommand::Tables => {
                match server.file_manager.get_file_paths() {
                    Ok(paths) => {
                        let header = vec!["TABLE NAME"];
                        let mut table_names: Vec<String> = paths.iter().map(|p| {
                            p.as_path()
                                .file_stem().unwrap()
                                .to_str().unwrap()
                                .into()
                        }).collect();
                        // For consistency, we sort the table names. This isn't a performance
                        // critical command anyway.
                        table_names.sort();

                        let table_rows: Vec<Vec<String>> = table_names.iter().map(|name| vec![name.clone()]).collect();
                        let tuple_results: Vec<TupleLiteral> = table_names.iter().map(|name| TupleLiteral::from_iter(vec![Literal::String(name.clone())])).collect();

                        match print_table(out, header, table_rows) {
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

        File::create(&dir.path().join("BAZ.tbl")).unwrap();
        File::create(&dir.path().join("FOO.tbl")).unwrap();
        File::create(&dir.path().join("BAR.tbl")).unwrap();

        let mut server = Server::with_data_path(dir.path());

        let mut cmd = ShowCommand::Tables;

        let foo_tup = TupleLiteral::from_iter(vec![Literal::String("FOO".into())]);
        let bar_tup = TupleLiteral::from_iter(vec![Literal::String("BAR".into())]);
        let baz_tup = TupleLiteral::from_iter(vec![Literal::String("BAZ".into())]);

        assert_eq!(Ok(Some(vec![bar_tup, baz_tup, foo_tup])), cmd.execute(&mut server, &mut ::std::io::sink()));
    }
}