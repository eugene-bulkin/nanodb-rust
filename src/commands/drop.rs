use ::Server;
use ::commands::{Command, CommandResult, ExecutionError};
use ::storage::table_manager::get_table_file_name;

#[derive(Debug, Clone, PartialEq)]
/// A command for destroying a database object.
pub enum DropCommand {
    /// A command variant for dropping a table.
    Table(String),
}

impl Command for DropCommand {
    fn execute(&mut self, server: &mut Server) -> CommandResult {
        match *self {
            DropCommand::Table(ref table_name) => {
                let table_exists = server.table_manager.table_exists(&server.file_manager, table_name.as_str());
                if table_exists {
                    match server.file_manager
                        .remove_dbfile(get_table_file_name(table_name.as_str())) {
                        Ok(_) => Ok(None),
                        Err(e) => Err(ExecutionError::CouldNotDeleteTable(e))
                    }
                } else {
                    Err(ExecutionError::TableDoesNotExist(table_name.clone()))
                }
            }
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
    use ::{Server, ColumnType};
    use ::commands::{Command, CreateCommand, ExecutionError};


    #[test]
    fn test_table() {
        let table_name: &'static str = "FOO";
        let dir = TempDir::new("test_dbfiles").unwrap();
        let mut server = Server::with_data_path(dir.path());
        // Create a FOO table.
        {
            let mut command = CreateCommand::Table {
                name: table_name.into(),
                temp: false,
                if_not_exists: false,
                decls: vec![("A".into(), ColumnType::Integer)],
            };
            command.execute(&mut server).unwrap();
        }

        // Try dropping the FOO table.
        {
            let table_path = dir.path().join(table_name.to_string() + ".tbl");
            assert!(table_path.exists());

            let mut command = DropCommand::Table(table_name.into());

            assert_eq!(Ok(None), command.execute(&mut server));
            assert!(!table_path.exists());
        }

        // Try dropping a table that doesn't exist.
        {
            let table_path = dir.path().join("BAR.tbl");
            assert!(!table_path.exists());

            let mut command = DropCommand::Table("BAR".into());

            assert_eq!(Err(ExecutionError::TableDoesNotExist("BAR".into())),
                       command.execute(&mut server));
        }
    }
}
