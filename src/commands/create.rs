use super::{Command, ExecutionError};

use super::super::{ColumnType, Schema, Server};

#[derive(Debug, Clone, PartialEq)]
/// A command for creating a new database object.
pub enum CreateCommand {
    /// A command variant for creating a new table.
    Table {
        /// The name of the table.
        name: String,
        /// Whether the table is temporary or not.
        temp: bool,
        /// Try to create the table only if one with the same name does not exist.
        if_not_exists: bool,
        /// Column declarations.
        decls: Vec<(String, ColumnType)>,
    },
    /// A command variant for creating a new view on a table or other view.
    View,
}

impl Command for CreateCommand {
    fn execute(&mut self, server: &Server) -> Result<(), ExecutionError> {
        println!("{:?}", self);
        match server.table_manager
            .create_table(&server.file_manager, "foo", Schema::new()) {
            Ok(_) => {
                println!("Created table foo.");
                Ok(())
            }
            Err(e) => {
                println!("ERROR: {:?}", e);
                Err(ExecutionError::Unimplemented)
            }
        }
    }

    fn as_any(&self) -> &::std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::CreateCommand;
    use super::super::Command;
    use super::super::super::Server;
    use super::super::super::column::ColumnType;

    #[test]
    #[ignore] // TODO: Implement
    fn test_table() {
        let mut server = Server::new();
        let mut command = CreateCommand::Table {
            name: "foo".into(),
            temp: false,
            if_not_exists: false,
            decls: vec![("A".into(), ColumnType::Integer)],
        };

        assert_eq!(Ok(()), command.execute(&server));
    }
}
