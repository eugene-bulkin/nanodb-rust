use super::{Command, ExecutionError};

use super::super::{ColumnInfo, ColumnType, Schema, Server};

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
    fn execute(&mut self, server: &mut Server) -> Result<(), ExecutionError> {
        match *self {
            CreateCommand::Table { ref name, ref decls, .. } => {
                let column_infos: Vec<ColumnInfo> = decls.iter()
                    .map(|decl| ColumnInfo::with_table_name(decl.1, decl.0.as_ref(), name.as_ref()))
                    .collect();
                let schema = try!(Schema::with_columns(column_infos));
                match server.table_manager
                    .create_table(&server.file_manager, name.as_ref(), schema) {
                    Ok(_) => {
                        println!("Created table {}.", name);
                        Ok(())
                    }
                    Err(e) => {
                        println!("ERROR: {:?}", e);
                        Err(ExecutionError::Unimplemented)
                    }
                }
            }
            CreateCommand::View => Err(ExecutionError::Unimplemented),
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

    use tempdir::TempDir;

    #[test]
    fn test_table() {
        let dir = TempDir::new("test_dbfiles").unwrap();
        let mut server = Server::with_data_path(dir.path());
        let mut command = CreateCommand::Table {
            name: "foo".into(),
            temp: false,
            if_not_exists: false,
            decls: vec![("A".into(), ColumnType::Integer)],
        };

        assert_eq!(Ok(()), command.execute(&mut server));
    }
}
