use ::{ColumnInfo, ColumnType, Schema, Server};
use ::commands::{Command, ExecutionError};

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
                debug!("Creating the new table {} on disk.", &name);
                match server.table_manager
                    .create_table(&server.file_manager, name.as_ref(), schema) {
                    Ok(_) => {
                        debug!("New table {} was created.", &name);
                        println!("Created table {}.", &name);
                        Ok(())
                    }
                    Err(e) => Err(ExecutionError::CouldNotCreateTable(e)),
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
    use tempdir::TempDir;

    use super::*;
    use ::{Server, ColumnType};
    use ::commands::Command;

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
