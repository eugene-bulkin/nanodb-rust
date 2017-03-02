//! This module contains utilities to handle tables themselves, including indexing and constraints.

use std::collections::HashMap;

use super::{DBFileType, FileManager, file_manager};
use super::super::Schema;
use super::tuple_files::HeapTupleFile;

/// This class represents a single table in the database, including the table's name, and the tuple
/// file that holds the table's data.
#[derive(Debug, PartialEq)]
pub struct Table {
    /// The name of the table.
    pub name: Option<String>,
    tuple_file: HeapTupleFile,
}

impl ::std::ops::Deref for Table {
    type Target = HeapTupleFile;
    fn deref(&self) -> &Self::Target {
        &self.tuple_file
    }
}

impl ::std::ops::DerefMut for Table {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tuple_file
    }
}

/// Given the name of a table, return the file name which will correspond to the table in the data
/// directory.
///
/// # Arguments
/// * table_name - The name of the table.
#[inline]
pub fn get_table_file_name<S: Into<String>>(table_name: S) -> String {
    table_name.into() + ".tbl"
}

#[derive(Debug, Clone, PartialEq)]
/// An error that can occur while handling tables.
pub enum Error {
    /// A file manager error occurred while using a table utility method.
    FileManagerError(file_manager::Error),
}

impl From<file_manager::Error> for Error {
    fn from(error: file_manager::Error) -> Error {
        Error::FileManagerError(error)
    }
}

/// This class provides utilities for tables that can have indexes and constraints on them.
pub struct TableManager {
    open_tables: HashMap<String, Table>,
}

impl TableManager {
    /// Instantiates the table manager.
    pub fn new() -> TableManager {
        TableManager { open_tables: HashMap::new() }
    }

    /// Return a reference to a table, if it exists, from the table manager.
    ///
    /// # Arguments
    /// * name - The name of the table.
    pub fn get_table<S: Into<String>>(&mut self, file_manager: &FileManager, name: S) -> Result<&mut Table, Error> {
        let name = name.into();

        if !self.open_tables.contains_key(&name) {
            match file_manager.open_dbfile(get_table_file_name(name.as_ref())) {
                Ok(db_file) => {
                    match HeapTupleFile::open(db_file) {
                        Ok(tuple_file) => {
                            let table = Table {
                                name: name.clone().into(),
                                tuple_file: tuple_file,
                            };

                            self.open_tables.insert(name.clone(), table);
                            Ok(self.open_tables.get_mut(&name).unwrap())
                        }
                        Err(e) => Err(Error::FileManagerError(e.into())),
                    }
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        } else {
            Ok(self.open_tables.get_mut(&name).unwrap())
        }
    }

    /// Checks if a table with the given name exists.
    pub fn table_exists<S: Into<String>>(&self, file_manager: &FileManager, name: S) -> bool {
        let name = name.into();
        match self.open_tables.get(&name) {
            Some(_) => true,
            _ => file_manager.dbfile_exists(get_table_file_name(name)),
        }
    }

    /// Creates a new table file with the table-name and schema specified in
    /// the passed-in
    /// [`Schema`](../schema/struct.Schema.html) object.
    ///
    /// TODO: Add properties
    pub fn create_table<S: Into<String>>(&mut self,
                                         file_manager: &FileManager,
                                         table_name: S,
                                         schema: Schema)
                                         -> Result<(), Error> {
        let table_name = table_name.into();
        let page_size = 512; // TODO: Change this to .get_current_pagesize()

        let table_filename = get_table_file_name(table_name.clone());

        match file_manager.create_dbfile(table_filename, DBFileType::HeapTupleFile, page_size) {
            Ok(db_file) => {
                let tuple_file = try!(HeapTupleFile::new(db_file, schema));

                let table = Table {
                    name: table_name.clone().into(),
                    tuple_file: tuple_file,
                };

                self.open_tables.insert(table_name, table);

                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::FileManager;
    use super::super::super::{ColumnInfo, ColumnType, Schema};

    use tempdir::TempDir;

    #[test]
    fn test_create_table() {
        const TABLE_NAME: &'static str = "foo";
        let dir = TempDir::new("test_dbfiles").expect("Unable to create test_dbfiles directory!");
        let file_manager = FileManager::with_directory(dir.path()).unwrap();
        let mut table_manager = TableManager::new();

        let schema = Schema::with_columns(vec![
            ColumnInfo::with_name(ColumnType::Integer, "A"),
            ColumnInfo::with_name(ColumnType::VarChar { length: 16 }, "B"),
        ])
            .unwrap();

        table_manager.create_table(&file_manager, TABLE_NAME, schema.clone()).unwrap();

        let table = table_manager.get_table(&file_manager, TABLE_NAME).unwrap();

        assert_eq!(table.tuple_file.schema, schema);
    }
}
