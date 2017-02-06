use std::collections::HashMap;

use super::{DBFileType, FileManager, file_manager};
use super::super::Schema;
use super::tuple_files::HeapTupleFile;

pub struct Table {
    tuple_file: HeapTupleFile,
}

#[inline]
pub fn get_table_file_name<S: Into<String>>(table_name: S) -> String {
    table_name.into() + ".tbl"
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum Error {
    FileManagerError(file_manager::Error),
}

impl From<file_manager::Error> for Error {
    fn from(error: file_manager::Error) -> Error {
        Error::FileManagerError(error)
    }
}

pub struct TableManager {
    pub open_tables: HashMap<String, Table>,
}

impl TableManager {
    /// Instantiates the table manager
    pub fn new() -> TableManager {
        TableManager { open_tables: HashMap::new() }
    }

    /// Return a reference to a table, if it exists, from the table manager.
    pub fn get_table<S: Into<String>>(&self, name: S) -> Option<&Table> {
        let name = name.into();
        let result = self.open_tables.get(&name);
        match result {
            Some(table) => Some(table),
            None => None,
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

                let table = Table { tuple_file: tuple_file };

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

        let table = table_manager.get_table(TABLE_NAME).unwrap();

        assert_eq!(table.tuple_file.schema, schema);
    }
}
