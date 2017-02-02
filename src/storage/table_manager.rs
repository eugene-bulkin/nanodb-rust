use super::{DBFileType, FileManager, file_manager};
use super::super::Schema;
use super::tuple_files::HeapTupleFile;

pub struct Table {
    tuple_file: HeapTupleFile,
}

fn get_table_file_name<S: Into<String>>(table_name: S) -> String {
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

pub struct TableManager {}

impl TableManager {
    /// Creates a new table file with the table-name and schema specified in
    /// the passed-in
    /// [`Schema`](../schema/struct.Schema.html) object.
    ///
    /// TODO: Add properties
    pub fn create_table<'a, S: Into<String>>(&self,
                                             file_manager: &FileManager,
                                             table_name: S,
                                             schema: Schema)
                                             -> Result<Table, Error> {
        let page_size = 512; // TODO: Change this to .get_current_pagesize()

        match file_manager.create_dbfile(get_table_file_name(table_name),
                                         DBFileType::HeapTupleFile,
                                         page_size) {
            Ok(db_file) => {
                let tuple_file = HeapTupleFile::new(db_file, schema);

                Ok(Table { tuple_file: tuple_file })
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
        let dir = TempDir::new("test_dbfiles").expect("Unable to create test_dbfiles directory!");
        let file_manager = FileManager::with_directory(dir.path()).unwrap();
        let table_manager = TableManager {};

        let schema = Schema::with_columns(vec![
            ColumnInfo::with_name(ColumnType::Integer, "A"),
            ColumnInfo::with_name(ColumnType::VarChar { length: 16 }, "B"),
        ])
            .unwrap();

        let table = table_manager.create_table(&file_manager, "foo", schema.clone()).unwrap();

        assert_eq!(table.tuple_file.schema, schema);
    }
}
