use std::fs::File;

use super::super::{DBFile, DBPage};
use super::super::dbpage;
use super::super::super::Schema;

pub struct HeapTupleFile {
    db_file: DBFile<File>,
    pub schema: Schema,
}

impl HeapTupleFile {
    pub fn new(db_file: DBFile<File>, schema: Schema) -> HeapTupleFile {
        HeapTupleFile {
            db_file: db_file,
            schema: schema,
        }
    }

    pub fn save_metadata(&self) -> Result<(), dbpage::Error> {
        let header_page = try!(DBPage::new(&self.db_file, 0));

        unimplemented!()
    }
}
