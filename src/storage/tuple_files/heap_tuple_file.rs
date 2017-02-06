use std::fs::File;

use super::super::{DBFile, DBPage, Pinnable};
use super::super::file_manager;
use super::super::super::Schema;

pub struct HeapTupleFile {
    db_file: DBFile<File>,
    pub schema: Schema,
}

impl HeapTupleFile {
    pub fn new(db_file: DBFile<File>, schema: Schema) -> Result<HeapTupleFile, file_manager::Error> {
        let mut result = HeapTupleFile {
            db_file: db_file,
            schema: schema,
        };
        try!(result.save_metadata());
        Ok(result)
    }

    pub fn save_metadata(&mut self) -> Result<(), file_manager::Error> {
        let mut header_page = try!(DBPage::new(&self.db_file, 0));

        header_page.pin();

        try!(self.schema.write(&mut header_page));

        try!(file_manager::save_page(&mut self.db_file, 0, &header_page.page_data));

        try!(header_page.unpin());

        Ok(())
    }
}
