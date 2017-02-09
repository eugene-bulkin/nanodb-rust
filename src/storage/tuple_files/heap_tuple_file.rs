//! This module contains utilities and classes for tuple files with a backing structure based on a
//! heap.

use std::fs::File;

use super::super::{DBFile, DBPage, Pinnable};
use super::super::file_manager;
use super::super::super::Schema;

/// This class implements tuple file processing for heap files.
pub struct HeapTupleFile {
    db_file: DBFile<File>,
    /// The schema of tuples in this tuple file.
    pub schema: Schema,
}

impl HeapTupleFile {
    /// Instantiates a new heap tuple file with a given `DBFile` and schema. *Note: this may fail.*
    ///
    /// # Arguments
    /// * db_file - The backing `DBFile`.
    /// * schema - The schema that the
    pub fn new(db_file: DBFile<File>, schema: Schema) -> Result<HeapTupleFile, file_manager::Error> {
        let mut result = HeapTupleFile {
            db_file: db_file,
            schema: schema,
        };
        try!(result.save_metadata());
        Ok(result)
    }

    /// Writes the metadata of the tuple file to disk.
    ///
    /// This handles writing of the schema and (TODO) stats on the table to the disk, and (TODO)
    /// handling all buffer management via the buffer manager.
    pub fn save_metadata(&mut self) -> Result<(), file_manager::Error> {
        let mut header_page = try!(DBPage::new(&self.db_file, 0));

        header_page.pin();

        try!(self.schema.write(&mut header_page));

        try!(file_manager::save_page(&mut self.db_file, 0, &header_page.page_data));

        try!(header_page.unpin());

        Ok(())
    }
}
