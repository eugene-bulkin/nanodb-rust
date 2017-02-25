//! This module contains utilities and classes for tuple files with a backing structure based on a
//! heap.

use std::fs::File;

use super::super::{DBFile, DBPage, Pinnable, Tuple, TupleError};
use super::super::file_manager;
use super::super::page_tuple::{PageTuple, get_tuple_storage_size};
use super::super::storage_manager::load_dbpage;
use super::super::super::Schema;

/// This class implements tuple file processing for heap files.
#[derive(Debug, PartialEq)]
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
    /// * schema - The schema that the file is based on.
    pub fn new(db_file: DBFile<File>, schema: Schema) -> Result<HeapTupleFile, file_manager::Error> {
        let mut result = HeapTupleFile {
            db_file: db_file,
            schema: schema,
        };
        try!(result.save_metadata());
        Ok(result)
    }

    /// Creates a new heap tuple file by reading a given `DBFile` and parsing the schema.
    ///
    /// # Arguments
    /// * db_file - The backing `DBFile`.
    pub fn open(mut db_file: DBFile<File>) -> Result<HeapTupleFile, file_manager::Error> {
        let mut header_page = try!(load_dbpage(&mut db_file, 0, false));

        let schema = try!(Schema::from_header_page(&mut header_page));

        Ok(HeapTupleFile {
            db_file: db_file,
            schema: schema,
        })
    }

    /// Writes the metadata of the tuple file to disk.
    ///
    /// This handles writing of the schema and (TODO) stats on the table to the disk, and (TODO)
    /// handling all buffer management via the buffer manager.
    pub fn save_metadata(&mut self) -> Result<(), file_manager::Error> {
        let mut header_page = try!(load_dbpage(&mut self.db_file, 0, false));

        header_page.pin();

        try!(self.schema.write(&mut header_page));

        try!(file_manager::save_page(&mut self.db_file, 0, &header_page.page_data));

        try!(header_page.unpin());

        Ok(())
    }

    /// Adds the specified tuple into the table file, returning a new object corresponding to the
    /// actual tuple added to the table.
    ///
    /// # Arguments
    /// * tuple - a tuple object containing the values to add to the table
    pub fn add_tuple<'a, T: Tuple + 'a>(&mut self, tuple: T) -> Result<Box<Tuple + 'a>, TupleError> {
        let tuple_size = try!(get_tuple_storage_size(self.schema.clone(), &tuple));
        debug!("Adding new tuple of size {} bytes.", tuple_size);

        if (tuple_size + 2) as u32 > self.db_file.get_page_size() {
            return Err(TupleError::TupleTooBig(tuple_size, self.db_file.get_page_size()));
        }

        let mut page_no = 1;
        let mut db_page: Option<DBPage> = None;
        loop {
            let cur_page = load_dbpage(&mut self.db_file, page_no, false);
            if cur_page.is_err() {
                // Couldn't load the current page, because it doesn't exist.
                // Break out of the loop.
                debug!("Reached end of data file without finding space for new tuple.");
                break;
            }

            // We can unwrap here because we would have broken if cur_page were not Ok.
            let mut cur_page = cur_page.unwrap();

            let free_space = try!(cur_page.get_free_space());
            trace!("Page {} has {} bytes of free space.", page_no, free_space);

            if free_space >= tuple_size + 2 {
                debug!("Found space for new tuple in page {}.", page_no);
                db_page = Some(cur_page);
                break;
            }

            // If we reached this point then the page doesn't have enough space, so go on to the
            // next data page.
            try!(cur_page.unpin());
            page_no += 1;
        }

        if db_page.is_none() {
            // Try to create a new page at the end of the file. In this circumstance, page_no is
            // *just past* the last page in the data file.
            debug!("Creating new page {} to store new tuple.", page_no);
            let mut cur_page = try!(load_dbpage(&mut self.db_file, page_no, true));
            try!(cur_page.init_new_page());
            db_page = Some(cur_page);
        }

        // At this point there is some DBPage here.
        let mut db_page = db_page.unwrap();

        let slot = try!(db_page.alloc_new_tuple(tuple_size));
        let tuple_offset = try!(db_page.get_slot_value(slot));

        debug!("New tuple will reside on page {}, slot {}.", page_no, slot);

        try!(db_page.store_new_tuple(tuple_offset, self.schema.clone(), &tuple));
        try!(file_manager::save_page(&mut self.db_file, page_no, &db_page.page_data));
        db_page.set_dirty(false);
        let page_tuple = try!(PageTuple::new(db_page, tuple_offset, self.schema.clone()));

        // TODO: Right now, page tuples consume the page. In the future, we should be able to hold
        // multiple references to the page because tuples cannot overlap.
        //        try!(db_page.unpin());

        Ok(Box::new(page_tuple))
    }
}
