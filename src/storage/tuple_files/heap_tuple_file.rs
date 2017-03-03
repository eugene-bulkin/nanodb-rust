//! This module contains utilities and classes for tuple files with a backing structure based on a
//! heap.

use std::fs::File;

use super::super::{DBFile, DBPage, Pinnable, Tuple, TupleError, PinError};
use super::super::file_manager;
use super::super::page_tuple::{PageTuple, get_tuple_storage_size};
use super::super::storage_manager::load_dbpage;
use super::super::super::expressions::Literal;
use super::super::super::Schema;
use super::super::dbpage::EMPTY_SLOT;

/// A page tuple stored in a heap file, so it has an associated slot.
pub struct HeapFilePageTuple {
    page_tuple: PageTuple,
    /// The slot at which the tuple is stored in the heap tuple file.
    pub slot: u16,
}

impl ::std::ops::Deref for HeapFilePageTuple {
    type Target = PageTuple;

    fn deref(&self) -> &Self::Target {
        &self.page_tuple
    }
}

impl Pinnable for HeapFilePageTuple {
    fn pin(&mut self) { self.page_tuple.pin() }

    fn unpin(&mut self) -> Result<(), PinError> { self.page_tuple.unpin() }

    fn get_pin_count(&self) -> u32 { self.page_tuple.get_pin_count() }
}

impl<'a> Pinnable for &'a mut HeapFilePageTuple {
    fn pin(&mut self) { self.page_tuple.pin() }

    fn unpin(&mut self) -> Result<(), PinError> { self.page_tuple.unpin() }

    fn get_pin_count(&self) -> u32 { self.page_tuple.get_pin_count() }
}

impl Tuple for HeapFilePageTuple {
    fn is_disk_backed(&self) -> bool { self.page_tuple.is_disk_backed() }

    fn is_null_value(&self, col_index: usize) -> Result<bool, TupleError> { self.page_tuple.is_null_value(col_index) }

    fn get_column_count(&self) -> usize { self.page_tuple.get_column_count() }

    fn get_column_value(&mut self, col_index: usize) -> Result<Literal, TupleError> { self.page_tuple.get_column_value(col_index) }
}

impl<'a> Tuple for &'a mut HeapFilePageTuple {
    fn is_disk_backed(&self) -> bool { self.page_tuple.is_disk_backed() }

    fn is_null_value(&self, col_index: usize) -> Result<bool, TupleError> { self.page_tuple.is_null_value(col_index) }

    fn get_column_count(&self) -> usize { self.page_tuple.get_column_count() }

    fn get_column_value(&mut self, col_index: usize) -> Result<Literal, TupleError> { self.page_tuple.get_column_value(col_index) }
}

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
    pub fn add_tuple<'a, T: Tuple + 'a>(&mut self, mut tuple: T) -> Result<Box<Tuple + 'a>, TupleError> {
        let tuple_size = try!(get_tuple_storage_size(self.schema.clone(), &mut tuple));
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

        try!(db_page.store_new_tuple(tuple_offset, self.schema.clone(), tuple));
        try!(file_manager::save_page(&mut self.db_file, page_no, &db_page.page_data));
        db_page.set_dirty(false);
        let mut page_tuple = try!(PageTuple::new(db_page, tuple_offset, self.schema.clone()));
        page_tuple.pin();

        // TODO: Right now, page tuples consume the page. In the future, we should be able to hold
        // multiple references to the page because tuples cannot overlap.
        //        try!(db_page.unpin());

        Ok(Box::new(page_tuple))
    }

    /// Returns the first tuple in this table file, or `None` if there are no tuples in the file.
    pub fn get_first_tuple(&mut self) -> Result<Option<HeapFilePageTuple>, file_manager::Error> {
        // Scan through the data pages until we hit the end of the table
        // file.  It may be that the first run of data pages is empty,
        // so just keep looking until we hit the end of the file.

        // Header page is page 0, so first data page is page 1.
        // So we can break out of the outer loop from inside the inner one
        let mut page_no = 1;
        loop {
            let page_result = load_dbpage(&mut self.db_file, page_no, false);
            if let Err(e) = page_result {
                match e {
                    file_manager::Error::NotFullyRead => {
                        break;
                    },
                    _ => {
                        return Err(e);
                    }
                }
            }
            let mut db_page = page_result.unwrap();
            let num_slots = try!(db_page.get_num_slots());

            for slot in 0..num_slots {
                let offset = try!(db_page.get_slot_value(slot));
                if offset == EMPTY_SLOT {
                    continue;
                }

                // This is the first tuple in the file.  Build up the
                // HeapFilePageTuple object and return it.  Note that
                // creating this page-tuple will increment the page's
                // pin-count by one, so we decrement it before breaking
                // out. TODO

                // TODO: Fix error handling here
                let mut tuple = try!(PageTuple::new(db_page, offset, self.schema.clone()).map_err(|_| file_manager::Error::IOError));
                tuple.pin();
                return Ok(Some(HeapFilePageTuple { page_tuple: tuple, slot: slot }));
            }

            page_no += 1;
        }

        Ok(None)
    }
    /// Returns the tuple that follows the specified tuple, or `None` if there are no more tuples in
    /// the file. This method must operate correctly regardless of whether the input tuple is pinned
    /// or unpinned.
    pub fn get_next_tuple(&mut self, cur_tuple: &HeapFilePageTuple) -> Result<Option<HeapFilePageTuple>, file_manager::Error> {
        /* Procedure:
         *   1)  Get slot index of current tuple.
         *   2)  If there are more slots in the current page, find the next
         *       non-empty slot.
         *   3)  If we get to the end of this page, go to the next page
         *       and try again.
         *   4)  If we get to the end of the file, we return None.
         */

        // Retrieve the location info from the previous tuple.  Since the
        // tuple (and/or its backing page) may already have a pin-count of 0,
        // we can't necessarily use the page itself.
        let ref prev_dbpage = cur_tuple.db_page;
        let prev_page_no = prev_dbpage.page_no;
        let prev_slot = cur_tuple.slot;

        // Retrieve the page itself so that we can access the internal data.
        // The page will come back pinned on behalf of the caller.  (If the
        // page is still in the Buffer Manager's cache, it will not be read
        // from disk, so this won't be expensive in that case.)
        let mut db_page: DBPage = try!(load_dbpage(&mut self.db_file, prev_page_no, false));

        // Start by looking at the slot immediately following the previous
        // tuple's slot.
        let mut next_slot = prev_slot + 1;

        loop {
            let num_slots = try!(db_page.get_num_slots());

            while next_slot < num_slots {
                let next_offset = try!(db_page.get_slot_value(next_slot));
                if next_offset != EMPTY_SLOT {
                    // Creating this tuple will pin the page a second time.
                    // Thus, we unpin the page after creating this tuple.
                    // TODO: Fix error handling here
                    let mut tuple = try!(PageTuple::new(db_page, next_offset, self.schema.clone()).map_err(|_| file_manager::Error::IOError));
                    tuple.pin();
                    return Ok(Some(HeapFilePageTuple { page_tuple: tuple, slot: next_slot }));
                }
                next_slot += 1;
            }

            // If we got here then we reached the end of this page with no
            // tuples.  Go on to the next data-page, and start with the first
            // tuple in that page.
            try!(db_page.unpin());
            match load_dbpage(&mut self.db_file, db_page.page_no + 1, false) {
                Ok(page) => {
                    db_page = page;
                    next_slot = 0;
                },
                Err(e) => {
                    match e {
                        file_manager::Error::NotFullyRead => {
                            break;
                        },
                        _ => {
                            return Err(e);
                        }
                    }
                }
            }
        }

        Ok(None)
    }
}
