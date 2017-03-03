//! This module contains utilities for general database file storage handling.

use std::fs::File;

use super::{DBFile, DBPage, file_manager, Pinnable};

/// This method returns a database page to use, retrieving it from the buffer manager if it is
/// already loaded, or reading it from the specified data file if it is not already loaded. If the
/// page must be loaded from the file, it will be added to the buffer manager. This operation may
/// cause other database pages to be evicted from the buffer manager, and written back to disk if
/// the evicted pages are dirty.
///
/// The `create` flag controls whether an error is propagated, if the requested page is past the
/// current end of the data file. (Note that if a new page is created, the file's size will not
/// reflect the new page until it is actually written to the file.)
///
/// # Arguments
/// * dbfile - The database file to load the page from.
/// * page_no - The number of the page to load.
/// * create - A flag specifying whether the page should be created if it doesn't already exist.
pub fn load_dbpage(dbfile: &mut DBFile<File>, page_no: u32, create: bool) -> Result<DBPage, file_manager::Error> {
    // TODO: Use BufferManager
    let mut page = try!(DBPage::new(&dbfile.file_info, page_no));
    match file_manager::load_page(dbfile, page_no, &mut page.page_data, create) {
        Ok(()) => {
            page.pin();
            Ok(page)
        },
        Err(e) => {
            page.invalidate();
            Err(e)
        }
    }
}
