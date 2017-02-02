use std::fs::File;

use super::{DBFile, DBPage, file_manager};

pub fn load_dbpage(dbfile: &mut DBFile<File>, page_no: u32, create: bool) -> Result<DBPage, file_manager::Error> {
    // TODO: Use BufferManager
    let mut page = try!(DBPage::new(&dbfile.file_info, page_no));
    match file_manager::load_page(dbfile, page_no, &mut page.page_data, create) {
        Ok(()) => Ok(page),
        Err(e) => {
            page.invalidate();
            Err(e)
        }
    }
}
