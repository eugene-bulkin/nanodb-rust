//! This module contains utility functions for handling the first page of a `DBPage`, the header
//! page.

use byteorder::{BigEndian, ReadBytesExt};
use std::io::{self, SeekFrom};

use std::io::prelude::*;
use std::ops::Deref;

use super::DBPage;

#[derive(Debug, Copy, Clone, PartialEq)]
/// Errors that can occur while using the header page of a [`DBFile`](../struct.DBFile.html).
pub enum Error {
    /// The header page *must* be page 0, but a different page was used.
    IncorrectPage(u32),
    /// An IO error occurred.
    IOError,
}

impl From<io::Error> for Error {
    fn from(_: io::Error) -> Error {
        Error::IOError
    }
}

/// The offset in the header page where the size of the table schema is stored.
/// This value is an
/// unsigned short.
pub const OFFSET_SCHEMA_SIZE: usize = 2;

/// The offset in the header page where the size of the table statistics are
/// stored. This value is
/// an unsigned short.
pub const OFFSET_STATS_SIZE: usize = 4;

/// The offset in the header page where the table schema starts. This value is
/// an unsigned short.
pub const OFFSET_SCHEMA_START: usize = 6;

/// This class contains constants and basic functionality for accessing and
/// manipulating the
/// contents of the header page of a heap table-file. **Note that the first two
/// bytes of the first
/// page is always devoted to the type and page-size of the data file.** (See
/// [`DBFile`](../dbfile/struct.DBFile.html) for details.) All other values
/// must follow the first two bytes.
///
/// Heap table-file header pages are laid out as follows:
///
/// 1. As with all `DBFile`s, the first two bytes are the file type and page
/// size, as always.
/// 2. After this come several values specifying the sizes of various areas in
/// the header page,
/// including the size of the table's schema specification, the statistics for
/// the table, and the
/// number of columns.
/// 3. Next the table's schema is recorded in the header page. See the
/// [`Schema`](../../schema/struct.Schema.html) class for details on how a
/// table's schema is stored.
/// 4. Finally, the table's statistics are stored. See the
/// {@link edu.caltech.nanodb.storage.StatsWriter} class for details on how a
/// table's statistics are
/// stored. *Note, not implemented yet.*
///
/// Even with all this information, usually only a few hundred bytes are
/// required for storing the details of most tables.
pub struct HeaderPage {
    db_page: DBPage,
}

impl From<DBPage> for HeaderPage {
    fn from(page: DBPage) -> HeaderPage {
        HeaderPage { db_page: page }
    }
}

impl Deref for HeaderPage {
    type Target = DBPage;
    fn deref(&self) -> &Self::Target {
        &self.db_page
    }
}

impl HeaderPage {
    /// This helper method simply verifies that the data page provided to the `HeaderPage` class is
    /// in fact a header-page (i.e. page 0 in the data file).
    ///
    /// # Errors
    /// This method will return an error if the page is not page 0.
    pub fn verify(&self) -> Result<(), Error> {
        if self.page_no != 0 {
            Err(Error::IncorrectPage(self.page_no))
        } else {
            Ok(())
        }
    }

    /// Returns the number of bytes that the table's schema occupies for storage in the header page.
    ///
    /// This method reads from the DB page in order to determine this number.
    pub fn get_schema_size(&mut self) -> Result<u16, Error> {
        try!(self.verify());
        try!(self.db_page.seek(SeekFrom::Start(OFFSET_SCHEMA_SIZE as u64)));
        self.db_page.read_u16::<BigEndian>().map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;
    use super::*;
    use super::super::{DBFile, DBFileType, storage_manager};

    use tempdir::TempDir;

    lazy_static! {
        static ref DIR: TempDir = {
            if let Ok(dir) = TempDir::new("test_dbfiles") {
                dir
            } else {
                panic!("Unable to create test_dbfiles directory!");
            }
        };
        static ref FOO_FILE: PathBuf = {
            let mut init = vec![0x01, 0x0D, 0x00, 0x17, 0x00, 0x0D, 0x01, 0x03,
                                0x46, 0x4F, 0x4F, 0x03, 0x01, 0x00, 0x01, 0x41,
                                0x16, 0x00, 0x14, 0x00, 0x01, 0x42, 0x01, 0x00,
                                0x01, 0x43, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F,
                                0x0F, 0x0F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
            init.extend_from_slice(&[0x00; 464]);
            let file_path = DIR.path().join("foo.tbl");

            let mut file = File::create(&file_path).unwrap();

            file.write(&init).unwrap();
            file.flush().unwrap();

            file_path.to_path_buf()
        };
    }

    #[test]
    fn test_schema_size() {
        let file = File::open(FOO_FILE.as_path()).unwrap();
        let mut dbfile = DBFile::with_path(0x01.into(), 512, file, FOO_FILE.as_path()).unwrap();
        let page = storage_manager::load_dbpage(&mut dbfile, 0, false).unwrap();
        let mut header_page: HeaderPage = page.into();

        assert_eq!(header_page.get_schema_size().unwrap(), 0x0017);
    }
}
