
use nom::IResult;

use nom::be_u8;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::dbfile::{self, DBFile, DBFileType};
use super::dbpage;
use super::PinError;

named!(parse_header (&[u8]) -> (u8, Result<u32, dbfile::Error>), do_parse!(
    type_id: be_u8 >>
    page_size: map!(be_u8, |e: u8| dbfile::decode_pagesize(e.into())) >>
    (type_id, page_size)
));

/// The File Manager provides unbuffered, low-level operations for working with
/// paged data files.
/// It really doesn't know anything about the internal file formats of the data
/// files, except that
/// the first two bytes of the first page must specify the type and page size
/// for the data file.
/// (This is a requirement of [`open_dbfile`](#method.open_dbfile)).)
///
/// # Design
/// Although it might make more sense to put per-file operations like "load
/// page" and "store page"
/// on the {@link DBFile} class, we provide higher-level operations on the
/// Storage Manager so that
/// we can provide global buffering capabilities in one place.
///
/// This class includes no multithreading support. It maintains no internal
/// state, so there isn't
/// anything that needs to be guarded, but still, other classes using this
/// class need to be careful
/// to maintain proper multithreading.
#[derive(Debug, Clone, PartialEq)]
pub struct FileManager {
    last_accessed: Option<(PathBuf, u32)>,
    base_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum Error {
    InvalidBaseDir,
    FilePathsError,
    DBFileExists,
    DBFileDoesNotExist,
    DBFileError(dbfile::Error),
    DBPageError(dbpage::Error),
    PinError(PinError),
    DBFileParseError,
    IOError,
    CantExtendDBFile,
    CantCreateFile,
    CantOpenFile,
    InvalidDBFilePageSize,
    InvalidDBFileType,
    IncorrectBufferSize,
    NotFullyWritten,
    NotFullyRead,
    PageSaveError,
}

impl From<dbfile::Error> for Error {
    fn from(error: dbfile::Error) -> Error {
        Error::DBFileError(error)
    }
}

impl From<dbpage::Error> for Error {
    fn from(error: dbpage::Error) -> Error {
        Error::DBPageError(error)
    }
}

impl From<io::Error> for Error {
    fn from(_: io::Error) -> Error {
        Error::IOError
    }
}

impl From<PinError> for Error {
    fn from(error: PinError) -> Error {
        Error::PinError(error)
    }
}

/// This helper function calculates the file-position of the specified page.
/// Obviously, this value
/// is dependent on the page size.
fn get_page_start<F: Read + Seek + Write>(dbfile: &DBFile<F>, page_no: u32) -> u64 {
    (page_no as u64) * (dbfile.get_page_size() as u64)
}

/// Saves a page to the DB file, and then clears the page's dirty flag.
///
/// Note that the data might not actually be written to disk until a sync
/// operation is performed.
///
/// # Arguments
/// * dbFile - the data file to write to
/// * pageNo - the page number to write the buffer to
/// * buffer - the data to write back to the page
///
/// # Errors
/// This function will return an error in the following situations:
///
/// * If the buffer length is not the same as the page size.
/// * If an I/O error occurs while writing.
pub fn save_page<F: Read + Seek + Write>(dbfile: &mut DBFile<F>, page_no: u32, buffer: &[u8]) -> Result<(), Error> {
    if buffer.len() as u32 != dbfile.get_page_size() {
        return Err(Error::IncorrectBufferSize);
    }

    // updateFileIOPerfStats(dbFile, pageNo, /* read */ false, buffer.length);

    let page_start = get_page_start(&dbfile, page_no);

    let save_result = dbfile.seek(SeekFrom::Start(page_start)).and_then(|_| dbfile.write(buffer));
    match save_result {
        Ok(written) => {
            if written == buffer.len() {
                Ok(())
            } else {
                Err(Error::NotFullyWritten)
            }
        }
        Err(_) => Err(Error::PageSaveError),
    }
}

/// Loads a page from the underlying data file, and returns a new {@link
/// DBPage} object
/// containing the data. The `create` flag controls whether an error is
/// propagated, if the
/// requested page is past the end of the file. (Note that if a new page is
/// created, the file's
/// size will not reflect the new page until it is actually written to the
/// file.)
///
/// *This function does no page caching whatsoever.* Requesting a particular
/// page multiple times
/// will return multiple page objects, with data loaded from the file each time.
///
/// # Arguments
///
/// * dbfile - the database file to load the page from
/// * page_no - the number of the page to load
/// * buffer - the buffer to read the page into
/// * create - a flag specifying whether the page should be created if it
/// doesn't already exist
///
/// # Errors
/// This function will return an error in the following situations:
///
/// * TODO
pub fn load_page(dbfile: &mut DBFile<File>, page_no: u32, mut buffer: &mut [u8], create: bool) -> Result<(), Error> {
    if buffer.len() as u32 != dbfile.get_page_size() {
        return Err(Error::IncorrectBufferSize);
    }

    // Update our file-IO performance counters
    //        updateFileIOPerfStats(dbFile, pageNo, /* read */ true, buffer.length);

    let page_start = get_page_start(&dbfile, page_no);

    dbfile.seek(SeekFrom::Start(page_start))
        .and_then(|_| dbfile.read_exact(&mut buffer))
        .or_else(|e| {
            if create {
                // Caller wants to create the page if it doesn't already exist yet.

                // logger.debug(String.format("Requested page %d doesn't yet
                // exist in file %s; creating.", pageNo, dbFile.getDataFile().getName()));

                // ...of course, we don't actually extend the file's size until the page is
                // stored back to the file...
                let new_length = (page_no as u64 + 1) * (dbfile.get_page_size() as u64);

                match dbfile.set_file_length(new_length).and_then(|_| dbfile.flush()) {
                    Ok(_) => unimplemented!(),
                    Err(_) => Err(Error::CantExtendDBFile),
                }
            } else {
                println!("{:?}", e);
                Err(Error::NotFullyRead)
            }
        })
}

impl FileManager {
    pub fn with_directory<P: AsRef<Path>>(base_dir: P) -> Result<FileManager, Error> {
        if !base_dir.as_ref().exists() || !base_dir.as_ref().is_dir() {
            return Err(Error::InvalidBaseDir);
        }
        Ok(FileManager {
            base_dir: base_dir.as_ref().to_path_buf(),
            last_accessed: None,
        })
    }

    pub fn get_file_paths(&self) -> Result<Vec<PathBuf>, Error> {
        let dir = fs::read_dir(self.base_dir.as_path()).map_err(|_| Error::FilePathsError);
        if let Ok(dir) = dir {
            dir.map(|entry| {
                    match entry {
                        Ok(e) => Ok(e.path()),
                        _ => Err(Error::FilePathsError),
                    }
                })
                .collect()
        } else {
            Err(Error::FilePathsError)
        }
    }

    /// This method checks if a database file exists.
    ///
    /// # Arguments
    /// * filename - the filename the DBFile is backed by.
    pub fn dbfile_exists<P: AsRef<Path>>(&self, filename: P) -> bool {
        self.base_dir.clone().join(filename).exists()
    }

    /// This method removes a database file in the storage directory.
    ///
    /// # Arguments
    /// * filename - the filename the DBFile is backed by.
    ///
    /// # Errors
    /// This function will return an error in the following situations:
    ///
    /// * The file does not exist.
    pub fn remove_dbfile<P: AsRef<Path>>(&self, filename: P) -> Result<(), Error> {
        if !self.dbfile_exists(&filename) {
            return Err(Error::DBFileDoesNotExist);
        }

        fs::remove_file(self.base_dir.clone().join(filename)).map_err(Into::into)
    }

    /// This method creates a new database file in the directory used by the
    /// storage manager.
    ///
    /// # Arguments
    /// * filename - the filename the DBFile is backed by.
    /// * file_type - The type of the DBFile.
    /// * page_size - The page size the DBFile will use.
    ///
    /// # Errors
    /// This function will return an error in the following situations:
    ///
    /// * The file already exists.
    /// * The DBFile cannot be created successfully.
    pub fn create_dbfile<P: AsRef<Path>>(&self,
                                         filename: P,
                                         file_type: DBFileType,
                                         page_size: u32)
                                         -> Result<DBFile<File>, Error> {
        let mut full_path = self.base_dir.clone();
        full_path.push(filename);

        if full_path.exists() {
            return Err(Error::DBFileExists);
        }

        let file = File::create(full_path.as_path());
        match file {
            Ok(f) => {
                match DBFile::with_path(file_type, page_size, f, full_path.clone()) {
                    Ok(mut db_file) => {
                        let mut buffer = vec![0; page_size as usize];
                        buffer[0] = file_type as u8;
                        buffer[1] = 0x09;

                        // logger.debug("Creating new database file " + f +
                        // ".");
                        let save_result = save_page(&mut db_file, 0, buffer.as_slice())
                            .and_then(|_| db_file.flush().map_err(Into::into));

                        match save_result {
                            Ok(()) => Ok(db_file),
                            Err(e) => Err(e.into()),
                        }
                    }
                    Err(e) => Err(e.into()),
                }
            }
            Err(_) => Err(Error::CantCreateFile),
        }
    }

    /// Attempts to rename the specified
    /// [`DBFile`](../dbfile/struct.DBFile.html) to a new filename.
    /// If successful, the `DBFile` object itself is updated with a new {@link
    /// File} object
    /// reflecting the new name. If failure, the `DBFile` object is left
    /// untouched.
    ///
    /// # Arguments
    /// * dbfile - The DBFile to be renamed.
    /// * new_name - The new filename.
    #[allow(unused_variables)]
    pub fn rename_dbfile<P: AsRef<Path>>(&self, dbfile: DBFile<File>, new_name: P) -> Result<(), Error> {
        unimplemented!()
    }

    /// This method opens a database file, and reads in the file's type and
    /// page size from the first
    /// two bytes of the first page. The method reads an unsigned short for the
    /// page size when the
    /// file is opened.
    ///
    /// # Arguments
    /// * filename - The name of the database file to open.
    ///
    /// # Errors
    /// This function will return an error in the following situations:
    ///
    /// * The file does not exist.
    /// * The DBFile's header is corrupted.
    /// * The DBFile's type or page size are invalid.
    pub fn open_dbfile<P: AsRef<Path>>(&self, filename: P) -> Result<DBFile<File>, Error> {
        let mut full_path = self.base_dir.clone();
        full_path.push(filename);

        if !full_path.exists() {
            return Err(Error::DBFileDoesNotExist);
        }

        let file = File::open(full_path.as_path());
        if file.is_err() {
            return Err(Error::CantOpenFile);
        }
        let mut file = file.unwrap();
        let mut buf = [0u8; 2];
        let read_result = file.read_exact(&mut buf)
            .map_err(|_| Error::DBFileParseError);
        if read_result.is_err() {
            return Err(Error::DBFileParseError);
        }

        match parse_header(&buf) {
            IResult::Done(_, (type_id, page_size)) => {
                let file_type = match type_id {
                    1 => DBFileType::HeapTupleFile,
                    2 => DBFileType::BTreeTupleFile,
                    3 => DBFileType::TxnStateFile,
                    4 => DBFileType::WriteAheadLogFile,
                    _ => {
                        return Err(Error::InvalidDBFileType);
                    }
                };
                match page_size {
                    Ok(size) => {
                        // logger.debug(String.format("Opened existing database file %s; type is
                        // %s, page size is %d.", f, type, pageSize));

                        DBFile::with_path(file_type, size, file, full_path.clone()).map_err(Into::into)
                    }
                    Err(_) => Err(Error::InvalidDBFilePageSize),
                }
            }
            _ => Err(Error::DBFileParseError),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{File, OpenOptions};
    use std::io::{Cursor, Write};
    use std::path::{Path, PathBuf};

    use super::{Error, FileManager, get_page_start, load_page, save_page};
    use super::super::dbfile::{DBFile, DBFileType};

    use tempdir::TempDir;

    #[test]
    fn test_file_manager_creation() {
        if let Ok(dir) = TempDir::new("test_dbfiles") {
            let file_path = dir.path().join("foo.tbl");
            File::create(&file_path).unwrap();

            assert_eq!(Err(Error::InvalidBaseDir),
                       FileManager::with_directory("bar.txt"));
            assert_eq!(Err(Error::InvalidBaseDir),
                       FileManager::with_directory(&file_path));
            assert_eq!(Ok(FileManager {
                           base_dir: dir.path().to_path_buf(),
                           last_accessed: None,
                       }),
                       FileManager::with_directory(dir.path()));
        } else {
            panic!("Unable to create test_dbfiles directory!");
        }
    }

    #[test]
    fn test_file_manager_list_files() {
        if let Ok(dir) = TempDir::new("test_dbfiles") {
            let file_path = dir.path().join("foo.tbl");
            File::create(&file_path).unwrap();

            let file_manager = FileManager::with_directory(dir.path()).unwrap();

            assert_eq!(Ok(vec![PathBuf::from(file_path.clone())]),
                       file_manager.get_file_paths());
        } else {
            panic!("Unable to create test_dbfiles directory!");
        }
    }

    #[test]
    fn test_create_dbfile() {
        if let Ok(dir) = TempDir::new("test_dbfiles") {
            let file_manager = FileManager::with_directory(dir.path()).unwrap();

            let file_path = dir.path().join("foo.tbl");
            File::create(&file_path).unwrap();

            assert_eq!(Err(Error::DBFileExists),
                       file_manager.create_dbfile("foo.tbl", DBFileType::HeapTupleFile, 512));

            let bar_file = file_manager.create_dbfile(Path::new("bar.tbl"), DBFileType::HeapTupleFile, 512);
            assert!(bar_file.is_ok());
        } else {
            panic!("Unable to create test_dbfiles directory!");
        }
    }

    #[test]
    fn test_open_dbfile() {
        if let Ok(dir) = TempDir::new("test_dbfiles") {
            let file_manager = FileManager::with_directory(dir.path()).unwrap();

            let file_path = dir.path().join("foo.tbl");
            // Haven't created file yet
            assert_eq!(Err(Error::DBFileDoesNotExist),
                       file_manager.open_dbfile(&file_path));

            let mut file = File::create(&file_path).unwrap();
            let file_type = DBFileType::BTreeTupleFile;

            // Empty file won't parse
            assert_eq!(Err(Error::DBFileParseError),
                       file_manager.open_dbfile(&file_path));

            file.write(&[file_type as u8, 0x09]).unwrap();
            file.flush().unwrap();

            let expected = DBFile::with_path(file_type, 512, file, &file_path).map_err(|e| Error::DBFileError(e));
            assert_eq!(expected, file_manager.open_dbfile(&file_path));
        } else {
            panic!("Unable to create test_dbfiles directory!");
        }
    }

    #[test]
    fn test_page_start() {
        let dbfile = DBFile::new(DBFileType::HeapTupleFile, 512, Cursor::new(vec![])).unwrap();
        assert_eq!(0u64, get_page_start(&dbfile, 0));
        assert_eq!(512u64, get_page_start(&dbfile, 1));
    }

    #[test]
    fn test_save_page() {
        let mut dbfile = DBFile::new(DBFileType::HeapTupleFile, 512, Cursor::new(vec![0; 512])).unwrap();

        let first_page = [0xac; 512];
        let second_page = [0xfd; 512];

        assert_eq!(Err(Error::IncorrectBufferSize),
                   save_page(&mut dbfile, 0, &[0; 5]));
        assert_eq!(Ok(()), save_page(&mut dbfile, 0, &first_page));

        let result = dbfile.get_contents().clone().into_inner();
        let mut expected = first_page.to_vec();
        assert_eq!(Vec::from(&result[..]), expected);

        expected.extend_from_slice(&second_page);
        assert_eq!(Ok(()), save_page(&mut dbfile, 1, &second_page));
        let result = dbfile.get_contents().clone().into_inner();
        assert_eq!(Vec::from(&result[..]), expected);
    }

    #[test]
    fn test_load_page() {
        let dir = TempDir::new("test_dbfiles").expect("Unable to create test_dbfiles directory!");

        let file_path = dir.path().join("foo.tbl");

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path.as_path())
            .unwrap();
        let file_type = DBFileType::HeapTupleFile;
        file.write(&[file_type as u8, 0x09]).unwrap();
        file.write(&[0xaf; 510]).unwrap();
        file.flush().unwrap();

        let mut dbfile = DBFile::with_path(file_type, 512, file, &file_path).unwrap();

        let mut result = [0u8; 512];
        let mut expected = vec![file_type.clone() as u8, 0x09];
        expected.extend_from_slice(&[0xaf; 510][..]);

        assert_eq!(Err(Error::NotFullyRead),
                   load_page(&mut dbfile, 1000, &mut result, false));
        assert_eq!(Ok(()), load_page(&mut dbfile, 0, &mut result, false));
        assert_eq!(expected.as_slice(), &result[..]);
    }
}
