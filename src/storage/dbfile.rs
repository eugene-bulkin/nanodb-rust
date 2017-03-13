//! This module contains utilities to handle database files for NanoDB.
//!
//! The main class in this module is `DBFile`, which handles page-level access to database files.
//! `DBFile`s are created by using the {@link StorageManager#openDBFile} method (or perhaps one of
//! the wrapper methods such as {@link StorageManager#openTable} or {@link
//! StorageManager#openWALFile(int)}). This allows the `StorageManager` to provide caching of
//! opened `DBFile`s so that unnecessary IOs can be avoided. (Internally, the `StorageManager` uses
//! [`FileManager.open_dbfile`](../file_manager/struct.FileManager.html#method.open_dbfile) to open
//! files, and the {@link BufferManager} to cache opened files and loaded pages.)

use std::cmp::PartialEq;
use std::fs::File;
use std::io::{self, SeekFrom};
use std::io::prelude::*;
use std::path::{Path, PathBuf};

/// The minimum page size is 512 bytes.
const MIN_PAGESIZE: u32 = 512;

/// The maximum page size is 64K bytes.
const MAX_PAGESIZE: u32 = 65536;

/// The default page size is 8K bytes.
#[allow(dead_code)]
const DEFAULT_PAGESIZE: u32 = 8192;

#[derive(Debug, Copy, Clone, PartialEq)]
/// An error in creating or using a `DBFile`.
pub enum Error {
    /// The page size provided to the file is invalid.
    InvalidPageSize(u32),
}

impl ::std::fmt::Display for Error {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            Error::InvalidPageSize(size) => write!(f, "The page size {} is not valid for a DB file.", size),
        }
    }
}

/// This static helper method returns true if the specified page size is valid; i.e. it must be
/// within the minimum and maximum page sizes, and it must be a power of two.
pub fn is_valid_pagesize(page_size: u32) -> bool {
    (page_size >= MIN_PAGESIZE && page_size <= MAX_PAGESIZE) && (page_size & (page_size - 1) == 0)
}


/// Given a valid page size, this method returns the base-2 logarithm of the page size for storing
/// in a data file.
///
/// # Examples
///
/// ```
/// # use self::nanodb::storage::dbfile::{encode_pagesize, Error};
/// assert_eq!(encode_pagesize(512), Ok(9));
/// assert_eq!(encode_pagesize(513), Err(Error::InvalidPageSize(513)));
/// ```
pub fn encode_pagesize(page_size: u32) -> Result<u32, Error> {
    if !is_valid_pagesize(page_size) {
        Err(Error::InvalidPageSize(page_size))
    } else {
        let mut encoded = 0;
        let mut cur_size = page_size;
        while cur_size > 1 {
            cur_size >>= 1;
            encoded += 1;
        }

        Ok(encoded)
    }
}

/// Given the base-2 logarithm of a page size, this method returns the actual page size.
///
/// # Examples
///
/// ```
/// # use self::nanodb::storage::dbfile::{decode_pagesize, Error};
/// assert_eq!(decode_pagesize(9), Ok(512));
/// assert_eq!(decode_pagesize(3), Err(Error::InvalidPageSize(8)));
/// assert_eq!(decode_pagesize(30), Err(Error::InvalidPageSize(1073741824)));
/// ```
pub fn decode_pagesize(encoded: u32) -> Result<u32, Error> {
    let page_size = 1 << encoded;
    if is_valid_pagesize(page_size) {
        Ok(page_size)
    } else {
        Err(Error::InvalidPageSize(page_size))
    }
}

/// This enumeration specifies the different types of data file that the database knows about. Each
/// file type is assigned a unique integer value in the range [0, 255], which is stored as the very
/// first byte of data files of that type.  This way, it's straightforward to determine a file's
/// type by examination.
#[derive(Debug, Clone, PartialEq, Copy)]
pub enum DBFileType {
    /// Represents a heap tuple file, which supports variable-size tuples and stores them in no
    /// particular order.
    HeapTupleFile = 1,
    /// Represents a B<sup>+</sup> tree tuple file that keeps tuples in a particular order.
    BTreeTupleFile = 2,
    /// Represents a transaction-state file used for write-ahead logging and recovery.
    TxnStateFile = 3,
    /// Represents a write-ahead log file used for transaction processing and recovery.
    WriteAheadLogFile = 4,
    /// Represents an unknown file type (usually due to corrupted data).
    Unknown,
}

impl From<u8> for DBFileType {
    fn from(byte: u8) -> DBFileType {
        match byte {
            1 => DBFileType::HeapTupleFile,
            2 => DBFileType::BTreeTupleFile,
            3 => DBFileType::TxnStateFile,
            4 => DBFileType::WriteAheadLogFile,
            _ => DBFileType::Unknown,
        }
    }
}

/// This struct stores identifying information on `DBFile`s. Since the contents may vary, but the
/// location of the file and other metadata will uniquely identify it, this is the best way to store
/// data about the `DBFile` itself.
#[derive(Clone, Debug)]
pub struct DBFileInfo {
    /// The storage type of the underlying `DBFile`.
    pub file_type: DBFileType,
    /// The page size of the data in the `DBFile`.
    pub page_size: u32,
    /// An optional path of the backing file.
    pub path: Option<PathBuf>,
}

impl PartialEq for DBFileInfo {
    fn eq(&self, other: &Self) -> bool {
        if self.page_size != other.page_size || self.file_type != other.file_type {
            return false;
        }
        // If there's no backing path, *always* return false.
        match self.path {
            Some(ref path) => {
                match other.path {
                    None => false,
                    Some(ref other_path) => path == other_path,
                }
            }
            None => false,
        }
    }
}

/// This class provides page-level access to a database file, which contains
/// some kind of data utilized in a database system.  This class may be utilized
/// for many different kinds of database files.  Here is an example of the kinds
/// of data that might be stored in files:
///
/// - Tuples in a database table.
/// - Table indexes in a hashtable, tree, or some other format.
/// - Recovery logs.
/// - Checkpoint files.
///
/// For a file to be opened as a `DBFile`, it must have specific details
/// stored in the first page of the file:
///
/// - __Byte 0:__  file type (unsigned byte) - value taken from [`DBFileType`](enum.DBFileType.html)
/// - __Byte 1:__  page size _p_ (unsigned byte) - file's page size is _P_ = 2<sup>p</sup>
#[derive(Debug, Clone)]
pub struct DBFile<F: Read + Seek + Write> {
    /// The DB file metadata, not dependent on the content.
    pub file_info: DBFileInfo,
    contents: F,
}

impl<F> ::std::ops::Deref for DBFile<F>
    where F: Read + Seek + Write
{
    type Target = DBFileInfo;
    fn deref(&self) -> &Self::Target {
        &self.file_info
    }
}

impl<F: Read + Seek + Write> DBFile<F> {
    /// Creates a new `DBFile` with some contents.
    ///
    /// # Errors
    ///
    /// If the page size passed in is invalid, this will return an
    /// [`InvalidPageSize`](enum.Error.html#variant.InvalidPageSize) error.
    pub fn new(file_type: DBFileType, page_size: u32, contents: F) -> Result<DBFile<F>, Error> {
        if !is_valid_pagesize(page_size) {
            Err(Error::InvalidPageSize(page_size))
        } else {
            Ok(DBFile {
                file_info: DBFileInfo {
                    file_type: file_type,
                    page_size: page_size,
                    path: None,
                },
                contents: contents,
            })
        }
    }

    /// Creates a new `DBFile` with some contents and a backing path, usually corresponding to a
    /// `File`.
    ///
    /// # Errors
    ///
    /// If the page size passed in is invalid, this will return an
    /// [`InvalidPageSize`](enum.Error.html#variant.InvalidPageSize) error.
    pub fn with_path<P: AsRef<Path>>(file_type: DBFileType,
                                     page_size: u32,
                                     contents: F,
                                     path: P)
                                     -> Result<DBFile<F>, Error> {
        if !is_valid_pagesize(page_size) {
            Err(Error::InvalidPageSize(page_size))
        } else {
            Ok(DBFile {
                file_info: DBFileInfo {
                    file_type: file_type,
                    page_size: page_size,
                    path: Some(path.as_ref().to_path_buf()),
                },
                contents: contents,
            })
        }
    }

    /// Retrieve the page size of the current `DBFile`.
    pub fn get_page_size(&self) -> u32 {
        self.page_size
    }

    /// Retrieve a reference to the contents of the current `DBFile`.
    pub fn get_contents(&self) -> &F {
        &self.contents
    }
}

impl DBFile<File> {
    /// Sets the file length of the underlying file, if the `DBFile` is backed by an actual file
    /// object.
    ///
    /// # Arguments
    /// * size - The new file size.
    pub fn set_file_length(&mut self, size: u64) -> io::Result<()> {
        self.contents.set_len(size)
    }
}

impl<F> Read for DBFile<F>
    where F: Read + Seek + Write
{
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        self.contents.read(&mut buf)
    }
}

impl<F> Seek for DBFile<F>
    where F: Read + Seek + Write
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.contents.seek(pos)
    }
}

impl<F> Write for DBFile<F>
    where F: Read + Seek + Write
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.contents.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.contents.flush()
    }
}

impl<F> PartialEq for DBFile<F>
    where F: Read + Seek + Write
{
    fn eq(&self, other: &Self) -> bool {
        self.file_info == other.file_info
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_pagesize() {
        // This is too small.
        assert!(!is_valid_pagesize(256));

        // This is too large.
        assert!(!is_valid_pagesize(131072));

        // These are not a power of 2.
        assert!(!is_valid_pagesize(511));
        assert!(!is_valid_pagesize(513));
        assert!(!is_valid_pagesize(1023));
        assert!(!is_valid_pagesize(1025));
        assert!(!is_valid_pagesize(6144));
        assert!(!is_valid_pagesize(65537));
        assert!(!is_valid_pagesize(515));
        assert!(!is_valid_pagesize(1063));
        assert!(!is_valid_pagesize(3072));
        assert!(!is_valid_pagesize(4095));
        assert!(!is_valid_pagesize(10000));
        assert!(!is_valid_pagesize(65535));

        // These are valid sizes.
        assert!(is_valid_pagesize(512));
        assert!(is_valid_pagesize(1024));
        assert!(is_valid_pagesize(2048));
        assert!(is_valid_pagesize(4096));
        assert!(is_valid_pagesize(8192));
        assert!(is_valid_pagesize(65536));
    }

    #[test]
    fn test_encode_pagesize() {
        assert_eq!(encode_pagesize(512), Ok(9));
        assert_eq!(encode_pagesize(1024), Ok(10));
        assert_eq!(encode_pagesize(2048), Ok(11));
        assert_eq!(encode_pagesize(4096), Ok(12));
        assert_eq!(encode_pagesize(8192), Ok(13));
        assert_eq!(encode_pagesize(16384), Ok(14));
        assert_eq!(encode_pagesize(32768), Ok(15));
        assert_eq!(encode_pagesize(65536), Ok(16));

        // Errors
        assert_eq!(encode_pagesize(32), Err(Error::InvalidPageSize(32)));
        assert_eq!(encode_pagesize(33), Err(Error::InvalidPageSize(33)));
        assert_eq!(encode_pagesize(131072), Err(Error::InvalidPageSize(131072)));
    }

    #[test]
    fn test_decode_pagesize() {
        assert_eq!(decode_pagesize(9), Ok(512));
        assert_eq!(decode_pagesize(10), Ok(1024));
        assert_eq!(decode_pagesize(11), Ok(2048));
        assert_eq!(decode_pagesize(12), Ok(4096));
        assert_eq!(decode_pagesize(13), Ok(8192));
        assert_eq!(decode_pagesize(14), Ok(16384));
        assert_eq!(decode_pagesize(15), Ok(32768));
        assert_eq!(decode_pagesize(16), Ok(65536));

        // Errors
        assert_eq!(decode_pagesize(5), Err(Error::InvalidPageSize(32)));
        assert_eq!(decode_pagesize(17), Err(Error::InvalidPageSize(131072)));
    }
}
