//! This module contains the classes for the Storage Manager, which is responsible for how data is
//! stored in and retrieved from database files.
//!
//! # Startup Sequence
//!
//! The start-up sequence for the storage layer is as follows:
//!
//! *TODO*
//!
//! # Implementing New Tuple-File Formats
//!
//! Adding new tuple-file formats to NanoDB should be reasonably straightforward, but there are
//! several interfaces that must all be implemented for the tuple file to be operational inside
//! NanoDB.
//!
//! - The core implementation of the tuple-file format must be provided as an implementation of the
//! [`TupleFile`]() interface, or one of its sub-interfaces. Depending on what the tuple-file format
//! can provide, it may be appropriate to implement [`SequentialTupleFile`]() for a format that
//! maintains a logical ordering over all tuples, or [`HashedTupleFile`]() for a format that
//! supports
//! constant-time tuple lookups using a subset of the tuple's columns. If none of these guarantees
//! can be provided, then the [`TupleFile`]() interface is the correct one to implement.
//! - Certain operations on tuple files can't be provided on the [`TupleFile`]() implementation
//! itself, so they are provided by the [`TupleFileManager`]() interface.

pub mod dbfile;
pub mod dbpage;
pub mod header_page;
pub mod file_manager;
pub mod table_manager;
pub mod tuple_files;
pub mod storage_manager;

use byteorder::WriteBytesExt;

pub use self::dbfile::{DBFile, DBFileInfo, DBFileType};
pub use self::dbpage::DBPage;
pub use self::file_manager::FileManager;
pub use self::header_page::HeaderPage;
pub use self::table_manager::TableManager;

use super::expressions::Literal;

use std::io;

#[derive(Debug, Copy, Clone, PartialEq)]
/// An error that may occur while pinning or unpinning a page in some file.
pub enum PinError {
    /// A caller attempted to unpin a `Pinnable` object, but the pin count was not positive; i.e.
    /// the page had not been pinned in the first place.
    PinCountNotPositive,
}

/// This interface provides the basic "pin" and "unpin" operations that pinnable
/// objects need to provide.  An object's pin-count is simply a reference count,
/// but with a shorter name so it's easier to type!
///
/// Currently, tuples and data pages are pinnable.
pub trait Pinnable {
    /// Increase the pin-count on the object by one.  An object with a nonzero
    /// pin-count cannot be released because it is in use.
    fn pin(&mut self);

    /// Decrease the pin-count on the object by one.  When the pin-count
    /// reaches zero, the object can be released.
    fn unpin(&mut self) -> Result<(), PinError>;

    /// Returns the total number of times the object has been pinned.
    fn get_pin_count(&self) -> u32;

    /// Returns true if the object is currently pinned, false otherwise.
    fn is_pinned(&self) -> bool;
}

/// This interface provides additional writing operations for writing any given column type.
pub trait WriteNanoDBExt: WriteBytesExt {
    /// Write a string to the output, assuming that it is a VARCHAR that fits in 255 bytes (i.e.
    /// the length can be represented in one byte).
    ///
    /// # Arguments
    /// * string - The string to write.
    ///
    /// # Errors
    /// This will fail if writing the length or the bytes in the string themselves fail.
    fn write_varchar255<S>(&mut self, string: S) -> io::Result<()>
        where S: Into<String>
    {
        let bytes = string.into().into_bytes();

        try!(self.write_u8(bytes.len() as u8));
        try!(self.write(&bytes));
        Ok(())
    }
}

impl<W: io::Write + ?Sized> WriteNanoDBExt for W {}

/// Errors that can occur while handling a tuple.
pub enum TupleError {
    /// For when an IO error occurs.
    IOError,
    /// For when a column type is not supported for storage.
    UnsupportedColumnType,
    /// For when the column index provided is out of range.
    InvalidColumnIndex,
}

impl From<io::Error> for TupleError {
    fn from(_: io::Error) -> Self {
        TupleError::IOError
    }
}

/// This interface provides the operations that can be performed with a tuple. In relational
/// database theory, a tuple is an ordered set of attribute-value pairs, but in this implementation
/// the tuple's data and its schema are kept completely separate. This tuple interface simply
/// provides an index-accessed collection of values; the schema would be represented separately
/// using the {@link Schema} class.
///
/// Different implementations of this interface store their data in different places. Some tuple
/// implementations (e.g. subclasses of {@link edu.caltech.nanodb.storage.PageTuple}) load and store
/// values straight out of a tuple file, and thus their data is backed by a buffer page that can be
/// written back to the filesystem. Other tuples may exist entirely in memory, with no corresponding
/// back-end storage.
pub trait Tuple: Pinnable {
    /// Returns true if this tuple is backed by a disk page that must be kept in memory as long as
    /// the
    /// tuple is in use. Some tuple implementations allocate memory to store their values, and are
    /// therefore not affected if disk pages are evicted from the Buffer Manager. Others are backed
    /// by
    /// disk pages, and the disk page cannot be evicted until the tuple is no longer being used. In
    /// cases where a plan-node needs to hold onto a tuple for a long time (e.g. for sorting or
    /// grouping), the plan node should probably make a copy of disk-backed tuples, or materialize
    /// the
    /// results, etc.
    fn is_disk_backed(&self) -> bool;

    /// Determine if the column at index `col_index` is `NULL`.
    ///
    /// # Arguments
    /// * col_index - The index of the column to check is `NULL`
    fn is_null_value(&self, col_index: usize) -> Result<bool, TupleError>;

    /// Returns a count of the number of columns in the tuple.
    fn get_column_count(&self) -> usize;

    /// Returns the value of a column.
    ///
    /// # Arguments
    /// * col_index - The index of the column
    fn get_column_value(&self, col_index: usize) -> Literal;

    /// Clones the tuple into a box.
    fn clone_boxed(&self) -> Box<Tuple>;
}
