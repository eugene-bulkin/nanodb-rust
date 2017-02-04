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
