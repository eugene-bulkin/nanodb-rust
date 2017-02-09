//! This module contains classes corresponding to handling tuple files in the data directory.
//!
//! Tuple files can be stored in two different ways: via a heap storage method or a B-tree storage
//! method.

pub mod heap_tuple_file;

pub use self::heap_tuple_file::HeapTupleFile;
