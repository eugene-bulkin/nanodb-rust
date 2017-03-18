#![deny(missing_docs)]
//! NanoDB is a simple SQL relational database suitable for use in courses about relational
//! database implementation.  Even with a simple implementation, NanoDB has grown to become quite a
//! substantial code-base.

#[macro_use]
extern crate nom;
extern crate rustyline;
extern crate tempdir;
extern crate byteorder;
#[cfg_attr(test, macro_use)]
extern crate lazy_static;
#[macro_use]
extern crate log;

pub mod commands;
pub mod expressions;
pub mod parser;
pub mod queries;
pub mod relations;
pub mod storage;
pub mod server;

pub use relations::{Schema, SchemaError, ColumnInfo, ColumnName, ColumnType};
pub use server::{Client, Server};
