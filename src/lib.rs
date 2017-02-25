#![deny(missing_docs)]
//! NanoDB is a simple SQL relational database suitable for use in courses about relational
//! database implementation.  Even with a simple implementation, NanoDB has grown to become quite a
//! substantial code-base.

#[macro_use]
extern crate nom;
extern crate rustyline;
extern crate tempdir;
extern crate byteorder;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;

pub mod parser;
pub mod storage;
pub mod column;
pub mod schema;
pub mod commands;
pub mod server;
pub mod expressions;

pub use column::{ColumnInfo, ColumnName, ColumnType};
pub use schema::Schema;
pub use server::{Client, Server};
