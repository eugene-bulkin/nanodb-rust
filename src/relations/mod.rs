//! This package contains the basic data-types for representing relations in NanoDB. Relations can
//! be many different things - tables, views, external tables, or results of subqueries in a
//! particular SQL query, for example. All of the common classes are in this package.

mod column;
mod schema;

pub use self::column::{ColumnName, ColumnType, ColumnInfo, EMPTY_CHAR, EMPTY_NUMERIC, EMPTY_VARCHAR,
                       column_name_to_string};
pub use self::schema::{Schema, Error as SchemaError};