//! This package contains modules for representing query execution plans, as well as analyzing their
//! costs.

pub mod simple_planner;
pub mod plan_nodes;

pub use self::plan_nodes::{PlanNode, FileScanNode, ProjectNode, NestedLoopJoinNode};
pub use self::simple_planner::SimplePlanner;

use super::super::expressions::{Expression, ExpressionError, SelectClause};
use super::super::storage::{PinError, file_manager, table_manager, FileManager, TableManager};
use ::{ColumnName, schema};
use ::column::column_name_to_string;

/// An error that could occur during planning.
#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    /// A file manager error occurred.
    FileManagerError(file_manager::Error),
    /// A table manager error occurred.
    TableManagerError(table_manager::Error),
    /// A schema error occurred.
    SchemaError(schema::Error),
    /// A pin error occurred.
    PinError(PinError),
    /// The operation is unimplemented.
    Unimplemented,
    /// The predicate does not evaluate to a boolean.
    InvalidPredicate,
    /// The predicate could not be evaluated.
    CouldNotApplyPredicate(ExpressionError),
    /// The specified column does not exist.
    ColumnDoesNotExist(ColumnName),
    /// The node was not prepared before using.
    NodeNotPrepared,
}

impl From<file_manager::Error> for Error {
    fn from(e: file_manager::Error) -> Error {
        Error::FileManagerError(e)
    }
}

impl From<table_manager::Error> for Error {
    fn from(e: table_manager::Error) -> Error {
        Error::TableManagerError(e)
    }
}

impl From<schema::Error> for Error {
    fn from(e: schema::Error) -> Error {
        Error::SchemaError(e)
    }
}

impl From<PinError> for Error {
    fn from(e: PinError) -> Error {
        Error::PinError(e)
    }
}

impl ::std::fmt::Display for Error {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            Error::FileManagerError(ref e) => {
                write!(f, "{}", e)
            },
            Error::TableManagerError(ref e) => {
                write!(f, "{}", e)
            },
            Error::SchemaError(ref e) => {
                write!(f, "{}", e)
            },
            Error::PinError(ref e) => {
                write!(f, "{}", e)
            },
            Error::Unimplemented => {
                write!(f, "The requested operation is not yet implemented.")
            },
            Error::InvalidPredicate => {
                write!(f, "The predicate is invalid.")
            },
            Error::CouldNotApplyPredicate(ref e) => {
                write!(f, "The predicate could not be applied: {}", e)
            },
            Error::ColumnDoesNotExist(ref col_name) => {
                write!(f, "The column {} does not exist.", column_name_to_string(col_name))
            },
            Error::NodeNotPrepared => {
                write!(f, "A node was not prepared.")
            },
        }
    }
}

pub use self::Error as PlanError;

/// A result that returns something and has a plan error.
pub type PlanResult<T> = Result<T, Error>;

/// A result that returns a plan node.
pub type NodeResult<'a> = Result<Box<PlanNode + 'a>, Error>;

/// Returns a plan tree for executing a simple select against a single table, whose tuples can
    /// also be used for updating and deletion.
    ///
    /// # Arguments
    /// * file_manager - A reference to the file manager.
    /// * table_manager - A reference to the table manager.
    /// * table_name - The name of the table to select on.
    /// * predicate - An optional predicate to filter on.
pub fn make_simple_select<'table, S: Into<String>>(file_manager: &FileManager, table_manager: &'table TableManager, table_name: S, predicate: Option<Expression>) -> NodeResult<'table> {
    let table_name = table_name.into();

    let table = try!(table_manager.get_table(file_manager, table_name));

    let mut select_node = FileScanNode::new(table, predicate);
    try!(select_node.prepare());
    Ok(Box::new(select_node))
}

/// This trait specifies the common entry-point for all query planner/optimizer implementations. The
/// trait is very simple, but a particular implementation might be very complicated depending on
/// what kinds of optimizations are implemented. Note that a new planner/optimizer is created for
/// each query being planned.
pub trait Planner {
    /// Create a plan given a SELECT clause.
    fn make_plan(&mut self, clause: SelectClause) -> NodeResult;
}