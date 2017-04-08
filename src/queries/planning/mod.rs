//! This package contains modules for representing query execution plans, as well as analyzing their
//! costs.

pub mod simple_planner;

pub use self::simple_planner::SimplePlanner;

use ::relations::SchemaError;
use ::expressions::{Expression, ExpressionError, SelectClause};
use ::queries::{FileScanNode, NodeResult, PlanNode};
use ::queries::plan_nodes::ProjectError;
use ::storage::{FileManager, PinError, TableManager, TupleError, TupleLiteral, file_manager,
                table_manager};

/// An error that could occur during planning.
#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    /// A file manager error occurred.
    FileManagerError(file_manager::Error),
    /// A table manager error occurred.
    TableManagerError(table_manager::Error),
    /// A schema error occurred.
    SchemaError(SchemaError),
    /// A pin error occurred.
    PinError(PinError),
    /// A projection error occurred.
    ProjectError(ProjectError),
    /// The operation is unimplemented.
    Unimplemented,
    /// The predicate does not evaluate to a boolean.
    InvalidPredicate,
    /// The predicate could not be evaluated.
    CouldNotApplyPredicate(ExpressionError),
    /// Unable to advance to the next tuple in a node.
    CouldNotAdvanceTuple(TupleError),
    /// The node was not prepared before using.
    NodeNotPrepared,
    /// Aggregates are not allowed in WHERE expressions.
    AggregatesInWhereExpr(Vec<Expression>),
    /// An expression error occurred while processing aggregates.
    CouldNotProcessAggregates(ExpressionError),
    /// A tuple was found in a plan that did not match the schema size. In the form of
    /// `(tuple size, schema size)`.
    WrongArity(usize, usize),
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

impl From<SchemaError> for Error {
    fn from(e: SchemaError) -> Error {
        Error::SchemaError(e)
    }
}

impl From<PinError> for Error {
    fn from(e: PinError) -> Error {
        Error::PinError(e)
    }
}

impl From<ProjectError> for Error {
    fn from(e: ProjectError) -> Error {
        Error::ProjectError(e)
    }
}

impl ::std::fmt::Display for Error {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            Error::FileManagerError(ref e) => write!(f, "{}", e),
            Error::TableManagerError(ref e) => write!(f, "{}", e),
            Error::SchemaError(ref e) => write!(f, "{}", e),
            Error::PinError(ref e) => write!(f, "{}", e),
            Error::Unimplemented => write!(f, "The requested operation is not yet implemented."),
            Error::InvalidPredicate => write!(f, "The predicate is invalid."),
            Error::CouldNotApplyPredicate(ref e) => write!(f, "The predicate could not be applied: {}", e),
            Error::CouldNotAdvanceTuple(ref e) => write!(f, "Unable to advance to next tuple in node: {}", e),
            Error::ProjectError(ref e) => write!(f, "Projection failed because {}.", e),
            Error::NodeNotPrepared => write!(f, "A node was not prepared."),
            Error::AggregatesInWhereExpr(ref exprs) => {
                let values: Vec<String> = exprs.iter().map(|e| format!("{}", e)).collect();
                write!(f, "WHERE clause cannot contain aggregates. Found: {}", values.join(", "))
            },
            Error::CouldNotProcessAggregates(ref e) => write!(f, "Could not process aggregates: {}.", e),
            Error::WrongArity(tup_size, schema_size) => {
                write!(f, "Tuple has different arity ({} columns) than target schema ({} columns).",
                       tup_size, schema_size)
            }
        }
    }
}

pub use self::Error as PlanError;

/// A result that returns something and has a plan error.
pub type PlanResult<T> = Result<T, Error>;

/// Executes a plan node and returns a vector of tuple literals that came from the plan.
pub fn get_plan_results(plan: &mut PlanNode) -> PlanResult<Vec<TupleLiteral>> {
    let mut tuples: Vec<TupleLiteral> = Vec::new();
    plan.initialize();

    while let Some(mut boxed_tuple) = try!(plan.get_next_tuple()) {
        let literal = TupleLiteral::from_tuple(&mut *boxed_tuple);
        tuples.push(literal);
    }

    Ok(tuples)
}

/// Returns a plan tree for executing a simple select against a single table, whose tuples can
/// also be used for updating and deletion.
///
/// # Arguments
/// * file_manager - A reference to the file manager.
/// * table_manager - A reference to the table manager.
/// * table_name - The name of the table to select on.
/// * predicate - An optional predicate to filter on.
pub fn make_simple_select<'table, S: Into<String>>(file_manager: &FileManager,
                                                   table_manager: &'table TableManager,
                                                   table_name: S,
                                                   predicate: Option<Expression>)
                                                   -> NodeResult<'table> {
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
    fn make_plan(&self, clause: SelectClause) -> NodeResult;
}
