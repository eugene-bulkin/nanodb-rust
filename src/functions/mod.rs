//! This module stores all functions that NanoDB uses, as well as utilities for calling them from
//! the database.

pub mod directory;

#[macro_use]
mod utils;

mod arithmetic;
mod coalesce;

pub use self::directory::Directory;
pub use self::{Error as FunctionError};

use ::expressions::{Environment, Expression, ExpressionError, Literal};
use ::relations::{ColumnType, Schema};

/// This is the root class of all kinds of functions in NanoDB.
///
/// Functions must support cloning because the implementation classes often carry their own internal
/// state values, and clearly the same function being invoked in two different parts of the same
/// query, or being invoked concurrently by two different queries, shouldn't have a single shared
/// set of state values. So, the simple thing to do is to just clone functions when they are
/// retrieved from the {@link FunctionDirectory}.
pub trait Function: Sync {
    /// Evaluates a function given an environment (if one exists) and some arguments.
    fn evaluate(&self, env: &mut Option<&mut Environment>, args: Vec<Expression>) -> FunctionResult;

    /// Returns the function as a ScalarFunction if possible. By default this doesn't work.
    fn get_as_scalar(&self) -> Option<Box<ScalarFunction>> { None }
}

/// This is a function that returns a scalar, and thus has a specific return column type.
pub trait ScalarFunction: Function {
    /// Returns the column type the function should typically return, given the table schema the
    /// function is being used on.
    fn get_return_type(&self, args: Vec<Expression>, schema: &Schema) -> Result<ColumnType, FunctionError>;
}

/// An error that can occur while calling or retrieving a function.
#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    /// The function requested does not exist.
    DoesNotExist(String),
    /// The function provided cannot take zero arguments.
    NeedsArguments(String),
    /// The function provided does not have enough arguments.
    NeedsMoreArguments(String, usize, usize),
    /// Could not retrieve a column type for an expression.
    CouldNotRetrieveExpressionColumnType(Expression, Box<ExpressionError>),
    /// Could not evaluate an expression.
    CouldNotEvaluateExpression(Expression, Box<ExpressionError>),
    /// The expression provided is not numeric.
    ExpressionNotNumeric(Expression),
    /// The function has not been implemented yet.
    Unimplemented(String),
}

impl ::std::fmt::Display for Error {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            Error::DoesNotExist(ref name) => {
                write!(f, "The function {} does not exist.", name)
            },
            Error::NeedsArguments(ref name) => {
                write!(f, "The function {} requires at least one argument.", name)
            },
            Error::NeedsMoreArguments(ref name, ref needs, ref got) => {
                write!(f, "The function {} requires {} arguments, got {}.", name, needs, got)
            },
            Error::CouldNotRetrieveExpressionColumnType(ref expr, ref e) => {
                write!(f, "Could not determine the column type for {}: {}", expr, e)
            },
            Error::CouldNotEvaluateExpression(ref expr, ref e) => {
                write!(f, "Could not evaluate the expression {}: {}", expr, e)
            },
            Error::ExpressionNotNumeric(ref expr) => {
                write!(f, "The expression {} is not numeric.", expr)
            },
            Error::Unimplemented(ref name) => {
                write!(f, "The function {} is not implmented.", name)
            }
        }
    }
}

/// A result from a function call. Either a literal is returned or an error occurred.
pub type FunctionResult = Result<Literal, Error>;