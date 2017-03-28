//! This module stores all functions that NanoDB uses, as well as utilities for calling them from
//! the database.

pub mod directory;

pub use self::directory::Directory;
pub use self::{Error as FunctionError};

use ::expressions::{Environment, Expression, Literal};

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
}

/// An error that can occur while calling or retrieving a function.
#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    /// The function requested does not exist.
    DoesNotExist(String),
    /// The function provided cannot take zero arguments.
    NeedsArguments(String),
}

impl ::std::fmt::Display for Error {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            Error::DoesNotExist(ref name) => {
                write!(f, "The function {} does not exist.", name)
            },
            Error::NeedsArguments(ref name) => {
                write!(f, "The function {} requires at least one argument.", name)
            }
        }
    }
}

/// A result from a function call. Either a literal is returned or an error occurred.
pub type FunctionResult = Result<Literal, Error>;