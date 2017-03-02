//! A module containing classes for representing and evaluating arithmetic and logical expressions.

pub mod expression;
pub mod processor;
pub mod environment;
pub mod literal;

pub use self::Error as ExpressionError;
pub use self::environment::Environment;
pub use self::expression::Expression;
pub use self::literal::Literal;
pub use self::processor::Processor as ExpressionProcessor;

use super::ColumnName;

fn col_name_to_string(col_name: &ColumnName) -> String {
    match *col_name {
        (Some(ref table_name), Some(ref column_name)) => format!("{}.{}", table_name, column_name),
        (None, Some(ref column_name)) => column_name.clone(),
        (Some(ref table_name), None) => format!("{}.*", table_name),
        (None, None) => "*".into(),
    }
}

/// Describes a comparison operation
#[derive(Debug, Clone, PartialEq, Copy)]
pub enum CompareType {
    /// Equality
    Equals,
    /// Inequality
    NotEquals,
    /// Less than
    LessThan,
    /// Less than or equal to
    LessThanEqual,
    /// Greater than or equal to
    GreaterThan,
    /// Greater than or equal to
    GreaterThanEqual,
}

impl<'a> From<&'a [u8]> for CompareType {
    fn from(bytes: &'a [u8]) -> Self {
        match bytes {
            b"!=" | b"<>" => CompareType::NotEquals,
            b"<" => CompareType::LessThan,
            b"<=" => CompareType::LessThanEqual,
            b">" => CompareType::GreaterThan,
            b">=" => CompareType::GreaterThanEqual,
            b"=" | b"==" | _ => CompareType::Equals,
        }
    }
}

impl ::std::fmt::Display for CompareType {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            CompareType::NotEquals => write!(f, "!="),
            CompareType::LessThan => write!(f, "<"),
            CompareType::LessThanEqual => write!(f, "<="),
            CompareType::GreaterThan => write!(f, ">"),
            CompareType::GreaterThanEqual => write!(f, ">="),
            CompareType::Equals => write!(f, "="),
        }
    }
}

/// Describes an arithmetic operation.
#[derive(Debug, Clone, PartialEq, Copy)]
pub enum ArithmeticType {
    /// Addition
    Plus,
    /// Subtraction
    Minus,
    /// Multiplication
    Multiply,
    /// Division
    Divide,
    /// Modulo
    Remainder,
}

impl<'a> From<&'a [u8]> for ArithmeticType {
    fn from(bytes: &'a [u8]) -> Self {
        match bytes {
            b"-" => ArithmeticType::Minus,
            b"*" => ArithmeticType::Multiply,
            b"/" => ArithmeticType::Divide,
            b"%" => ArithmeticType::Remainder,
            b"+" | _ => ArithmeticType::Plus,
        }
    }
}

impl ::std::fmt::Display for ArithmeticType {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            ArithmeticType::Minus => write!(f, "-"),
            ArithmeticType::Multiply => write!(f, "*"),
            ArithmeticType::Divide => write!(f, "/"),
            ArithmeticType::Remainder => write!(f, "%"),
            ArithmeticType::Plus => write!(f, "+"),
        }
    }
}

/// An error that occurs while working with expressions.
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// The column name given is ambiguous.
    AmbiguousColumnName(ColumnName),
    /// Unable to resolve the column name.
    CouldNotResolve(ColumnName),
    /// An expression requiring numeric values was provided with a non-numeric value.
    NotNumeric(Literal),
    /// An expression was expecting a boolean value and received a non-boolean value.
    NotBoolean(Literal),
    /// The expression provided needs more than one clause.
    EmptyExpression,
    /// This expression's evaluation has not been implemented yet.
    Unimplemented,
}

impl ::std::fmt::Display for Error {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            Error::AmbiguousColumnName(ref col_name) => {
                write!(f, "The column {} is ambiguous.", col_name_to_string(col_name))
            },
            Error::CouldNotResolve(ref col_name) => {
                write!(f, "The column {} could not be resolved.", col_name_to_string(col_name))
            },
            Error::NotNumeric(ref literal) => {
                write!(f, "The expression was expected to evaluate to a numeric literal, got {}.",
                       literal)
            },
            Error::NotBoolean(ref literal) => {
                write!(f, "The expression was expected to evaluate to a boolean literal, got {}.",
                       literal)
            },
            Error::EmptyExpression => {
                write!(f, "The expression was expecting a set of clauses and got none.")
            },
            Error::Unimplemented => {
                write!(f, "The expression's evaluation has not yet been implemented.")
            },
        }
    }
}