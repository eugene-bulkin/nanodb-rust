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

/// An error that occurs while working with expressions.
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// The column name given is ambiguous.
    AmbiguousColumnName(ColumnName),
    /// Unable to resolve the column name.
    CouldNotResolve(ColumnName),
    /// An expression requiring numeric values was provided with a non-numeric value.
    NotNumeric(Literal),
    /// The expression provided needs more than one clause.
    EmptyExpression,
    /// An expression was expecting a boolean value and received a non-boolean value.
    NotBoolean(Literal),
    /// This expression's evaluation has not been implemented yet.
    Unimplemented,
}
