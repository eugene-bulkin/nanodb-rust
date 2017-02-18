//! A module containing classes for representing and evaluating arithmetic and logical expressions.

pub mod expression;
pub mod literal;

pub use self::expression::Expression;
pub use self::literal::Literal;

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