//! This module contains utilities for dealing with expressions, including the `Expression` struct.

use super::{ArithmeticType, CompareType, Literal};

/// A SQL-supported expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// A Boolean OR expression
    OR(Vec<Expression>),
    /// A Boolean AND expression
    AND(Vec<Expression>),
    /// A Boolean NOT expression
    NOT(Box<Expression>),
    /// A comparison expression
    Compare(Box<Expression>, CompareType, Box<Expression>),
    /// An IS NULL operator
    IsNull(Box<Expression>),
    /// An arithmetic expression
    Arithmetic(Box<Expression>, ArithmeticType, Box<Expression>),
    /// NULL
    Null,
    /// TRUE
    True,
    /// FALSE
    False,
    /// An integer
    Int(i32),
    /// A long
    Long(i64),
    /// A float
    Float(f32),
    /// A double
    Double(f64),
    /// A string
    String(String),
}

impl From<Literal> for Expression {
    fn from(literal: Literal) -> Self {
        match literal {
            Literal::Int(i) => Expression::Int(i),
            Literal::Long(l) => Expression::Long(l),
            Literal::Float(f) => Expression::Float(f),
            Literal::Double(d) => Expression::Double(d),
            Literal::String(s) => Expression::String(s),
            Literal::Null => Expression::Null,
            Literal::True => Expression::True,
            Literal::False => Expression::False,
        }
    }
}
