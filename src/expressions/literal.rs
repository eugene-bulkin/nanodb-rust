//! This module contains utilities and classes for SQL literals.

/// An enum representing a SQL literal.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// An integer
    Int(i32),
    /// A big integer (long)
    Long(i64),
    /// A double
    Double(f64),
    /// A float
    Float(f32),
    /// A string
    String(String),
    /// A `NULL` value
    Null,
    /// A `TRUE` value
    True,
    /// A `FALSE` value
    False,
}
