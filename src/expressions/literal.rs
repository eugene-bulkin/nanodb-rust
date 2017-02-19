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

impl From<bool> for Literal {
    fn from(value: bool) -> Literal {
        if value {
            Literal::True
        } else {
            Literal::False
        }
    }
}

impl Literal {
    /// A utility function for determining if the literal is numeric.
    pub fn is_numeric(&self) -> bool {
        match *self {
            Literal::Double(_) | Literal::Int(_) | Literal::Float(_) | Literal::Long(_) => true,
            _ => false,
        }
    }
    /// A utility function for determining if the literal is a DOUBLE.
    pub fn is_double(&self) -> bool {
        match *self {
            Literal::Double(_) => true,
            _ => false,
        }
    }
    /// A utility function for determining if the literal is a FLOAT.
    pub fn is_float(&self) -> bool {
        match *self {
            Literal::Float(_) => true,
            _ => false,
        }
    }
    /// A utility function for determining if the literal is a LONG.
    pub fn is_long(&self) -> bool {
        match *self {
            Literal::Long(_) => true,
            _ => false,
        }
    }
    /// A utility function for converting to a DOUBLE if possible. If not, returns None.
    pub fn as_double(&self) -> Option<Literal> {
        match *self {
            Literal::Double(d) => Some(Literal::Double(d)),
            Literal::Float(f) => Some(Literal::Double(f as f64)),
            Literal::Int(i) => Some(Literal::Double(i as f64)),
            Literal::Long(l) => Some(Literal::Double(l as f64)),
            _ => None,
        }
    }
    /// A utility function for converting to a FLOAT if possible. If not, returns None.
    pub fn as_float(&self) -> Option<Literal> {
        match *self {
            Literal::Double(d) => Some(Literal::Float(d as f32)),
            Literal::Float(f) => Some(Literal::Float(f as f32)),
            Literal::Int(i) => Some(Literal::Float(i as f32)),
            Literal::Long(l) => Some(Literal::Float(l as f32)),
            _ => None,
        }
    }
    /// A utility function for converting to a DOUBLE if possible. If not, returns None.
    pub fn as_long(&self) -> Option<Literal> {
        match *self {
            Literal::Double(d) => Some(Literal::Long(d as i64)),
            Literal::Float(f) => Some(Literal::Long(f as i64)),
            Literal::Int(i) => Some(Literal::Long(i as i64)),
            Literal::Long(l) => Some(Literal::Long(l)),
            _ => None,
        }
    }
    /// A utility function for converting to a DOUBLE if possible. If not, returns None.
    pub fn as_int(&self) -> Option<Literal> {
        match *self {
            Literal::Double(d) => Some(Literal::Int(d as i32)),
            Literal::Float(f) => Some(Literal::Int(f as i32)),
            Literal::Int(i) => Some(Literal::Int(i)),
            Literal::Long(l) => Some(Literal::Int(l as i32)),
            _ => None,
        }
    }
}