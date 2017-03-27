//! This module contains utilities and classes for SQL literals.

use ::ColumnType;

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
    /// A file pointer. This can never be provided by a user, but it may show up in certain cases,
    /// such as a B-tree tuple file.
    FilePointer {
        /// The page number in the table file.
        page_no: u16,
        /// The offset of the data within the page.
        offset: u16
    },
}

impl ::std::hash::Hash for Literal {
    fn hash<H: ::std::hash::Hasher>(&self, state: &mut H) {
        match *self {
            Literal::Int(i) => {
                i.hash(state);
                state.write_u8(1u8);
            },
            Literal::Long(l) => {
                l.hash(state);
                state.write_u8(1u8);
            },
            Literal::String(ref s) => {
                s.hash(state);
                state.write_u8(1u8);
            },
            Literal::Null => {
                0.hash(state);
                state.write_u8(0u8);
            },
            Literal::True => {
                1.hash(state);
                state.write_u8(0u8);
            },
            Literal::False => {
                2.hash(state);
                state.write_u8(0u8);
            },
            Literal::Double(d) => {
                (d as u64).hash(state);
                state.write_u8(1u8);
            },
            Literal::Float(f) => {
                (f as u32).hash(state);
                state.write_u8(1u8);
            },
            Literal::FilePointer { page_no, offset } => {
                page_no.hash(state);
                offset.hash(state);
                state.write_u8(1u8);
            }
        }
    }
}

impl ::std::fmt::Display for Literal {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            Literal::True => write!(f, "TRUE"),
            Literal::False => write!(f, "FALSE"),
            Literal::Null => write!(f, "NULL"),
            Literal::Int(num) => write!(f, "{}", num),
            Literal::Long(num) => write!(f, "{}", num),
            Literal::Float(num) => write!(f, "{}", num),
            Literal::Double(num) => write!(f, "{}", num),
            Literal::String(ref s) => write!(f, "\'{}\'", s),
            Literal::FilePointer { page_no, offset } => write!(f, "FP({}, {})", page_no, offset),
        }
    }
}

impl From<bool> for Literal {
    fn from(value: bool) -> Literal {
        if value { Literal::True } else { Literal::False }
    }
}

impl Literal {
    /// A utility function for determining if the literal is numeric.
    pub fn is_numeric(&self) -> bool {
        match *self {
            Literal::Double(_) |
            Literal::Int(_) |
            Literal::Float(_) |
            Literal::Long(_) => true,
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
    /// A utility function for converting to a String if possible. If not, returns None.
    pub fn as_string(&self) -> Option<String> {
        match *self {
            Literal::String(ref s) => Some(s.clone()),
            _ => None,
        }
    }

    /// A utility function for getting a column type based on the literal.
    pub fn get_column_type(&self) -> ColumnType {
        match *self {
            Literal::Int(_) => ColumnType::Integer,
            Literal::Long(_) => ColumnType::BigInt,
            Literal::Float(_) => ColumnType::Float,
            Literal::Double(_) => ColumnType::Double,
            Literal::String(ref s) => ColumnType::VarChar { length: s.len() as u16 },
            Literal::Null => ColumnType::Null,
            Literal::True | Literal::False => ColumnType::TinyInt,
            Literal::FilePointer { .. } => ColumnType::FilePointer,
        }
    }
}
