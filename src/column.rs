//! This module contains classes and enums for column information.

use std::fmt;

/// A shorthand type for storing a column name in (table_name, column_name) form.
pub type ColumnName = (Option<String>, Option<String>);

/// Convert a column name to string form, with wildcards.
pub fn column_name_to_string(name: &ColumnName) -> String {
    match *name {
        (Some(ref table_name), Some(ref col_name)) => format!("{}.{}", table_name, col_name),
        (None, Some(ref col_name)) => format!("{}", col_name),
        (Some(ref table_name), None) => format!("{}.*", table_name),
        (None, None) => format!("*"),
    }
}

use super::expressions::Literal;

/// An empty Char column type. Useful for comparing type IDs.
pub static EMPTY_CHAR: ColumnType = ColumnType::Char { length: 0 };

/// An empty VarChar column type. Useful for comparing type IDs.
pub static EMPTY_VARCHAR: ColumnType = ColumnType::VarChar { length: 0 };

/// An empty VarChar column type. Useful for comparing type IDs.
pub static EMPTY_NUMERIC: ColumnType = ColumnType::Numeric {
    scale: 0,
    precision: 0,
};

/// The type of a single column in a relation.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ColumnType {
    /// A placeholder type for `NULL` literals.
    Null,
    /// A 1-byte signed integer.
    TinyInt,
    /// A 2-byte signed integer.
    SmallInt,
    /// A 4-byte signed integer.
    Integer,
    /// An 8-byte signed integer.
    BigInt,
    /// A 4-byte signed floating-point number with 24 bits of precision.
    Float,
    /// An 8-byte signed floating-point number with 53 bits of precision.
    Double,
    /// A decimal value with a specified precision and scale.
    Numeric {
        /// The number of digits stored to the right of the decimal point.
        scale: u16,
        /// The total number of digits stored.
        precision: u16,
    },
    /// A fixed-length character-sequence, with a specified length.
    Char {
        /// The length of the string.
        length: u16,
    },
    /// A variable-length character-sequence, with a specified maximum length.
    VarChar {
        /// The length of the string.
        length: u16,
    },
    /// A large character-sequence, with a very large maximum length.
    Text,
    /// A large byte-sequence, with a very large maximum length.
    Blob,
    /// A date value containing year, month, and day.
    Date,
    /// A time value containing hours, minutes, and seconds.
    Time,
    /// A combination date and time value, containing all the fields of
    /// [ColumnType::Date](enum.ColumnType.html#variant.Date) and
    /// [ColumnType::Time](enum.ColumnType.html#variant.Time).
    DateTime,
    /// A date/time value with higher precision than
    /// [ColumnType::DateTime](enum.ColumnType.html#variant.DateTime).
    Timestamp,
    /// A file-pointer value. This is not exposed in SQL, but is used
    /// internally.
    FilePointer,
}

impl From<ColumnType> for u8 {
    fn from(col_type: ColumnType) -> u8 {
        match col_type {
            ColumnType::Null => 0,
            ColumnType::Integer => 1,
            ColumnType::SmallInt => 2,
            ColumnType::BigInt => 3,
            ColumnType::TinyInt => 4,
            ColumnType::Float => 5,
            ColumnType::Double => 6,
            ColumnType::Numeric { scale: _, precision: _ } => 7,
            ColumnType::Char { length: _ } => 21,
            ColumnType::VarChar { length: _ } => 22,
            ColumnType::Text => 23,
            ColumnType::Blob => 24,
            ColumnType::Date => 31,
            ColumnType::Time => 32,
            ColumnType::DateTime => 33,
            ColumnType::Timestamp => 34,
            ColumnType::FilePointer => 41,
        }
    }
}

impl From<u8> for ColumnType {
    fn from(byte: u8) -> ColumnType {
        match byte {
            1 => ColumnType::Integer,
            2 => ColumnType::SmallInt,
            3 => ColumnType::BigInt,
            4 => ColumnType::TinyInt,
            5 => ColumnType::Float,
            6 => ColumnType::Double,
            7 => EMPTY_NUMERIC,
            21 => EMPTY_CHAR,
            22 => EMPTY_VARCHAR,
            23 => ColumnType::Text,
            24 => ColumnType::Blob,
            31 => ColumnType::Date,
            32 => ColumnType::Time,
            33 => ColumnType::DateTime,
            34 => ColumnType::Timestamp,
            41 => ColumnType::FilePointer,
            0 | _ => ColumnType::Null,
        }
    }
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ColumnType::Numeric { scale, precision } => write!(f, "NUMERIC({}, {})", scale, precision),
            ColumnType::Char { length } => write!(f, "CHAR({})", length),
            ColumnType::VarChar { length } => write!(f, "VARCHAR({})", length),
            _ => write!(f, "{}", format!("{:?}", self).to_uppercase()),
        }
    }
}

impl ColumnType {
    /// Determines whether the column type can handle the expression given.
    ///
    /// # Arguments
    /// * expr - The expression to check.
    pub fn can_store_literal(&self, value: Literal) -> bool {
        match value {
            Literal::Long(_) => {
                match *self {
                    ColumnType::BigInt => true,
                    _ => false,
                }
            }
            Literal::Int(_) => {
                match *self {
                    ColumnType::Integer | ColumnType::TinyInt | ColumnType::SmallInt | ColumnType::Date |
                    ColumnType::Time | ColumnType::Timestamp => true,
                    _ => false,
                }
            }
            Literal::Double(_) => {
                match *self {
                    ColumnType::Double => true,
                    _ => false,
                }
            }
            Literal::Float(_) => {
                match *self {
                    ColumnType::Double | ColumnType::Float => true,
                    _ => false,
                }
            }
            Literal::String(s) => {
                match *self {
                    ColumnType::Char { length } |
                    ColumnType::VarChar { length } => s.len() as u16 <= length,
                    ColumnType::Blob | ColumnType::Text => true,
                    _ => false,
                }
            }
            Literal::True | Literal::False => {
                match *self {
                    ColumnType::TinyInt => true,
                    _ => false,
                }
            }
            Literal::Null => true,
        }
    }

    /// Generates a default literal for the given type.
    pub fn default_literal(&self) -> Literal {
        match *self {
            ColumnType::TinyInt | ColumnType::SmallInt | ColumnType::Integer => Literal::Int(0),
            ColumnType::BigInt => Literal::Long(0),
            ColumnType::Float => Literal::Float(0.0),
            ColumnType::Double => Literal::Double(0.0),
            ColumnType::Char { .. } | ColumnType::VarChar { .. } | ColumnType::Text => Literal::String("".into()),
            // TODO
            _ => Literal::Null,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Basic information about a table column, including its name and SQL type.
/// Constraints, even
/// `NOT NULL` constraints, appear at the table level, since some constraints
/// can involve multiple
/// columns.
pub struct ColumnInfo {
    /// The type information for the column.
    pub column_type: ColumnType,
    /// The name of the attribute. If the name is `None`, that means this is a wildcard.
    pub name: Option<String>,
    /// An optional table-name for the attribute, in cases where a join or Cartesian product
    /// generates a result with duplicate attribute-names. In most cases it is expected that this
    /// table-name will be `None`.
    pub table_name: Option<String>,
}

impl ColumnInfo {
    /// Create a new column-info object with a name, but not associated with a table.
    ///
    /// # Arguments
    /// * column_type - The type information for the column.
    /// * name - The column name.
    pub fn with_name<S: Into<String>>(column_type: ColumnType, name: S) -> ColumnInfo {
        ColumnInfo {
            column_type: column_type,
            name: Some(name.into()),
            table_name: None,
        }
    }

    /// Create a new column-info object with a name that is associated with a table.
    ///
    /// # Arguments
    /// * column_type - The type information for the column.
    /// * name - The column name.
    /// * table_name - The table name.
    pub fn with_table_name<S1: Into<String>, S2: Into<String>>(column_type: ColumnType,
                                                               name: S1,
                                                               table_name: S2)
                                                               -> ColumnInfo {
        ColumnInfo {
            column_type: column_type,
            name: Some(name.into()),
            table_name: Some(table_name.into()),
        }
    }

    /// Create a new column-info object that corresponds to a wildcard selection on a table.
    ///
    /// # Arguments
    /// * column_type - The type information for the column.
    /// * table_name - The table name.
    pub fn with_wildcard<S: Into<String>>(column_type: ColumnType, table_name: S) -> ColumnInfo {
        ColumnInfo {
            column_type: column_type,
            name: None,
            table_name: Some(table_name.into()),
        }
    }

    /// Returns the column name for a column-info object.
    pub fn get_column_name(&self) -> ColumnName {
        (self.table_name.clone(), self.name.clone())
    }
}

impl fmt::Display for ColumnInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match (self.table_name.clone(), self.name.clone()) {
            (None, None) => write!(f, "ColumnInfo[*:{}]", self.column_type),
            (None, Some(name)) => write!(f, "ColumnInfo[{}:{}]", name, self.column_type),
            (Some(table_name), None) => write!(f, "ColumnInfo[{}.*:{}]", table_name, self.column_type),
            (Some(table_name), Some(name)) => {
                write!(f,
                       "ColumnInfo[{}.{}:{}]",
                       table_name,
                       name,
                       self.column_type)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ColumnInfo, ColumnType};

    #[test]
    fn test_column_type_display() {
        assert_eq!(format!("{}", ColumnType::Null), "NULL");
        assert_eq!(format!("{}", ColumnType::Char { length: 12 }), "CHAR(12)");
        assert_eq!(format!("{}", ColumnType::VarChar { length: 13 }),
                   "VARCHAR(13)");
        assert_eq!(format!("{}",
                           ColumnType::Numeric {
                               scale: 2,
                               precision: 16,
                           }),
                   "NUMERIC(2, 16)");
    }

    #[test]
    fn test_column_info_display() {
        assert_eq!(format!("{}",
                           ColumnInfo {
                               column_type: ColumnType::Integer,
                               name: None,
                               table_name: None,
                           }),
                   "ColumnInfo[*:INTEGER]");
        assert_eq!(format!("{}",
                           ColumnInfo {
                               column_type: ColumnType::Integer,
                               name: Some("foo".into()),
                               table_name: None,
                           }),
                   "ColumnInfo[foo:INTEGER]");
        assert_eq!(format!("{}",
                           ColumnInfo {
                               column_type: ColumnType::Integer,
                               name: None,
                               table_name: Some("foo".into()),
                           }),
                   "ColumnInfo[foo.*:INTEGER]");
        assert_eq!(format!("{}",
                           ColumnInfo {
                               column_type: ColumnType::Integer,
                               name: Some("bar".into()),
                               table_name: Some("foo".into()),
                           }),
                   "ColumnInfo[foo.bar:INTEGER]");
    }
}
