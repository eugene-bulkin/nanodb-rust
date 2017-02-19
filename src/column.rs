//! This module contains classes and enums for column information.

use std::fmt;

/// A shorthand type for storing a column name in (table_name, column_name) form.
pub type ColumnName = (Option<String>, Option<String>);

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
        scale: u32,
        /// The total number of digits stored.
        precision: u32,
    },
    /// A fixed-length character-sequence, with a specified length.
    Char {
        /// The length of the string.
        length: u32,
    },
    /// A variable-length character-sequence, with a specified maximum length.
    VarChar {
        /// The length of the string.
        length: u32,
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
        (self.name.clone(), self.table_name.clone())
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
