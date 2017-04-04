//! This module contains utilities and classes for handling table schemas.

use std::collections::{HashMap, HashSet};
use std::default::Default;
use std::error::Error as ErrorTrait;
use std::io;
use std::io::{Seek, SeekFrom};
use std::iter::{FromIterator, IntoIterator};
use std::ops::Index;
use std::slice::Iter;

use byteorder::{BigEndian, ReadBytesExt};

use ::expressions::{Expression, Environment, SelectValue};
use ::relations::{ColumnInfo, ColumnName, ColumnType, EMPTY_CHAR, EMPTY_NUMERIC, EMPTY_VARCHAR};
use ::storage::{DBPage, ReadNanoDBExt, TupleLiteral, WriteNanoDBExt};
use ::storage::header_page::OFFSET_SCHEMA_START;

#[derive(Debug, Clone, PartialEq)]
/// An error that occurs when the name of a column results in an invalid schema state.
pub enum NameError {
    /// No columns exist with the requested name.
    NoName(ColumnInfo),
    /// Multiple columns with the same name already existed in the schema.
    MultipleNames(ColumnInfo),
    /// The specified column is a duplicate of an existing one.
    Duplicate(ColumnInfo),
    /// The name of the column is not uniquely identifying.
    Ambiguous(ColumnInfo),
}

impl ::std::fmt::Display for NameError {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            NameError::Ambiguous(ref ci) => write!(f, "The column info {} is ambiguous.", ci),
            NameError::NoName(ref ci) => write!(f, "No columns with a name matching {} exist.", ci),
            NameError::Duplicate(ref ci) => write!(f, "The column info {} is a duplicate of an existing one.", ci),
            NameError::MultipleNames(ref ci) => write!(f, "Multiple columns with the same name as {} exist.", ci),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// An error that can occur while handling schemas.
pub enum Error {
    /// An error occurred while performing I/O.
    IOError(String),
    /// An error occurred that had to do with parsing a schema.
    ParseError,
    /// An error occurred that had to do with the names of columns passed in.
    Name(NameError),
    /// Tables must have at least one column.
    NoColumns,
    /// The column name at the given index was empty.
    EmptyColumnName(usize),
    /// Setting all of the tables on the schema to a certain name would result in ambiguous column
    /// names.
    AmbiguousColumnsAfterTableRename(String, Vec<String>),
    /// A select value could not be resolved.
    CouldNotResolveSelectValue(SelectValue),
}

impl ::std::fmt::Display for Error {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            Error::IOError(ref e) => {
                write!(f, "An IO error occurred: {}", e)
            }
            Error::ParseError => {
                // TODO: What was the parsing error?
                write!(f, "A parsing error occurred.")
            }
            Error::Name(ref e) => write!(f, "{}", e),
            Error::NoColumns => write!(f, "All schemas must have at least one column."),
            Error::EmptyColumnName(idx) => write!(f, "The column name at index {} does not have a name.", idx),
            Error::AmbiguousColumnsAfterTableRename(ref table_name, ref ambiguous_columns) => {
                write!(f, "Overriding table-name to \"{}\" would produce ambiguous columns: {}",
                       table_name, ambiguous_columns.join(", "))
            },
            Error::CouldNotResolveSelectValue(ref value) => {
                write!(f, "The select value {} could not be resolved.", value)
            }
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::IOError(e.description().into())
    }
}

#[derive(Debug, Clone, PartialEq)]
/// A schema is an ordered collection of column names and associated types.
///
/// Many different entities in the database code can have schema associated with them. Both tables
/// and tuples have schemas, for obvious reasons. `SELECT` and `FROM` clauses also have schemas,
/// used by the database engine to verify the semantics of database queries. Finally, relational
/// algebra plan nodes also have schemas, which specify the kinds of tuples that they generate.
pub struct Schema {
    column_infos: Vec<ColumnInfo>,
    cols_hashed_by_table: HashMap<Option<String>, HashMap<Option<String>, usize>>,
    cols_hashed_by_column: HashMap<Option<String>, Vec<usize>>,
}

impl Index<usize> for Schema {
    type Output = ColumnInfo;

    fn index(&self, i: usize) -> &Self::Output {
        &self.column_infos[i]
    }
}

impl IntoIterator for Schema {
    type Item = ColumnInfo;
    type IntoIter = ::std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.column_infos.into_iter()
    }
}

impl Schema {
    /// Instantiates a new schema with no columns in it.
    pub fn new() -> Schema {
        Schema {
            column_infos: vec![],
            cols_hashed_by_table: Default::default(),
            cols_hashed_by_column: Default::default(),
        }
    }

    /// Checks if the schema is empty.
    pub fn is_empty(&self) -> bool {
        self.column_infos.is_empty()
    }

    /// Creates a new schema by reading a header page.
    pub fn from_header_page(page: &mut DBPage) -> Result<Schema, Error> {
        let mut result = Schema::new();

        try!(page.seek(SeekFrom::Start(OFFSET_SCHEMA_START as u64)));

        let num_tables = try!(page.read_u8());
        let mut table_names: Vec<String> = Vec::new();

        for _ in 0..num_tables {
            let table_name = try!(page.read_varchar255());
            table_names.push(table_name);
        }

        let num_cols = try!(page.read_u8());
        debug! ("Table has {} columns.", num_cols);

        if num_cols < 1 {
            return Err(Error::NoColumns);
        }

        for i in 0..num_cols {
            // Determine the column type here.
            let type_id = try!(page.read_u8());
            let col_type = if type_id == u8::from(EMPTY_CHAR) {
                let length = try!(page.read_u16::<BigEndian>());
                ColumnType::Char { length: length }
            } else if type_id == u8::from(EMPTY_VARCHAR) {
                let length: u16 = try!(page.read_u16::<BigEndian>());
                ColumnType::VarChar { length: length }
            } else if type_id == u8::from(EMPTY_NUMERIC) {
                // TODO: When NUMERICs are written properly, read them properly here too.
                EMPTY_NUMERIC.clone()
            } else {
                type_id.into()
            };

            let table_index = try!(page.read_u8());
            let ref table_name = table_names[table_index as usize];

            let col_name = try!(page.read_varchar255());

            if col_name.len() == 0 {
                return Err(Error::EmptyColumnName(i as usize));
            }

            try!(result.add_column(ColumnInfo::with_table_name(col_type, col_name.as_ref(), table_name.as_ref())));
        }

        Ok(result)
    }

    /// Creates a new schema from select values by attempting to evaluate them.
    pub fn from_select_values(values: Vec<SelectValue>, mut env: &mut Option<&mut Environment>) -> Result<Schema, Error> {
        let mut result = Schema::new();
        for value in values.iter() {
            match ColumnInfo::from_select_value(value, env) {
                Some(info) => {
                    try!(result.add_column(info));
                },
                None => {
                    return Err(Error::CouldNotResolveSelectValue(value.clone()));
                }
            }
        }
        Ok(result)
    }

    /// Returns an iterator on the column infos.
    pub fn iter(&self) -> Iter<ColumnInfo> {
        self.column_infos.iter()
    }

    /// Returns the number of columns currently in the schema.
    pub fn num_columns(&self) -> usize {
        self.column_infos.len()
    }

    /// Checks if the schema has a column with the provided name.
    ///
    /// # Arguments
    /// * name - The desired column name.
    pub fn has_column<S: Into<String>>(&self, name: S) -> bool {
        self.get_column(name).is_some()
    }


    /// This helper method returns true if this schema contains any columns with the same column
    /// name but different table names. If so, the schema is not valid for use on one side of a
    /// `NATURAL` join.
    pub fn has_multiple_columns_with_same_name(&self) -> bool {
        for name in self.cols_hashed_by_column.keys() {
            if let Some(names) = self.cols_hashed_by_column.get(name) {
                if names.len() > 1 {
                    return true;
                }
            }
        }
        false
    }

    /// Returns the number of columns that have the specified column name. Note that multiple
    /// columns can have the same column name but different table names.
    pub fn num_columns_with_name<S: Into<String>>(&self, name: S) -> usize {
        if let Some(names) = self.cols_hashed_by_column.get(&Some(name.into())) {
            return names.len();
        }
        0
    }

    /// Returns the names of columns that are common between this schema and the specified schema.
    /// This kind of operation is mainly used for resolving `NATURAL` joins.
    pub fn get_common_column_names(&self, other: &Schema) -> HashSet<String> {
        let left_names: HashSet<&Option<String>> = HashSet::from_iter(self.cols_hashed_by_column.keys());
        let right_names = HashSet::from_iter(other.cols_hashed_by_column.keys());

        let mut result = HashSet::new();
        for common in left_names.intersection(&right_names) {
            if let Some(ref name) = **common {
                result.insert(name.clone());
            }
        }
        result
    }

    /// If the schema has a column with the provided name, return that column.
    ///
    /// # Arguments
    /// * name - The desired column name.
    pub fn get_column<S: Into<String>>(&self, name: S) -> Option<&ColumnInfo> {
        let name = name.into();
        for col_info in &self.column_infos {
            match col_info.name {
                Some(ref col_name) => {
                    if col_name == &name {
                        return Some(&col_info);
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// Instantiates a schema with the given columns.
    ///
    /// # Arguments
    /// * column_infos - Some collection of column infos.
    ///
    /// # Errors
    /// This constructor will fail if adding a column would fail at any point.
    pub fn with_columns<I: IntoIterator<Item=ColumnInfo>>(column_infos: I) -> Result<Schema, Error> {
        let mut result = Schema::new();
        result.add_columns(column_infos).map(|_| result)
    }

    /// Add one column to the schema.
    ///
    /// # Arguments
    /// * column - The information about the desired column to add.
    ///
    /// # Errors
    /// This will fail if the column cannot be added, either because one with that name already
    /// exists, or it may result in an ambiguity.
    pub fn add_column(&mut self, column: ColumnInfo) -> Result<(), Error> {
        if column.name.is_some() {
            // If the column is named, make sure it's not already in the schema with that
            // table name
            let table_col_names = self.cols_hashed_by_table.get(&column.table_name);
            if let Some(table_map) = table_col_names {
                if table_map.get(&column.name).is_some() {
                    return Err(Error::Name(NameError::Duplicate(column)));
                }
            }
        }

        let index = self.column_infos.len();

        let table_map = self.cols_hashed_by_table
            .entry(column.table_name.clone())
            .or_insert(Default::default());
        table_map.insert(column.name.clone(), index);

        let column_list = self.cols_hashed_by_column
            .entry(column.name.clone())
            .or_insert(Default::default());
        column_list.push(index);

        self.column_infos.push(column);
        Ok(())
    }

    /// Add multiple columns to the schema.
    ///
    /// # Arguments
    /// * schema - Some collection of column info.
    ///
    /// # Errors
    /// This method will fail if adding a column would fail at any point.
    pub fn add_columns<T: IntoIterator<Item=ColumnInfo>>(&mut self, schema: T) -> Result<(), Error> {
        let result: Result<Vec<()>, Error> = schema.into_iter().map(|column| self.add_column(column)).collect();
        result.map(|_| ())
    }

    ///
    /// Given a (possibly wildcard) column-name, this method returns the collection of all columns
    /// that match the specified column name. The collection is a mapping from integer indexes (the
    /// keys) to `ColumnInfo` objects from the schema.

    /// Any valid column-name object will work, so all of these options are available:
    ///
    ///   * **No table, only a column name** - to resolve an unqualified
    ///     column name, e.g. in an expression or predicate
    ///   * **A table and column name** - to check whether the schema contains
    ///     such a column
    ///   * **A wildcard without a table name** - to retrieve all columns in
    ///     the schema
    ///   * **A wildcard with a table name** - to retrieve all columns
    ///     associated with a particular table name
    pub fn find_columns(&self, col_name: &ColumnName) -> Vec<(usize, ColumnInfo)> {
        let mut found: Vec<(usize, ColumnInfo)> = Vec::new();

        match *col_name {
            (Some(ref table_name), Some(ref column_name)) => {
                // Column name with a table name:  tbl.col
                // Find the table info and see if it has the specified column.
                let table_key = Some(table_name.clone());
                let column_key = Some(column_name.clone());
                if let Some(table_cols) = self.cols_hashed_by_table.get(&table_key) {
                    if let Some(index) = table_cols.get(&column_key) {
                        found.push((*index, self.column_infos[*index].clone()));
                    }
                }
            }
            (Some(ref table_name), None) => {
                // Wildcard with a table name:  tbl.*
                // Find the table info and add its columns to the result.
                let key = Some(table_name.clone());
                if let Some(table_cols) = self.cols_hashed_by_table.get(&key) {
                    found.extend(table_cols.values().map(|idx| (*idx, self.column_infos[*idx].clone())));
                }
            }
            (None, Some(ref column_name)) => {
                // Column name with no table name:  col
                // Look up the list of column-info objects grouped by column name.
                let key = Some(column_name.clone());
                if let Some(columns) = self.cols_hashed_by_column.get(&key) {
                    for index in columns {
                        found.push((*index, self.column_infos[*index].clone()));
                    }
                }
            }
            (None, None) => {
                // Wildcard with no table name:  *
                // Add all columns in the schema to the result.

                for (idx, val) in self.column_infos.iter().enumerate() {
                    found.push((idx, val.clone()));
                }
            }
        }

        found
    }

    /// Write the schema to some output.
    ///
    /// # Arguments
    /// * output - The output to write the schema to.
    ///
    /// # Errors
    /// This function can fail if anything goes wrong trying to write to the given output.
    pub fn write<W: WriteNanoDBExt + Seek>(&self, mut output: &mut W) -> Result<(), io::Error> {
        info! ("Writing table schema: {}", self);

        try!(output.seek(SeekFrom::Start(OFFSET_SCHEMA_START as u64)));

        let mut table_mapping: HashMap<Option<String>, usize> = Default::default();
        let mut cur_table: usize = 0;
        let num_tables: u8 = self.cols_hashed_by_table.keys().len() as u8;
        debug! ("Recording {} table names.", num_tables);
        try!(output.write_u8(num_tables));
        for table_name in self.cols_hashed_by_table.keys() {
            // Ignore None table names (which shouldn't happen here).
            match *table_name {
                Some(ref table_name) => {
                    try!(output.write_varchar255(table_name.clone()));
                    table_mapping.insert(Some(table_name.clone()), cur_table);
                }
                None => {}
            }
            cur_table += 1;
        }
        let num_columns: u8 = self.column_infos.len() as u8;
        debug! ("Recording {} columns.", num_columns);
        try!(output.write_u8(num_columns));
        for ref column_info in &self.column_infos {
            let column_type_byte: u8 = column_info.column_type.into();
            try!(output.write_u8(column_type_byte));

            match column_info.column_type {
                ColumnType::Char { length } |
                ColumnType::VarChar { length } => {
                    try!(output.write_u16::<BigEndian>(length as u16));
                }
                // TODO: Handle NUMERIC here.
                _ => {}
            }

            if column_info.table_name.is_some() {
                if let Some(ref table_index) = table_mapping.get(&column_info.table_name) {
                    try!(output.write_u8(**table_index as u8));
                }
            }

            if let Some(ref column_name) = column_info.name {
                try!(output.write_varchar255(column_name.clone()));
            }
        }
        Ok(())
    }

    /// Creates a tuple literal with default values (for getting a schema with an environment).
    pub fn default_tuple(&self) -> TupleLiteral {
        let mut result = TupleLiteral::new();
        for info in self.column_infos.iter() {
            let value = info.column_type.default_literal();
            result.add_value(value);
        }
        result
    }

    /// This method iterates through all columns in this schema and sets them all to be on the
    /// specified table. This method will return an error if the result would be an invalid schema
    /// with duplicate column names.
    pub fn set_table_name<S: Into<String>>(&mut self, name: S) -> Result<(), Error> {
        let name = name.into();
        // First, verify that overriding the table names will not produce multiple ambiguous column
        // names.
        let mut duplicates: Vec<String> = Vec::new();

        for (name, indices) in self.cols_hashed_by_column.iter() {
            match *name {
                Some(ref name) => {
                    if indices.len() > 1 {
                        duplicates.push(name.clone());
                    }
                },
                None => continue,
            }
        }

        if !duplicates.is_empty() {
            return Err(Error::AmbiguousColumnsAfterTableRename(name.clone(), duplicates));
        }

        // If we get here, we know that we can safely override the table name for
        // all columns.

        let old_infos = self.column_infos.clone();

        self.column_infos.clear();
        self.cols_hashed_by_column.clear();
        self.cols_hashed_by_table.clear();

        // Iterate over the columns in the same order as they were in originally. For each one,
        // override the table name, then use add_column() to properly update the internal hash
        // structure.

        for info in old_infos.iter() {
            let mut new_info: ColumnInfo = info.clone();
            new_info.table_name = Some(name.clone());

            try!(self.add_column(new_info));
        }

        Ok(())
    }
}

impl ::std::fmt::Display for Schema {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        let infos: Vec<String> = self.column_infos.iter().map(|f| format!("{}", f)).collect();
        write!(f, "Schema[cols={}]", infos.join(", "))
    }
}

impl ::std::ops::Add for Schema {
    type Output = Self;
    fn add(self, other: Schema) -> Schema {
        let mut result = Schema::with_columns(self.column_infos).unwrap();
        // Yes, this may panic, but you shouldn't really be using this without checking first.
        result.add_columns(other.column_infos).unwrap();
        result
    }
}

impl ::std::ops::AddAssign for Schema {
    #[inline]
    fn add_assign(&mut self, other: Schema) {
        // Yes, this may panic, but you shouldn't really be using this without checking first.
        self.add_columns(other.column_infos).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use ::relations::{ColumnType, ColumnInfo};

    #[test]
    fn test_index() {
        let info1 = ColumnInfo::with_name(ColumnType::Integer, "foo");
        let info2 = ColumnInfo::with_name(ColumnType::Float, "bar");
        let schema = Schema::with_columns(vec![info1.clone(), info2.clone()]).unwrap();

        assert_eq! (schema[0], info1);
        assert_eq! (schema[1], info2);
    }

    #[test]
    fn test_iter() {
        let info1 = ColumnInfo::with_name(ColumnType::Integer, "foo");
        let info2 = ColumnInfo::with_name(ColumnType::Float, "bar");
        let schema = Schema::with_columns(vec![info1.clone(), info2.clone()]).unwrap();

        assert_eq! (schema.into_iter().collect::<Vec<ColumnInfo>>(),
        vec![info1.clone(), info2.clone()]);
    }

    #[test]
    fn test_write() {
        let schema = Schema::with_columns(vec![
            ColumnInfo::with_table_name(ColumnType::Integer, "A", "FOO"),
            ColumnInfo::with_table_name(ColumnType::VarChar { length: 20 }, "B", "FOO"),
            ColumnInfo::with_table_name(ColumnType::Integer, "C", "FOO"),
        ])
            .unwrap();
        let buffer = vec![0x00; 512];
        let mut expected = vec![0x00; 6];
        expected.extend_from_slice(&[0x01, 0x03, 0x46, 0x4F, 0x4F, 0x03, 0x01, 0x00, 0x01, 0x41, 0x16, 0x00, 0x14, 0x00,
            0x01, 0x42, 0x01, 0x00, 0x01, 0x43]);
        expected.extend_from_slice(&[0x00; 486]);

        let mut cursor = Cursor::new(buffer);
        schema.write(&mut cursor).unwrap();
        assert_eq! (cursor.into_inner(), expected);
    }

    #[test]
    fn test_find_columns() {
        let foo_a = ColumnInfo::with_table_name(ColumnType::Integer, "A", "FOO");
        let foo_b = ColumnInfo::with_table_name(ColumnType::VarChar { length: 20 }, "B", "FOO");
        let foo_c = ColumnInfo::with_table_name(ColumnType::Integer, "C", "FOO");
        let bar_a = ColumnInfo::with_table_name(ColumnType::Integer, "A", "BAR");
        let bar_b = ColumnInfo::with_table_name(ColumnType::VarChar { length: 20 }, "B", "BAR");
        let bar_c = ColumnInfo::with_table_name(ColumnType::Integer, "C", "BAR");
        let b = ColumnInfo::with_name(ColumnType::BigInt, "B");
        let c = ColumnInfo::with_name(ColumnType::Integer, "C");

        let schema = Schema::with_columns(vec![
            foo_a.clone(),
            foo_b.clone(),
            foo_c.clone(),
            bar_a.clone(),
            bar_b.clone(),
            bar_c.clone(),
            b.clone(),
            c.clone(),
        ])
            .unwrap();

        assert_eq! (vec![
            (0, foo_a.clone()),
            (1, foo_b.clone()),
            (2, foo_c.clone()),
        ], {
            let mut result = schema.find_columns(&(Some("FOO".into()), None));
            result.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            result
        });

        assert_eq! (vec![
            (2, foo_c.clone()),
        ], {
            let mut result = schema.find_columns(&(Some("FOO".into()), Some("C".into())));
            result.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            result
        });

        assert_eq! (vec![
            (2, foo_c.clone()),
            (5, bar_c.clone()),
            (7, c.clone()),
        ], {
            let mut result = schema.find_columns(&(None, Some("C".into())));
            result.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            result
        });

        assert_eq! (vec![
            (0, foo_a.clone()),
            (1, foo_b.clone()),
            (2, foo_c.clone()),
            (3, bar_a.clone()),
            (4, bar_b.clone()),
            (5, bar_c.clone()),
            (6, b.clone()),
            (7, c.clone()),
        ], {
            let mut result = schema.find_columns(&(None, None));
            result.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            result
        });
    }

    #[test]
    fn test_set_table_name() {
        let a = ColumnInfo::with_name(ColumnType::Integer, "A");
        let b = ColumnInfo::with_name(ColumnType::Float, "B");
        let a_foo = ColumnInfo::with_table_name(ColumnType::Integer, "A", "FOO");
        let b_foo = ColumnInfo::with_table_name(ColumnType::Float, "B", "FOO");
        let a_bar = ColumnInfo::with_table_name(ColumnType::Integer, "A", "BAR");
        let b_bar = ColumnInfo::with_table_name(ColumnType::Float, "B", "BAR");
        let a_abc = ColumnInfo::with_table_name(ColumnType::Integer, "A", "ABC");
        let b_abc = ColumnInfo::with_table_name(ColumnType::Float, "B", "ABC");

        let mut schema1 = Schema::with_columns(vec![a.clone(), b.clone()]).unwrap();
        let mut schema2 = Schema::with_columns(vec![a_foo.clone(), b_foo.clone()]).unwrap();
        let mut schema3 = Schema::with_columns(vec![a_foo.clone(), b_bar.clone()]).unwrap();
        let mut schema4 = Schema::with_columns(vec![a_foo.clone(), a_bar.clone()]).unwrap();

        assert_eq!(Ok(()), schema1.set_table_name("ABC"));
        assert_eq!(vec![a_abc.clone(), b_abc.clone()], schema1.column_infos);

        assert_eq!(Ok(()), schema2.set_table_name("ABC"));
        assert_eq!(vec![a_abc.clone(), b_abc.clone()], schema2.column_infos);

        assert_eq!(Ok(()), schema3.set_table_name("ABC"));
        assert_eq!(vec![a_abc.clone(), b_abc.clone()], schema3.column_infos);

        assert_eq!(Err(Error::AmbiguousColumnsAfterTableRename("ABC".into(),
                                                               vec!["A".into()])),
        schema4.set_table_name("ABC"));
        assert_eq!(vec![a_foo.clone(), a_bar.clone()], schema4.column_infos);
    }
}
