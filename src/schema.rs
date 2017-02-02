
use std::collections::HashMap;
use std::default::Default;
use std::io::Write;
use std::iter::IntoIterator;
use std::ops::Index;
use super::column::ColumnInfo;

#[derive(Debug, Clone, PartialEq)]
pub enum NameError {
    NoName(ColumnInfo),
    MultipleNames(ColumnInfo),
    Duplicate(ColumnInfo),
    Ambiguous(ColumnInfo),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    Name(NameError),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    column_infos: Vec<ColumnInfo>,
    cols_hashed_by_table: HashMap<Option<String>, HashMap<Option<String>, usize>>,
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
    pub fn new() -> Schema {
        Schema {
            column_infos: vec![],
            cols_hashed_by_table: Default::default(),
        }
    }
    pub fn with_columns(column_infos: Vec<ColumnInfo>) -> Result<Schema, Error> {
        let mut result = Schema::new();
        if !column_infos.is_empty() {
            result.add_columns(column_infos).map(|_| result)
        } else {
            Ok(result)
        }
    }

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

        self.column_infos.push(column);
        Ok(())
    }

    pub fn add_columns<T: IntoIterator<Item = ColumnInfo>>(&mut self, schema: T) -> Result<(), Error> {
        let result: Result<Vec<()>, Error> = schema.into_iter().map(|column| self.add_column(column)).collect();
        result.map(|_| ())
    }

    pub fn write<W: Write>(&self, mut output: &W) -> Result<(), ()> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::Schema;
    use super::super::column::{ColumnInfo, ColumnType};

    #[test]
    fn test_index() {
        let info1 = ColumnInfo {
            column_type: ColumnType::Integer,
            name: Some("foo".into()),
            table_name: None,
        };
        let info2 = ColumnInfo {
            column_type: ColumnType::Float,
            name: Some("bar".into()),
            table_name: None,
        };
        let schema = Schema::with_columns(vec![info1.clone(), info2.clone()]).unwrap();

        assert_eq!(schema[0], info1);
        assert_eq!(schema[1], info2);
    }

    #[test]
    fn test_iter() {
        let info1 = ColumnInfo {
            column_type: ColumnType::Integer,
            name: Some("foo".into()),
            table_name: None,
        };
        let info2 = ColumnInfo {
            column_type: ColumnType::Float,
            name: Some("bar".into()),
            table_name: None,
        };
        let schema = Schema::with_columns(vec![info1.clone(), info2.clone()]).unwrap();

        assert_eq!(schema.into_iter().collect::<Vec<ColumnInfo>>(),
                   vec![info1.clone(), info2.clone()]);
    }
}
