use byteorder::BigEndian;
use std::collections::HashMap;
use std::default::Default;
use std::io;
use std::io::{Seek, SeekFrom, Write};
use std::iter::IntoIterator;
use std::ops::Index;

use super::column::{ColumnInfo, ColumnType};
use super::storage::WriteNanoDBExt;
use super::storage::header_page::OFFSET_SCHEMA_START;

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

    pub fn write<W: WriteNanoDBExt + Seek>(&self, mut output: &mut W) -> Result<(), io::Error> {
        output.seek(SeekFrom::Start(OFFSET_SCHEMA_START as u64));

        let mut table_mapping: HashMap<Option<String>, usize> = Default::default();
        let mut cur_table: usize = 0;
        let num_tables: u8 = self.cols_hashed_by_table.keys().len() as u8;
        println!("Recording {} table names.", num_tables);
        output.write_u8(num_tables);
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
        println!("Recording {} columns.", num_columns);
        try!(output.write_u8(num_columns));
        for ref column_info in &self.column_infos {
            println!("{:?}", column_info);
            let column_type_byte: u8 = column_info.column_type.into();
            try!(output.write_u8(column_type_byte));

            match column_info.column_type {
                ColumnType::Char { length: length } |
                ColumnType::VarChar { length: length } => {
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
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use super::Schema;
    use super::super::column::{ColumnInfo, ColumnType};

    #[test]
    fn test_index() {
        let info1 = ColumnInfo::with_name(ColumnType::Integer, "foo");
        let info2 = ColumnInfo::with_name(ColumnType::Float, "bar");
        let schema = Schema::with_columns(vec![info1.clone(), info2.clone()]).unwrap();

        assert_eq!(schema[0], info1);
        assert_eq!(schema[1], info2);
    }

    #[test]
    fn test_iter() {
        let info1 = ColumnInfo::with_name(ColumnType::Integer, "foo");
        let info2 = ColumnInfo::with_name(ColumnType::Float, "bar");
        let schema = Schema::with_columns(vec![info1.clone(), info2.clone()]).unwrap();

        assert_eq!(schema.into_iter().collect::<Vec<ColumnInfo>>(),
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
        let mut buffer = vec![0x00; 512];
        let mut expected = vec![0x00; 6];
        expected.extend_from_slice(&[0x01, 0x03, 0x46, 0x4F, 0x4F, 0x03, 0x01, 0x00, 0x01, 0x41,
            0x16, 0x00, 0x14, 0x00, 0x01, 0x42, 0x01, 0x00, 0x01, 0x43]);
        expected.extend_from_slice(&[0x00; 486]);

        let mut cursor = Cursor::new(buffer);
        schema.write(&mut cursor).unwrap();
        assert_eq!(cursor.into_inner(), expected);
    }
}
