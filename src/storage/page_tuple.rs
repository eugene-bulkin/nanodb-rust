//! A module which stores utilities for a basic page tuple.


use byteorder::{BigEndian, ReadBytesExt};

use std::io::{Seek, SeekFrom};
use super::{DBPage, PinError, Pinnable, Tuple, TupleError};
use super::super::{ColumnType, Schema};
use super::super::expressions::Literal;

/// This value is used in [`valueOffsets`](#) when a column value is set to `NULL`.
pub static NULL_OFFSET: u16 = 0;

/// This helper function takes a tuple (from an arbitrary source) and computes how much space it
/// would require to be stored in a heap table file with the specified schema. This is used to
/// insert new tuples into a table file by computing how much space will be needed, so that an
/// appropriate page can be found.
pub fn get_tuple_storage_size<T: Tuple>(schema: Schema, tuple: &T) -> Result<u16, TupleError> {
    let mut storage_size = get_null_flags_size(schema.num_columns()) as u16;
    let mut col_idx = 0;
    for col_info in schema {
        let value = tuple.get_column_value(col_idx);
        if value != Literal::Null {
            let data_length = match col_info.column_type {
                ColumnType::VarChar { length: _ } => value.as_string().unwrap().len(),
                _ => 0,
            };
            storage_size += try!(get_storage_size(col_info.column_type, data_length as u16));
        }

        col_idx += 1;
    }

    Ok(storage_size)
}

/// This method computes and returns the number of bytes that are used to store the null-flags in
/// each tuple.
///
/// # Arguments
/// * num_cols - the total number of columns in the table.
pub fn get_null_flags_size(num_cols: usize) -> u16 {
    if num_cols > 0 {
        1 + (num_cols as u16 - 1) / 8
    } else {
        0
    }
}

/// Returns the storage size of a particular column's (non-`NULL`) value, in bytes. The length of
/// the value is required in cases where the column value can be variable size, such as if the type
/// is a `VARCHAR`. Note that the data-length is actually *not* required when the type is `CHAR`,
/// since `CHAR` fields always have a specific size.
///
/// # Arguments
/// * col_type - The column's data type.
/// * data_length - for column-types that specify a length, this is the length value.
fn get_storage_size(col_type: ColumnType, data_length: u16) -> Result<u16, TupleError> {
    match col_type {
        ColumnType::Integer | ColumnType::Float => Ok(4),
        ColumnType::SmallInt => Ok(2),
        ColumnType::BigInt | ColumnType::Double => Ok(8),
        ColumnType::TinyInt => Ok(1),
        // CHAR values are of a fixed size, but the size is specified in
        // the length field and there is no other storage required.
        ColumnType::Char { length } => Ok(length),
        // VARCHAR values are of a variable size, but there is always a
        // two byte length specified at the start of the value.
        ColumnType::VarChar { length: _ } => Ok(2 + data_length),
        // File-pointers are comprised of a two-byte page number and a
        // two-byte offset in the page.
        ColumnType::FilePointer => Ok(4),
        // Unsupported types have no size.
        _ => Err(TupleError::UnsupportedColumnType),
    }
}

/// This class is a partial implementation of the {@link Tuple} interface that handles reading and
/// writing tuple data against a {@link DBPage} object. This can be used to read and write tuples in
/// a table file, keys in an index file, etc. It could also be used to store and manage tuples in
/// memory, although it's generally much faster and simpler to use an optimized in-memory
/// representation for tuples in memory.
///
/// Each tuple is stored in a layout like this:
///
/// * The first one or more bytes are dedicated to a `NULL`-bitmap, which records columns that are
///   currently `NULL`.
/// * The remaining bytes are dedicated to storing the non-`NULL` values for the columns in the
///   tuple.
///
/// In order to make this class' functionality generic, certain operations must be implemented by
/// subclasses: specifically, any operation that changes a tuple's size (e.g. writing a non-`NULL`
/// value to a previously `NULL` column or vice versa, or changing the size of a variable-size
/// column). The issue with these operations is that they require page-level data management, which
/// is beyond the scope of what this class can provide. Thus, concrete subclasses of this class can
/// provide page-level data management as needed.
pub struct PageTuple {
    db_page: DBPage,
    page_offset: u16,
    schema: Schema,
    value_offsets: Vec<u16>,
    pin_count: u32,
}

impl PageTuple {
    /// Construct a new tuple object that is backed by the data in the database page. This tuple is
    /// able to be read from or written to.
    ///
    /// # Arguments
    /// * db_page - the specific database page that holds the tuple
    /// * page_offset - the offset of the tuple's actual data in the page
    /// * schema - the details of the columns that appear within the tuple
    pub fn new(db_page: DBPage, page_offset: u16, schema: Schema) -> Result<PageTuple, TupleError> {
        let value_offsets = vec![0; schema.num_columns()];
        let mut result = PageTuple {
            db_page: db_page,
            page_offset: page_offset,
            schema: schema,
            value_offsets: value_offsets,
            pin_count: 0,
        };
        try!(result.compute_value_offsets());
        Ok(result)
    }

    fn check_column_index(&self, col_index: usize) -> Result<(), TupleError> {
        if col_index < self.schema.num_columns() {
            Ok(())
        } else {
            Err(TupleError::InvalidColumnIndex)
        }
    }

    fn get_column_value_size(&mut self, col_type: ColumnType, offset: u16) -> Result<u16, TupleError> {
        let data_length = match col_type {
            ColumnType::VarChar { length: _ } => {
                // The storage size depends on the size of the data value being stored. In this
                // case, read out the data length.
                try!(self.db_page.seek(SeekFrom::Start(offset as u64)));
                try!(self.db_page.read_u16::<BigEndian>())
            }
            _ => 0,
        };
        get_storage_size(col_type, data_length)
    }

    /// Returns the offset where the tuple's data actually starts. This is past the bytes used to
    /// store NULL-flags.
    fn get_data_start_offset(&self) -> u16 {
        let null_flag_bytes = get_null_flags_size(self.schema.num_columns()) as u16;
        self.page_offset + null_flag_bytes
    }

    /// This is a helper function to find out the current value of a column's `NULL` flag. It is not
    /// intended to be used to determine if a column's value is `NULL` since the method does a lot
    /// of work; instead, use the [`is_column_null_cached`](#method.is_column_null_cached) method
    /// which relies on cached column information.
    ///
    /// # Arguments
    /// * col_index - The index of the column to retrieve the null-flag for
    fn check_if_column_null(&mut self, col_index: usize) -> Result<bool, TupleError> {
        try!(self.check_column_index(col_index));

        let col_index = col_index as u16;

        // Skip to the byte that contains the NULL-flag for this specific column.
        let null_flag_offset = self.page_offset + (col_index / 8);

        try!(self.db_page.seek(SeekFrom::Start(null_flag_offset as u64)));
        let mut null_flag = try!(self.db_page.read_u8());
        null_flag = null_flag >> (col_index % 8);

        Ok((null_flag & 0x01) != 0)
    }

    /// This helper function computes and caches the offset of each column value in the tuple.
    /// If a column has a `NULL` value then [NULL_OFFSET](#const.NULL_OFFSET) is used for the
    /// offset.
    fn compute_value_offsets(&mut self) -> Result<(), TupleError> {
        let mut value_offset = self.get_data_start_offset();

        for i in 0..self.schema.num_columns() {
            if try!(self.check_if_column_null(i)) {
                self.value_offsets[i] = NULL_OFFSET;
            } else {
                self.value_offsets[0] = value_offset;

                let col_type = self.schema[i].column_type;
                value_offset += try!(self.get_column_value_size(col_type, value_offset)) as u16;
            }
        }

        Ok(())
    }
}

impl Pinnable for PageTuple {
    fn pin(&mut self) {
        self.db_page.pin();
        self.pin_count += 1;
    }

    fn unpin(&mut self) -> Result<(), PinError> {
        if self.pin_count <= 0 {
            Err(PinError::PinCountNotPositive(self.pin_count))
        } else {
            self.pin_count -= 1;
            self.db_page.unpin()
        }
    }

    fn get_pin_count(&self) -> u32 {
        self.pin_count
    }
}

impl Tuple for PageTuple {
    fn is_disk_backed(&self) -> bool {
        // TODO: Should check if the DBFile has a path?
        true
    }

    fn is_null_value(&self, col_index: usize) -> Result<bool, TupleError> {
        try!(self.check_column_index(col_index));

        Ok(self.value_offsets[col_index] == NULL_OFFSET)
    }

    fn get_column_count(&self) -> usize {
        self.schema.num_columns()
    }

    fn get_column_value(&self, col_index: usize) -> Literal {
        unimplemented!()
    }
}
