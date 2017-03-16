//! This module contains utilities to handle pages within database files for NanoDB.

use std::error::Error as ErrorTrait;
use std::io::{self, ErrorKind, SeekFrom};
use std::io::prelude::*;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use ::{ColumnType, Schema};
use ::expressions::Literal;
use ::storage::{DBFileInfo, PinError, Pinnable, Tuple, TupleError, WriteNanoDBExt};
use ::storage::page_tuple::get_null_flags_size;

/// The offset in the data page where the number of slots in the slot table is stored.
const OFFSET_NUM_SLOTS: u16 = 0;

/// This offset-value is stored into a slot when it is empty. It is set to zero because this is
/// where the page's slot-count is stored and therefore this is obviously an invalid offset for a
/// tuple to be located at.
pub const EMPTY_SLOT: u16 = 0;

#[derive(Debug, Clone, PartialEq)]
/// An error that can occur during the operations on a `DBPage`.
pub enum Error {
    /// Some I/O error occurred.
    IOError(String),
    /// For when a tuple error occurs.
    TupleError(Box<TupleError>),
    /// The slot asked for is at an invalid position. In the form of (num slots, slot desired).
    InvalidSlot(u16, u16),
    /// The page does not have enough space for the tuple. In the form of (needed, free space).
    NotEnoughFreeSpace(u16, u16),
    /// The provided offset is not in the tuple data portion of the page. In the form of (offset,
    /// tuple data start).
    OffsetNotInTuplePortion(u16, u16),
    /// The tuple provided does not have the same arity as the schema provided. In the form of
    /// (tuple size, schema size).
    WrongArity(usize, usize),
}

impl ::std::fmt::Display for Error {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            Error::IOError(ref e) => {
                write!(f, "An IO error occurred: {}", e)
            }
            Error::TupleError(ref e) => write!(f, "{}", e),
            Error::InvalidSlot(num_slots, slot) => {
                write!(f, "Valid slots are in range [0, {}). Got {}.", num_slots, slot)
            }
            Error::NotEnoughFreeSpace(needed, free) => {
                write!(f, "Requested {} bytes, but not enough free space in the page ({} bytes).",
                       needed, free)
            }
            Error::OffsetNotInTuplePortion(offset, tuple_data_start) => {
                write!(f, "Specified offset {} is not actually in the tuple data portion of this \
                page (data starts at offset {}).", offset, tuple_data_start)
            }
            Error::WrongArity(tup_size, schema_size) => {
                write!(f, "Tuple has different arity ({} columns) than target schema ({} columns).",
                       tup_size, schema_size)
            }
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::IOError(e.description().into())
    }
}

impl From<TupleError> for Error {
    fn from(error: TupleError) -> Error {
        Error::TupleError(Box::new(error))
    }
}

#[inline]
fn get_slot_offset(slot: u16) -> u16 {
    (1 + slot) * 2
}

/// This class represents a single page in a database file. The page's (zero-based) index in the
/// file, and whether the page has been changed in memory, are tracked by the object.
///
/// Database pages do not provide any locking mechanisms to guard against concurrent access.
/// Locking must be managed at a level above what this class provides.
///
/// The class provides methods to read and write a wide range of data types. Multibyte values are
/// stored in big-endian format, with the most significant byte (MSB) stored at the lowest index,
/// and the least significant byte (LSB) stored at the highest index. (This is also the network
/// byte order specified by the Internet Protocol.)
///
/// # Design
/// It is very important that the page is marked dirty *before* any changes are made, because this
/// is the point when the old version of the page data is copied before changes are made.
/// Additionally, the page's data must not be manipulated separately from the methods provided by
/// this class, or else the old version of the page won't be recorded properly.
pub struct DBPage {
    /// The page number of the `DBPage`.
    pub page_no: u32,
    pin_count: u32,
    dirty: bool,
    /// The data contained in the page.
    ///
    /// TODO: Ideally, this should be a `&[u8]`.
    pub page_data: Vec<u8>,
    old_page_data: Option<Vec<u8>>,

    cur_page_position: u64,
}

impl DBPage {
    /// Instantiate a new `DBPage` instance, referring to a page number on a `DBFile` with the
    /// provided information.
    ///
    /// # Arguments
    /// * db_file_info - The `DBFile` metadata.
    /// * page_no - The page number.
    ///
    /// # Error
    /// An error can occur if the buffer manager was unable to allocate the space to store the page.
    /// Currently the buffer manager does not exist, so this should never return an error.
    pub fn new(db_file_info: &DBFileInfo, page_no: u32) -> Result<DBPage, Error> {
        let page = DBPage {
            page_no: page_no,
            pin_count: 0,
            dirty: false,
            // Right now we just allocate a new buffer no matter what.
            // TODO: Use BufferManager to get a page.
            page_data: vec![0; db_file_info.page_size as usize],
            old_page_data: None,
            cur_page_position: 0,
        };
        // TODO: Use buffer manager
        Ok(page)
    }

    /// Sets the dirty flag to true or false, indicating whether the page's data has or has not been
    /// changed in memory.
    ///
    /// # Arguments
    /// - *is_dirty* - the dirty flag; true if the page's data is dirty, or false otherwise
    pub fn set_dirty(&mut self, is_dirty: bool) {
        if !self.dirty && is_dirty {
            self.old_page_data = Some(self.page_data.clone());
        } else if self.dirty && !is_dirty {
            self.old_page_data = None;
        }

        self.dirty = is_dirty;
    }


    /// Given a position within the page, read a given amount of data into a buffer at the provided
    /// offset.
    ///
    /// # Arguments
    /// position - The byte at which to start reading.
    /// buffer - The buffer being read into.
    /// offset - The offset at which the buffer is being read into.
    /// length - The number of bytes to read from the page.
    ///
    /// # Errors
    /// An error can occur if the read would result in a buffer overflow.
    pub fn read_at_position_into_offset(&self,
                                        position: usize,
                                        mut buffer: &mut [u8],
                                        offset: usize,
                                        length: usize)
                                        -> Result<usize, ()> {
        if offset + length > buffer.len() {
            return Err(());
        }
        buffer[offset..(offset + length)].copy_from_slice(&self.page_data[position..(position + length)]);
        Ok(length)
    }

    /// Given a position within the page, read enough data to fill the provided buffer.
    ///
    /// # Arguments
    /// position - The byte at which to start reading.
    /// buffer - The buffer being read into.
    ///
    /// # Errors
    /// An error can occur if [`read_at_position_into_offset`](#method.read_at_position_into_offset)
    /// fails.
    #[inline]
    pub fn read_at_position(&self, position: usize, mut buffer: &mut [u8]) -> Result<usize, ()> {
        let len = buffer.len();
        self.read_at_position_into_offset(position, buffer, 0, len)
    }

    /// Given a position within the page, write data from the provided buffer starting from a given
    /// offset into the page data.
    ///
    /// # Arguments
    /// position - The byte at which to start writing.
    /// buffer - The buffer being written from.
    /// offset - The offset at which to start copying bytes.
    ///
    /// # Errors
    /// An error can occur if a buffer overflow could occur while writing.
    pub fn write_at_position_into_offset(&mut self,
                                         position: usize,
                                         buffer: &[u8],
                                         offset: usize)
                                         -> Result<usize, ()> {
        let length = buffer.len();
        if offset + length > self.page_data.len() {
            return Err(());
        }
        self.set_dirty(true);
        &self.page_data[position..(position + length)].copy_from_slice(buffer);
        Ok(length)
    }

    /// Given a position within the page, write data from the provided buffer into the page data.
    ///
    /// # Arguments
    /// position - The byte at which to start writing.
    /// buffer - The buffer being written from.
    ///
    /// # Errors
    /// An error can occur if
    /// [`write_at_position_into_offset`](#method.write_at_position_into_offset) fails.
    #[inline]
    pub fn write_at_position(&mut self, position: usize, buffer: &[u8]) -> Result<usize, ()> {
        self.write_at_position_into_offset(position, buffer, 0)
    }

    /// This method makes the `DBPage` invalid by clearing all of its internal references. It is
    /// used by the Buffer Manager when a page is removed from the cache so that no other database
    /// code will continue to try to use the page.
    ///
    /// Since the buffer manager does not currently exist, this function does nothing.
    pub fn invalidate(&mut self) {
        // TODO: Do stuff with buffer manager here.
    }

    /// This helper function returns the amount of free space in a tuple data page. It simply uses
    /// other methods in this class to perform the simple computation.
    #[inline]
    pub fn get_free_space(&mut self) -> Result<u16, Error> {
        let data_start = try!(self.get_tuple_data_start());
        let slot_end = try!(self.get_slots_end_index());
        Ok(data_start - slot_end)
    }

    /// Initialize a newly allocated data page. Currently this involves setting the number of slots
    /// to 0. There is no other internal structure in data pages at this point.
    #[inline]
    pub fn init_new_page(&mut self) -> Result<(), Error> {
        self.set_num_slots(0)
    }

    fn set_num_slots(&mut self, num_slots: u16) -> Result<(), Error> {
        try!(self.seek(SeekFrom::Start(OFFSET_NUM_SLOTS as u64)));
        self.write_u16::<BigEndian>(num_slots).map_err(Into::into)
    }

    fn set_slot_value(&mut self, slot: u16, value: u16) -> Result<(), Error> {
        let num_slots = try!(self.get_num_slots());

        if slot >= num_slots {
            return Err(Error::InvalidSlot(num_slots, slot));
        }

        try!(self.seek(SeekFrom::Start(get_slot_offset(slot) as u64)));
        self.write_u16::<BigEndian>(value).map_err(Into::into)
    }

    fn get_slots_end_index(&mut self) -> Result<u16, Error> {
        self.get_num_slots().map(|num_slots| get_slot_offset(num_slots))
    }

    fn get_tuple_data_start(&mut self) -> Result<u16, Error> {
        let num_slots = try!(self.get_num_slots());
        // If there are no tuples in this page, "data start" is the top of the page data.
        let mut data_start = self.page_data.len() as u16;

        if num_slots > 0 {
            let mut slot = num_slots - 1;
            loop {
                let slot_value = try!(self.get_slot_value(slot));
                if slot_value != EMPTY_SLOT {
                    data_start = slot_value;
                    break;
                }

                if slot == 0 {
                    break;
                }

                slot -= 1;
            }
        }

        Ok(data_start)
    }

    /// This helper function returns the value stored in the specified slot. This will either be the
    /// offset of the start of a tuple in the data page, or it will be `EMPTY_SLOT` if the slot is
    /// empty.
    ///
    /// # Arguments
    /// * slot - the slot to retrieve the value for.
    ///
    /// # Errors
    /// Returns an `InvalidSlot` error if the slot provided is not within the range [0, num_slots).
    pub fn get_slot_value(&mut self, slot: u16) -> Result<u16, Error> {
        let num_slots = try!(self.get_num_slots());

        if slot >= num_slots {
            return Err(Error::InvalidSlot(num_slots, slot));
        }

        try!(self.seek(SeekFrom::Start(get_slot_offset(slot) as u64)));
        self.read_u16::<BigEndian>().map_err(Into::into)
    }

    /// Returns the number of slots in this data page.  This can be considered to be the current
    /// "capacity" of the page, since any number of the slots could be set to {@link #EMPTY_SLOT} to
    /// indicate that they are empty.
    pub fn get_num_slots(&mut self) -> Result<u16, Error> {
        try!(self.seek(SeekFrom::Start(OFFSET_NUM_SLOTS as u64)));
        self.read_u16::<BigEndian>().map_err(Into::into)
    }

    /// Update the data page so that it has space for a new tuple of the specified size. The new
    /// tuple is assigned a slot (whose index is returned by this method), and the space for the
    /// tuple is initialized to all zero values.
    ///
    /// Returns the slot-index for the new tuple. The offset to the start of the requested space is
    /// available via that slot. (Use `get_slot_value` to retrieve that offset.)
    ///
    /// # Arguments
    /// * len - The length of the new tuple's data.
    pub fn alloc_new_tuple(&mut self, len: u16) -> Result<u16, Error> {
        let mut space_needed = len;

        debug!("Allocating space for new {}-byte tuple.", len);

        let mut num_slots = try!(self.get_num_slots());
        debug!("Current number of slots on page: {}", num_slots);

        // This variable tracks where the new tuple should END. It starts
        // as the page-size, and gets moved down past each valid tuple in
        // the page, until we find an available slot in the page.
        let mut new_tuple_end = self.page_data.len() as u16;

        let mut slot = 0;
        while slot < num_slots {
            // cur_slot_value is either the start of that slot's tuple-data, or it is set to
            // EMPTY_SLOT.
            let cur_slot_value = try!(self.get_slot_value(slot));
            if cur_slot_value == EMPTY_SLOT {
                break;
            } else {
                new_tuple_end = cur_slot_value;
            }
            slot += 1;
        }

        // First make sure we actually have enough space for the new tuple.

        if slot == num_slots {
            // We'll need to add a new slot to the list. Make sure there's room.
            space_needed += 2;
        }

        let free_space = try!(self.get_free_space());
        if space_needed > free_space {
            return Err(Error::NotEnoughFreeSpace(space_needed, free_space));
        }

        // Now we know we have space for the tuple. Update the slot list,
        // and the update page's layout to make room for the new tuple.
        if slot == num_slots {
            debug!("No empty slot available. Adding a new slot.");

            // Add the new slot to the page, and update the total number of
            // slots.
            num_slots += 1;
            try!(self.set_num_slots(num_slots));
            try!(self.set_slot_value(slot, EMPTY_SLOT));
        }

        debug!("Tuple will get slot {}. Final number of slots: {}", slot, num_slots);

        let new_tuple_start = new_tuple_end - len;

        debug!("New tuple of {} bytes will reside at location [{}, {}).", len, new_tuple_start,
                 new_tuple_end);

        // Make room for the new tuple's data to be stored into. Since tuples are stored from the END of
        // the page going backwards, we specify the new tuple's END index, and the tuple's length. (Note:
        // This call also updates all affected slots whose offsets would be changed.)
        try!(self.insert_tuple_data_range(new_tuple_end, len));

        // Set the slot's value to be the starting offset of the tuple. We have to do this *after* we
        // insert the new space for the new tuple, or else insertTupleDataRange() will clobber the
        // slot-value of this tuple.
        try!(self.set_slot_value(slot, new_tuple_start));

        // Finally, return the slot-index of the new tuple.
        Ok(slot)
    }

    fn move_data_range(&mut self, src_pos: usize, dest_pos: usize, length: usize) {
        self.set_dirty(true);

        let src_data = self.page_data[src_pos..(src_pos + length)].to_vec();
        &self.page_data[dest_pos..(dest_pos + length)].copy_from_slice(&src_data);
    }

    fn set_data_range(&mut self, position: usize, length: usize, value: u8) {
        self.set_dirty(true);
        for i in 0..length {
            self.page_data[position + i] = value;
        }
    }

    fn insert_tuple_data_range(&mut self, offset: u16, len: u16) -> Result<(), Error> {
        let tuple_data_start = try!(self.get_tuple_data_start());

        if offset < tuple_data_start {
            return Err(Error::OffsetNotInTuplePortion(offset, tuple_data_start));
        }

        let free_space = try!(self.get_free_space());
        if len > free_space {
            return Err(Error::NotEnoughFreeSpace(len, free_space));
        }

        // If off == tupDataStart then there's no need to move anything.
        if offset > tuple_data_start {
            // Move the data in the range [tupDataStart, off) to
            // [tupDataStart - len, off - len). Thus there will be a gap in the
            // range [off - len, off) after the operation is completed.
            self.move_data_range(tuple_data_start as usize,
                                 (tuple_data_start - len) as usize,
                                 (offset - tuple_data_start) as usize);
        }

        // Zero out the gap that was just created.
        let start_offset = offset - len;
        self.set_data_range(start_offset as usize, len as usize, 0);

        // Update affected slots; this includes all slots below the specified
        // offset. The update is easy; slot values just move down by len bytes.
        let num_slots = try!(self.get_num_slots());
        for slot in 0..num_slots {
            let slot_value = try!(self.get_slot_value(slot));
            if slot_value != EMPTY_SLOT && slot_value < offset {
                // Update this slot's offset.
                try!(self.set_slot_value(slot, slot_value - len));
            }
        }

        Ok(())
    }

    /// This is a helper function to set or clear the value of a column's `NULL` flag.
    ///
    /// # Arguments
    /// * tuple_start - the byte-offset in the page where the tuple starts
    /// * col_index - the index of the column to set the null-flag for
    /// * value - the new value for the null-flag
    pub fn set_null_flag(&mut self, tuple_start: u16, col_index: usize, value: u8) -> Result<(), Error> {
        // Skip to the byte that contains the NULL-flag for this specific column.
        let null_flag_offset = tuple_start + (col_index as u16 / 8);

        // Create a bit-mask for setting or clearing the specified NULL flag, then
        // set/clear the flag in the mask byte.
        let mask = 1 << (col_index % 8);

        try!(self.seek(SeekFrom::Start(null_flag_offset as u64)));
        let mut null_flag = try!(self.read_u8());

        null_flag = if value == 1 {
            null_flag | mask
        } else {
            null_flag & !mask
        };


        try!(self.seek(SeekFrom::Start(null_flag_offset as u64)));
        self.write_u8(null_flag).map_err(Into::into)
    }

    fn write_non_null_value(&mut self, offset: u16, col_type: ColumnType, value: Literal) -> Result<u16, Error> {
        let offset = offset as u64;
        try!(self.seek(SeekFrom::Start(offset as u64)));

        // We use unwraps here because we shouldn't be able to get to this point without the value
        // being storeable with that column type.
        match col_type {
            ColumnType::TinyInt => {
                let value = match value.as_int().unwrap() {
                    Literal::Int(i) => i,
                    _ => 0,
                } as u8;
                try!(self.write_u8(value));
                Ok(1)
            }
            ColumnType::SmallInt => {
                let value = match value.as_int().unwrap() {
                    Literal::Int(i) => i,
                    _ => 0,
                } as u16;
                try!(self.write_u16::<BigEndian>(value));
                Ok(2)
            }
            ColumnType::Integer => {
                let value = match value.as_int().unwrap() {
                    Literal::Int(i) => i,
                    _ => 0,
                } as u32;
                try!(self.write_u32::<BigEndian>(value));
                Ok(4)
            }
            ColumnType::BigInt => {
                let value = match value.as_long().unwrap() {
                    Literal::Long(l) => l,
                    _ => 0,
                } as u64;
                try!(self.write_u64::<BigEndian>(value));
                Ok(8)
            }
            ColumnType::Float => {
                let value = match value.as_float().unwrap() {
                    Literal::Float(f) => f,
                    _ => 0.0,
                } as f32;
                try!(self.write_f32::<BigEndian>(value));
                Ok(4)
            }
            ColumnType::Double => {
                let value = match value.as_double().unwrap() {
                    Literal::Double(d) => d,
                    _ => 0.0,
                } as f64;
                try!(self.write_f64::<BigEndian>(value));
                Ok(8)
            }
            ColumnType::Char { length } => {
                let value = value.as_string().unwrap();
                try!(self.write_fixed_size_string(value, length));
                Ok(length)

            }
            ColumnType::VarChar { length: _ } => {
                let value = value.as_string().unwrap();
                let str_len = value.len();
                try!(self.write_varchar65535(value));
                Ok(2 + str_len as u16)
            }
            _ => unimplemented!(),
        }
    }

    /// Store a new tuple in the page.
    ///
    /// # Arguments
    /// * offset - The offset at which to put the tuple.
    /// * schema - A reference to the schema the tuple should follow.
    /// * tuple - A reference to the tuple itself.
    pub fn store_new_tuple<T: Tuple>(&mut self, offset: u16, schema: Schema, mut tuple: T) -> Result<(), Error> {
        if schema.num_columns() != tuple.get_column_count() {
            return Err(Error::WrongArity(tuple.get_column_count(), schema.num_columns()));
        }

        let mut cur_offset = offset + get_null_flags_size(schema.num_columns());
        let mut col_idx = 0usize;
        for col_info in schema.clone() {
            let col_type = col_info.column_type;
            let value = try!(tuple.get_column_value(col_idx));
            let mut data_size = 0;

            if value == Literal::Null {
                try!(self.set_null_flag(offset, col_idx, 1));
            } else {
                try!(self.set_null_flag(offset, col_idx, 0));
                data_size = try!(self.write_non_null_value(cur_offset, col_type, value));
            }

            cur_offset += data_size;
            col_idx += 1;
        }
        Ok(())
    }
}

impl Read for DBPage {
    #[inline]
    fn read(&mut self, mut buffer: &mut [u8]) -> io::Result<usize> {
        match self.read_at_position(self.cur_page_position as usize, buffer) {
            Ok(bytes) => {
                self.cur_page_position += bytes as u64;
                Ok(bytes)
            }
            Err(_) => Err(ErrorKind::Other.into()),
        }
    }
}

impl Write for DBPage {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.set_dirty(true);
        let position = self.cur_page_position as usize;
        match self.write_at_position(position, buffer) {
            Ok(bytes) => {
                self.cur_page_position += bytes as u64;
                Ok(bytes)
            }
            Err(_) => Err(ErrorKind::Other.into()),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.dirty {
            return Err(io::Error::from(ErrorKind::InvalidData));
        }
        Ok(())
    }
}

impl Seek for DBPage {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match pos {
            SeekFrom::Current(offset) => {
                self.cur_page_position = self.cur_page_position + offset as u64;
            }
            SeekFrom::Start(offset) => {
                if offset > self.page_data.len() as u64 {
                    return Err(ErrorKind::Other.into());
                }
                self.cur_page_position = offset;
            }
            SeekFrom::End(offset) => {
                let new_pos: i64 = self.page_data.len() as i64 + offset as i64;
                if new_pos < 0 {
                    return Err(ErrorKind::Other.into());
                }
                self.cur_page_position = new_pos as u64;
            }
        }
        Ok(self.cur_page_position)
    }
}

impl Pinnable for DBPage {
    fn pin(&mut self) {
        self.pin_count += 1;
        // TODO: Record page pinned in buffer manager
    }

    fn unpin(&mut self) -> Result<(), PinError> {
        if self.pin_count <= 0 {
            return Err(PinError::PinCountNotPositive(self.pin_count));
        }

        self.pin_count -= 1;

        // TODO: Record page unpinned in buffer manager

        Ok(())
    }

    fn get_pin_count(&self) -> u32 {
        self.pin_count
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use ::storage::{DBFile, DBFileType, PinError, Pinnable};

    #[test]
    fn test_pinning() {
        let contents = vec![0; 512];
        let dbfile = DBFile::new(DBFileType::HeapTupleFile, 512, Cursor::new(contents)).unwrap();

        let mut page = DBPage::new(&dbfile, 0).unwrap();

        assert_eq!(Err(PinError::PinCountNotPositive(0)), page.unpin());
        page.pin();
        assert_eq!(Ok(()), page.unpin());
        page.pin();
        page.pin();
        assert_eq!(Ok(()), page.unpin());
        assert_eq!(Ok(()), page.unpin());
        assert_eq!(Err(PinError::PinCountNotPositive(0)), page.unpin());
    }
}
