use std::io::{self, ErrorKind, SeekFrom};
use std::io::prelude::*;

use super::{DBFileInfo, PinError, Pinnable};

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum Error {}

pub struct DBPage {
    pub page_no: u32,
    pin_count: u32,
    dirty: bool,
    pub page_data: Vec<u8>,
    old_page_data: Option<Vec<u8>>,

    cur_page_position: u64,
}

impl DBPage {
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
    fn set_dirty(&mut self, is_dirty: bool) {
        if !self.dirty && is_dirty {
            self.old_page_data = Some(self.page_data.clone());
        } else if self.dirty && !is_dirty {
            self.old_page_data = None;
        }

        self.dirty = is_dirty;
    }

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

    pub fn read_at_position(&self, position: usize, mut buffer: &mut [u8]) -> Result<usize, ()> {
        let len = buffer.len();
        self.read_at_position_into_offset(position, buffer, 0, len)
    }

    pub fn write_at_position_into_offset(&mut self, position: usize, buffer: &[u8], offset: usize) -> Result<usize, ()> {
        let length = buffer.len();
        if offset + length > self.page_data.len() {
            return Err(());
        }
        self.set_dirty(true);
        &self.page_data[position..(position + length)].copy_from_slice(buffer);
        Ok(length)
    }

    pub fn write_at_position(&mut self, position: usize, buffer: &[u8]) -> Result<usize, ()> {
        self.write_at_position_into_offset(position, buffer, 0)
    }

    pub fn invalidate(&mut self) {
        // TODO: Do stuff with buffer manager here.
    }
}

impl Read for DBPage {
    fn read(&mut self, mut buffer: &mut [u8]) -> io::Result<usize> {
        self.read_at_position(self.cur_page_position as usize, buffer).map_err(|_| ErrorKind::Other.into())
    }
}

impl Write for DBPage {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let position = self.cur_page_position as usize;
        match self.write_at_position(position, buffer) {
            Ok(bytes) => {
                self.cur_page_position += bytes as u64;
                Ok(bytes)
            },
            Err(_) => { Err(ErrorKind::Other.into()) }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        unimplemented!()
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
            return Err(PinError::PinCountNotPositive);
        }

        self.pin_count -= 1;

        // TODO: Record page unpinned in buffer manager

        Ok(())
    }

    fn get_pin_count(&self) -> u32 {
        self.pin_count
    }

    fn is_pinned(&self) -> bool {
        self.pin_count > 0
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use super::super::{DBFile, DBFileType, Pinnable};

    #[test]
    fn test_pinning() {
        let mut contents = vec![0; 512];
        contents.extend_from_slice(&[0xac; 512]);
        contents.extend_from_slice(&[0xaf; 512]);
        let mut dbfile = DBFile::new(DBFileType::HeapTupleFile, 512, Cursor::new(contents)).unwrap();

        let page0 = DBPage::new(&dbfile, 0).unwrap();
        let page1 = DBPage::new(&dbfile, 1).unwrap();
        let page2 = DBPage::new(&dbfile, 2).unwrap();
    }
}
