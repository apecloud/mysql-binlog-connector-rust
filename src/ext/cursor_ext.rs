use std::io::{BufRead, Cursor, Read, Seek, SeekFrom};

use byteorder::{LittleEndian, ReadBytesExt};

use crate::{binlog_error::BinlogError, constants};

pub trait CursorExt {
    fn read_string(&mut self, size: usize) -> Result<String, BinlogError>;

    fn read_string_without_terminater(&mut self, size: usize) -> Result<String, BinlogError>;

    fn read_null_terminated_string(&mut self) -> Result<String, BinlogError>;

    fn read_packed_number(&mut self) -> Result<usize, BinlogError>;

    fn read_bits(&mut self, size: usize, big_endian: bool) -> Result<Vec<bool>, BinlogError>;

    fn available(&mut self) -> usize;

    fn read_bytes(&mut self, size: usize) -> Result<Vec<u8>, BinlogError>;

    fn parse_rows_event_common_header(
        &mut self,
        row_event_version: u8,
    ) -> Result<(u64, usize, Vec<bool>), BinlogError>;
}

impl CursorExt for Cursor<&Vec<u8>> {
    /**
     * read bytes from cursor and parse into utf8 string
     */
    fn read_string(&mut self, size: usize) -> Result<String, BinlogError> {
        let mut buf = vec![0; size];
        self.read_exact(&mut buf)?;
        Ok(String::from_utf8(buf)?)
    }

    /**
     * read a utf8 string from cursor and skip the end signal
     */
    fn read_string_without_terminater(&mut self, size: usize) -> Result<String, BinlogError> {
        let res = self.read_string(size);
        self.seek(SeekFrom::Current(1))?;
        res
    }

    /**
     * read variable-length string, the end is 0x00
     */
    fn read_null_terminated_string(&mut self) -> Result<String, BinlogError> {
        let mut buf = Vec::new();
        self.read_until(constants::NULL_TERMINATOR, &mut buf)?;
        buf.pop();
        Ok(String::from_utf8(buf)?)
    }

    /**
     * Format (first-byte-based):
     * 0-250 - The first byte is the number (in the range 0-250). No additional bytes are used.<br>
     * 251 - SQL NULL value<br>
     * 252 - Two more bytes are used. The number is in the range 251-0xffff.<br>
     * 253 - Three more bytes are used. The number is in the range 0xffff-0xffffff.<br>
     * 254 - Eight more bytes are used. The number is in the range 0xffffff-0xffffffffffffffff.
     */
    fn read_packed_number(&mut self) -> Result<usize, BinlogError> {
        let first = self.read_u8()?;
        if first < 0xfb {
            Ok(first as usize)
        } else if first == 0xfc {
            Ok(self.read_u16::<LittleEndian>()? as usize)
        } else if first == 0xfd {
            Ok(self.read_u24::<LittleEndian>()? as usize)
        } else if first == 0xfe {
            Ok(self.read_u64::<LittleEndian>()? as usize)
        } else {
            Err(BinlogError::ReadBinlogError {
                error: "read packed number failed".to_string(),
            })
        }
    }

    /**
     * read n bits from cursor, if the origin data is encoded in BigEndian, reverse the order first
     */
    fn read_bits(&mut self, size: usize, big_endian: bool) -> Result<Vec<bool>, BinlogError> {
        // the number of bytes needed is int((count + 7) / 8)
        let mut bytes = vec![0u8; (size + 7) >> 3];
        self.read_exact(&mut bytes)?;

        if big_endian {
            bytes.reverse();
        }

        let mut bits = vec![false; size];
        for i in 0..size {
            let belong_to_byte = bytes[i >> 3];
            let index_in_byte = 1 << (i % 8);
            bits[i] = belong_to_byte & index_in_byte != 0;
        }
        Ok(bits)
    }

    /**
     * read n bytes from cursor and return the buf
     */
    fn read_bytes(&mut self, size: usize) -> Result<Vec<u8>, BinlogError> {
        let mut buf = vec![0; size];
        self.read_exact(&mut buf)?;
        Ok(buf)
    }

    /**
     * return the available bytes count in cursor
     */
    fn available(&mut self) -> usize {
        self.get_ref().len() - self.position() as usize
    }

    /**
     * parse the common header for rows events:
     * WriteRows / UpdateRows / DeleteRows
     * ExtWriteRows / ExtUpdateRows / ExtDeleteRows
     */
    fn parse_rows_event_common_header(
        &mut self,
        row_event_version: u8,
    ) -> Result<(u64, usize, Vec<bool>), BinlogError> {
        let table_id = self.read_u48::<LittleEndian>()?;
        let _flags = self.read_u16::<LittleEndian>()?;

        // ExtWriteRows/ExtUpdateRows/ExtDeleteRows, version 2, MySQL only
        if row_event_version == 2 {
            let extra_data_length = self.read_u16::<LittleEndian>()? as i64;
            self.seek(SeekFrom::Current(extra_data_length - 2))?;
        }

        let column_count = self.read_packed_number()?;
        let included_columns = self.read_bits(column_count, false)?;

        Ok((table_id, column_count, included_columns))
    }
}
