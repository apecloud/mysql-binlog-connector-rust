use std::io::{BufRead, Cursor, Read, Seek, SeekFrom};

use byteorder::{LittleEndian, ReadBytesExt};

use crate::{binlog_error::BinlogError, constants};

use super::buf_ext::BufExt;

pub trait CursorExt {
    fn read_string(&mut self, size: usize) -> Result<String, BinlogError>;

    fn read_string_without_terminator(&mut self, size: usize) -> Result<String, BinlogError>;

    fn read_null_terminated_string(&mut self) -> Result<String, BinlogError>;

    fn read_packed_number(&mut self) -> Result<usize, BinlogError>;

    fn read_bits(&mut self, size: usize, big_endian: bool) -> Result<Vec<bool>, BinlogError>;

    fn read_bits_as_bytes(&mut self, size: usize, big_endian: bool)
        -> Result<Vec<u8>, BinlogError>;

    fn available(&mut self) -> usize;

    fn read_bytes(&mut self, size: usize) -> Result<Vec<u8>, BinlogError>;
}

impl CursorExt for Cursor<&Vec<u8>> {
    /// Read bytes from cursor and parse into utf8 string
    fn read_string(&mut self, size: usize) -> Result<String, BinlogError> {
        let mut buf = vec![0; size];
        self.read_exact(&mut buf)?;
        Ok(buf.to_utf8_string())
    }

    /// Read a utf8 string from cursor and skip the end signal
    fn read_string_without_terminator(&mut self, size: usize) -> Result<String, BinlogError> {
        let res = self.read_string(size);
        self.seek(SeekFrom::Current(1))?;
        res
    }

    /// Read variable-length string, the end is 0x00
    fn read_null_terminated_string(&mut self) -> Result<String, BinlogError> {
        let mut buf = Vec::new();
        self.read_until(constants::NULL_TERMINATOR, &mut buf)?;
        buf.pop();
        Ok(buf.to_utf8_string())
    }

    /// Format (first-byte-based):
    /// 0-250 - The first byte is the number (in the range 0-250). No additional bytes are used.<br>
    /// 251 - SQL NULL value<br>
    /// 252 - Two more bytes are used. The number is in the range 251-0xffff.<br>
    /// 253 - Three more bytes are used. The number is in the range 0xffff-0xffffff.<br>
    /// 254 - Eight more bytes are used. The number is in the range 0xffffff-0xffffffffffffffff.
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
            Err(BinlogError::UnexpectedData(
                "read packed number failed".into(),
            ))
        }
    }

    /// Read n bits from cursor to Vec<bool>, if the origin data is encoded in BigEndian, reverse the order first
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

    /// Read n bits from cursor, if the origin data is encoded in BigEndian, reverse the order first
    fn read_bits_as_bytes(
        &mut self,
        size: usize,
        big_endian: bool,
    ) -> Result<Vec<u8>, BinlogError> {
        // the number of bytes needed is int((count + 7) / 8)
        let mut bytes = vec![0u8; (size + 7) >> 3];
        self.read_exact(&mut bytes)?;

        if big_endian {
            bytes.reverse();
        }
        Ok(bytes)
    }

    /// Read n bytes from cursor and return the buf
    fn read_bytes(&mut self, size: usize) -> Result<Vec<u8>, BinlogError> {
        let mut buf = vec![0; size];
        self.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Return the available bytes count in cursor
    fn available(&mut self) -> usize {
        self.get_ref().len() - self.position() as usize
    }
}
