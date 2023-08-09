use std::io::Write;

use byteorder::WriteBytesExt;

use crate::{binlog_error::BinlogError, constants};

pub trait BufExt {
    fn write_null_terminated_string(&mut self, to_write: &str) -> Result<(), BinlogError>;

    fn reverse(&mut self);

    fn xor(&mut self, buf2: Vec<u8>) -> Vec<u8>;
}

impl BufExt for Vec<u8> {
    /// Write a string to buf with 0x00 as end
    fn write_null_terminated_string(&mut self, to_write: &str) -> Result<(), BinlogError> {
        self.write_all(to_write.as_bytes())?;
        self.write_u8(constants::NULL_TERMINATOR)?;
        Ok(())
    }

    /// Reverse the order of contents in the buf
    fn reverse(&mut self) {
        for i in 0..self.len() >> 1 {
            let j = self.len() - 1 - i;
            self.swap(i, j);
        }
    }

    fn xor(&mut self, buf2: Vec<u8>) -> Vec<u8> {
        let mut res = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            res.push(self[i] ^ buf2[i % buf2.len()]);
        }
        res
    }
}
