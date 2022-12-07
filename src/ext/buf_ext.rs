use std::io::Write;

use byteorder::WriteBytesExt;

use crate::{binlog_error::BinlogError, constants};

pub trait BufExt {
    fn write_null_terminated_string(&mut self, to_write: &str) -> Result<(), BinlogError>;

    fn reverse(&mut self);
}

impl BufExt for Vec<u8> {
    /**
     * write a string to buf with 0x00 as end
     */
    fn write_null_terminated_string(&mut self, to_write: &str) -> Result<(), BinlogError> {
        self.write(to_write.as_bytes())?;
        self.write_u8(constants::NULL_TERMINATOR)?;
        Ok(())
    }

    /**
     * reverse the order of contents in the buf
     */
    fn reverse(&mut self) {
        for i in 0..self.len() >> 1 {
            let j = self.len() - 1 - i;
            let tmp = self[i];
            self[i] = self[j];
            self[j] = tmp;
        }
    }
}
