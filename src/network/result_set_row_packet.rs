use std::io::Cursor;

use crate::{binlog_error::BinlogError, ext::cursor_ext::CursorExt};

pub struct ResultSetRowPacket {
    pub values: Vec<String>,
}

impl ResultSetRowPacket {
    pub fn new(buf: &Vec<u8>) -> Result<Self, BinlogError> {
        let mut cursor = Cursor::new(buf);
        let mut values = Vec::new();

        while cursor.available() > 0 {
            let length = cursor.read_packed_number()?;
            let value = cursor.read_string(length)?;
            values.push(value);
        }

        Ok(Self { values })
    }
}
