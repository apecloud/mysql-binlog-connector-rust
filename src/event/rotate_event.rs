use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};

use crate::{binlog_error::BinlogError, ext::cursor_ext::CursorExt};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RotateEvent {
    pub binlog_filename: String,
    pub binlog_position: u64,
}

impl RotateEvent {
    pub fn parse(cursor: &mut Cursor<&Vec<u8>>) -> Result<Self, BinlogError> {
        let binlog_position = cursor.read_u64::<LittleEndian>()?;
        let binlog_filename = cursor.read_string_without_terminator(cursor.get_ref().len() - 8)?;
        Ok(Self {
            binlog_filename,
            binlog_position,
        })
    }
}
