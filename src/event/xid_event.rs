use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};

use crate::binlog_error::BinlogError;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct XidEvent {
    pub xid: u64,
}

impl XidEvent {
    pub fn parse(cursor: &mut Cursor<&Vec<u8>>) -> Result<Self, BinlogError> {
        Ok(XidEvent {
            xid: cursor.read_u64::<LittleEndian>()?,
        })
    }
}
