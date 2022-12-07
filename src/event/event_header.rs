use std::io::{Cursor, Read, Seek};

use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};

use crate::{binlog_error::BinlogError, constants};

#[derive(Debug, Deserialize, Serialize)]
pub struct EventHeader {
    pub timestamp: u32,
    pub event_type: u8,
    pub server_id: u32,
    pub event_length: u32,
    pub next_event_position: u32,
    pub event_flags: u16,
}

impl EventHeader {
    pub fn parse<S: Read + Seek>(stream: &mut S) -> Result<Self, BinlogError> {
        // refer: https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Log__event__header.html
        let mut buf = [0u8; constants::EVENT_HEADER_LENGTH as usize];
        stream.read_exact(&mut buf)?;

        let mut cursor = Cursor::new(&buf);
        Ok(Self {
            timestamp: cursor.read_u32::<LittleEndian>()?,
            event_type: cursor.read_u8()?,
            server_id: cursor.read_u32::<LittleEndian>()?,
            event_length: cursor.read_u32::<LittleEndian>()?,
            next_event_position: cursor.read_u32::<LittleEndian>()?,
            event_flags: cursor.read_u16::<LittleEndian>()?,
        })
    }
}
