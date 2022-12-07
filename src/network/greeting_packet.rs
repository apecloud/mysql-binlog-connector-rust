use std::io::{Cursor, Seek, SeekFrom};

use byteorder::{LittleEndian, ReadBytesExt};

use crate::{binlog_error::BinlogError, ext::cursor_ext::CursorExt};

pub struct GreetingPacket {
    pub protocol_version: u8,
    pub server_version: String,
    pub thread_id: u32,
    pub server_capabilities: u16,
    pub server_collation: u8,
    pub server_status: u16,
    pub scramble: String,
    pub plugin_provided_data: String,
}

impl GreetingPacket {
    pub fn new(buf: Vec<u8>) -> Result<Self, BinlogError> {
        let mut cursor = Cursor::new(&buf);
        let protocol_version = cursor.read_u8()?;
        let server_version = cursor.read_null_terminated_string()?;
        let thread_id = cursor.read_u32::<LittleEndian>()?;
        let mut scramble = cursor.read_null_terminated_string()?;
        let server_capabilities = cursor.read_u16::<LittleEndian>()?;
        let server_collation = cursor.read_u8()?;
        let server_status = cursor.read_u16::<LittleEndian>()?;

        // reserved
        cursor.seek(SeekFrom::Current(13))?;
        scramble.push_str(cursor.read_null_terminated_string()?.as_str());

        let mut plugin_provided_data = "".to_string();
        if cursor.available() > 0 {
            plugin_provided_data = cursor.read_null_terminated_string()?;
        }

        Ok(Self {
            protocol_version,
            server_version,
            thread_id,
            scramble,
            server_capabilities,
            server_collation,
            server_status,
            plugin_provided_data,
        })
    }
}
