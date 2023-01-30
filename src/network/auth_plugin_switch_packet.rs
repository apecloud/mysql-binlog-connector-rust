use std::io::{Cursor, Seek, SeekFrom};

use crate::{binlog_error::BinlogError, ext::cursor_ext::CursorExt};

pub struct AuthPluginSwitchPacket {
    pub auth_plugin_name: String,
    pub scramble: String,
}

impl AuthPluginSwitchPacket {
    pub fn new(packet: &Vec<u8>) -> Result<Self, BinlogError> {
        // refer to: https://mariadb.com/kb/en/connection/#authentication-switch-request
        let mut cursor = Cursor::new(packet);
        cursor.seek(SeekFrom::Current(1))?;

        let auth_plugin_name = cursor.read_null_terminated_string()?;
        let scramble = cursor.read_null_terminated_string()?;
        Ok(Self {
            auth_plugin_name,
            scramble,
        })
    }
}
