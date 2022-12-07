use std::io::{Cursor, Read, Seek, SeekFrom};

use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};

use crate::{binlog_error::BinlogError, ext::cursor_ext::CursorExt};

#[derive(Debug, Deserialize, Serialize)]
pub struct QueryEvent {
    pub thread_id: u32,
    pub exec_time: u32,
    pub error_code: u16,
    pub schema: String,
    pub query: String,
}

impl QueryEvent {
    pub fn parse(cursor: &mut Cursor<&Vec<u8>>) -> Result<Self, BinlogError> {
        // refer: https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Query__event.html
        // Post-Header for Query_event
        let thread_id = cursor.read_u32::<LittleEndian>()?;
        let exec_time = cursor.read_u32::<LittleEndian>()?;
        let schema_length = cursor.read_u8()?;
        let error_code = cursor.read_u16::<LittleEndian>()?;
        let status_vars_length = cursor.read_u16::<LittleEndian>()? as i64;

        // skip, Body for Query_event
        cursor.seek(SeekFrom::Current(status_vars_length))?;

        // Format: schema_length + 1, The currently selected database, as a null-terminated string.
        let schema = cursor.read_string_without_terminater(schema_length as usize)?;

        let mut query = String::new();
        cursor.read_to_string(&mut query)?;

        Ok(Self {
            thread_id,
            exec_time,
            error_code,
            schema,
            query,
        })
    }
}
