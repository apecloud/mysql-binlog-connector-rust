use std::io::{Cursor, Read, Seek, SeekFrom};

use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};

use crate::{binlog_error::BinlogError, constants, ext::cursor_ext::CursorExt};

#[derive(Debug, Deserialize, Serialize, Clone)]
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
        let mut buf = [0u8; constants::EVENT_HEADER_LENGTH];
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

    // Parse the common header for rows events:
    // WriteRows / UpdateRows / DeleteRows
    // ExtWriteRows / ExtUpdateRows / ExtDeleteRows
    pub fn parse_rows_event_common_header(
        cursor: &mut Cursor<&Vec<u8>>,
        row_event_version: u8,
    ) -> Result<(u64, usize, Vec<bool>), BinlogError> {
        let table_id = cursor.read_u48::<LittleEndian>()?;
        let _flags = cursor.read_u16::<LittleEndian>()?;

        // ExtWriteRows/ExtUpdateRows/ExtDeleteRows, version 2, MySQL only
        if row_event_version == 2 {
            let extra_data_length = cursor.read_u16::<LittleEndian>()? as i64;
            cursor.seek(SeekFrom::Current(extra_data_length - 2))?;
        }

        let column_count = cursor.read_packed_number()?;
        let included_columns = cursor.read_bits(column_count, false)?;

        Ok((table_id, column_count, included_columns))
    }
}
