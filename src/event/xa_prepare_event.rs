use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};

use crate::{binlog_error::BinlogError, ext::cursor_ext::CursorExt};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct XaPrepareEvent {
    pub one_phase: bool,
    pub format_id: u32,
    pub gtrid: String,
    pub bqual: String,
}

impl XaPrepareEvent {
    // refer: https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/src/control_events.cpp#L590
    pub fn parse(cursor: &mut Cursor<&Vec<u8>>) -> Result<Self, BinlogError> {
        let one_phase = cursor.read_u8()? == 0;
        let format_id = cursor.read_u32::<LittleEndian>()?;
        let gtrid_length = cursor.read_u32::<LittleEndian>()?;
        let bqual_length = cursor.read_u32::<LittleEndian>()?;
        let gtrid = cursor.read_string(gtrid_length as usize)?;
        let bqual = cursor.read_string(bqual_length as usize)?;

        Ok(Self {
            one_phase,
            format_id,
            gtrid,
            bqual,
        })
    }
}
