use crate::binlog_error::BinlogError;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};
use std::fmt::Write;
use std::io::Cursor;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct GtidEvent {
    pub flags: u8,
    pub gtid: String,
}

impl GtidEvent {
    pub fn parse(cursor: &mut Cursor<&Vec<u8>>) -> Result<Self, BinlogError> {
        // refer: https://dev.mysql.com/doc/refman/8.0/en/replication-gtids-concepts.html
        // refer: https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Gtid__event.html
        let flags = cursor.read_u8()?;
        let sid = Self::read_uuid(cursor)?;
        let gno = cursor.read_u64::<LittleEndian>()?;

        Ok(GtidEvent {
            flags,
            gtid: format!("{}:{}", sid, gno),
        })
    }

    pub fn read_uuid(cursor: &mut Cursor<&Vec<u8>>) -> Result<String, BinlogError> {
        Ok(format!(
            "{}-{}-{}-{}-{}",
            Self::bytes_to_hex_string(cursor, 4)?,
            Self::bytes_to_hex_string(cursor, 2)?,
            Self::bytes_to_hex_string(cursor, 2)?,
            Self::bytes_to_hex_string(cursor, 2)?,
            Self::bytes_to_hex_string(cursor, 6)?,
        ))
    }

    fn bytes_to_hex_string(
        cursor: &mut Cursor<&Vec<u8>>,
        byte_count: u8,
    ) -> Result<String, BinlogError> {
        let mut res = String::new();
        for _ in 0..byte_count {
            write!(&mut res, "{:02x}", cursor.read_u8()?)?;
        }
        Ok(res)
    }
}
