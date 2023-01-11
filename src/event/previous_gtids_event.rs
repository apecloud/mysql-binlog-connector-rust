use super::gtid_event::GtidEvent;
use crate::binlog_error::BinlogError;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};
use std::io::Cursor;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PreviousGtidsEvent {
    pub gtid_set: String,
}

impl PreviousGtidsEvent {
    pub fn parse(cursor: &mut Cursor<&Vec<u8>>) -> Result<Self, BinlogError> {
        let uuid_count = cursor.read_u64::<LittleEndian>()?;
        let mut gtids: Vec<String> = Vec::with_capacity(uuid_count as usize);

        for _ in 0..uuid_count {
            let uuid = GtidEvent::read_uuid(cursor)?;
            let intervals = Self::read_interval(cursor)?;
            gtids.push(format!("{}:{}", uuid, intervals));
        }

        Ok(Self {
            gtid_set: gtids.join(","),
        })
    }

    fn read_interval(cursor: &mut Cursor<&Vec<u8>>) -> Result<String, BinlogError> {
        let interval_count = cursor.read_u64::<LittleEndian>()?;
        let mut intervals = Vec::with_capacity(interval_count as usize);

        for _ in 0..interval_count {
            let start = cursor.read_u64::<LittleEndian>()?;
            let end = cursor.read_u64::<LittleEndian>()?;
            // mysql "show binlog events in 'mysql-bin.000005'" returns:
            // "58cf6502-63db-11ed-8079-0242ac110002:1-8" while we get interval_start = 1, interval_end = 9
            intervals.push(format!("{}-{}", start, end - 1));
        }

        Ok(intervals.join(":"))
    }
}
