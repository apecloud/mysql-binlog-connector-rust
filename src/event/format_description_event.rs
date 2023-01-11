use crate::{binlog_error::BinlogError, event::event_type::EventType};
use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Read, Seek, SeekFrom};

use super::checksum_type::ChecksumType;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct FormatDescriptionEvent {
    pub binlog_version: u16,
    pub server_version: String,
    pub create_timestamp: u32,
    pub header_length: u8,
    pub checksum_type: ChecksumType,
}

impl FormatDescriptionEvent {
    pub fn parse(cursor: &mut Cursor<&Vec<u8>>, data_length: usize) -> Result<Self, BinlogError> {
        // refer: https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Format__description__event.html
        // binlog_version: 2 bytes
        let binlog_version = cursor.read_u16::<LittleEndian>()?;

        // server_version: 50 bytes
        let mut server_version_buf = [0u8; 50];
        cursor.read_exact(&mut server_version_buf)?;
        let server_version = std::str::from_utf8(&server_version_buf)
            .unwrap()
            .to_string();

        // create_timestamp: 4 bytes
        let create_timestamp = cursor.read_u32::<LittleEndian>()?;

        // header_length: 1 byte
        // Length of the Binlog Event Header of next events. Should always be 19.
        let header_length = cursor.read_u8()?;

        // post-header (76 : n), it is an array of n bytes,
        // one byte per event type that the server knows about, n = count of all event types,
        // the 14th (EventType::FormatDescription - 1) byte contains the payload length of FormatDescription,
        cursor.seek(SeekFrom::Current(EventType::FormatDescription as i64 - 1))?;
        let payload_length = cursor.read_u8()? as usize;

        // after the header and payload, it is the checksum type, 1 byte
        let mut checksum_type = 0;
        let checksum_block_length = data_length - payload_length;
        if checksum_block_length > 0 {
            // seek to the end of payload
            let current_pos = 2 + 50 + 4 + 1 + EventType::FormatDescription as u8;
            cursor.seek(SeekFrom::Current(
                payload_length as i64 - current_pos as i64,
            ))?;
            // read checksum type, refer: https://mariadb.com/kb/en/format_description_event/
            checksum_type = cursor.read_u8()?;
        }

        Ok(Self {
            binlog_version,
            server_version,
            create_timestamp,
            header_length,
            checksum_type: ChecksumType::from_code(checksum_type),
        })
    }
}
