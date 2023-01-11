use std::{
    collections::HashMap,
    io::{Cursor, Seek, SeekFrom},
};

use serde::{Deserialize, Serialize};

use crate::{binlog_error::BinlogError, binlog_parser::BinlogParser, ext::cursor_ext::CursorExt};

use super::{event_data::EventData, event_header::EventHeader};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TransactionPayloadEvent {
    pub uncompressed_size: u32,
    pub uncompressed_events: Vec<(EventHeader, EventData)>,
}

impl TransactionPayloadEvent {
    pub fn parse(cursor: &mut Cursor<&Vec<u8>>) -> Result<Self, BinlogError> {
        // refer: https://dev.mysql.com/doc/refman/8.0/en/binary-log-transaction-compression.html
        let (_compress_type, uncompressed_size) = Self::parse_meta(cursor)?;

        // read the rest data as payload and decompress it, currently only support zstd
        let mut uncompressed_payload: Vec<u8> = Vec::new();
        zstd::stream::copy_decode(cursor, &mut uncompressed_payload)?;

        // construct a new parser from the payload
        let mut payload_cursor = Cursor::new(uncompressed_payload);
        let mut parser = BinlogParser {
            checksum_length: 0,
            table_map_event_by_table_id: HashMap::new(),
        };

        // parse events in payload
        let mut uncompressed_events: Vec<(EventHeader, EventData)> = Vec::new();
        while let Ok(e) = parser.next(&mut payload_cursor) {
            uncompressed_events.push(e);
        }

        Ok(Self {
            uncompressed_size: uncompressed_size as u32,
            uncompressed_events,
        })
    }

    fn parse_meta(cursor: &mut Cursor<&Vec<u8>>) -> Result<(usize, usize), BinlogError> {
        let mut payload_size = 0;
        let mut compress_type = 0;
        let mut uncompressed_size = 0;

        while cursor.available() > 0 {
            let field_type = if cursor.available() >= 1 {
                cursor.read_packed_number()?
            } else {
                0
            };

            // we have reached the end of the Event Data Header
            if field_type == 0 {
                break;
            }

            let field_length = if cursor.available() >= 1 {
                cursor.read_packed_number()?
            } else {
                0
            };

            match field_type {
                1 => payload_size = cursor.read_packed_number()?,

                2 => compress_type = cursor.read_packed_number()?,

                3 => uncompressed_size = cursor.read_packed_number()?,

                _ => {
                    cursor.seek(SeekFrom::Current(field_length as i64))?;
                }
            }
        }

        if uncompressed_size == 0 {
            uncompressed_size = payload_size;
        }

        Ok((compress_type, uncompressed_size))
    }
}
