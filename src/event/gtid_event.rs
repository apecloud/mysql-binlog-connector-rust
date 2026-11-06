use crate::binlog_error::BinlogError;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};
use std::fmt::Write;
use std::io::Cursor;

use super::gtid_serialization::{read_signed, read_tag, read_unsigned};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct GtidEvent {
    pub flags: u8,
    pub gtid: String,
}

impl GtidEvent {
    pub(crate) fn parse_tagged(cursor: &mut Cursor<impl AsRef<[u8]>>) -> Result<Self, BinlogError> {
        // GTID_TAGGED_LOG_EVENT (42) uses MySQL serialization, unlike event 33's
        // fixed prefix. Official field IDs 0..11 and optional suffix predicates:
        // https://github.com/mysql/mysql-server/blob/mysql-8.4.0/libs/mysql/binlog/event/control_events.h#L1111-L1177
        // Root metadata: serialization version, total message size (including
        // metadata), and the last non-ignorable field ID + 1:
        // https://github.com/mysql/mysql-server/blob/mysql-8.4.0/libs/mysql/serialization/serializer_default_impl.hpp#L435-L465
        // Message boundary calculation and skipping unknown ignorable suffixes:
        // https://github.com/mysql/mysql-server/blob/mysql-8.4.0/libs/mysql/serialization/serializer_impl.hpp#L178-L198
        let start = cursor.position();
        if read_unsigned(cursor)? != 1 {
            return Err(BinlogError::UnexpectedData(
                "unknown GTID serialization version".into(),
            ));
        }
        let size = read_unsigned(cursor)?;
        if size != cursor.get_ref().as_ref().len() as u64 - start {
            return Err(BinlogError::UnexpectedData(
                "invalid GTID message size".into(),
            ));
        }
        if read_unsigned(cursor)? > 12 {
            return Err(BinlogError::UnexpectedData(
                "unknown non-ignorable GTID field".into(),
            ));
        }

        let mut flags = 0;
        let mut uuid = [0; 16];
        let mut gno = 0;
        let mut tag = String::new();
        let mut next_id = 0;
        while cursor.position() < start + size {
            let id = read_unsigned(cursor)?;
            if id < next_id || (next_id < 4 && id != next_id) {
                return Err(BinlogError::UnexpectedData(
                    "missing or unordered GTID field".into(),
                ));
            }
            match id {
                0 => flags = Self::read_serialized_byte(cursor)?,
                1 => {
                    // Fixed-size arrays serialize each element as an integer,
                    // so the UUID is not a raw 16-byte slice:
                    // https://github.com/mysql/mysql-server/blob/mysql-8.4.0/libs/mysql/serialization/serializer_default_impl.hpp#L219-L228
                    for byte in &mut uuid {
                        *byte = Self::read_serialized_byte(cursor)?;
                    }
                }
                2 => gno = read_signed(cursor)?,
                3 => tag = read_tag(cursor)?,
                4..=11 => {
                    // Consume clocks (4/5), timestamps (6/7), transaction length
                    // (8), server versions (9/10), and commit group ticket (11).
                    read_unsigned(cursor)?;
                }
                // Unknown ignorable suffixes are skipped to the message boundary.
                _ => {
                    cursor.set_position(start + size);
                    break;
                }
            }
            next_id = id + 1;
        }
        if next_id < 4 {
            return Err(BinlogError::UnexpectedData(
                "missing GTID identity field".into(),
            ));
        }
        let sid = Self::read_uuid(&mut Cursor::new(uuid))?;
        let tsid = if tag.is_empty() {
            sid
        } else {
            format!("{}:{}", sid, tag)
        };
        Ok(Self {
            flags,
            gtid: format!("{}:{}", tsid, gno),
        })
    }

    fn read_serialized_byte(cursor: &mut Cursor<impl AsRef<[u8]>>) -> Result<u8, BinlogError> {
        u8::try_from(read_unsigned(cursor)?)
            .map_err(|_| BinlogError::UnexpectedData("GTID byte out of range".into()))
    }

    pub fn parse(cursor: &mut Cursor<impl AsRef<[u8]>>) -> Result<Self, BinlogError> {
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

    pub fn read_uuid(cursor: &mut Cursor<impl AsRef<[u8]>>) -> Result<String, BinlogError> {
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
        cursor: &mut Cursor<impl AsRef<[u8]>>,
        byte_count: u8,
    ) -> Result<String, BinlogError> {
        let mut res = String::new();
        for _ in 0..byte_count {
            write!(&mut res, "{:02x}", cursor.read_u8()?)?;
        }
        Ok(res)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // Identity prefix from mysql:8.4.13, mysql-bin.000001 at position 448.
    // Size adjusted to omit commit metadata; no encoder from this crate is used.
    fn tagged_identity() -> Vec<u8> {
        vec![
            0x02, 0x4a, 0x00, 0x00, 0x00, 0x02, 0x3d, 0x02, 0xaa, 0xb2, 0x49, 0x02, 0x4d, 0x03,
            0xe0, 0x22, 0xc5, 0x03, 0x89, 0x02, 0x72, 0x04, 0x84, 0xb1, 0x02, 0x22, 0x00, 0x04,
            0x04, 0x04, 0x06, 0x0a, b'a', b'd', b'm', b'i', b'n',
        ]
    }

    #[test]
    fn test_tagged_optional_fields_and_empty_tag() {
        for suffix in [vec![], vec![8, 2], vec![14, 0, 20, 0, 22, 0], vec![24, 255]] {
            let mut body = tagged_identity();
            body.extend(suffix);
            body[1] = (body.len() * 2) as u8;
            let event = GtidEvent::parse_tagged(&mut Cursor::new(body)).unwrap();
            assert_eq!(event.flags, 0);
            assert_eq!(event.gtid, "8f555992-d370-11f1-a239-0242ac110002:admin:1");
        }
        let mut body = tagged_identity();
        body.truncate(32);
        body[1] = 64;
        body[31] = 0;
        body[4] = 2; // flags = 1
        let event = GtidEvent::parse_tagged(&mut Cursor::new(body)).unwrap();
        assert_eq!(event.flags, 1);
        assert_eq!(event.gtid, "8f555992-d370-11f1-a239-0242ac110002:1");
    }

    #[test]
    fn test_tagged_rejects_corrupt_messages() {
        let original = tagged_identity();
        for length in 0..original.len() {
            let mut body = original[..length].to_vec();
            if length > 1 {
                body[1] = (length * 2) as u8;
            }
            assert!(
                GtidEvent::parse_tagged(&mut Cursor::new(body)).is_err(),
                "length {}",
                length
            );
        }
        for (offset, value) in [
            (0, 4), // unknown version
            (1, 0),
            (1, 0xfe), // invalid message size
            (2, 26),   // unknown non-ignorable field 12
            (3, 2),
            (28, 6),   // missing identity field
            (7, 4),    // UUID element 271 does not fit u8
            (31, 66),  // tag length 33
            (32, 255), // invalid UTF-8
        ] {
            let mut body = original.clone();
            body[offset] = value;
            assert!(
                GtidEvent::parse_tagged(&mut Cursor::new(body)).is_err(),
                "offset {}",
                offset
            );
        }
        let mut body = original;
        body.extend([8, 0, 8, 0]); // duplicate field
        body[1] = (body.len() * 2) as u8;
        assert!(GtidEvent::parse_tagged(&mut Cursor::new(body)).is_err());
    }
}
