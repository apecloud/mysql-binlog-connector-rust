use super::gtid_event::GtidEvent;
use super::gtid_serialization::read_tag;
use crate::binlog_error::BinlogError;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};
use std::io::Cursor;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PreviousGtidsEvent {
    pub gtid_set: String,
}

impl PreviousGtidsEvent {
    pub fn parse(cursor: &mut Cursor<impl AsRef<[u8]>>) -> Result<Self, BinlogError> {
        let header = cursor.read_u64::<LittleEndian>()?;
        let tagged = match header >> 56 {
            0 => false,
            1 if header & 0xff == 1 => true,
            _ => {
                return Err(BinlogError::UnexpectedData(
                    "unknown PreviousGtids format".into(),
                ))
            }
        };
        let uuid_count = if tagged {
            (header & 0x00ff_ffff_ffff_ff00) >> 8
        } else {
            header
        };
        // A TSID needs at least a UUID, interval count and (if tagged) tag length.
        Self::check_count(cursor, uuid_count, if tagged { 25 } else { 24 })?;
        let mut gtids: Vec<String> = Vec::with_capacity(uuid_count as usize);

        for _ in 0..uuid_count {
            let uuid = GtidEvent::read_uuid(cursor)?;
            let tag = if tagged {
                read_tag(cursor)?
            } else {
                String::new()
            };
            let tsid = if tag.is_empty() {
                uuid
            } else {
                format!("{}:{}", uuid, tag)
            };
            let intervals = Self::read_interval(cursor)?;
            gtids.push(format!("{}:{}", tsid, intervals));
        }

        Ok(Self {
            gtid_set: gtids.join(","),
        })
    }

    fn read_interval(cursor: &mut Cursor<impl AsRef<[u8]>>) -> Result<String, BinlogError> {
        let interval_count = cursor.read_u64::<LittleEndian>()?;
        Self::check_count(cursor, interval_count, 16)?;
        let mut intervals = Vec::with_capacity(interval_count as usize);

        for _ in 0..interval_count {
            let start = cursor.read_u64::<LittleEndian>()?;
            let end = cursor.read_u64::<LittleEndian>()?;
            if start == 0 || end <= start {
                return Err(BinlogError::UnexpectedData(
                    "invalid PreviousGtids interval".into(),
                ));
            }
            // mysql "show binlog events in 'mysql-bin.000005'" returns:
            // "58cf6502-63db-11ed-8079-0242ac110002:1-8" while we get interval_start = 1, interval_end = 9
            intervals.push(format!("{}-{}", start, end - 1));
        }

        Ok(intervals.join(":"))
    }

    fn check_count(
        cursor: &Cursor<impl AsRef<[u8]>>,
        count: u64,
        entry_size: u64,
    ) -> Result<(), BinlogError> {
        let remaining = cursor.get_ref().as_ref().len() as u64 - cursor.position();
        if count > remaining / entry_size {
            return Err(BinlogError::UnexpectedData(
                "PreviousGtids count exceeds event body".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_empty_previous_gtids_formats() {
        for header in [0u64, 0x0100_0000_0000_0001] {
            assert_eq!(
                PreviousGtidsEvent::parse(&mut Cursor::new(header.to_le_bytes()))
                    .unwrap()
                    .gtid_set,
                ""
            );
        }
    }

    #[test]
    fn test_legacy_multiple_uuids_and_intervals() {
        let mut body = 2u64.to_le_bytes().to_vec();
        for uuid in [[0; 16], [0xff; 16]] {
            body.extend(uuid);
            for value in [2u64, 1, 3, 5, 6] {
                body.extend(value.to_le_bytes());
            }
        }
        assert_eq!(PreviousGtidsEvent::parse(&mut Cursor::new(body)).unwrap().gtid_set,
            "00000000-0000-0000-0000-000000000000:1-2:5-5,ffffffff-ffff-ffff-ffff-ffffffffffff:1-2:5-5");
    }

    #[test]
    fn test_previous_gtids_rejects_corrupt_bodies() {
        let file = include_bytes!("../../tests/parse_file_tests/mysql-bin.000084-compressed");
        let body = &file[127 + 19..362 - 4];
        for end in 0..body.len() {
            assert!(
                PreviousGtidsEvent::parse(&mut Cursor::new(&body[..end])).is_err(),
                "length {}",
                end
            );
        }
        for header in [
            u64::MAX,
            0x0200_0000_0000_0000,
            0x0100_0000_0000_0002,
            0x00ff_ffff_ffff_ffff,
        ] {
            assert!(PreviousGtidsEvent::parse(&mut Cursor::new(header.to_le_bytes())).is_err());
        }
        for (start, end) in [(0u64, 1u64), (1, 0), (2, 2), (3, 2)] {
            let mut body = 1u64.to_le_bytes().to_vec();
            body.extend([0; 16]);
            for value in [1u64, start, end] {
                body.extend(value.to_le_bytes());
            }
            assert!(PreviousGtidsEvent::parse(&mut Cursor::new(body)).is_err());
        }
    }
}
