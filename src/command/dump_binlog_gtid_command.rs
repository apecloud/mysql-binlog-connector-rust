use std::io::Write;

use byteorder::{LittleEndian, WriteBytesExt};

use crate::binlog_error::BinlogError;

use super::{
    command_type::CommandType,
    gtid_set::{GtidSet, UuidSet},
};

pub struct DumpBinlogGtidCommand {
    pub server_id: u64,
    pub gtid_set: GtidSet,
}

impl DumpBinlogGtidCommand {
    fn encode_request(server_id: u64, data: &[u8]) -> Result<Vec<u8>, BinlogError> {
        let mut buf = Vec::new();
        buf.write_u8(CommandType::BinlogDumpGtid as u8)?;

        // BINLOG_DUMP_NEVER_STOP           = 0x00
        // BINLOG_DUMP_NON_BLOCK            = 0x01
        // BINLOG_SEND_ANNOTATE_ROWS_EVENT  = 0x02
        // BINLOG_THROUGH_POSITION          = 0x02
        // BINLOG_THROUGH_GTID              = 0x04
        let binlog_flags = 4;
        buf.write_u16::<LittleEndian>(binlog_flags)?;

        buf.write_u32::<LittleEndian>(server_id as u32)?;
        // binlog-filename-len
        buf.write_u32::<LittleEndian>(0)?;
        // binlog-filename, none
        // binlog-pos
        buf.write_u64::<LittleEndian>(4)?;

        let data_size = u32::try_from(data.len())
            .map_err(|_| BinlogError::UnexpectedData("GTID request exceeds u32 length".into()))?;
        buf.write_u32::<LittleEndian>(data_size)?;
        buf.write_all(data)?;
        Ok(buf)
    }

    pub fn to_bytes(&mut self) -> Result<Vec<u8>, BinlogError> {
        let mut entries: Vec<_> = self.gtid_set.map.values().collect();
        let tagged = entries.iter().any(|set| set.uuid.contains(':'));
        if tagged {
            entries.sort_unstable_by(|a, b| a.uuid.cmp(&b.uuid));
        }
        let mut data = Vec::new();
        let count = entries.len() as u64;
        // Format 1 includes a tag string for every TSID, including empty tags.
        // https://github.com/mysql/mysql-server/blob/mysql-8.4.0/sql/rpl_gtid_set.cc#L1341-L1425
        let header = if tagged {
            (1 << 56) | (count << 8) | 1
        } else {
            count
        };
        data.write_u64::<LittleEndian>(header)?;
        for set in entries {
            let (uuid, tag) = UuidSet::source_id_parts(&set.uuid)?;
            let hex = uuid.replace('-', "");
            for index in (0..32).step_by(2) {
                data.write_u8(u8::from_str_radix(&hex[index..index + 2], 16)?)?;
            }
            if tagged {
                let tag = tag.unwrap_or("");
                // A tag is at most 32 bytes: its serialization length fits in one byte.
                // https://github.com/mysql/mysql-server/blob/mysql-8.4.0/libs/mysql/gtid/tag.cpp#L65-L73
                data.write_u8((tag.len() as u8) << 1)?;
                data.write_all(tag.as_bytes())?;
            }
            data.write_u64::<LittleEndian>(set.intervals.len() as u64)?;
            for interval in &set.intervals {
                if interval.end < interval.start
                    || (tagged && (interval.start == 0 || interval.end >= i64::MAX as u64))
                {
                    return Err(BinlogError::InvalidGtid(set.to_string()));
                }
                data.write_u64::<LittleEndian>(interval.start)?;
                data.write_u64::<LittleEndian>(
                    interval
                        .end
                        .checked_add(1)
                        .ok_or_else(|| BinlogError::InvalidGtid(set.to_string()))?,
                )?;
            }
        }
        Self::encode_request(self.server_id, &data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::previous_gtids_event::PreviousGtidsEvent;
    use std::io::Cursor;

    fn encode(server_id: u64, text: &str) -> Result<Vec<u8>, BinlogError> {
        DumpBinlogGtidCommand {
            server_id,
            gtid_set: GtidSet::new(text)?,
        }
        .to_bytes()
    }

    const UUID: &str = "8f555992-d370-11f1-a239-0242ac110002";

    #[test]
    fn test_tagged_request_matches_mysql84_encoding() {
        let set = format!(
            "{}:1-2:abcdefghijklmnopqrstuvwxyz_12345:1:admin:1:biz:1",
            UUID
        );
        let packet = encode(84, &set).unwrap();
        // Independent golden bytes: mixed PreviousGtids written by MySQL 8.4.13.
        let file = include_bytes!("../../tests/parse_file_tests/mysql-bin.000084-compressed");
        assert_eq!(&packet[23..], &file[127 + 19..362 - 4]);
        let history =
            PreviousGtidsEvent::parse(&mut Cursor::new(&file[127 + 19..362 - 4])).unwrap();
        assert_eq!(encode(84, &history.gtid_set).unwrap(), packet);
        assert_eq!(
            &packet[..19],
            &[30, 4, 0, 84, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            u32::from_le_bytes(packet[19..23].try_into().unwrap()) as usize,
            packet.len() - 23
        );
    }

    #[test]
    fn test_untagged_request_keeps_legacy_bytes() {
        let packet = encode(7, "00000000-0000-0000-0000-000000000000:1-2").unwrap();
        assert_eq!(
            &packet[..23],
            &[30, 4, 0, 7, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 48, 0, 0, 0]
        );
        assert_eq!(&packet[23..31], &1u64.to_le_bytes());
        assert_eq!(&packet[31..47], &[0; 16]);
        assert_eq!(&packet[47..55], &1u64.to_le_bytes());
        assert_eq!(&packet[55..63], &1u64.to_le_bytes());
        assert_eq!(&packet[63..], &3u64.to_le_bytes());
        let empty = encode(7, "").unwrap();
        assert_eq!(empty.len(), 31);
        assert_eq!(&empty[19..23], &8u32.to_le_bytes());
        assert_eq!(&empty[23..], &0u64.to_le_bytes());
        let zero = encode(7, "00000000-0000-0000-0000-000000000000:0").unwrap();
        assert_eq!(&zero[55..63], &0u64.to_le_bytes());
        assert_eq!(&zero[63..], &1u64.to_le_bytes());
    }

    #[test]
    fn test_tagged_request_merges_only_matching_tsids() {
        let text = format!("{0}:Admin:5-7:1-3,{0}:ADMIN:3-6,{0}:biz:1", UUID);
        let packet = encode(7, &text).unwrap();
        let set = PreviousGtidsEvent::parse(&mut Cursor::new(&packet[23..])).unwrap();
        assert_eq!(set.gtid_set, format!("{0}:admin:1-7,{0}:biz:1-1", UUID));
    }
}
