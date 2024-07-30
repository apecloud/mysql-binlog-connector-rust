use std::io::Write;

use byteorder::{LittleEndian, WriteBytesExt};

use crate::binlog_error::BinlogError;

use super::{command_type::CommandType, gtid_set::GtidSet};

pub struct DumpBinlogGtidCommand {
    pub server_id: u64,
    pub gtid_set: GtidSet,
}

impl DumpBinlogGtidCommand {
    pub fn to_bytes(&mut self) -> Result<Vec<u8>, BinlogError> {
        let mut buf = Vec::new();
        buf.write_u8(CommandType::BinlogDumpGtid as u8)?;

        let binlog_flags = 4;
        buf.write_u16::<LittleEndian>(binlog_flags)?;

        buf.write_u32::<LittleEndian>(self.server_id as u32)?;
        // binlog-filename-len
        buf.write_u32::<LittleEndian>(0)?;
        // binlog-filename, none
        // binlog-pos
        buf.write_u64::<LittleEndian>(4)?;

        let mut data_size = 8; // number of uuid_sets
        for uuid_set in self.gtid_set.map.values() {
            data_size += 16; // uuid
            data_size += 8; // number of intervals
            data_size += uuid_set.intervals.len() * 16; // start to end
        }
        buf.write_u32::<LittleEndian>(data_size as u32)?;

        buf.write_u64::<LittleEndian>(self.gtid_set.map.len() as u64)?;
        for (uuid, uuid_set) in self.gtid_set.map.iter() {
            let uuid_bytes = Self::hex_to_byte_array(&uuid.replace('-', ""))?;
            buf.write_all(&uuid_bytes)?;

            // intervals
            buf.write_u64::<LittleEndian>(uuid_set.intervals.len() as u64)?;
            for interval in &uuid_set.intervals {
                buf.write_u64::<LittleEndian>(interval.start)?;
                buf.write_u64::<LittleEndian>(interval.end + 1)?; // right-open
            }
        }

        Ok(buf)
    }

    fn hex_to_byte_array(uuid: &str) -> Result<Vec<u8>, BinlogError> {
        let mut bytes = Vec::with_capacity(uuid.len() / 2);
        for i in (0..uuid.len()).step_by(2) {
            let hex_byte = &uuid[i..i + 2];
            bytes.push(u8::from_str_radix(hex_byte, 16)?);
        }
        Ok(bytes)
    }
}
