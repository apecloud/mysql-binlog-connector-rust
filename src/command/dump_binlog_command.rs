use std::io::Write;

use byteorder::{LittleEndian, WriteBytesExt};

use crate::binlog_error::BinlogError;

use super::command_type::CommandType;

pub struct DumpBinlogCommand {
    pub server_id: u64,
    pub binlog_filename: String,
    pub binlog_position: u32,
}

impl DumpBinlogCommand {
    pub fn to_bytes(&mut self) -> Result<Vec<u8>, BinlogError> {
        let mut buf = Vec::new();
        buf.write_u8(CommandType::BinlogDump as u8)?;
        buf.write_u32::<LittleEndian>(self.binlog_position)?;

        let binlog_flags = 0;
        buf.write_u16::<LittleEndian>(binlog_flags)?;

        buf.write_u32::<LittleEndian>(self.server_id as u32)?;
        buf.write_all(self.binlog_filename.as_bytes())?;

        Ok(buf)
    }
}
