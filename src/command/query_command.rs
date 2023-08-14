use std::io::Write;

use byteorder::WriteBytesExt;

use crate::binlog_error::BinlogError;

use super::command_type::CommandType;

pub struct QueryCommand {
    pub sql: String,
}

impl QueryCommand {
    pub fn to_bytes(&mut self) -> Result<Vec<u8>, BinlogError> {
        let mut buf = Vec::new();
        buf.write_u8(CommandType::Query as u8)?;
        buf.write_all(self.sql.as_bytes())?;
        Ok(buf)
    }
}
