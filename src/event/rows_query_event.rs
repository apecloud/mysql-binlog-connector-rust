use serde::{Deserialize, Serialize};

use std::io::{Cursor, Seek, SeekFrom};

use crate::{binlog_error::BinlogError, ext::cursor_ext::CursorExt};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RowsQueryEvent {
    pub query: String,
}

impl RowsQueryEvent {
    pub fn parse(cursor: &mut Cursor<&Vec<u8>>) -> Result<Self, BinlogError> {
        // refer: https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Rows__query__event.html
        // query length is stored using one byte, but it is ignored and the left bytes contain the full query
        cursor.seek(SeekFrom::Current(1))?;

        let query = cursor.read_string(cursor.get_ref().len() - 1)?;
        Ok(Self { query })
    }
}
