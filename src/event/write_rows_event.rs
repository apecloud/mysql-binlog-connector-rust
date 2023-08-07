use std::{collections::HashMap, io::Cursor};

use serde::{Deserialize, Serialize};

use crate::{binlog_error::BinlogError, ext::cursor_ext::CursorExt};

use super::{row_event::RowEvent, table_map_event::TableMapEvent};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct WriteRowsEvent {
    pub table_id: u64,
    pub included_columns: Vec<bool>,
    pub rows: Vec<RowEvent>,
}

impl WriteRowsEvent {
    pub fn parse(
        cursor: &mut Cursor<&Vec<u8>>,
        table_map_event_by_table_id: &mut HashMap<u64, TableMapEvent>,
        row_event_version: u8,
    ) -> Result<Self, BinlogError> {
        // refer: https://mariadb.com/kb/en/rows_event_v1v2-rows_compressed_event_v1/
        let (table_id, _column_count, included_columns) =
            cursor.parse_rows_event_common_header(row_event_version)?;
        let table_map_event = table_map_event_by_table_id.get(&table_id).unwrap();

        let mut rows: Vec<RowEvent> = Vec::new();
        while cursor.available() > 0 {
            let row = RowEvent::parse(cursor, table_map_event, &included_columns)?;
            rows.push(row);
        }

        Ok(Self {
            table_id,
            included_columns,
            rows,
        })
    }
}
