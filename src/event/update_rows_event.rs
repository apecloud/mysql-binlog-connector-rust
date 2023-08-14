use std::{collections::HashMap, io::Cursor};

use serde::{Deserialize, Serialize};

use crate::{binlog_error::BinlogError, ext::cursor_ext::CursorExt};

use super::{event_header::EventHeader, row_event::RowEvent, table_map_event::TableMapEvent};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct UpdateRowsEvent {
    pub table_id: u64,
    pub included_columns_before: Vec<bool>,
    pub included_columns_after: Vec<bool>,
    pub rows: Vec<(RowEvent, RowEvent)>,
}

impl UpdateRowsEvent {
    pub fn parse(
        cursor: &mut Cursor<&Vec<u8>>,
        table_map_event_by_table_id: &mut HashMap<u64, TableMapEvent>,
        row_event_version: u8,
    ) -> Result<Self, BinlogError> {
        let (table_id, column_count, included_columns_before) =
            EventHeader::parse_rows_event_common_header(cursor, row_event_version)?;
        let included_columns_after = cursor.read_bits(column_count, false)?;
        let table_map_event = table_map_event_by_table_id.get(&table_id).unwrap();

        let mut rows: Vec<(RowEvent, RowEvent)> = Vec::new();
        while cursor.available() > 0 {
            let before = RowEvent::parse(cursor, table_map_event, &included_columns_before)?;
            let after = RowEvent::parse(cursor, table_map_event, &included_columns_after)?;
            rows.push((before, after));
        }

        Ok(Self {
            table_id,
            included_columns_before,
            included_columns_after,
            rows,
        })
    }
}
