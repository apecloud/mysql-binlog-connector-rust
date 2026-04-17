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
        let table_map_event = table_map_event_by_table_id.get(&table_id).ok_or_else(|| {
            BinlogError::UnexpectedData(format!(
                "missing TableMap event for table_id {table_id} while parsing UpdateRows"
            ))
        })?;

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

#[cfg(test)]
mod tests {
    use super::UpdateRowsEvent;
    use crate::binlog_error::BinlogError;
    use std::{collections::HashMap, io::Cursor};

    #[test]
    fn update_rows_returns_error_when_table_map_is_missing() {
        let payload = vec![1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1];
        let mut cursor = Cursor::new(&payload);
        let err = UpdateRowsEvent::parse(&mut cursor, &mut HashMap::new(), 1)
            .expect_err("missing table map should return an error");

        match err {
            BinlogError::UnexpectedData(message) => {
                assert!(message.contains("missing TableMap event for table_id 1"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
