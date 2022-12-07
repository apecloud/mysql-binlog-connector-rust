use std::io::Cursor;

use serde::{Deserialize, Serialize};

use crate::{
    binlog_error::BinlogError,
    column::{column_type::ColumnType, column_value::ColumnValue},
    ext::cursor_ext::CursorExt,
};

use super::table_map_event::TableMapEvent;

#[derive(Debug, Deserialize, Serialize)]
pub struct RowEvent {
    pub column_values: Vec<Option<ColumnValue>>,
}

impl RowEvent {
    pub fn parse(
        cursor: &mut Cursor<&Vec<u8>>,
        table_map_event: &TableMapEvent,
        included_columns: &Vec<bool>,
    ) -> Result<Self, BinlogError> {
        let null_columns = cursor.read_bits(included_columns.len(), false)?;
        let mut column_values = Vec::with_capacity(table_map_event.column_types.len());
        let mut skipped_column_count = 0;
        for i in 0..table_map_event.column_types.len() {
            if !included_columns[i] {
                skipped_column_count += 1;
                column_values.push(None);
                continue;
            }

            let index = i - skipped_column_count;
            if null_columns[index] {
                column_values.push(None);
                continue;
            }

            let column_meta = table_map_event.column_metas[i];
            let mut column_type = table_map_event.column_types[i];
            let mut column_length = column_meta;

            if column_type == ColumnType::String as u8 && column_meta >= 256 {
                (column_type, column_length) =
                    ColumnType::parse_string_column_meta(column_meta, column_type)?;
            }

            let col_value = ColumnValue::parse(
                cursor,
                ColumnType::from_code(column_type),
                column_meta,
                column_length,
            )?;
            column_values.push(Some(col_value));
        }

        Ok(Self { column_values })
    }
}
