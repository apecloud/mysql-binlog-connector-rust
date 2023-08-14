use std::io::{Cursor, Read};

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};

use crate::{
    binlog_error::BinlogError, column::column_type::ColumnType, ext::cursor_ext::CursorExt,
};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TableMapEvent {
    pub table_id: u64,
    pub database_name: String,
    pub table_name: String,
    pub column_types: Vec<u8>,
    pub column_metas: Vec<u16>,
    pub null_bits: Vec<bool>,
}

impl TableMapEvent {
    pub fn parse(cursor: &mut Cursor<&Vec<u8>>) -> Result<Self, BinlogError> {
        // refer: https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html
        // table_id
        let table_id = cursor.read_u48::<LittleEndian>()?;

        // flags, Reserved for future use; currently always 0.
        let _flags = cursor.read_u16::<LittleEndian>();

        // database_name
        let database_name_length = cursor.read_u8()?;
        let database_name = cursor.read_string_without_terminator(database_name_length as usize)?;

        // table_name
        let table_name_length = cursor.read_u8()?;
        let table_name = cursor.read_string_without_terminator(table_name_length as usize)?;

        // column_count
        let column_count = cursor.read_packed_number()?;

        // column_types
        let mut column_types = vec![0u8; column_count];
        cursor.read_exact(&mut column_types)?;

        // metadata_length, won't be used
        let _metadata_length = cursor.read_packed_number()?;

        // metadata
        let column_metas = Self::read_metadatas(cursor, &column_types)?;

        // nullable_bits
        let null_bits = cursor.read_bits(column_count, false)?;

        // TODO: table_metadata

        Ok(Self {
            table_id,
            database_name,
            table_name,
            column_types,
            column_metas,
            null_bits,
        })
    }

    fn read_metadatas(
        cursor: &mut Cursor<&Vec<u8>>,
        column_types: &Vec<u8>,
    ) -> Result<Vec<u16>, BinlogError> {
        let mut column_metadatas = Vec::with_capacity(column_types.len());
        for column_type in column_types {
            let column_metadata = match ColumnType::from_code(*column_type) {
                ColumnType::Float
                | ColumnType::Double
                | ColumnType::Blob
                | ColumnType::TinyBlob
                | ColumnType::MediumBlob
                | ColumnType::LongBlob
                | ColumnType::Json
                | ColumnType::Geometry
                | ColumnType::Time2
                | ColumnType::DateTime2
                | ColumnType::TimeStamp2 => cursor.read_u8()? as u16,

                ColumnType::Bit | ColumnType::VarChar | ColumnType::NewDecimal => {
                    cursor.read_u16::<LittleEndian>()?
                }

                ColumnType::Set | ColumnType::Enum | ColumnType::String => {
                    cursor.read_u16::<BigEndian>()?
                }

                _ => 0,
            };
            column_metadatas.push(column_metadata);
        }

        Ok(column_metadatas)
    }
}
