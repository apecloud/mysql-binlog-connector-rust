use num_enum::{IntoPrimitive, TryFromPrimitive};

use serde::{Deserialize, Serialize};

use crate::binlog_error::BinlogError;

/**
 * Refer to: https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html#Table_table_map_event_column_types
 */
#[derive(Debug, Deserialize, Serialize, Clone, IntoPrimitive, TryFromPrimitive)]
#[repr(i16)]
pub enum ColumnType {
    #[num_enum(default)]
    Unkown = -1,
    Decimal = 0,
    Tiny = 1,
    Short = 2,
    Long = 3,
    Float = 4,
    Double = 5,
    Null = 6,
    TimeStamp = 7,
    LongLong = 8,
    Int24 = 9,
    Date = 10,
    Time = 11,
    DateTime = 12,
    Year = 13,
    NewDate = 14,
    VarChar = 15,
    Bit = 16,
    TimeStamp2 = 17,
    DateTime2 = 18,
    Time2 = 19,
    Json = 245,
    NewDecimal = 246,
    Enum = 247,
    Set = 248,
    TinyBlob = 249,
    MediumBlob = 250,
    LongBlob = 251,
    Blob = 252,
    VarString = 253,
    String = 254,
    Geometry = 255,
}

impl ColumnType {
    pub fn from_code(code: u8) -> ColumnType {
        ColumnType::try_from(code as i16).unwrap()
    }

    /**
     * The column type of MYSQL_TYPE_STRING and MYSQL_TYPE_ENUM are String in binlog, we need to get
     * the real column type for parsing column values.
     * Refer to: https://github.com/mysql/mysql-server/blob/5.7/sql/log_event.cc#L2047
     */
    pub fn parse_string_column_meta(
        column_meta: u16,
        column_type: u8,
    ) -> Result<(u8, u16), BinlogError> {
        let mut real_column_type = column_type;
        let mut column_length = column_meta;

        if column_type == ColumnType::String as u8 && column_meta >= 256 {
            let byte0 = column_meta >> 8;
            let byte1 = column_meta & 0xFF;
            if (byte0 & 0x30) != 0x30 {
                real_column_type = (byte0 | 0x30) as u8;
                column_length = byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
            } else {
                if byte0 == ColumnType::Enum as u16 || byte0 == ColumnType::Set as u16 {
                    real_column_type = byte0 as u8;
                }
                column_length = byte1;
            }
        }

        Ok((real_column_type, column_length))
    }
}
