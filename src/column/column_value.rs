use super::column_type::ColumnType;
use crate::{binlog_error::BinlogError, ext::cursor_ext::CursorExt};
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Read};

#[derive(Debug, Deserialize, Serialize)]
pub enum ColumnValue {
    // A 8 bit signed integer
    Tiny(i8),
    // A 8 bit unsigned integer
    UnsignedTiny(u8),
    // A 16 bit signed integer
    Short(i16),
    // A 16 bit unsigned integer
    UnsignedShort(u16),
    // A 32 bit signed integer
    Long(i32),
    // A 32 bit unsigned integer
    UnsignedLong(u32),
    // A 64 bit signed integer
    LongLong(i64),
    // A 64 bit unsigned integer
    UnsignedLongLong(u64),
    // A 32 bit floating point number
    Float(f32),
    // A 64 bit floating point number
    Double(f64),
    // A decimal value
    Decimal(String),
    // A datatype to store a time value
    Time(String),
    // A datatype to store a date value
    Date(String),
    // A datatype containing timestamp values ranging from
    // '1000-01-01 00:00:00' to '9999-12-31 23:59:59'.
    DateTime(String),
    // A datatype containing timestamp values ranging from
    // 1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07' UTC.
    Timestamp(u64),
    // A datatype to store year with a range of 1901 to 2155,
    // refer: https://dev.mysql.com/doc/refman/8.0/en/year.html
    Year(u16),
    // A datatype for string values
    String(Vec<u8>),
    // A datatype containing binary large objects
    Blob(Vec<u8>),
    // A datatype containing a set of bit
    Bit(Vec<bool>),
    // A user defined set type
    // refer: https://dev.mysql.com/doc/refman/8.0/en/set.html
    // A SET column can have a maximum of 64 distinct members.
    Set(u64),
    // A user defined enum type
    // refer: https://dev.mysql.com/doc/refman/8.0/en/enum.html
    // An ENUM column can have a maximum of 65,535 distinct elements.
    Enum(u32),
    // TODO
    Json(Vec<u8>),
}

const DIG_PER_DEC: usize = 9;
const COMPRESSED_BYTES: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];

impl ColumnValue {
    // refer: https://mariadb.com/kb/en/rows_event_v1v2-rows_compressed_event_v1/
    pub fn parse(
        cursor: &mut Cursor<&Vec<u8>>,
        column_type: ColumnType,
        column_meta: u16,
        column_length: u16,
    ) -> Result<Self, BinlogError> {
        let value = match column_type {
            ColumnType::Bit => ColumnValue::Bit(Self::parse_bit(cursor, column_meta)?),

            ColumnType::Tiny => ColumnValue::Tiny(cursor.read_i8()?),

            ColumnType::Short => ColumnValue::Short(cursor.read_i16::<LittleEndian>()?),

            ColumnType::Int24 => ColumnValue::Long(cursor.read_i24::<LittleEndian>()?),

            ColumnType::Long => ColumnValue::Long(cursor.read_i32::<LittleEndian>()?),

            ColumnType::LongLong => ColumnValue::LongLong(cursor.read_i64::<LittleEndian>()?),

            ColumnType::Float => ColumnValue::Float(cursor.read_f32::<LittleEndian>()?),

            ColumnType::Double => ColumnValue::Double(cursor.read_f64::<LittleEndian>()?),

            ColumnType::NewDecimal => {
                ColumnValue::Decimal(Self::parse_decimal(cursor, column_meta)?)
            }

            ColumnType::Date => ColumnValue::Date(Self::parse_date(cursor)?),

            ColumnType::Time => ColumnValue::Time(Self::parse_time(cursor)?),

            ColumnType::Time2 => ColumnValue::Time(Self::parse_time2(cursor, column_meta)?),

            ColumnType::TimeStamp => ColumnValue::Timestamp(Self::parse_timestamp(cursor)?),

            ColumnType::TimeStamp2 => {
                ColumnValue::Timestamp(Self::parse_timestamp2(cursor, column_meta)?)
            }

            ColumnType::DateTime => ColumnValue::DateTime(Self::parse_datetime(cursor)?),

            ColumnType::DateTime2 => {
                ColumnValue::DateTime(Self::parse_datetime2(cursor, column_meta)?)
            }

            ColumnType::Year => ColumnValue::Year(cursor.read_u8()? as u16 + 1900),

            ColumnType::VarChar | ColumnType::VarString => {
                ColumnValue::String(Self::parse_string(cursor, column_meta)?)
            }

            ColumnType::String => ColumnValue::String(Self::parse_string(cursor, column_length)?),

            ColumnType::Blob
            | ColumnType::Geometry
            | ColumnType::TinyBlob
            | ColumnType::MediumBlob
            | ColumnType::LongBlob => ColumnValue::Blob(Self::parse_blob(cursor, column_meta)?),

            ColumnType::Enum => {
                ColumnValue::Enum(cursor.read_int::<LittleEndian>(column_length as usize)? as u32)
            }

            ColumnType::Set => {
                ColumnValue::Set(cursor.read_int::<LittleEndian>(column_length as usize)? as u64)
            }

            ColumnType::Json => ColumnValue::Blob(Self::parse_blob(cursor, column_meta)?),

            _ => {
                return Err(BinlogError::UnsupportedColumnType {
                    error: format!("{:?}", column_type),
                })
            }
        };

        Ok(value)
    }

    fn parse_bit(
        cursor: &mut Cursor<&Vec<u8>>,
        column_meta: u16,
    ) -> Result<Vec<bool>, BinlogError> {
        let bit_count = (column_meta >> 8) * 8 + (column_meta & 0xFF);
        cursor.read_bits(bit_count as usize, true)
    }

    fn parse_date(cursor: &mut Cursor<&Vec<u8>>) -> Result<String, BinlogError> {
        // Stored as a 3 byte value where bits 1 to 5 store the day,
        // bits 6 to 9 store the month and the remaining bits store the year.
        let date_val = cursor.read_u24::<LittleEndian>()?;
        let day = date_val % 32;
        let month = (date_val >> 5) % 16;
        let year = date_val >> 9;
        Ok(format!("{}-{:02}-{:02}", year, month, day))
    }

    fn parse_time(cursor: &mut Cursor<&Vec<u8>>) -> Result<String, BinlogError> {
        // refer: https://dev.mysql.com/doc/refman/8.0/en/time.html
        let time_val = cursor.read_u24::<LittleEndian>()?;
        let hour = (time_val / 100) / 100;
        let minute = (time_val / 100) % 100;
        let second = time_val % 100;
        Ok(format!("{:02}:{:02}:{:02}", hour, minute, second))
    }

    fn parse_time2(cursor: &mut Cursor<&Vec<u8>>, column_meta: u16) -> Result<String, BinlogError> {
        // Stored as 3-byte value,
        // The number of decimals for the fractional part is stored in the table metadata
        // as a one byte value. The number of bytes that follow the 3 byte time
        // value can be calculated with the following formula: (decimals + 1) / 2
        let time_val = cursor.read_u24::<BigEndian>()?;
        let hour = (time_val >> 12) % (1 << 10);
        let minute = (time_val >> 6) % (1 << 6);
        let second = time_val % (1 << 6);
        let micros = Self::parse_fraction(cursor, column_meta)?;
        Ok(format!(
            "{:02}:{:02}:{:02}.{:06}",
            hour, minute, second, micros
        ))
    }

    fn parse_fraction(cursor: &mut Cursor<&Vec<u8>>, column_meta: u16) -> Result<u32, BinlogError> {
        let mut fraction = 0;
        let length = ((column_meta + 1) / 2) as u32;
        if length > 0 {
            fraction = cursor.read_uint::<BigEndian>(length as usize)?;
            fraction = fraction * u64::pow(100, 3 - length);
        }
        Ok(fraction as u32)
    }

    fn parse_timestamp(cursor: &mut Cursor<&Vec<u8>>) -> Result<u64, BinlogError> {
        // Stored as a 4 byte UNIX timestamp (number of seconds since 00:00, Jan 1 1970 UTC).
        Ok((cursor.read_u32::<LittleEndian>()? * 1000) as u64)
    }

    fn parse_timestamp2(
        cursor: &mut Cursor<&Vec<u8>>,
        column_meta: u16,
    ) -> Result<u64, BinlogError> {
        let second = cursor.read_u32::<BigEndian>()?;
        let micros = Self::parse_fraction(cursor, column_meta)?;
        Ok(1000000 * second as u64 + micros as u64)
    }

    fn parse_datetime(cursor: &mut Cursor<&Vec<u8>>) -> Result<String, BinlogError> {
        let datetime_val = cursor.read_u64::<LittleEndian>()? * 1000;
        let date_val = datetime_val / 1000000;
        let time_val = datetime_val % 1000000;
        let year = ((date_val / 100) / 100) as u32;
        let month = ((date_val / 100) % 100) as u32;
        let day = (date_val % 100) as u32;
        let hour = ((time_val / 100) / 100) as u32;
        let minute = ((time_val / 100) % 100) as u32;
        let second = (time_val % 100) as u32;
        Ok(format!(
            "{}-{:02}-{:02} {:02}:{:02}:{:02}",
            year, month, day, hour, minute, second,
        ))
    }

    fn parse_datetime2(
        cursor: &mut Cursor<&Vec<u8>>,
        column_meta: u16,
    ) -> Result<String, BinlogError> {
        // Stored as 4-byte value,
        // The number of decimals for the fractional part is stored in the table metadata as a one byte value.
        // The number of bytes that follow the 5 byte datetime value can be calculated
        // with the following formula: (decimals + 1) / 2
        let val = cursor.read_uint::<BigEndian>(5)? - 0x8000000000;
        let micros = Self::parse_fraction(cursor, column_meta)?;
        let d_val = val >> 17;
        let t_val = val % (1 << 17);
        let year = ((d_val >> 5) / 13) as u32;
        let month = ((d_val >> 5) % 13) as u32;
        let day = (d_val % (1 << 5)) as u32;
        let hour = ((val >> 12) % (1 << 5)) as u32;
        let minute = ((t_val >> 6) % (1 << 6)) as u32;
        let second = (t_val % (1 << 6)) as u32;
        Ok(format!(
            "{}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
            year, month, day, hour, minute, second, micros,
        ))
    }

    pub fn parse_string(
        cursor: &mut Cursor<&Vec<u8>>,
        column_meta: u16,
    ) -> Result<Vec<u8>, BinlogError> {
        let size = if column_meta < 256 {
            cursor.read_u8()? as usize
        } else {
            cursor.read_u16::<LittleEndian>()? as usize
        };
        // charset is not present in the binary log, return byte[] instead of an actual String
        cursor.read_bytes(size)
    }

    pub fn parse_blob(
        cursor: &mut Cursor<&Vec<u8>>,
        column_meta: u16,
    ) -> Result<Vec<u8>, BinlogError> {
        let size = cursor.read_uint::<LittleEndian>(column_meta as usize)? as usize;
        cursor.read_bytes(size)
    }

    pub fn parse_decimal(
        cursor: &mut Cursor<&Vec<u8>>,
        column_meta: u16,
    ) -> Result<String, BinlogError> {
        // Given a column to be DECIMAL(13,4), the numbers mean:
        // 13: precision, the maximum number of digits, the maximum precesion for DECIMAL is 65.
        // 4: scale, the number of digits to the right of the decimal point.
        // 13 - 4: integral, the maximum number of digits to the left of the decimal point.
        let precision = (column_meta & 0xFF) as usize;
        let scale = (column_meta >> 8) as usize;
        let integral = precision - scale;

        // A decimal is stored in binlog like following:
        // ([compressed bytes, 1-4]) ([fixed bytes: 4] * n) . ([fixed bytes: 4] * n) ([compressed bytes, 1-4])
        // Both integral and scale are stored in BigEndian.
        // refer: https://github.com/mysql/mysql-server/blob/8.0/strings/decimal.cc#L1488
        // Examples:
        // DECIMAL(10,4): [3 bytes] . [2 bytes]
        // DECIMAL(18,9): [4 bytes] . [4 bytes]
        // DECIMAL(27,13): [3 bytes][4 bytes] . [4 bytes][2 bytes]
        // DECIMAL(47,25): [2 bytes][4 bytes][4 bytes] . [4 bytes][4 bytes][4 bytes]
        // DIG_PER_DEC = 9: each 4 bytes represent 9 digits in a decimal number.
        // COMPRESSED_BYTES = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4]: bytes needed to compress n digits.
        let uncomp_intg = integral / DIG_PER_DEC;
        let uncomp_frac = scale / DIG_PER_DEC;
        let comp_intg = integral - (uncomp_intg * DIG_PER_DEC);
        let comp_frac = scale - (uncomp_frac * DIG_PER_DEC);

        let comp_frac_bytes = COMPRESSED_BYTES[comp_frac];
        let comp_intg_bytes = COMPRESSED_BYTES[comp_intg];

        let total_bytes = 4 * uncomp_intg + 4 * uncomp_frac + comp_frac_bytes + comp_intg_bytes;
        let mut buf = vec![0u8; total_bytes];
        cursor.read_exact(&mut buf)?;

        // handle negative
        let is_negative = (buf[0] & 0x80) == 0;
        buf[0] ^= 0x80;
        if is_negative {
            for i in 0..buf.len() {
                buf[i] ^= 0xFF;
            }
        }

        // negative sign
        let mut intg_str = String::new();
        if is_negative {
            intg_str = "-".to_string();
        }

        let mut decimal_cursor = Cursor::new(buf);
        let mut is_intg_empty = true;
        // compressed integral
        if comp_intg_bytes > 0 {
            let value = decimal_cursor.read_uint::<BigEndian>(comp_intg_bytes)?;
            if value > 0 {
                intg_str += value.to_string().as_str();
                is_intg_empty = false;
            }
        }

        // uncompressed integral
        for _ in 0..uncomp_intg {
            let value = decimal_cursor.read_u32::<BigEndian>()?;
            if is_intg_empty {
                if value > 0 {
                    intg_str += value.to_string().as_str();
                    is_intg_empty = false;
                }
            } else {
                intg_str += format!("{value:0size$}", value = value, size = DIG_PER_DEC).as_str();
            }
        }

        if is_intg_empty {
            intg_str += "0";
        }

        let mut frac_str = String::new();
        // uncompressed fractional
        for _ in 0..uncomp_frac {
            let value = decimal_cursor.read_u32::<BigEndian>()?;
            frac_str += format!("{value:0size$}", value = value, size = DIG_PER_DEC).as_str();
        }

        // compressed fractional
        if comp_frac_bytes > 0 {
            let value = decimal_cursor.read_uint::<BigEndian>(comp_frac_bytes)?;
            frac_str += format!("{value:0size$}", value = value, size = comp_frac).as_str();
        }

        if frac_str.is_empty() {
            Ok(intg_str)
        } else {
            Ok(intg_str + "." + frac_str.as_str())
        }
    }
}
