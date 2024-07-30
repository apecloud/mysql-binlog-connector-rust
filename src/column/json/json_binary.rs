use crate::{
    binlog_error::BinlogError,
    column::{column_type::ColumnType, column_value::ColumnValue},
    ext::buf_ext::BufExt,
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{Cursor, Read, Seek, SeekFrom};

use super::{
    json_formatter::JsonFormatter, json_string_formatter::JsonStringFormatter,
    value_type::ValueType,
};

// refer: https://github.com/osheroff/mysql-binlog-connector-java/blob/master/src/main/java/com/github/shyiko/mysql/binlog/event/deserialization/json/JsonBinary.java
pub struct JsonBinary<'a> {
    reader: Cursor<&'a [u8]>,
}

impl JsonBinary<'_> {
    pub fn parse_as_string(bytes: &[u8]) -> Result<String, BinlogError> {
        /* check for mariaDB-format JSON strings inside columns marked JSON */
        let is_json_string = bytes[0] > 0x0f;
        if is_json_string {
            return Ok(String::from_utf8_lossy(bytes).to_string());
        }

        let mut formatter = JsonStringFormatter::default();
        Self::parse(bytes, &mut formatter)?;
        Ok(formatter.get_string())
    }

    pub fn parse<F: JsonFormatter>(bytes: &[u8], formatter: &mut F) -> Result<(), BinlogError> {
        let mut binary = JsonBinary {
            reader: Cursor::new(bytes),
        };
        let value_type = binary.read_value_type()?;
        binary.parse_internal(&value_type, formatter)
    }

    fn parse_internal<F: JsonFormatter>(
        &mut self,
        type_: &ValueType,
        formatter: &mut F,
    ) -> Result<(), BinlogError> {
        match type_ {
            ValueType::SmallDocument => self.parse_object(true, false, formatter),
            ValueType::LargeDocument => self.parse_object(false, false, formatter),
            ValueType::SmallArray => self.parse_object(true, true, formatter),
            ValueType::LargeArray => self.parse_object(false, true, formatter),
            ValueType::Literal => self.parse_literal(formatter),
            ValueType::Int16 => self.parse_int16(formatter),
            ValueType::Uint16 => self.parse_uint16(formatter),
            ValueType::Int32 => self.parse_int32(formatter),
            ValueType::Uint32 => self.parse_uint32(formatter),
            ValueType::Int64 => self.parse_int64(formatter),
            ValueType::Uint64 => self.parse_uint64(formatter),
            ValueType::Double => self.parse_double(formatter),
            ValueType::String => self.parse_string(formatter),
            ValueType::Custom => self.parse_opaque(formatter),
        }
    }

    fn parse_object<F: JsonFormatter>(
        &mut self,
        is_small: bool,
        is_array: bool,
        formatter: &mut F,
    ) -> Result<(), BinlogError> {
        let object_offset = self.reader.position();

        // Read the header ...
        let num_elements = self.read_unsigned_index(u32::MAX, is_small, "number of elements in")?;
        let num_bytes = self.read_unsigned_index(u32::MAX, is_small, "size of")?;
        let value_size = if is_small { 2 } else { 4 };

        // Read each key-entry, consisting of the offset and length of each key ...
        let mut keys = Vec::with_capacity(num_elements as usize);

        if !is_array {
            for _i in 0..num_elements {
                keys.push(KeyEntry {
                    index: self.read_unsigned_index(num_bytes, is_small, "key offset in")? as u64,
                    length: self.read_uint16()? as usize,
                    name: String::new(),
                });
            }
        }

        // Read each key value value-entry
        let mut entries = Vec::with_capacity(num_elements as usize);
        for _i in 0..num_elements as usize {
            // Parse the value ...
            let type_ = self.read_value_type()?;
            let entry_value = match type_ {
                ValueType::Literal => {
                    let value = self.read_literal()?;
                    self.reader.seek(SeekFrom::Current(value_size - 1))?;
                    Some(DirectEntryValue::Literal(value))
                }
                ValueType::Int16 => {
                    let value = Some(DirectEntryValue::Numeric(self.read_int16()? as i64));
                    self.reader.seek(SeekFrom::Current(value_size - 2))?;
                    value
                }
                ValueType::Uint16 => {
                    let value = Some(DirectEntryValue::Numeric(self.read_uint16()? as i64));
                    self.reader.seek(SeekFrom::Current(value_size - 2))?;
                    value
                }
                ValueType::Int32 => {
                    if !is_small {
                        Some(DirectEntryValue::Numeric(self.read_int32()? as i64))
                    } else {
                        None
                    }
                }
                ValueType::Uint32 => {
                    if !is_small {
                        Some(DirectEntryValue::Numeric(self.read_uint32()? as i64))
                    } else {
                        None
                    }
                }
                _ => None,
            };

            if entry_value.is_some() {
                entries.push(ValueEntry::new(type_).set_value(entry_value));
            } else {
                // It is an offset, not a value ...
                let index = self.read_unsigned_index(num_bytes, is_small, "value offset in")?;
                entries.push(ValueEntry::new_with_index(type_, index));
            }
        }

        if !is_array {
            // Read each key ...
            for key in keys.iter_mut() {
                let skip_bytes = key.index + object_offset - self.reader.position();
                // Skip to a start of a field name if the current position does not point to it
                // This can happen for MySQL 8
                if skip_bytes != 0 {
                    self.reader.seek(SeekFrom::Current(skip_bytes as i64))?;
                }
                key.name = self.read_as_string(key.length)?;
            }
        }

        if is_array {
            formatter.begin_array(num_elements)
        } else {
            formatter.begin_object(num_elements);
        }

        // Read and parse the values ...
        for i in 0..num_elements as usize {
            if i != 0 {
                formatter.next_entry();
            }

            if !is_array {
                formatter.name(&keys[i].name);
            }

            let entry = &entries[i];
            if entry.resolved {
                if let Some(entry_value) = &entry.value {
                    match entry_value {
                        DirectEntryValue::Literal(value) => {
                            if let Some(bool_value) = value {
                                formatter.value_bool(*bool_value);
                            } else {
                                formatter.value_null();
                            }
                        }
                        DirectEntryValue::Numeric(value) => {
                            formatter.value_long(*value);
                        }
                    }
                } else {
                    formatter.value_null();
                }
            } else {
                // Parse the value ...
                self.reader
                    .seek(SeekFrom::Start(object_offset + entry.index as u64))?;
                self.parse_internal(&entry.value_type, formatter)?;
            }
        }

        if is_array {
            formatter.end_array();
        } else {
            formatter.end_object();
        }

        Ok(())
    }

    pub fn parse_literal<F: JsonFormatter>(
        &mut self,
        formatter: &mut F,
    ) -> Result<(), BinlogError> {
        if let Some(value) = self.read_literal()? {
            formatter.value_bool(value);
        } else {
            formatter.value_null();
        }
        Ok(())
    }

    fn parse_int16<F: JsonFormatter>(&mut self, formatter: &mut F) -> Result<(), BinlogError> {
        let value = self.read_int16()?;
        formatter.value_int(value as i32);
        Ok(())
    }

    fn parse_uint16<F: JsonFormatter>(&mut self, formatter: &mut F) -> Result<(), BinlogError> {
        let value = self.read_uint16()?;
        formatter.value_int(value as i32);
        Ok(())
    }

    fn parse_int32<F: JsonFormatter>(&mut self, formatter: &mut F) -> Result<(), BinlogError> {
        let value = self.read_int32()?;
        formatter.value_int(value);
        Ok(())
    }

    fn parse_uint32<F: JsonFormatter>(&mut self, formatter: &mut F) -> Result<(), BinlogError> {
        let value = self.read_uint32()?;
        formatter.value_long(value as i64);
        Ok(())
    }

    fn parse_int64<F: JsonFormatter>(&mut self, formatter: &mut F) -> Result<(), BinlogError> {
        let value = self.read_int64()?;
        formatter.value_long(value);
        Ok(())
    }

    pub fn parse_uint64<F: JsonFormatter>(&mut self, formatter: &mut F) -> Result<(), BinlogError> {
        let value = self.read_uint64()?;
        formatter.value_big_int(value as i128);
        Ok(())
    }

    pub fn parse_double(&mut self, formatter: &mut dyn JsonFormatter) -> Result<(), BinlogError> {
        let raw_value = self.read_int64()? as u64;
        let value = f64::from_bits(raw_value);
        formatter.value_double(value);
        Ok(())
    }

    fn parse_string<F: JsonFormatter>(&mut self, formatter: &mut F) -> Result<(), BinlogError> {
        let length = self.read_var_int()?;
        let mut bytes = vec![0; length as usize];
        self.reader.read_exact(&mut bytes)?;
        let value = bytes.to_utf8_string();
        formatter.value_string(&value);
        Ok(())
    }

    fn parse_date<F: JsonFormatter>(&mut self, formatter: &mut F) -> Result<(), BinlogError> {
        let raw = self.read_int64()?;
        let value = raw >> 24;
        let year_month = (value >> 22) % (1 << 17);
        let year = year_month / 13;
        let month = year_month % 13;
        let day = (value >> 17) % (1 << 5);
        formatter.value_date(year as i32, month as i32, day as i32);
        Ok(())
    }

    pub fn parse_time<F: JsonFormatter>(&mut self, formatter: &mut F) -> Result<(), BinlogError> {
        let raw = self.read_int64()?;
        let value = raw >> 24;
        let negative = value < 0;
        let hour = (value >> 12) % (1 << 10); // 10 bits starting at 12th
        let min = (value >> 6) % (1 << 6); // 6 bits starting at 6th
        let sec = value % (1 << 6); // 6 bits starting at 0th
        let hour = if negative { -hour } else { hour };
        let micro_seconds = (raw % (1 << 24)) as u32;
        formatter.value_time(hour as i32, min as i32, sec as i32, micro_seconds as i32);
        Ok(())
    }

    fn parse_datetime<F: JsonFormatter>(&mut self, formatter: &mut F) -> Result<(), BinlogError> {
        let raw = self.read_int64()?;
        let value = raw >> 24;
        let year_month = ((value >> 22) % (1 << 17)) as i32; // 17 bits starting at 22nd
        let year = year_month / 13;
        let month = year_month % 13;
        let day = ((value >> 17) % (1 << 5)) as i32; // 5 bits starting at 17th
        let hour = ((value >> 12) % (1 << 5)) as i32; // 5 bits starting at 12th
        let min = ((value >> 6) % (1 << 6)) as i32; // 6 bits starting at 6th
        let sec = (value % (1 << 6)) as i32; // 6 bits starting at 0th
        let micro_seconds = (raw % (1 << 24)) as i32;
        formatter.value_datetime(year, month, day, hour, min, sec, micro_seconds);
        Ok(())
    }

    fn parse_decimal<F: JsonFormatter>(
        &mut self,
        length: usize,
        formatter: &mut F,
    ) -> Result<(), BinlogError> {
        // First two bytes are the precision and scale ...
        let precision = self.reader.read_u8()? as usize;
        let scale = self.reader.read_u8()? as usize;

        // Followed by the binary representation
        let mut buf = vec![0; length - 2];
        self.reader.read_exact(&mut buf)?;
        let mut cursor = Cursor::new(&buf);
        let decimal = ColumnValue::parse_decimal(&mut cursor, precision, scale)?;

        formatter.value_decimal(&decimal);
        Ok(())
    }

    fn parse_opaque_value<F: JsonFormatter>(
        &mut self,
        type_: &ColumnType,
        length: usize,
        formatter: &mut F,
    ) -> Result<(), BinlogError> {
        let mut bytes = vec![0; length];
        self.reader.read_exact(&mut bytes)?;
        formatter.value_opaque(type_, &bytes);
        Ok(())
    }

    pub fn parse_opaque<F: JsonFormatter>(&mut self, formatter: &mut F) -> Result<(), BinlogError> {
        // Read the custom type, which should be a standard ColumnType ...
        let custom_type = self.reader.read_u8()?;
        let type_ = ColumnType::from_code(custom_type);
        // Read the data length ...
        let length = self.read_var_int()? as usize;

        match type_ {
            ColumnType::Decimal | ColumnType::NewDecimal => {
                self.parse_decimal(length, formatter)?
            }
            ColumnType::Date => self.parse_date(formatter)?,
            ColumnType::Time | ColumnType::Time2 => self.parse_time(formatter)?,
            ColumnType::DateTime
            | ColumnType::DateTime2
            | ColumnType::TimeStamp
            | ColumnType::TimeStamp2 => self.parse_datetime(formatter)?,
            _ => self.parse_opaque_value(&type_, length, formatter)?,
        }
        Ok(())
    }

    fn read_unsigned_index(
        &mut self,
        max_value: u32,
        is_small: bool,
        desc: &str,
    ) -> Result<u32, BinlogError> {
        let result = if is_small {
            self.read_uint16()? as u32
        } else {
            self.read_uint32()?
        };

        if result > max_value {
            return Err(BinlogError::ParseJsonError(format!(
                "{}, the JSON document is {} and is too big for the binary form of the document ({})",
                desc,
                result,
                max_value
            )));
        }

        Ok(result)
    }

    fn read_int16(&mut self) -> Result<i16, BinlogError> {
        Ok(self.reader.read_i16::<LittleEndian>()?)
    }

    fn read_uint16(&mut self) -> Result<u16, BinlogError> {
        Ok(self.reader.read_u16::<LittleEndian>()?)
    }

    fn read_int32(&mut self) -> Result<i32, BinlogError> {
        Ok(self.reader.read_i32::<LittleEndian>()?)
    }

    fn read_uint32(&mut self) -> Result<u32, BinlogError> {
        Ok(self.reader.read_u32::<LittleEndian>()?)
    }

    fn read_int64(&mut self) -> Result<i64, BinlogError> {
        Ok(self.reader.read_i64::<LittleEndian>()?)
    }

    fn read_uint64(&mut self) -> Result<u64, BinlogError> {
        Ok(self.reader.read_u64::<LittleEndian>()?)
    }

    fn read_as_string(&mut self, length: usize) -> Result<String, BinlogError> {
        let mut bytes = vec![0; length];
        self.reader.read_exact(&mut bytes)?;
        Ok(bytes.to_utf8_string())
    }

    fn read_var_int(&mut self) -> Result<i32, BinlogError> {
        let mut length: i32 = 0;
        for i in 0..5 {
            let b = self.reader.read_u8()? as i32;
            length |= (b & 0x7F) << (7 * i);
            if (b & 0x80) == 0 {
                return Ok(length);
            }
        }

        Err(BinlogError::ParseJsonError(
            "Unexpected byte sequence".into(),
        ))
    }

    fn read_literal(&mut self) -> Result<Option<bool>, BinlogError> {
        let b = self.reader.read_u8()?;
        match b {
            0x00 => Ok(None),
            0x01 => Ok(Some(true)),
            0x02 => Ok(Some(false)),
            _ => Err(BinlogError::ParseJsonError(format!(
                "Unexpected value: '{}' for literal",
                self.as_hex(b)
            ))),
        }
    }

    fn read_value_type(&mut self) -> Result<ValueType, BinlogError> {
        let b = self.reader.read_u8()?;
        if let Some(result) = ValueType::by_code(b) {
            Ok(result)
        } else {
            Err(BinlogError::ParseJsonError(format!(
                "Unknown value type code: '{}'",
                self.as_hex(b)
            )))
        }
    }

    fn as_hex(&mut self, b: u8) -> String {
        format!("{:02X} ", b)
    }
}

#[derive(Default)]
struct KeyEntry {
    pub index: u64,
    pub length: usize,
    pub name: String,
}

struct ValueEntry {
    pub value_type: ValueType,
    pub index: u32,
    pub value: Option<DirectEntryValue>,
    pub resolved: bool,
}

enum DirectEntryValue {
    Literal(Option<bool>),
    Numeric(i64),
}

impl ValueEntry {
    fn new(value_type: ValueType) -> Self {
        Self {
            value_type,
            index: 0,
            value: None,
            resolved: false,
        }
    }

    fn new_with_index(value_type: ValueType, index: u32) -> Self {
        Self {
            value_type,
            index,
            value: None,
            resolved: false,
        }
    }

    fn set_value(mut self, value: Option<DirectEntryValue>) -> Self {
        self.value = value;
        self.resolved = true;
        self
    }
}
