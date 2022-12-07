use crate::column::column_value::ColumnValue;

pub struct Assert {}

#[allow(dead_code)]
impl Assert {
    pub fn assert_numeric_eq(column_value: &Option<ColumnValue>, value: i64) {
        match column_value {
            Some(ColumnValue::Tiny(v)) => {
                assert_eq!(*v, value as i8);
            }
            Some(ColumnValue::Short(v)) => {
                assert_eq!(*v, value as i16);
            }
            Some(ColumnValue::Long(v)) => {
                assert_eq!(*v, value as i32);
            }
            Some(ColumnValue::LongLong(v)) => {
                assert_eq!(*v, value as i64);
            }
            Some(ColumnValue::Year(v)) => {
                assert_eq!(*v, value as u16);
            }
            Some(ColumnValue::Enum(v)) => {
                assert_eq!(*v, value as u32);
            }
            Some(ColumnValue::Set(v)) => {
                assert_eq!(*v, value as u64);
            }
            None => {}
            Some(_) => {}
        }
    }

    pub fn assert_unsigned_numeric_eq(column_value: &Option<ColumnValue>, value: u64) {
        match column_value {
            Some(ColumnValue::Enum(v)) => {
                assert_eq!(*v, value as u32);
            }
            Some(ColumnValue::Set(v)) => {
                assert_eq!(*v, value as u64);
            }
            None => {}
            Some(_) => {}
        }
    }

    pub fn assert_float_eq(column_value: &Option<ColumnValue>, value: f32) {
        match column_value {
            Some(ColumnValue::Float(v)) => {
                assert_eq!(*v, value);
            }
            None => {}
            Some(_) => {}
        }
    }

    pub fn assert_double_eq(column_value: &Option<ColumnValue>, value: f64) {
        match column_value {
            Some(ColumnValue::Double(v)) => {
                assert_eq!(*v, value);
            }
            None => {}
            Some(_) => {}
        }
    }

    pub fn assert_bit_eq(column_value: &Option<ColumnValue>, value: Vec<bool>) {
        match column_value {
            Some(ColumnValue::Bit(v)) => {
                assert_eq!(*v, value);
            }
            None => {}
            Some(_) => {}
        }
    }

    pub fn assert_bytes_eq(column_value: &Option<ColumnValue>, value: Vec<u8>) {
        match column_value {
            Some(ColumnValue::String(v)) | Some(ColumnValue::Blob(v)) => {
                assert_eq!(*v, value);
            }
            None => {}
            Some(_) => {}
        }
    }

    pub fn assert_string_eq(column_value: &Option<ColumnValue>, value: String) {
        match column_value {
            Some(ColumnValue::Time(v))
            | Some(ColumnValue::Date(v))
            | Some(ColumnValue::DateTime(v))
            | Some(ColumnValue::Decimal(v)) => {
                assert_eq!(*v, value);
            }
            None => {}
            Some(_) => {}
        }
    }

    pub fn assert_timestamp_eq(column_value: &Option<ColumnValue>, value: u64) {
        match column_value {
            Some(ColumnValue::Timestamp(v)) => {
                assert_eq!(*v, value as u64);
            }
            None => {}
            Some(_) => {}
        }
    }
}
