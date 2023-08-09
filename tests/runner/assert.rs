pub(crate) mod test {
    use mysql_binlog_connector_rust::column::column_value::ColumnValue;

    pub struct Assert {}

    #[allow(dead_code)]
    impl Assert {
        pub fn assert_numeric_eq(column_value: &ColumnValue, value: i128) {
            match column_value {
                ColumnValue::Tiny(v) => {
                    assert_eq!(*v, value as i8);
                }
                ColumnValue::Short(v) => {
                    assert_eq!(*v, value as i16);
                }
                ColumnValue::Long(v) => {
                    assert_eq!(*v, value as i32);
                }
                ColumnValue::LongLong(v) => {
                    assert_eq!(*v, value as i64);
                }
                ColumnValue::Year(v) => {
                    assert_eq!(*v, value as u16);
                }
                ColumnValue::Enum(v) => {
                    assert_eq!(*v, value as u32);
                }
                ColumnValue::Set(v) => {
                    assert_eq!(*v, value as u64);
                }
                ColumnValue::Bit(v) => {
                    assert_eq!(*v, value as u64);
                }
                _ => {
                    assert!(false)
                }
            }
        }

        pub fn assert_unsigned_numeric_eq(column_value: &ColumnValue, value: u64) {
            match column_value {
                ColumnValue::Enum(v) => {
                    assert_eq!(*v, value as u32);
                }
                ColumnValue::Set(v) => {
                    assert_eq!(*v, value as u64);
                }
                _ => {
                    assert!(false)
                }
            }
        }

        pub fn assert_float_eq(column_value: &ColumnValue, value: f32) {
            match column_value {
                ColumnValue::Float(v) => {
                    assert_eq!(*v, value);
                }
                _ => {
                    assert!(false)
                }
            }
        }

        pub fn assert_double_eq(column_value: &ColumnValue, value: f64) {
            match column_value {
                ColumnValue::Double(v) => {
                    assert_eq!(*v, value);
                }
                _ => {
                    assert!(false)
                }
            }
        }

        pub fn assert_bytes_eq(column_value: &ColumnValue, value: Vec<u8>) {
            match column_value {
                ColumnValue::String(v) | ColumnValue::Blob(v) => {
                    assert_eq!(*v, value);
                }
                _ => {
                    assert!(false)
                }
            }
        }

        pub fn assert_string_eq(column_value: &ColumnValue, value: String) {
            match column_value {
                ColumnValue::Time(v)
                | ColumnValue::Date(v)
                | ColumnValue::DateTime(v)
                | ColumnValue::Decimal(v) => {
                    assert_eq!(*v, value);
                }
                _ => {
                    assert!(false)
                }
            }
        }

        pub fn assert_timestamp_eq(column_value: &ColumnValue, value: i64) {
            match column_value {
                ColumnValue::Timestamp(v) => {
                    assert_eq!(*v, value);
                }
                _ => {
                    assert!(false)
                }
            }
        }

        pub fn assert_json_string_eq(json_1: &str, json_2: &str) {
            let json_1: serde_json::Value = serde_json::from_str(json_1).unwrap();
            let json_2: serde_json::Value = serde_json::from_str(json_2).unwrap();
            assert_eq!(json_1, json_2);
        }
    }
}
