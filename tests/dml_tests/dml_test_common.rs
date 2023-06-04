#[cfg(test)]
pub mod test {
    use mysql_binlog_connector_rust::event::row_event::RowEvent;

    use crate::assert::test::Assert;

    pub struct DmlTestCommon {}

    impl DmlTestCommon {
        pub fn get_create_table_sql_with_all_types(db: &str, tb: &str) -> String {
            format!(
                "CREATE TABLE {}.{} (f_0 {}, f_1 {}, f_2 {}, f_3 {}, f_4 {}, f_5 {}, f_6 {}, f_7 {}, f_8 {}, f_9 {}, f_10 {}, f_11 {}, f_12 {}, f_13 {}, f_14 {}, f_15 {}, f_16 {}, f_17 {}, f_18 {}, f_19 {}, f_20 {}, f_21 {}, f_22 {}, f_23 {}, f_24 {}, f_25 {}, f_26 {}, f_27 {})",
                db,
                tb,
                "TINYINT NULL",
                "SMALLINT NULL",
                "MEDIUMINT NULL",
                "INT NULL",
                "BIGINT NULL",
                "DECIMAL(10,4) NULL",
                "FLOAT(6,2) NULL",
                "DOUBLE(8,3) NULL",
                "BIT(3) NULL",
                "DATETIME(6) NULL",
                "TIME(6) NULL",
                "DATE NULL",
                "YEAR NULL",
                "TIMESTAMP(6) NULL",
                "CHAR(255) NULL",
                "VARCHAR(255) NULL",
                "BINARY(255) NULL",
                "VARBINARY(255) NULL",
                "TINYTEXT NULL",
                "TEXT NULL",
                "MEDIUMTEXT NULL",
                "LONGTEXT NULL",
                "TINYBLOB NULL",
                "BLOB NULL",
                "MEDIUMBLOB NULL",
                "LONGBLOB NULL",
                "ENUM('x-small', 'small', 'medium', 'large', 'x-large') NULL",
                "SET('1', '2', '3', '4', '5') NULL",
            )
        }

        pub fn get_update_table_sql_with_all_types(db: &str, tb: &str, f_0_value: String, values: Vec<String>) -> String {
            format!(
                "UPDATE {}.{} SET f_1={}, f_2={}, f_3={}, f_4={}, f_5={}, f_6={}, f_7={}, f_8={}, f_9={}, f_10={}, f_11={}, f_12={}, f_13={}, f_14={}, f_15={}, f_16={}, f_17={}, f_18={}, f_19={}, f_20={}, f_21={}, f_22={}, f_23={}, f_24={}, f_25={}, f_26={}, f_27={} WHERE f_0={}",
                db, tb,
                values[1], values[2], values[3], values[4], values[5], values[6], 
                values[7], values[8], values[9], values[10], values[11], values[12], 
                values[13], values[14], values[15], values[16], values[17], values[18], 
                values[19], values[20], values[21], values[22], values[23], values[24], values[25], 
                values[26], values[27], f_0_value
            )
        }

        pub fn generate_basic_dml_test_data() -> Vec<Vec<String>> {
            let values = vec![
                vec![
                    "1".to_string(),
                    "2".to_string(),
                    "3".to_string(),
                    "4".to_string(),
                    "5".to_string(),
                    "123456.1234".to_string(),
                    "1234.12".to_string(),
                    "12345.123".to_string(),
                    "3".to_string(),
                    "'2022-01-02 03:04:05.123456'".to_string(),
                    "'03:04:05.123456'".to_string(),
                    "'2022-01-02'".to_string(),
                    "2022".to_string(),
                    "'2022-01-02 03:04:05.123456'".to_string(),
                    "'ab'".to_string(),
                    "'cd'".to_string(),
                    "'ef'".to_string(),
                    "'gh'".to_string(),
                    "'ij'".to_string(),
                    "'kl'".to_string(),
                    "'mn'".to_string(),
                    "'op'".to_string(),
                    "'qr'".to_string(),
                    "'st'".to_string(),
                    "'uv'".to_string(),
                    "'wx'".to_string(),
                    "'x-small'".to_string(),
                    "'1'".to_string(),
                ],
                vec![
                    "10".to_string(),
                    "20".to_string(),
                    "30".to_string(),
                    "40".to_string(),
                    "50".to_string(),
                    "654321.4321".to_string(),
                    "4321.21".to_string(),
                    "54321.321".to_string(),
                    "4".to_string(),
                    "'2021-02-01 04:05:06.654321'".to_string(),
                    "'04:05:06.654321'".to_string(),
                    "'2012-02-01'".to_string(),
                    "2021".to_string(),
                    "'2021-02-01 04:05:06.654321'".to_string(),
                    "'1'".to_string(),
                    "'2'".to_string(),
                    "'3'".to_string(),
                    "'4'".to_string(),
                    "'5'".to_string(),
                    "'6'".to_string(),
                    "'7'".to_string(),
                    "'8'".to_string(),
                    "'9'".to_string(),
                    "'10'".to_string(),
                    "'11'".to_string(),
                    "'12'".to_string(),
                    "'small'".to_string(),
                    "'2'".to_string(),
                ],
                vec![
                    "6".to_string(),
                    "7".to_string(),
                    "8".to_string(),
                    "9".to_string(),
                    "10".to_string(),
                    "234561.2341".to_string(),
                    "2341.12".to_string(),
                    "23451.231".to_string(),
                    "5".to_string(),
                    "'2020-03-04 05:06:07.234561'".to_string(),
                    "'05:06:07.234561'".to_string(),
                    "'2022-05-06'".to_string(),
                    "2020".to_string(),
                    "'2020-03-04 05:06:07.234561'".to_string(),
                    "'a'".to_string(),
                    "'b'".to_string(),
                    "'c'".to_string(),
                    "'d'".to_string(),
                    "'e'".to_string(),
                    "'f'".to_string(),
                    "'g'".to_string(),
                    "'h'".to_string(),
                    "'i'".to_string(),
                    "'j'".to_string(),
                    "'k'".to_string(),
                    "'l'".to_string(),
                    "'medium'".to_string(),
                    "'3'".to_string(),
                ],
                vec![
                    "11".to_string(),
                    "NULL".to_string(),
                    "3".to_string(),
                    "NULL".to_string(),
                    "5".to_string(),
                    "NULL".to_string(),
                    "1234.12".to_string(),
                    "NULL".to_string(),
                    "3".to_string(),
                    "NULL".to_string(),
                    "'03:04:05.123456'".to_string(),
                    "NULL".to_string(),
                    "2022".to_string(),
                    "NULL".to_string(),
                    "'ab'".to_string(),
                    "NULL".to_string(),
                    "'ef'".to_string(),
                    "NULL".to_string(),
                    "'ij'".to_string(),
                    "NULL".to_string(),
                    "'mn'".to_string(),
                    "NULL".to_string(),
                    "'qr'".to_string(),
                    "NULL".to_string(),
                    "'uv'".to_string(),
                    "NULL".to_string(),
                    "'x-small'".to_string(),
                    "NULL".to_string(),
                ],
                vec![
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                    "NULL".to_string(),
                ],
            ];
            values
        }

        pub fn check_values(
            event: &RowEvent,
            f0_v: i8,
            f1_v: i16,
            f2_v: i32,
            f3_v: i32,
            f4_v: i64,
            f5_v: String,
            f6_v: f32,
            f7_v: f64,
            f8_v: i64,
            f9_v: String,
            f10_v: String,
            f11_v: String,
            f12_v: u16,
            f13_v: i64,
            f14_v: Vec<u8>,
            f15_v: Vec<u8>,
            f16_v: Vec<u8>,
            f17_v: Vec<u8>,
            f18_v: Vec<u8>,
            f19_v: Vec<u8>,
            f20_v: Vec<u8>,
            f21_v: Vec<u8>,
            f22_v: Vec<u8>,
            f23_v: Vec<u8>,
            f24_v: Vec<u8>,
            f25_v: Vec<u8>,
            f26_v: u32,
            f27_v: u64,
        ) {
            // TINYINT
            Assert::assert_numeric_eq(&event.column_values[0], f0_v as i64);
            // SMALLINT
            Assert::assert_numeric_eq(&event.column_values[1], f1_v as i64);
            // MEDIUMINT
            Assert::assert_numeric_eq(&event.column_values[2], f2_v as i64);
            // INT
            Assert::assert_numeric_eq(&event.column_values[3], f3_v as i64);
            // BIGINT
            Assert::assert_numeric_eq(&event.column_values[4], f4_v);
            // DECIMAL(10,4)
            Assert::assert_string_eq(&event.column_values[5], f5_v);
            // FLOAT(6,2)
            Assert::assert_float_eq(&event.column_values[6], f6_v);
            // DOUBLE(8,3)
            Assert::assert_double_eq(&event.column_values[7], f7_v);
            // BIT(3)
            Assert::assert_numeric_eq(&event.column_values[8], f8_v);
            // DATETIME(6)
            Assert::assert_string_eq(&event.column_values[9], f9_v);
            // TIME(6)
            Assert::assert_string_eq(&event.column_values[10], f10_v);
            // DATE
            Assert::assert_string_eq(&event.column_values[11], f11_v);
            // YEAR
            Assert::assert_numeric_eq(&event.column_values[12], f12_v as i64);
            // TIMESTAMP(6)
            Assert::assert_timestamp_eq(&event.column_values[13], f13_v);
            // CHAR(255)
            Assert::assert_bytes_eq(&event.column_values[14], f14_v);
            // VARCHAR(255)
            Assert::assert_bytes_eq(&event.column_values[15], f15_v);
            // BINARY(255)
            Assert::assert_bytes_eq(&event.column_values[16], f16_v);
            // VARBINARY(255)
            Assert::assert_bytes_eq(&event.column_values[17], f17_v);
            // TINYTEXT
            Assert::assert_bytes_eq(&event.column_values[18], f18_v);
            // TEXT
            Assert::assert_bytes_eq(&event.column_values[19], f19_v);
            // MEDIUMTEXT
            Assert::assert_bytes_eq(&event.column_values[20], f20_v);
            // LONGTEXT
            Assert::assert_bytes_eq(&event.column_values[21], f21_v);
            // TINYBLOB
            Assert::assert_bytes_eq(&event.column_values[22], f22_v);
            // BLOB
            Assert::assert_bytes_eq(&event.column_values[23], f23_v);
            // MEDIUMBLOB
            Assert::assert_bytes_eq(&event.column_values[24], f24_v);
            // LONGBLOB
            Assert::assert_bytes_eq(&event.column_values[25], f25_v);
            // ENUM
            Assert::assert_unsigned_numeric_eq(&event.column_values[26], f26_v as u64);
            // SET
            Assert::assert_unsigned_numeric_eq(&event.column_values[27], f27_v);
        }
    }
}
