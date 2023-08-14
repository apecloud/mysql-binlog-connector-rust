pub(crate) mod test {

    use mysql_binlog_connector_rust::event::row_event::RowEvent;

    use crate::runner::{assert::test::Assert, env::test::Env};

    const DEFAULT_COL_NAMES: [&str; 29] = [
        "pk", "f_1", "f_2", "f_3", "f_4", "f_5", "f_6", "f_7", "f_8", "f_9", "f_10", "f_11",
        "f_12", "f_13", "f_14", "f_15", "f_16", "f_17", "f_18", "f_19", "f_20", "f_21", "f_22",
        "f_23", "f_24", "f_25", "f_26", "f_27", "f_28",
    ];

    const DEFAULT_COL_TYPES: [&str; 29] = [
        "INT NOT NULL",
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
    ];

    const DEFAULT_COL_VALUES_1: [&str; 29] = [
        "0",
        "1",
        "2",
        "3",
        "4",
        "5",
        "123456.1234",
        "1234.12",
        "12345.123",
        "3",
        "'2022-01-02 03:04:05.123456'",
        "'03:04:05.123456'",
        "'2022-01-02'",
        "2022",
        "'2022-01-02 03:04:05.123456'",
        "'ab'",
        "'cd'",
        "'ef'",
        "'gh'",
        "'ij'",
        "'kl'",
        "'mn'",
        "'op'",
        "'qr'",
        "'st'",
        "'uv'",
        "'wx'",
        "'x-small'",
        "'1'",
    ];

    const DEFAULT_COL_VALUES_2: [&str; 29] = [
        "1",
        "10",
        "20",
        "30",
        "40",
        "50",
        "654321.4321",
        "4321.21",
        "54321.321",
        "4",
        "'2021-02-01 04:05:06.654321'",
        "'04:05:06.654321'",
        "'2012-02-01'",
        "2021",
        "'2021-02-01 04:05:06.654321'",
        "'1'",
        "'2'",
        "'3'",
        "'4'",
        "'5'",
        "'6'",
        "'7'",
        "'8'",
        "'9'",
        "'10'",
        "'11'",
        "'12'",
        "'small'",
        "'2'",
    ];

    const DEFAULT_COL_VALUES_3: [&str; 29] = [
        "2",
        "6",
        "7",
        "8",
        "9",
        "10",
        "234561.2341",
        "2341.12",
        "23451.231",
        "5",
        "'2020-03-04 05:06:07.234561'",
        "'05:06:07.234561'",
        "'2022-05-06'",
        "2020",
        "'2020-03-04 05:06:07.234561'",
        "'a'",
        "'b'",
        "'c'",
        "'d'",
        "'e'",
        "'f'",
        "'g'",
        "'h'",
        "'i'",
        "'j'",
        "'k'",
        "'l'",
        "'medium'",
        "'3'",
    ];

    const DEFAULT_COL_VALUES_4: [&str; 29] = [
        "3",
        "11",
        "NULL",
        "3",
        "NULL",
        "5",
        "NULL",
        "1234.12",
        "NULL",
        "3",
        "NULL",
        "'03:04:05.123456'",
        "NULL",
        "2022",
        "NULL",
        "'ab'",
        "NULL",
        "'ef'",
        "NULL",
        "'ij'",
        "NULL",
        "'mn'",
        "NULL",
        "'qr'",
        "NULL",
        "'uv'",
        "NULL",
        "'x-small'",
        "NULL",
    ];

    const DEFAULT_COL_VALUES_5: [&str; 29] = [
        "4", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL",
        "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL",
        "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL",
    ];

    pub struct Mock {}

    impl Mock {
        pub fn default_col_names() -> Vec<&'static str> {
            DEFAULT_COL_NAMES.to_vec()
        }

        pub fn default_insert_values() -> Vec<Vec<&'static str>> {
            vec![
                DEFAULT_COL_VALUES_1.to_vec(),
                DEFAULT_COL_VALUES_2.to_vec(),
                DEFAULT_COL_VALUES_3.to_vec(),
                DEFAULT_COL_VALUES_4.to_vec(),
                DEFAULT_COL_VALUES_5.to_vec(),
            ]
        }

        pub fn default_create_sql() -> String {
            let mut fields = vec![];
            for i in 0..DEFAULT_COL_NAMES.len() {
                fields.push(format!("{} {}", DEFAULT_COL_NAMES[i], DEFAULT_COL_TYPES[i]));
            }

            let (db, tb) = Self::get_db_tb();
            format!(
                "CREATE TABLE {}.{} ({}, PRIMARY KEY(pk))",
                db,
                tb,
                fields.join(",")
            )
        }

        pub fn default_check_values(
            event: &RowEvent,
            pk: i32,
            f_1_v: i8,
            f_2_v: i16,
            f_3_v: i32,
            f_4_v: i32,
            f_5_v: i64,
            f_6_v: String,
            f_7_v: f32,
            f_8_v: f64,
            f_9_v: i64,
            f_10_v: String,
            f_11_v: String,
            f_12_v: String,
            f_13_v: u16,
            f_14_v: i64,
            f_15_v: Vec<u8>,
            f_16_v: Vec<u8>,
            f_17_v: Vec<u8>,
            f_18_v: Vec<u8>,
            f_19_v: Vec<u8>,
            f_20_v: Vec<u8>,
            f_21_v: Vec<u8>,
            f_22_v: Vec<u8>,
            f_23_v: Vec<u8>,
            f_24_v: Vec<u8>,
            f_25_v: Vec<u8>,
            f_26_v: Vec<u8>,
            f_27_v: u32,
            f_28_v: u64,
        ) {
            // INT
            Assert::assert_numeric_eq(&event.column_values[0], pk as i128);
            // TINYINT
            Assert::assert_numeric_eq(&event.column_values[1], f_1_v as i128);
            // SMALLINT
            Assert::assert_numeric_eq(&event.column_values[2], f_2_v as i128);
            // MEDIUMINT
            Assert::assert_numeric_eq(&event.column_values[3], f_3_v as i128);
            // INT
            Assert::assert_numeric_eq(&event.column_values[4], f_4_v as i128);
            // BIGINT
            Assert::assert_numeric_eq(&event.column_values[5], f_5_v as i128);
            // DECIMAL(10,4)
            Assert::assert_string_eq(&event.column_values[6], f_6_v);
            // FLOAT(6,2)
            Assert::assert_float_eq(&event.column_values[7], f_7_v);
            // DOUBLE(8,3)
            Assert::assert_double_eq(&event.column_values[8], f_8_v);
            // BIT(3)
            Assert::assert_numeric_eq(&event.column_values[9], f_9_v as i128);
            // DATETIME(6)
            Assert::assert_string_eq(&event.column_values[10], f_10_v);
            // TIME(6)
            Assert::assert_string_eq(&event.column_values[11], f_11_v);
            // DATE
            Assert::assert_string_eq(&event.column_values[12], f_12_v);
            // YEAR
            Assert::assert_numeric_eq(&event.column_values[13], f_13_v as i128);
            // TIMESTAMP(6)
            Assert::assert_timestamp_eq(&event.column_values[14], f_14_v);
            // CHAR(255)
            Assert::assert_bytes_eq(&event.column_values[15], f_15_v);
            // VARCHAR(255)
            Assert::assert_bytes_eq(&event.column_values[16], f_16_v);
            // BINARY(255)
            Assert::assert_bytes_eq(&event.column_values[17], f_17_v);
            // VARBINARY(255)
            Assert::assert_bytes_eq(&event.column_values[18], f_18_v);
            // TINYTEXT
            Assert::assert_bytes_eq(&event.column_values[19], f_19_v);
            // TEXT
            Assert::assert_bytes_eq(&event.column_values[20], f_20_v);
            // MEDIUMTEXT
            Assert::assert_bytes_eq(&event.column_values[21], f_21_v);
            // LONGTEXT
            Assert::assert_bytes_eq(&event.column_values[22], f_22_v);
            // TINYBLOB
            Assert::assert_bytes_eq(&event.column_values[23], f_23_v);
            // BLOB
            Assert::assert_bytes_eq(&event.column_values[24], f_24_v);
            // MEDIUMBLOB
            Assert::assert_bytes_eq(&event.column_values[25], f_25_v);
            // LONGBLOB
            Assert::assert_bytes_eq(&event.column_values[26], f_26_v);
            // ENUM
            Assert::assert_unsigned_numeric_eq(&event.column_values[27], f_27_v as u64);
            // SET
            Assert::assert_unsigned_numeric_eq(&event.column_values[28], f_28_v);
        }

        pub fn insert_sql(values_list: &[Vec<&str>]) -> String {
            let mut rows = vec![];
            for values in values_list {
                let row = format!("({})", values.join(","));
                rows.push(row);
            }

            let (db, tb) = Self::get_db_tb();
            format!("INSERT INTO {}.{} VALUES {}", db, tb, rows.join(","))
        }

        pub fn update_sql(
            pk_col: &str,
            pk_value: &str,
            update_col_names: &[&str],
            update_col_values: &[&str],
        ) -> String {
            let mut set_values = vec![];

            for i in 0..update_col_names.len() {
                let set_value = format!("{}={}", update_col_names[i], update_col_values[i]);
                set_values.push(set_value);
            }

            let (db, tb) = Self::get_db_tb();
            format!(
                "UPDATE {}.{} SET {} WHERE {}={}",
                db,
                tb,
                set_values.join(","),
                pk_col,
                pk_value
            )
        }

        pub fn delete_sql(pk_col: &str, pk_values: &[&str]) -> String {
            let (db, tb) = Self::get_db_tb();
            if pk_values.is_empty() {
                format!("DELETE FROM {}.{} ", db, tb,)
            } else {
                format!(
                    "DELETE FROM {}.{} WHERE {} IN ({})",
                    db,
                    tb,
                    pk_col,
                    pk_values.join(",")
                )
            }
        }

        pub fn one_col_create_sql(col_type: &str) -> String {
            let (db, tb) = Self::get_db_tb();
            format!("CREATE TABLE {}.{} (f_0 {})", db, tb, col_type)
        }

        pub fn one_col_insert_sql(values: &[&str]) -> String {
            let mut values_list = vec![];
            for v in values {
                values_list.push(vec![*v]);
            }
            Self::insert_sql(&values_list)
        }

        fn get_db_tb() -> (String, String) {
            let env = Env::load_vars();
            (
                env.get(Env::DEFAULT_DB).unwrap().to_string(),
                env.get(Env::DEFAULT_TB).unwrap().to_string(),
            )
        }
    }
}
