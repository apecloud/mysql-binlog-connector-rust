use mysql_binlog_connector_rust::column::{column_type::ColumnType, column_value::ColumnValue};

use crate::runner::test_runner::test::TestRunner;

/// A util to run tests for column types whose parsed binlog values are raw bytes, including:
/// CHAR, VARCHAR, BINARY, VARBINARY, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT, TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB
pub(crate) struct BytesTestUtil {}

// The visible character number range is 32-126
const MAX_TEST_STR_LENGTH: u8 = 95;

impl BytesTestUtil {
    pub fn run_and_check(
        mysql_column_type: &str,
        binlog_column_type: ColumnType,
        values: &Vec<String>,
        check_values: &Vec<Vec<u8>>,
    ) {
        let values: Vec<&str> = values.into_iter().map(|i| i.as_str()).collect();
        let runner = TestRunner::run_one_col_test(mysql_column_type, &values, &vec![]);
        for i in 0..check_values.len() {
            let column_value = match binlog_column_type {
                ColumnType::String | ColumnType::VarChar => {
                    ColumnValue::String(check_values[i].clone())
                }
                ColumnType::Blob => ColumnValue::Blob(check_values[i].clone()),
                _ => ColumnValue::None,
            };
            assert_eq!(
                runner.insert_events[0].rows[i].column_values[0],
                column_value
            );
        }
    }

    pub fn generate_visible_char_values() -> (Vec<String>, Vec<Vec<u8>>) {
        let mut values = Vec::new();
        let mut check_values = Vec::new();

        let non_blank_str = |n: u8| -> (String, Vec<u8>) {
            let mut str = String::new();
            let mut bytes = Vec::new();
            // The visible character number range is 32-126
            for i in 1..n {
                bytes.push(32 + i as u8);
                str.push(char::from_u32(32 + i as u32).unwrap());
            }
            (str, bytes)
        };

        // generate non-blank string by visible characters,
        // the first visible character is space, the corresponding ascii code is 32
        // if MAX_TEST_STR_LENGTH = 4, then below strings will be generated
        // ' !'
        // ' !"'
        // ' !"#'
        for i in 0..MAX_TEST_STR_LENGTH {
            let (mut str, bytes) = non_blank_str(i + 1);
            // character escapes
            str = str.replace("\\", "\\\\");
            str = str.replace("'", "\\\'");
            values.push(str);
            check_values.push(bytes);
        }

        (Self::get_test_values(&values), check_values)
    }

    pub fn generate_trailing_space_values(
        keep_trailing_space_in_binlog: bool,
    ) -> (Vec<String>, Vec<Vec<u8>>) {
        let mut values = Vec::new();
        let mut check_values = Vec::new();

        // cases with spaces
        values.push("".to_string());
        values.push("           ".to_string());
        values.push("a          ".to_string());
        values.push("          a".to_string());
        values.push("  a        ".to_string());
        values.push("  a    b   ".to_string());

        for i in check_values.len()..values.len() {
            let mut bytes = Vec::new();

            let str = if keep_trailing_space_in_binlog {
                &values[i]
            } else {
                values[i].trim_end()
            };

            for i in 0..str.len() {
                bytes.push(str.chars().nth(i).unwrap() as u8);
            }
            check_values.push(bytes);
        }

        (Self::get_test_values(&values), check_values)
    }

    pub fn generate_trailing_nul_values(
        keep_trailing_nul_in_binlog: bool,
    ) -> (Vec<String>, Vec<Vec<u8>>) {
        let mut values = Vec::new();
        let mut check_values = Vec::new();

        values.push("\0\0\0\0\0\0".to_string());
        values.push("a\0\0\0\0\0".to_string());
        values.push("\0\0\0\0\0a".to_string());
        values.push("\0\0a\0\0\0".to_string());
        values.push("\0a\0a\0\0".to_string());

        if keep_trailing_nul_in_binlog {
            check_values.push(vec![0u8, 0, 0, 0, 0, 0]);
            check_values.push(vec![97, 0, 0, 0, 0, 0]);
            check_values.push(vec![0, 0, 0, 0, 0, 97]);
            check_values.push(vec![0, 0, 97, 0, 0, 0]);
            check_values.push(vec![0, 97, 0, 97, 0, 0]);
        } else {
            check_values.push(vec![]);
            check_values.push(vec![97]);
            check_values.push(vec![0, 0, 0, 0, 0, 97]);
            check_values.push(vec![0, 0, 97]);
            check_values.push(vec![0, 97, 0, 97]);
        }

        (Self::get_test_values(&values), check_values)
    }

    fn get_test_values(values: &Vec<String>) -> Vec<String> {
        // ["a", "ab"] -> ["('a')", "('ab')"]
        let mut test_values = Vec::new();
        for s in values.clone() {
            test_values.push(format!("('{}')", s));
        }
        test_values
    }
}
