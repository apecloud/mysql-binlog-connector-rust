#[cfg(test)]
mod test {
    use std::vec;

    use serial_test::serial;

    use crate::test::{
        assert::Assert,
        test_common::test::{TestCommon, I_EVENTS},
    };

    // The visible character number range is 32-126
    pub const MAX_TEST_STR_LENGTH: u8 = 95;

    #[test]
    #[serial]
    fn test_char_255() {
        let (test_values, check_values) = generate_visible_char_values();
        run_bytes_tests("CHAR(255)".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_space_values(false);
        run_bytes_tests("CHAR(255)".to_string(), test_values, check_values);
    }

    #[test]
    #[serial]
    fn test_varchar_255() {
        let (test_values, check_values) = generate_visible_char_values();
        run_bytes_tests("VARCHAR(255)".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_space_values(true);
        run_bytes_tests("VARCHAR(255)".to_string(), test_values, check_values);
    }

    #[test]
    #[serial]
    fn test_binary_255() {
        let (test_values, check_values) = generate_visible_char_values();
        run_bytes_tests("BINARY(255)".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_space_values(true);
        run_bytes_tests("BINARY(255)".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_nul_values(false);
        run_bytes_tests("BINARY(255)".to_string(), test_values, check_values);
    }

    #[test]
    #[serial]
    fn test_varbinary_255() {
        let (test_values, check_values) = generate_visible_char_values();
        run_bytes_tests("VARBINARY(255)".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_space_values(true);
        run_bytes_tests("VARBINARY(255)".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_nul_values(true);
        run_bytes_tests("VARBINARY(255)".to_string(), test_values, check_values);
    }

    #[test]
    #[serial]
    fn test_tinytext() {
        let (test_values, check_values) = generate_visible_char_values();
        run_bytes_tests("TINYTEXT".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_space_values(true);
        run_bytes_tests("TINYTEXT".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_nul_values(true);
        run_bytes_tests("TINYTEXT".to_string(), test_values, check_values);
    }

    #[test]
    #[serial]
    fn test_text() {
        let (test_values, check_values) = generate_visible_char_values();
        run_bytes_tests("TEXT".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_space_values(true);
        run_bytes_tests("TEXT".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_nul_values(true);
        run_bytes_tests("TEXT".to_string(), test_values, check_values);
    }

    #[test]
    #[serial]
    fn test_mediumtext() {
        let (test_values, check_values) = generate_visible_char_values();
        run_bytes_tests("MEDIUMTEXT".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_space_values(true);
        run_bytes_tests("MEDIUMTEXT".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_nul_values(true);
        run_bytes_tests("MEDIUMTEXT".to_string(), test_values, check_values);
    }

    #[test]
    #[serial]
    fn test_longtext() {
        let (test_values, check_values) = generate_visible_char_values();
        run_bytes_tests("LONGTEXT".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_space_values(true);
        run_bytes_tests("LONGTEXT".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_nul_values(true);
        run_bytes_tests("LONGTEXT".to_string(), test_values, check_values);
    }

    #[test]
    #[serial]
    fn test_tinyblob() {
        let (test_values, check_values) = generate_visible_char_values();
        run_bytes_tests("TINYBLOB".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_space_values(true);
        run_bytes_tests("TINYBLOB".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_nul_values(true);
        run_bytes_tests("TINYBLOB".to_string(), test_values, check_values);
    }

    #[test]
    #[serial]
    fn test_blob() {
        let (test_values, check_values) = generate_visible_char_values();
        run_bytes_tests("BLOB".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_space_values(true);
        run_bytes_tests("BLOB".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_nul_values(true);
        run_bytes_tests("BLOB".to_string(), test_values, check_values);
    }

    #[test]
    #[serial]
    fn test_mediumblob() {
        let (test_values, check_values) = generate_visible_char_values();
        run_bytes_tests("MEDIUMBLOB".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_space_values(true);
        run_bytes_tests("MEDIUMBLOB".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_nul_values(true);
        run_bytes_tests("MEDIUMBLOB".to_string(), test_values, check_values);
    }

    #[test]
    #[serial]
    fn test_longblob() {
        let (test_values, check_values) = generate_visible_char_values();
        run_bytes_tests("LONGBLOB".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_space_values(true);
        run_bytes_tests("LONGBLOB".to_string(), test_values, check_values);

        let (test_values, check_values) = generate_trailing_nul_values(true);
        run_bytes_tests("LONGBLOB".to_string(), test_values, check_values);
    }

    fn run_bytes_tests(data_type: String, test_values: Vec<String>, check_values: Vec<Vec<u8>>) {
        TestCommon::before_dml();
        let prepare_sqls = vec![TestCommon::get_create_table_sql_with_one_field(data_type)];
        TestCommon::execute_insert_sqls_and_get_binlogs(prepare_sqls, test_values);
        unsafe {
            for i in 0..check_values.len() {
                Assert::assert_bytes_eq(
                    &I_EVENTS[i].rows[0].column_values[0],
                    check_values[i].clone(),
                );
            }
        }
    }

    fn generate_visible_char_values() -> (Vec<String>, Vec<Vec<u8>>) {
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

        (get_test_values(values), check_values)
    }

    fn generate_trailing_space_values(
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

        (get_test_values(values), check_values)
    }

    fn generate_trailing_nul_values(
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

        (get_test_values(values), check_values)
    }

    fn get_test_values(values: Vec<String>) -> Vec<String> {
        // ["a", "ab"] -> ["('a')", "('ab')"]
        let mut test_values = Vec::new();
        for s in values.clone() {
            test_values.push(format!("('{}')", s));
        }
        test_values
    }
}
