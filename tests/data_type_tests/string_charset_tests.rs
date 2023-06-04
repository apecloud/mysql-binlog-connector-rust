#[cfg(test)]
mod test {
    use std::vec;

    use serial_test::serial;

    use crate::{assert::test::Assert, test_runner::test::TestRunner};

    #[test]
    #[serial]
    fn test_utf8mb4() {
        let data_type = "VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci".to_string();
        let test_values = vec!["('123abc中文😀')".to_string()];
        // "123": [49, 50, 51]
        // "abc": [97, 98, 99])
        // "中文": [228, 184, 173, 230, 150, 135]
        // "😀": [240, 159, 152, 128]
        let check_values = vec![vec![
            49, 50, 51, 97, 98, 99, 228, 184, 173, 230, 150, 135, 240, 159, 152, 128,
        ]];

        run_bytes_tests(
            data_type,
            "SET names utf8mb4".to_string(),
            test_values,
            check_values,
        );
    }

    #[test]
    #[serial]
    fn test_utf8() {
        let data_type = "VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci".to_string();
        let test_values = vec!["('123abc中文')".to_string()];
        // "123": [49, 50, 51]
        // "abc": [97, 98, 99])
        // "中文": [228, 184, 173, 230, 150, 135]
        let check_values = vec![vec![49, 50, 51, 97, 98, 99, 228, 184, 173, 230, 150, 135]];

        run_bytes_tests(
            data_type,
            "SET names utf8".to_string(),
            test_values,
            check_values,
        );
    }

    #[test]
    #[serial]
    fn test_latin1() {
        let data_type = "VARCHAR(255) CHARACTER SET latin1".to_string();
        let test_values = vec!["('123abc')".to_string()];
        // "123": [49, 50, 51]
        // "abc": [97, 98, 99])
        let check_values = vec![vec![49, 50, 51, 97, 98, 99]];

        run_bytes_tests(
            data_type,
            "SET names utf8".to_string(),
            test_values,
            check_values,
        );
    }

    #[test]
    #[serial]
    fn test_gbk() {
        let data_type = "VARCHAR(255) CHARACTER SET gbk".to_string();
        let test_values = vec!["('123abc中文')".to_string()];
        // "123": [49, 50, 51]
        // "abc": [97, 98, 99])
        // "中文": [214, 208, 206, 196]
        let check_values = vec![vec![49, 50, 51, 97, 98, 99, 214, 208, 206, 196]];

        run_bytes_tests(
            data_type,
            "SET names utf8".to_string(),
            test_values,
            check_values,
        );
    }

    #[test]
    #[serial]
    fn test_gb18030() {
        let data_type = "VARCHAR(255) CHARACTER SET gb18030".to_string();
        let test_values = vec!["('123abc中文😀')".to_string()];
        // "123": [49, 50, 51]
        // "abc": [97, 98, 99])
        // "中文": [214, 208, 206, 196]
        // "😀": [148, 57, 252, 54]
        let check_values = vec![vec![
            49, 50, 51, 97, 98, 99, 214, 208, 206, 196, 148, 57, 252, 54,
        ]];

        run_bytes_tests(
            data_type,
            "SET names utf8mb4".to_string(),
            test_values,
            check_values,
        );
    }

    fn run_bytes_tests(
        data_type: String,
        init_sql: String,
        test_values: Vec<String>,
        check_values: Vec<Vec<u8>>,
    ) {
        let mut runner = TestRunner::new();
        let prepare_sqls = vec![
            runner.get_create_table_sql_with_one_field(data_type),
            init_sql,
        ];
        runner.execute_insert_sqls_and_get_binlogs(prepare_sqls, test_values);

        for i in 0..check_values.len() {
            Assert::assert_bytes_eq(
                &runner.insert_events[i].rows[0].column_values[0],
                check_values[i].clone(),
            );
        }
    }
}
