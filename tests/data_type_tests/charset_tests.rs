#[cfg(test)]
mod test {
    use std::vec;

    use mysql_binlog_connector_rust::column::column_value::ColumnValue;
    use serial_test::serial;

    use crate::runner::test_runner::test::TestRunner;

    #[test]
    #[serial]
    fn test_utf8mb4() {
        let col_type = "VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
        let values = vec!["('123abc中文😀')"];
        // "123": [49, 50, 51]
        // "abc": [97, 98, 99])
        // "中文": [228, 184, 173, 230, 150, 135]
        // "😀": [240, 159, 152, 128]
        let check_values = vec![vec![
            49, 50, 51, 97, 98, 99, 228, 184, 173, 230, 150, 135, 240, 159, 152, 128,
        ]];

        run_and_check(col_type, "SET names utf8mb4", &values, &check_values);
    }

    #[test]
    #[serial]
    fn test_utf8() {
        let col_type = "VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
        let values = vec!["('123abc中文')"];
        // "123": [49, 50, 51]
        // "abc": [97, 98, 99])
        // "中文": [228, 184, 173, 230, 150, 135]
        let check_values = vec![vec![49, 50, 51, 97, 98, 99, 228, 184, 173, 230, 150, 135]];

        run_and_check(col_type, "SET names utf8", &values, &check_values);
    }

    #[test]
    #[serial]
    fn test_latin1() {
        let col_type = "VARCHAR(255) CHARACTER SET latin1";
        let values = vec!["('123abc')"];
        // "123": [49, 50, 51]
        // "abc": [97, 98, 99])
        let check_values = vec![vec![49, 50, 51, 97, 98, 99]];

        run_and_check(col_type, "SET names utf8", &values, &check_values);
    }

    #[test]
    #[serial]
    fn test_gbk() {
        let col_type = "VARCHAR(255) CHARACTER SET gbk";
        let values = vec!["('123abc中文')"];
        // "123": [49, 50, 51]
        // "abc": [97, 98, 99])
        // "中文": [214, 208, 206, 196]
        let check_values = vec![vec![49, 50, 51, 97, 98, 99, 214, 208, 206, 196]];

        run_and_check(col_type, "SET names utf8", &values, &check_values);
    }

    #[test]
    #[serial]
    fn test_gb18030() {
        let col_type = "VARCHAR(255) CHARACTER SET gb18030";
        let values = vec!["('123abc中文😀')"];
        // "123": [49, 50, 51]
        // "abc": [97, 98, 99])
        // "中文": [214, 208, 206, 196]
        // "😀": [148, 57, 252, 54]
        let check_values = vec![vec![
            49, 50, 51, 97, 98, 99, 214, 208, 206, 196, 148, 57, 252, 54,
        ]];

        run_and_check(col_type, "SET names utf8mb4", &values, &check_values);
    }

    fn run_and_check(
        col_type: &str,
        init_sql: &str,
        values: &Vec<&str>,
        check_values: &Vec<Vec<u8>>,
    ) {
        let runner = TestRunner::run_one_col_test(col_type, values, &vec![init_sql]);
        for i in 0..check_values.len() {
            assert_eq!(
                runner.insert_events[0].rows[i].column_values[0],
                ColumnValue::String(check_values[i].clone())
            );
        }
    }
}
