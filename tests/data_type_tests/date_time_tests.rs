#[cfg(test)]
mod test {
    use mysql_binlog_connector_rust::column::column_value::ColumnValue;
    use serial_test::serial;

    use crate::runner::test_runner::test::TestRunner;

    #[test]
    #[serial]
    fn test_datetime6() {
        // https://dev.mysql.com/doc/refman/8.0/en/datetime.html
        let col_type = "DATETIME(6)";

        // With the fractional part included, the format for these values is 'YYYY-MM-DD hh:mm:ss[.fraction]',
        // the range for DATETIME values is '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'
        let values = vec![
            "'1000-01-01 00:00:00.000000'",
            "'9999-12-31 23:59:59.999999'",
            "'2022-01-02 03:04:05.0'",
            "'2022-01-02 03:04:05.1'",
            "'2022-01-02 03:04:05.12'",
            "'2022-01-02 03:04:05.123'",
            "'2022-01-02 03:04:05.1234'",
            "'2022-01-02 03:04:05.12345'",
            "'2022-01-02 03:04:05.123456'",
            "'2022-01-02 03:04:05.000001'",
            "'2022-01-02 03:04:05.000012'",
            "'2022-01-02 03:04:05.000123'",
            "'2022-01-02 03:04:05.001234'",
            "'2022-01-02 03:04:05.012345'",
        ];

        let check_values = [
            "1000-01-01 00:00:00.000000",
            "9999-12-31 23:59:59.999999",
            "2022-01-02 03:04:05.000000",
            "2022-01-02 03:04:05.100000",
            "2022-01-02 03:04:05.120000",
            "2022-01-02 03:04:05.123000",
            "2022-01-02 03:04:05.123400",
            "2022-01-02 03:04:05.123450",
            "2022-01-02 03:04:05.123456",
            "2022-01-02 03:04:05.000001",
            "2022-01-02 03:04:05.000012",
            "2022-01-02 03:04:05.000123",
            "2022-01-02 03:04:05.001234",
            "2022-01-02 03:04:05.012345",
        ];

        run_and_check(col_type, &values, &check_values);
    }

    #[test]
    #[serial]
    fn test_datetime3() {
        let col_type = "DATETIME(3)";

        let values = vec![
            "'1000-01-01 00:00:00.000'",
            "'9999-12-31 23:59:59.999'",
            "'2022-01-02 03:04:05.0'",
            "'2022-01-02 03:04:05.1'",
            "'2022-01-02 03:04:05.12'",
            "'2022-01-02 03:04:05.123'",
            "'2022-01-02 03:04:05.001'",
            "'2022-01-02 03:04:05.012'",
        ];

        let check_values = [
            "1000-01-01 00:00:00.000000",
            "9999-12-31 23:59:59.999000",
            "2022-01-02 03:04:05.000000",
            "2022-01-02 03:04:05.100000",
            "2022-01-02 03:04:05.120000",
            "2022-01-02 03:04:05.123000",
            "2022-01-02 03:04:05.001000",
            "2022-01-02 03:04:05.012000",
        ];

        run_and_check(col_type, &values, &check_values);
    }

    #[test]
    #[serial]
    fn test_datetime() {
        // https://dev.mysql.com/doc/refman/8.0/en/datetime.html
        let col_type = "DATETIME";
        let values = vec!["'1000-01-01 00:00:00.000000'", "'9999-12-31 23:59:59'"];
        let check_values = ["1000-01-01 00:00:00.000000", "9999-12-31 23:59:59.000000"];
        run_and_check(col_type, &values, &check_values);
    }

    fn run_and_check(col_type: &str, values: &[&str], check_values: &[&str]) {
        let runner =
            TestRunner::run_one_col_test(col_type, values, &vec!["SET @@session.time_zone='UTC'"]);

        assert_eq!(runner.insert_events[0].rows.len(), check_values.len());
        for i in 0..check_values.len() {
            assert_eq!(
                runner.insert_events[0].rows[i].column_values[0],
                ColumnValue::DateTime(check_values[i].to_string()),
            );
        }
    }
}
