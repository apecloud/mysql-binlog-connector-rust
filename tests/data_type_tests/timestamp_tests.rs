#[cfg(test)]
mod test {
    use mysql_binlog_connector_rust::column::column_value::ColumnValue;
    use serial_test::serial;

    use crate::runner::test_runner::test::TestRunner;

    #[test]
    #[serial]
    fn test_timestamp6() {
        let col_type = "TIMESTAMP(6)";

        // refer: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
        // The range is '1970-01-01 00:00:01.000000' UTC to '2038-01-19 03:14:07.999999' UTC.
        // TIMESTAMP values are stored as the number of seconds since the epoch ('1970-01-01 00:00:00' UTC).
        let values = vec![
            "'1970-01-01 00:00:01.000000'",
            "'2038-01-19 03:14:07.999999'",
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

        // MySQL converts TIMESTAMP values from the current time zone to UTC for storage,
        // and back from UTC to the current time zone for retrieval.
        // (This does not occur for other types such as DATETIME.)
        let micros_per_second = 1000000i64;
        // 2147483647 is the timestamp (UTC) for 2022-01-02 03:04:05 (UTC)
        // 1641092645 is the timestamp (UTC) for 2038-01-19 03:14:07 (UTC)
        let test_utc_timestamp = 1641092645 * micros_per_second;
        let check_values = [
            1000000,
            2147483647 * micros_per_second + 999999,
            test_utc_timestamp,
            test_utc_timestamp + 100000,
            test_utc_timestamp + 120000,
            test_utc_timestamp + 123000,
            test_utc_timestamp + 123400,
            test_utc_timestamp + 123450,
            test_utc_timestamp + 123456,
            test_utc_timestamp + 000001,
            test_utc_timestamp + 000012,
            test_utc_timestamp + 000123,
            test_utc_timestamp + 001234,
            test_utc_timestamp + 012345,
        ];

        run_and_check(col_type, &values, &check_values);
    }

    #[test]
    #[serial]
    fn test_timestamp3() {
        let col_type = "TIMESTAMP(3)";

        let values = vec![
            "'1970-01-01 00:00:01.000'",
            "'2038-01-19 03:14:07.999'",
            "'2022-01-02 03:04:05.0'",
            "'2022-01-02 03:04:05.1'",
            "'2022-01-02 03:04:05.12'",
            "'2022-01-02 03:04:05.123'",
            "'2022-01-02 03:04:05.001'",
            "'2022-01-02 03:04:05.012'",
        ];

        let micros_per_second = 1000000i64;
        let test_utc_timestamp = 1641092645 * micros_per_second;
        let check_values = [
            1000000,
            2147483647 * micros_per_second + 999000,
            test_utc_timestamp,
            test_utc_timestamp + 100000,
            test_utc_timestamp + 120000,
            test_utc_timestamp + 123000,
            test_utc_timestamp + 1000,
            test_utc_timestamp + 12000,
        ];

        run_and_check(col_type, &values, &check_values);
    }

    #[test]
    #[serial]
    fn test_timestamp() {
        let col_type = "TIMESTAMP";

        // since the precesion for TIMESTAMP is 0,
        // '2038-01-19 03:14:07.123456' will be truncated to '2038-01-19 03:14:07'
        let values = vec![
            "'1970-01-01 00:00:01.000'",
            "'2038-01-19 03:14:07.123456'",
            "'2022-01-02 03:04:05.0'",
            "'2022-01-02 03:04:05.1'",
            "'2022-01-02 03:04:05.12'",
            "'2022-01-02 03:04:05.123'",
            "'2022-01-02 03:04:05.001'",
            "'2022-01-02 03:04:05.012'",
        ];

        let micros_per_second = 1000000i64;
        let test_utc_timestamp = 1641092645 * micros_per_second;
        let check_values = [
            1000000,
            2147483647 * micros_per_second,
            test_utc_timestamp,
            test_utc_timestamp,
            test_utc_timestamp,
            test_utc_timestamp,
            test_utc_timestamp,
            test_utc_timestamp,
        ];

        run_and_check(col_type, &values, &check_values);
    }

    fn run_and_check(col_type: &str, values: &[&str], check_values: &[i64]) {
        let runner =
            TestRunner::run_one_col_test(col_type, values, &vec!["SET @@session.time_zone='UTC'"]);

        assert_eq!(runner.insert_events[0].rows.len(), check_values.len());
        for i in 0..check_values.len() {
            assert_eq!(
                runner.insert_events[0].rows[i].column_values[0],
                ColumnValue::Timestamp(check_values[i]),
            );
        }
    }
}
