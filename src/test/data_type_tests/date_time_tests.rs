#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test::{assert::Assert, test_runner::test::TestRunner};

    #[test]
    #[serial]
    fn test_datetime6() {
        let mut runner = TestRunner::new();
        // https://dev.mysql.com/doc/refman/8.0/en/datetime.html
        let prepare_sqls =
            vec![runner.get_create_table_sql_with_one_field("DATETIME(6)".to_string())];
        // With the fractional part included, the format for these values is 'YYYY-MM-DD hh:mm:ss[.fraction]',
        // the range for DATETIME values is '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'
        let values = vec!["('1000-01-01 00:00:00.000000'),
                    ('9999-12-31 23:59:59.999999'),
                    ('2022-01-02 03:04:05.0'),
                    ('2022-01-02 03:04:05.1'),
                    ('2022-01-02 03:04:05.12'),
                    ('2022-01-02 03:04:05.123'),
                    ('2022-01-02 03:04:05.1234'),
                    ('2022-01-02 03:04:05.12345'),
                    ('2022-01-02 03:04:05.123456'),
                    ('2022-01-02 03:04:05.000001'),
                    ('2022-01-02 03:04:05.000012'),
                    ('2022-01-02 03:04:05.000123'),
                    ('2022-01-02 03:04:05.001234'),
                    ('2022-01-02 03:04:05.012345')"
            .to_string()];
        runner.execute_insert_sqls_and_get_binlogs(prepare_sqls, values);

        let check_values = [
            "1000-01-01 00:00:00.000000".to_string(),
            "9999-12-31 23:59:59.999999".to_string(),
            "2022-01-02 03:04:05.000000".to_string(),
            "2022-01-02 03:04:05.100000".to_string(),
            "2022-01-02 03:04:05.120000".to_string(),
            "2022-01-02 03:04:05.123000".to_string(),
            "2022-01-02 03:04:05.123400".to_string(),
            "2022-01-02 03:04:05.123450".to_string(),
            "2022-01-02 03:04:05.123456".to_string(),
            "2022-01-02 03:04:05.000001".to_string(),
            "2022-01-02 03:04:05.000012".to_string(),
            "2022-01-02 03:04:05.000123".to_string(),
            "2022-01-02 03:04:05.001234".to_string(),
            "2022-01-02 03:04:05.012345".to_string(),
        ];

        assert_eq!(runner.insert_events[0].rows.len(), check_values.len());
        for i in 0..check_values.len() {
            Assert::assert_string_eq(
                &runner.insert_events[0].rows[i].column_values[0],
                check_values[i].clone(),
            );
        }
    }

    #[test]
    #[serial]
    fn test_datetime3() {
        let mut runner = TestRunner::new();
        let prepare_sqls =
            vec![runner.get_create_table_sql_with_one_field("DATETIME(3)".to_string())];
        let values = vec!["('1000-01-01 00:00:00.000'),
                    ('9999-12-31 23:59:59.999'),
                    ('2022-01-02 03:04:05.0'),
                    ('2022-01-02 03:04:05.1'),
                    ('2022-01-02 03:04:05.12'),
                    ('2022-01-02 03:04:05.123'),
                    ('2022-01-02 03:04:05.001'),
                    ('2022-01-02 03:04:05.012')"
            .to_string()];
        runner.execute_insert_sqls_and_get_binlogs(prepare_sqls, values);

        let check_values = [
            "1000-01-01 00:00:00.000000".to_string(),
            "9999-12-31 23:59:59.999000".to_string(),
            "2022-01-02 03:04:05.000000".to_string(),
            "2022-01-02 03:04:05.100000".to_string(),
            "2022-01-02 03:04:05.120000".to_string(),
            "2022-01-02 03:04:05.123000".to_string(),
            "2022-01-02 03:04:05.001000".to_string(),
            "2022-01-02 03:04:05.012000".to_string(),
        ];

        assert_eq!(runner.insert_events[0].rows.len(), check_values.len());
        for i in 0..check_values.len() {
            Assert::assert_string_eq(
                &runner.insert_events[0].rows[i].column_values[0],
                check_values[i].clone(),
            );
        }
    }

    #[test]
    #[serial]
    fn test_datetime() {
        let mut runner = TestRunner::new();
        // https://dev.mysql.com/doc/refman/8.0/en/datetime.html
        let prepare_sqls = vec![runner.get_create_table_sql_with_one_field("DATETIME".to_string())];
        let values = vec!["('1000-01-01 00:00:00.000000'),('9999-12-31 23:59:59')".to_string()];
        runner.execute_insert_sqls_and_get_binlogs(prepare_sqls, values);

        let check_values = [
            "1000-01-01 00:00:00.000000".to_string(),
            "9999-12-31 23:59:59.000000".to_string(),
        ];

        assert_eq!(runner.insert_events[0].rows.len(), check_values.len());
        for i in 0..check_values.len() {
            Assert::assert_string_eq(
                &runner.insert_events[0].rows[i].column_values[0],
                check_values[i].clone(),
            );
        }
    }

    #[test]
    #[serial]
    fn test_time6() {
        let mut runner = TestRunner::new();
        let prepare_sqls = vec![runner.get_create_table_sql_with_one_field("TIME(6)".to_string())];
        let values = vec!["('00:00:00.000000'),
                    ('23:59:59.999999'),
                    ('03:04:05.0'),
                    ('03:04:05.1'),
                    ('03:04:05.12'),
                    ('03:04:05.123'),
                    ('03:04:05.1234'),
                    ('03:04:05.12345'),
                    ('03:04:05.123456'),
                    ('03:04:05.000001'),
                    ('03:04:05.000012'),
                    ('03:04:05.000123'),
                    ('03:04:05.001234'),
                    ('03:04:05.012345')"
            .to_string()];
        runner.execute_insert_sqls_and_get_binlogs(prepare_sqls, values);

        let check_values = [
            "00:00:00.000000".to_string(),
            "23:59:59.999999".to_string(),
            "03:04:05.000000".to_string(),
            "03:04:05.100000".to_string(),
            "03:04:05.120000".to_string(),
            "03:04:05.123000".to_string(),
            "03:04:05.123400".to_string(),
            "03:04:05.123450".to_string(),
            "03:04:05.123456".to_string(),
            "03:04:05.000001".to_string(),
            "03:04:05.000012".to_string(),
            "03:04:05.000123".to_string(),
            "03:04:05.001234".to_string(),
            "03:04:05.012345".to_string(),
        ];

        assert_eq!(runner.insert_events[0].rows.len(), check_values.len());
        for i in 0..check_values.len() {
            Assert::assert_string_eq(
                &runner.insert_events[0].rows[i].column_values[0],
                check_values[i].clone(),
            );
        }
    }

    #[test]
    #[serial]
    fn test_time3() {
        let mut runner = TestRunner::new();
        let prepare_sqls = vec![runner.get_create_table_sql_with_one_field("TIME(3)".to_string())];
        let values = vec!["('00:00:00.000'),
                    ('23:59:59.999'),
                    ('03:04:05.0'),
                    ('03:04:05.1'),
                    ('03:04:05.12'),
                    ('03:04:05.123'),
                    ('03:04:05.001'),
                    ('03:04:05.012')"
            .to_string()];
        runner.execute_insert_sqls_and_get_binlogs(prepare_sqls, values);

        let check_values = [
            "00:00:00.000000".to_string(),
            "23:59:59.999000".to_string(),
            "03:04:05.000000".to_string(),
            "03:04:05.100000".to_string(),
            "03:04:05.120000".to_string(),
            "03:04:05.123000".to_string(),
            "03:04:05.001000".to_string(),
            "03:04:05.012000".to_string(),
        ];

        assert_eq!(runner.insert_events[0].rows.len(), check_values.len());
        for i in 0..check_values.len() {
            Assert::assert_string_eq(
                &runner.insert_events[0].rows[i].column_values[0],
                check_values[i].clone(),
            );
        }
    }

    #[test]
    #[serial]
    fn test_time() {
        let mut runner = TestRunner::new();
        let prepare_sqls = vec![runner.get_create_table_sql_with_one_field("TIME".to_string())];
        let values = vec!["('00:00:00'),('23:59:59')".to_string()];
        runner.execute_insert_sqls_and_get_binlogs(prepare_sqls, values);
        // the db values are actual: ["00:00:00", "23:59:59"]
        // the parsed binlog values are ["00:00:00.000000", "23:59:59.000000"]
        // we keep the 6 pending zeros since we don't know the field precision when parsing binlog
        let check_values = ["00:00:00.000000".to_string(), "23:59:59.000000".to_string()];

        assert_eq!(runner.insert_events[0].rows.len(), check_values.len());
        for i in 0..check_values.len() {
            Assert::assert_string_eq(
                &runner.insert_events[0].rows[i].column_values[0],
                check_values[i].clone(),
            );
        }
    }

    #[test]
    #[serial]
    fn test_date() {
        let mut runner = TestRunner::new();
        let prepare_sqls = vec![runner.get_create_table_sql_with_one_field("DATE".to_string())];
        let values = vec!["('1000-01-01'),('9999-12-31')".to_string()];
        runner.execute_insert_sqls_and_get_binlogs(prepare_sqls, values);

        let check_values = ["1000-01-01".to_string(), "9999-12-31".to_string()];

        assert_eq!(runner.insert_events[0].rows.len(), check_values.len());
        for i in 0..check_values.len() {
            Assert::assert_string_eq(
                &runner.insert_events[0].rows[i].column_values[0],
                check_values[i].clone(),
            );
        }
    }

    #[test]
    #[serial]
    fn test_year() {
        let mut runner = TestRunner::new();
        let prepare_sqls = vec![runner.get_create_table_sql_with_one_field("YEAR".to_string())];
        let values = vec!["('1901'),('2155')".to_string()];
        runner.execute_insert_sqls_and_get_binlogs(prepare_sqls, values);

        let check_values = [1901, 2155];

        assert_eq!(runner.insert_events[0].rows.len(), check_values.len());
        for i in 0..check_values.len() {
            Assert::assert_numeric_eq(
                &runner.insert_events[0].rows[i].column_values[0],
                check_values[i],
            );
        }
    }

    #[test]
    #[serial]
    fn test_timestamp6() {
        let mut runner = TestRunner::new();
        let prepare_sqls = vec![
            runner.get_create_table_sql_with_one_field("TIMESTAMP(6)".to_string()),
            "SET @@session.time_zone='UTC'".to_string(),
        ];
        // refer: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
        // The range is '1970-01-01 00:00:01.000000' UTC to '2038-01-19 03:14:07.999999' UTC.
        // TIMESTAMP values are stored as the number of seconds since the epoch ('1970-01-01 00:00:00' UTC).
        let values = vec!["('1970-01-01 00:00:01.000000'),
                    ('2038-01-19 03:14:07.999999'),
                    ('2022-01-02 03:04:05.0'),
                    ('2022-01-02 03:04:05.1'),
                    ('2022-01-02 03:04:05.12'),
                    ('2022-01-02 03:04:05.123'),
                    ('2022-01-02 03:04:05.1234'),
                    ('2022-01-02 03:04:05.12345'),
                    ('2022-01-02 03:04:05.123456'),
                    ('2022-01-02 03:04:05.000001'),
                    ('2022-01-02 03:04:05.000012'),
                    ('2022-01-02 03:04:05.000123'),
                    ('2022-01-02 03:04:05.001234'),
                    ('2022-01-02 03:04:05.012345')"
            .to_string()];
        runner.execute_insert_sqls_and_get_binlogs(prepare_sqls, values);

        // MySQL converts TIMESTAMP values from the current time zone to UTC for storage,
        // and back from UTC to the current time zone for retrieval.
        // (This does not occur for other types such as DATETIME.)
        let micros_per_second = 1000000u64;
        // 2147483647 is the timestamp (UTC) for 2022-01-02 03:04:05 (UTC)
        // 1641092645 is the timestamp (UTC) for 2022-01-02 03:04:05 (UTC)
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

        assert_eq!(runner.insert_events[0].rows.len(), check_values.len());
        for i in 0..check_values.len() {
            Assert::assert_timestamp_eq(
                &runner.insert_events[0].rows[i].column_values[0],
                check_values[i],
            );
        }
    }

    #[test]
    #[serial]
    fn test_timestamp3() {
        let mut runner = TestRunner::new();
        let prepare_sqls = vec![
            runner.get_create_table_sql_with_one_field("TIMESTAMP(3)".to_string()),
            "SET @@session.time_zone='UTC'".to_string(),
        ];

        let values = vec!["('1970-01-01 00:00:01.000'),
                    ('2038-01-19 03:14:07.999'),
                    ('2022-01-02 03:04:05.0'),
                    ('2022-01-02 03:04:05.1'),
                    ('2022-01-02 03:04:05.12'),
                    ('2022-01-02 03:04:05.123'),
                    ('2022-01-02 03:04:05.001'),
                    ('2022-01-02 03:04:05.012')"
            .to_string()];
        runner.execute_insert_sqls_and_get_binlogs(prepare_sqls, values);

        let micros_per_second = 1000000u64;
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

        assert_eq!(runner.insert_events[0].rows.len(), check_values.len());
        for i in 0..check_values.len() {
            Assert::assert_timestamp_eq(
                &runner.insert_events[0].rows[i].column_values[0],
                check_values[i],
            );
        }
    }

    #[test]
    #[serial]
    fn test_timestamp() {
        let mut runner = TestRunner::new();
        let prepare_sqls = vec![
            runner.get_create_table_sql_with_one_field("TIMESTAMP".to_string()),
            "SET @@session.time_zone='UTC'".to_string(),
        ];

        // since the precesion for TIMESTAMP is 0,
        // '2038-01-19 03:14:07.123456' will be truncated to '2038-01-19 03:14:07'
        let values = vec!["('1970-01-01 00:00:01.000'),
                    ('2038-01-19 03:14:07.123456'),
                    ('2022-01-02 03:04:05.0'),
                    ('2022-01-02 03:04:05.1'),
                    ('2022-01-02 03:04:05.12'),
                    ('2022-01-02 03:04:05.123'),
                    ('2022-01-02 03:04:05.001'),
                    ('2022-01-02 03:04:05.012')"
            .to_string()];
        runner.execute_insert_sqls_and_get_binlogs(prepare_sqls, values);

        let micros_per_second = 1000000u64;
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

        assert_eq!(runner.insert_events[0].rows.len(), check_values.len());
        for i in 0..check_values.len() {
            Assert::assert_timestamp_eq(
                &runner.insert_events[0].rows[i].column_values[0],
                check_values[i],
            );
        }
    }
}
