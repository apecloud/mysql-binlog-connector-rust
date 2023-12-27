#[cfg(test)]
mod test {

    use mysql_binlog_connector_rust::column::column_value::ColumnValue;
    use serial_test::serial;

    use crate::runner::test_runner::test::TestRunner;

    // refer to: https://dev.mysql.com/doc/refman/8.0/en/data-types.html

    #[test]
    #[serial]
    fn test_tinyint() {
        let runner = run("TINYINT", &vec!["-128", "127"]);
        assert_eq!(
            runner.insert_events[0].rows[0].column_values[0],
            ColumnValue::Tiny(-128)
        );
        assert_eq!(
            runner.insert_events[0].rows[1].column_values[0],
            ColumnValue::Tiny(127)
        );
    }

    #[test]
    #[serial]
    fn test_tinyint_unsigned() {
        let runner = run("TINYINT UNSIGNED", &vec!["255", "127"]);
        assert_eq!(
            runner.insert_events[0].rows[0].column_values[0],
            ColumnValue::Tiny(-1)
        );
        assert_eq!(
            runner.insert_events[0].rows[1].column_values[0],
            ColumnValue::Tiny(127)
        );
    }

    #[test]
    #[serial]
    fn test_smallint() {
        let runner = run("SMALLINT", &vec!["-32768", "32767"]);
        assert_eq!(
            runner.insert_events[0].rows[0].column_values[0],
            ColumnValue::Short(-32768)
        );
        assert_eq!(
            runner.insert_events[0].rows[1].column_values[0],
            ColumnValue::Short(32767)
        );
    }

    #[test]
    #[serial]
    fn test_smallint_unsigned() {
        let runner = run("SMALLINT UNSIGNED", &vec!["65535", "32767"]);
        assert_eq!(
            runner.insert_events[0].rows[0].column_values[0],
            ColumnValue::Short(-1)
        );
        assert_eq!(
            runner.insert_events[0].rows[1].column_values[0],
            ColumnValue::Short(32767)
        );
    }

    #[test]
    #[serial]
    fn test_mediumint() {
        let runner = run("MEDIUMINT", &vec!["-8388608", "8388607"]);
        assert_eq!(
            runner.insert_events[0].rows[0].column_values[0],
            ColumnValue::Long(-8388608)
        );
        assert_eq!(
            runner.insert_events[0].rows[1].column_values[0],
            ColumnValue::Long(8388607)
        );
    }

    #[test]
    #[serial]
    fn test_mediumint_unsigned() {
        let runner = run("MEDIUMINT UNSIGNED", &vec!["16777215", "8388607"]);
        assert_eq!(
            runner.insert_events[0].rows[0].column_values[0],
            ColumnValue::Long(-1)
        );
        assert_eq!(
            runner.insert_events[0].rows[1].column_values[0],
            ColumnValue::Long(8388607)
        );
    }

    #[test]
    #[serial]
    fn test_int() {
        let runner = run("INT", &vec!["-2147483648", "2147483647"]);
        assert_eq!(
            runner.insert_events[0].rows[0].column_values[0],
            ColumnValue::Long(-2147483648)
        );
        assert_eq!(
            runner.insert_events[0].rows[1].column_values[0],
            ColumnValue::Long(2147483647)
        );
    }

    #[test]
    #[serial]
    fn test_int_unsigned() {
        let runner = run("INT UNSIGNED", &vec!["4294967295", "2147483647"]);
        assert_eq!(
            runner.insert_events[0].rows[0].column_values[0],
            ColumnValue::Long(-1)
        );
        assert_eq!(
            runner.insert_events[0].rows[1].column_values[0],
            ColumnValue::Long(2147483647)
        );
    }

    #[test]
    #[serial]
    fn test_bigint() {
        let runner = run(
            "BIGINT",
            &vec!["-9223372036854775808", "9223372036854775807"],
        );
        assert_eq!(
            runner.insert_events[0].rows[0].column_values[0],
            ColumnValue::LongLong(-9223372036854775808)
        );
        assert_eq!(
            runner.insert_events[0].rows[1].column_values[0],
            ColumnValue::LongLong(9223372036854775807)
        );
    }

    #[test]
    #[serial]
    fn test_bigint_unsigned() {
        let runner = run(
            "BIGINT UNSIGNED",
            &vec!["18446744073709551615", "9223372036854775807"],
        );
        assert_eq!(
            runner.insert_events[0].rows[0].column_values[0],
            ColumnValue::LongLong(-1)
        );
        assert_eq!(
            runner.insert_events[0].rows[1].column_values[0],
            ColumnValue::LongLong(9223372036854775807)
        );
    }

    #[test]
    #[serial]
    fn test_float() {
        let runner = run("FLOAT(10,5)", &vec!["1234.12345"]);
        assert_eq!(
            runner.insert_events[0].rows[0].column_values[0],
            ColumnValue::Float(1234.12345),
        );
    }

    #[test]
    #[serial]
    fn test_double() {
        let runner = run("DOUBLE(20, 10)", &vec!["1234567890.0123456789"]);
        assert_eq!(
            runner.insert_events[0].rows[0].column_values[0],
            ColumnValue::Double(1234567890.0123456789)
        );
    }

    fn run(col_type: &str, values: &[&str]) -> TestRunner {
        TestRunner::run_one_col_test(col_type, values, &vec![])
    }
}
