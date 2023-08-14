#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::runner::{assert::test::Assert, test_runner::test::TestRunner};

    // refer to: https://dev.mysql.com/doc/refman/8.0/en/data-types.html

    #[test]
    #[serial]
    fn test_tinyint() {
        let runner = run_numeric_tests("TINYINT", &vec!["-128", "127"]);
        Assert::assert_numeric_eq(&runner.insert_events[0].rows[0].column_values[0], -128);
        Assert::assert_numeric_eq(&runner.insert_events[0].rows[1].column_values[0], 127);
    }

    #[test]
    #[serial]
    fn test_tinyint_unsigned() {
        let runner = run_numeric_tests("TINYINT UNSIGNED", &vec!["255", "127"]);
        Assert::assert_numeric_eq(&runner.insert_events[0].rows[0].column_values[0], -1);
        Assert::assert_numeric_eq(&runner.insert_events[0].rows[1].column_values[0], 127);
    }

    #[test]
    #[serial]
    fn test_smallint() {
        let runner = run_numeric_tests("SMALLINT", &vec!["-32768", "32767"]);
        Assert::assert_numeric_eq(&runner.insert_events[0].rows[0].column_values[0], -32768);
        Assert::assert_numeric_eq(&runner.insert_events[0].rows[1].column_values[0], 32767);
    }

    #[test]
    #[serial]
    fn test_smallint_unsigned() {
        let runner = run_numeric_tests("SMALLINT UNSIGNED", &vec!["65535", "32767"]);
        Assert::assert_numeric_eq(&runner.insert_events[0].rows[0].column_values[0], -1);
        Assert::assert_numeric_eq(&runner.insert_events[0].rows[1].column_values[0], 32767);
    }

    #[test]
    #[serial]
    fn test_mediumint() {
        let runner = run_numeric_tests("MEDIUMINT", &vec!["-8388608", "8388607"]);
        Assert::assert_numeric_eq(&runner.insert_events[0].rows[0].column_values[0], -8388608);
        Assert::assert_numeric_eq(&runner.insert_events[0].rows[1].column_values[0], 8388607);
    }

    #[test]
    #[serial]
    fn test_mediumint_unsigned() {
        let runner = run_numeric_tests("MEDIUMINT UNSIGNED", &vec!["16777215", "8388607"]);
        Assert::assert_numeric_eq(&runner.insert_events[0].rows[0].column_values[0], -1);
        Assert::assert_numeric_eq(&runner.insert_events[0].rows[1].column_values[0], 8388607);
    }

    #[test]
    #[serial]
    fn test_int() {
        let runner = run_numeric_tests("INT", &vec!["-2147483648", "2147483647"]);
        Assert::assert_numeric_eq(
            &runner.insert_events[0].rows[0].column_values[0],
            -2147483648,
        );
        Assert::assert_numeric_eq(
            &runner.insert_events[0].rows[1].column_values[0],
            2147483647,
        );
    }

    #[test]
    #[serial]
    fn test_int_unsigned() {
        let runner = run_numeric_tests("INT UNSIGNED", &vec!["4294967295", "2147483647"]);
        Assert::assert_numeric_eq(&runner.insert_events[0].rows[0].column_values[0], -1);
        Assert::assert_numeric_eq(
            &runner.insert_events[0].rows[1].column_values[0],
            2147483647,
        );
    }

    #[test]
    #[serial]
    fn test_bigint() {
        let runner = run_numeric_tests(
            "BIGINT",
            &vec!["-9223372036854775808", "9223372036854775807"],
        );
        Assert::assert_numeric_eq(
            &runner.insert_events[0].rows[0].column_values[0],
            -9223372036854775808,
        );
        Assert::assert_numeric_eq(
            &runner.insert_events[0].rows[1].column_values[0],
            9223372036854775807,
        );
    }

    #[test]
    #[serial]
    fn test_bigint_unsigned() {
        let runner = run_numeric_tests(
            "BIGINT UNSIGNED",
            &vec!["18446744073709551615", "9223372036854775807"],
        );
        Assert::assert_numeric_eq(&runner.insert_events[0].rows[0].column_values[0], -1);
        Assert::assert_numeric_eq(
            &runner.insert_events[0].rows[1].column_values[0],
            9223372036854775807,
        );
    }

    #[test]
    #[serial]
    fn test_float() {
        let runner = run_numeric_tests("FLOAT(10,5)", &vec!["1234.12345"]);
        Assert::assert_float_eq(
            &runner.insert_events[0].rows[0].column_values[0],
            1234.12345,
        );
    }

    #[test]
    #[serial]
    fn test_double() {
        let runner = run_numeric_tests("DOUBLE(20, 10)", &vec!["1234567890.0123456789"]);
        Assert::assert_double_eq(
            &runner.insert_events[0].rows[0].column_values[0],
            1234567890.0123456789,
        );
    }

    fn run_numeric_tests(col_type: &str, values: &[&str]) -> TestRunner {
        TestRunner::run_one_col_test(col_type, values, &vec![])
    }
}
