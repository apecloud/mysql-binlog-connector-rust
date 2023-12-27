#[cfg(test)]
mod test {
    use std::vec;

    use mysql_binlog_connector_rust::column::column_value::ColumnValue;
    use serial_test::serial;

    use crate::runner::test_runner::test::TestRunner;

    #[test]
    #[serial]
    fn test_enum() {
        // refer: https://dev.mysql.com/doc/refman/8.0/en/enum.html
        // An ENUM column can have a maximum of 65,535 distinct elements.
        let col_type = "ENUM('x-small', 'small', 'medium', 'large', 'x-large')";
        let values = vec!["'x-small'", "'small'", "'medium'", "'large'", "'x-large'"];
        let check_values = vec![1, 2, 3, 4, 5];
        run_and_check(col_type, &values, &check_values);
    }

    fn run_and_check(col_type: &str, values: &Vec<&str>, check_values: &Vec<u32>) {
        let runner = TestRunner::run_one_col_test(col_type, values, &vec![]);
        for i in 0..check_values.len() {
            assert_eq!(
                runner.insert_events[0].rows[i].column_values[0],
                ColumnValue::Enum(check_values[i]),
            );
        }
    }
}
