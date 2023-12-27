#[cfg(test)]
mod test {
    use mysql_binlog_connector_rust::column::column_value::ColumnValue;
    use serial_test::serial;

    use crate::runner::test_runner::test::TestRunner;

    #[test]
    #[serial]
    fn test_date() {
        let col_type = "DATE";
        let values = vec!["'1000-01-01'", "'9999-12-31'"];
        let check_values = ["1000-01-01", "9999-12-31"];

        let runner =
            TestRunner::run_one_col_test(col_type, &values, &vec!["SET @@session.time_zone='UTC'"]);

        assert_eq!(runner.insert_events[0].rows.len(), check_values.len());
        for i in 0..check_values.len() {
            assert_eq!(
                runner.insert_events[0].rows[i].column_values[0],
                ColumnValue::Date(check_values[i].to_string())
            );
        }
    }
}
