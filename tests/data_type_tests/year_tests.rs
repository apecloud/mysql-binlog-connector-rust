#[cfg(test)]
mod test {
    use mysql_binlog_connector_rust::column::column_value::ColumnValue;
    use serial_test::serial;

    use crate::runner::test_runner::test::TestRunner;

    #[test]
    #[serial]
    fn test_year() {
        let runner = TestRunner::run_one_col_test("YEAR", &vec!["'1901'", "'2155'"], &vec![]);
        let check_values = [1901, 2155];
        assert_eq!(runner.insert_events[0].rows.len(), check_values.len());
        for i in 0..check_values.len() {
            assert_eq!(
                runner.insert_events[0].rows[i].column_values[0],
                ColumnValue::Year(check_values[i]),
            );
        }
    }
}
