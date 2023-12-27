#[cfg(test)]
mod test {
    use std::vec;

    use mysql_binlog_connector_rust::column::column_value::ColumnValue;
    use serial_test::serial;

    use crate::runner::test_runner::test::TestRunner;

    #[test]
    #[serial]
    fn test_set_name() {
        // refer: https://dev.mysql.com/doc/refman/8.0/en/set.html
        // A SET column can have a maximum of 64 distinct members.
        let col_type =
            "SET('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18')";
        let values = vec![
            "'1'", "'2'", "'3'", "'4'", "'5'", "'6'", "'7'", "'8'", "'9'", "'10'", "'11'", "'12'",
            "'13'", "'14'", "'15'", "'16'", "'17'", "'18'",
        ];
        let check_values = vec![
            1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536,
            131072,
        ];
        run_and_check(col_type, &values, &check_values);
    }

    #[test]
    #[serial]
    fn test_set_ordinal() {
        let col_type = "SET('1', '2', '3', '4', '5')";
        let values = vec!["1", "2", "3", "4", "5"];
        let check_values = vec![1, 2, 3, 4, 5];
        run_and_check(col_type, &values, &check_values);
    }

    fn run_and_check(col_type: &str, values: &Vec<&str>, check_values: &Vec<u64>) {
        let runner = TestRunner::run_one_col_test(col_type, values, &vec![]);
        for i in 0..check_values.len() {
            assert_eq!(
                runner.insert_events[0].rows[i].column_values[0],
                ColumnValue::Set(check_values[i]),
            );
        }
    }
}
