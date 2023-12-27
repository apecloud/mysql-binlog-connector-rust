#[cfg(test)]
mod test {

    use mysql_binlog_connector_rust::column::column_value::ColumnValue;
    use serial_test::serial;

    use crate::runner::test_runner::test::TestRunner;

    #[test]
    #[serial]
    fn test_bit3() {
        let col_type = "BIT(3)";
        let values = vec!["0", "1", "2", "3", "4", "5", "6", "7"];
        run_and_check(col_type, &values);
    }

    #[test]
    #[serial]
    fn test_bit64() {
        let col_type = "BIT(64)";
        let values = vec![
            "1234567890123",
            "2345678901234",
            "3456789012345",
            "4567890123456",
            "5678901234567",
            "6789012345678",
            "7890123456789",
        ];
        run_and_check(col_type, &values);
    }

    fn run_and_check(col_type: &str, values: &Vec<&str>) {
        let runner = TestRunner::run_one_col_test(col_type, values, &vec![]);
        for i in 0..values.len() {
            let value: u64 = values[i].parse::<u64>().unwrap();
            assert_eq!(
                runner.insert_events[0].rows[i].column_values[0],
                ColumnValue::Bit(value)
            );
        }
    }
}
