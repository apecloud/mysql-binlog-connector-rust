#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test::{assert::Assert, test_runner::test::TestRunner};

    #[test]
    #[serial]
    fn test_bit() {
        let mut runner = TestRunner::new();
        let prepare_sqls = vec![runner.get_create_table_sql_with_one_field("BIT(3)".to_string())];
        let values = vec![
            "(0)".to_string(),
            "(1)".to_string(),
            "(2)".to_string(),
            "(3)".to_string(),
            "(4)".to_string(),
            "(5)".to_string(),
            "(6)".to_string(),
            "(7)".to_string(),
        ];
        runner.execute_insert_sqls_and_get_binlogs(prepare_sqls, values);

        Assert::assert_bit_eq(
            &runner.insert_events[0].rows[0].column_values[0],
            vec![false, false, false],
        );
        Assert::assert_bit_eq(
            &runner.insert_events[1].rows[0].column_values[0],
            vec![true, false, false],
        );
        Assert::assert_bit_eq(
            &runner.insert_events[2].rows[0].column_values[0],
            vec![false, true, false],
        );
        Assert::assert_bit_eq(
            &runner.insert_events[3].rows[0].column_values[0],
            vec![true, true, false],
        );
        Assert::assert_bit_eq(
            &runner.insert_events[4].rows[0].column_values[0],
            vec![false, false, true],
        );
        Assert::assert_bit_eq(
            &runner.insert_events[5].rows[0].column_values[0],
            vec![true, false, true],
        );
        Assert::assert_bit_eq(
            &runner.insert_events[6].rows[0].column_values[0],
            vec![false, true, true],
        );
        Assert::assert_bit_eq(
            &runner.insert_events[7].rows[0].column_values[0],
            vec![true, true, true],
        );
    }
}
