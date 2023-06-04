#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::{assert::test::Assert, test_runner::test::TestRunner};

    #[test]
    #[serial]
    fn test_bit3() {
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

        Assert::assert_numeric_eq(&runner.insert_events[0].rows[0].column_values[0], 0);
        Assert::assert_numeric_eq(&runner.insert_events[1].rows[0].column_values[0], 1);
        Assert::assert_numeric_eq(&runner.insert_events[2].rows[0].column_values[0], 2);
        Assert::assert_numeric_eq(&runner.insert_events[3].rows[0].column_values[0], 3);
        Assert::assert_numeric_eq(&runner.insert_events[4].rows[0].column_values[0], 4);
        Assert::assert_numeric_eq(&runner.insert_events[5].rows[0].column_values[0], 5);
        Assert::assert_numeric_eq(&runner.insert_events[6].rows[0].column_values[0], 6);
        Assert::assert_numeric_eq(&runner.insert_events[7].rows[0].column_values[0], 7);
    }

    #[test]
    #[serial]
    fn test_bit64() {
        let mut runner = TestRunner::new();
        let prepare_sqls = vec![runner.get_create_table_sql_with_one_field("BIT(64)".to_string())];
        let values = vec![
            "(1234567890123)".to_string(),
            "(2345678901234)".to_string(),
            "(3456789012345)".to_string(),
            "(4567890123456)".to_string(),
            "(5678901234567)".to_string(),
            "(6789012345678)".to_string(),
            "(7890123456789)".to_string(),
        ];
        runner.execute_insert_sqls_and_get_binlogs(prepare_sqls, values);

        Assert::assert_numeric_eq(
            &runner.insert_events[0].rows[0].column_values[0],
            1234567890123,
        );
        Assert::assert_numeric_eq(
            &runner.insert_events[1].rows[0].column_values[0],
            2345678901234,
        );
        Assert::assert_numeric_eq(
            &runner.insert_events[2].rows[0].column_values[0],
            3456789012345,
        );
        Assert::assert_numeric_eq(
            &runner.insert_events[3].rows[0].column_values[0],
            4567890123456,
        );
        Assert::assert_numeric_eq(
            &runner.insert_events[4].rows[0].column_values[0],
            5678901234567,
        );
        Assert::assert_numeric_eq(
            &runner.insert_events[5].rows[0].column_values[0],
            6789012345678,
        );
        Assert::assert_numeric_eq(
            &runner.insert_events[6].rows[0].column_values[0],
            7890123456789,
        );
    }
}
