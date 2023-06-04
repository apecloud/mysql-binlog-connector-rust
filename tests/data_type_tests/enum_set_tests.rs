#[cfg(test)]
mod test {
    use std::vec;

    use serial_test::serial;

    use crate::{assert::test::Assert, test_runner::test::TestRunner};

    #[test]
    #[serial]
    fn test_enum() {
        // refer: https://dev.mysql.com/doc/refman/8.0/en/enum.html
        // An ENUM column can have a maximum of 65,535 distinct elements.
        let data_type = "ENUM('x-small', 'small', 'medium', 'large', 'x-large')".to_string();
        let test_values = vec![
            "('x-small')".to_string(),
            "('small')".to_string(),
            "('medium')".to_string(),
            "('large')".to_string(),
            "('x-large')".to_string(),
        ];
        let check_values = vec![1, 2, 3, 4, 5];

        run_numeric_tests(data_type, test_values, check_values);
    }

    #[test]
    #[serial]
    fn test_set_name() {
        // refer: https://dev.mysql.com/doc/refman/8.0/en/set.html
        // A SET column can have a maximum of 64 distinct members.
        let data_type =
            "SET('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18')".to_string();
        let test_values = vec![
            "('1')".to_string(),
            "('2')".to_string(),
            "('3')".to_string(),
            "('4')".to_string(),
            "('5')".to_string(),
            "('6')".to_string(),
            "('7')".to_string(),
            "('8')".to_string(),
            "('9')".to_string(),
            "('10')".to_string(),
            "('11')".to_string(),
            "('12')".to_string(),
            "('13')".to_string(),
            "('14')".to_string(),
            "('15')".to_string(),
            "('16')".to_string(),
            "('17')".to_string(),
            "('18')".to_string(),
        ];
        let check_values = vec![
            1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536,
            131072,
        ];

        run_numeric_tests(data_type, test_values, check_values);
    }

    #[test]
    #[serial]
    fn test_set_ordinal() {
        let data_type = "SET('1', '2', '3', '4', '5')".to_string();
        let test_values = vec![
            "(1)".to_string(),
            "(2)".to_string(),
            "(3)".to_string(),
            "(4)".to_string(),
            "(5)".to_string(),
        ];
        let check_values = vec![1, 2, 3, 4, 5];
        run_numeric_tests(data_type, test_values, check_values);
    }

    fn run_numeric_tests(data_type: String, test_values: Vec<String>, check_values: Vec<u64>) {
        let mut runner = TestRunner::new();
        let prepare_sqls = vec![runner.get_create_table_sql_with_one_field(data_type)];
        runner.execute_insert_sqls_and_get_binlogs(prepare_sqls, test_values);

        for i in 0..check_values.len() {
            Assert::assert_unsigned_numeric_eq(
                &runner.insert_events[i].rows[0].column_values[0],
                check_values[i],
            );
        }
    }
}
