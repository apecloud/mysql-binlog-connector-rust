#[cfg(test)]
mod test {

    use std::i128;

    use serial_test::serial;

    use crate::runner::{assert::test::Assert, test_runner::test::TestRunner};

    #[test]
    #[serial]
    fn test_bit3() {
        let col_type = "BIT(3)";
        let values = vec!["0", "1", "2", "3", "4", "5", "6", "7"];
        run_bit_tests(col_type, &values);
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
        run_bit_tests(col_type, &values);
    }

    fn run_bit_tests(col_type: &str, values: &Vec<&str>) {
        let runner = TestRunner::run_one_col_test(col_type, values, &vec![]);
        for i in 0..values.len() {
            Assert::assert_numeric_eq(
                &runner.insert_events[0].rows[i].column_values[0],
                values[i].parse::<i128>().unwrap(),
            );
        }
    }
}
