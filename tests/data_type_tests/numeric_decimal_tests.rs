#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::{assert::test::Assert, test_runner::test::TestRunner};

    // refer to: https://dev.mysql.com/doc/refman/8.0/en/data-types.html
    // refer to: https://dev.mysql.com/doc/refman/8.0/en/fixed-point-types.html

    #[test]
    #[serial]
    fn test_decimal_4_0() {
        // DECIMAL(4,0), binlog: [2 bytes] . [0 bytes]
        let mut runner = TestRunner::new();
        run_decimal_tests(&mut runner, 4, 0);
    }

    #[test]
    #[serial]
    fn test_decimal_4_4() {
        // DECIMAL(4,4), binlog: [0 bytes] . [2 bytes]
        let mut runner = TestRunner::new();
        run_decimal_tests(&mut runner, 4, 4);
    }

    #[test]
    #[serial]
    fn test_decimal_10_0() {
        // DECIMAL(10,0), binlog: [1 byte][4 bytes] . [0 bytes]
        let mut runner = TestRunner::new();
        run_decimal_tests(&mut runner, 10, 0);
    }

    #[test]
    #[serial]
    fn test_decimal_10_10() {
        // DECIMAL(10,10), binlog: [0 bytes] . [4 bytes][1 byte]
        let mut runner = TestRunner::new();
        run_decimal_tests(&mut runner, 10, 10);
    }

    #[test]
    #[serial]
    fn test_decimal_10_4() {
        // DECIMAL(10,4), binlog: [3 bytes] . [2 bytes]
        let mut runner = TestRunner::new();
        run_decimal_tests(&mut runner, 10, 4);
    }

    #[test]
    #[serial]
    fn test_decimal_18_9() {
        // DECIMAL(18,9), binlog: [4 bytes] . [4 bytes]
        let mut runner = TestRunner::new();
        runner.binlog_parse_millis = 500;
        run_decimal_tests(&mut runner, 18, 9);
    }

    #[test]
    #[serial]
    fn test_decimal_27_13() {
        // DECIMAL(27,13), binlog: [3 bytes][4 bytes] . [4 bytes][2 bytes]
        let mut runner = TestRunner::new();
        runner.binlog_parse_millis = 500;
        run_decimal_tests(&mut runner, 27, 13);
    }

    #[test]
    #[serial]
    fn test_decimal_47_25() {
        // DECIMAL(47,25), binlog: [2 bytes][4 bytes][4 bytes] . [4 bytes][4 bytes][4 bytes]
        let mut runner = TestRunner::new();
        runner.binlog_parse_millis = 1000;
        run_decimal_tests(&mut runner, 47, 25);
    }

    fn run_decimal_tests(runner: &mut TestRunner, precision: u8, scale: u8) {
        let data_type = format!("DECIMAL({},{})", precision, scale);
        let prepare_sqls = vec![runner.get_create_table_sql_with_one_field(data_type)];

        let (test_values, check_values) = generate_decimal_values(precision, scale);
        runner.execute_insert_sqls_and_get_binlogs(prepare_sqls, test_values);

        for i in 0..check_values.len() {
            Assert::assert_string_eq(
                &runner.insert_events[i].rows[0].column_values[0],
                check_values[i].clone(),
            );
        }
    }

    fn generate_decimal_values(precision: u8, scale: u8) -> (Vec<String>, Vec<String>) {
        // given precesion = 10, scale = 4, integral = 6
        let integral = precision - scale;
        let mut tmp_values = Vec::new();

        let n_digit_str = |c: char, n: u8| -> String {
            let mut res = String::new();
            for _ in 0..n {
                res.push(c);
            }
            res
        };

        // 9, 99, ... 999999
        for i in 0..integral {
            let intg = n_digit_str('9', i + 1);
            tmp_values.push(intg.clone());
        }

        // 0.9, 0.99, ... 0.9999
        for j in 0..scale {
            let frac = n_digit_str('9', j + 1);
            tmp_values.push("0.".to_string() + &frac);
        }

        // 9.9, 9.99, 99.9, 99.99 ... 999999.9999
        for i in 0..integral {
            let intg = n_digit_str('9', i + 1);
            for j in 0..scale {
                let frac = n_digit_str('9', j + 1);
                tmp_values.push(intg.clone() + "." + &frac);
            }
        }

        // 9.9, 90.09, ... 900000.0009
        for i in 0..integral {
            let intg = n_digit_str('0', i);
            for j in 0..scale {
                let frac = n_digit_str('0', j);
                tmp_values.push("9".to_string() + &intg + "." + &frac + "9");
            }
        }

        // negative values
        let mut values = tmp_values.clone();
        for i in 0..tmp_values.len() {
            values.push("-".to_string() + &tmp_values[i]);
        }

        // 0
        values.push("0".to_string());

        // ["0", "1.1"] -> ["(0)", "(1.1)"]
        let mut test_values = Vec::new();
        for s in values.clone() {
            test_values.push("(".to_string() + &s + ")");
        }

        // if scale = 4
        // ["0", "1.1"] -> ["(0.0000)", "(1.1000)"]
        let mut check_values = Vec::new();
        for s in values.clone() {
            if scale <= 0 {
                check_values.push(s.clone());
            } else if !s.contains(".") {
                check_values.push(s + "." + &n_digit_str('0', scale));
            } else {
                let point_index = s.find(".").unwrap() as u8;
                let append_zeros = n_digit_str('0', scale - (s.len() as u8 - point_index - 1));
                check_values.push(s + &append_zeros);
            }
        }

        (test_values, check_values)
    }
}
