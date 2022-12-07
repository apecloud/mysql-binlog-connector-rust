#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test::{
        assert::Assert,
        test_common::test::{TestCommon, I_EVENTS},
    };

    // refer to: https://dev.mysql.com/doc/refman/8.0/en/data-types.html

    #[test]
    #[serial]
    fn test_tinyint() {
        TestCommon::before_dml();
        let prepare_sqls = vec![TestCommon::get_create_table_sql_with_one_field(
            "TINYINT".to_string(),
        )];
        let values = vec!["(-128),(127)".to_string()];
        TestCommon::execute_insert_sqls_and_get_binlogs(prepare_sqls, values);
        unsafe {
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[0].column_values[0], -128);
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[1].column_values[0], 127);
        }
    }

    #[test]
    #[serial]
    fn test_tinyint_unsigned() {
        TestCommon::before_dml();
        let prepare_sqls = vec![TestCommon::get_create_table_sql_with_one_field(
            "TINYINT UNSIGNED".to_string(),
        )];
        let values = vec!["(255),(127)".to_string()];
        TestCommon::execute_insert_sqls_and_get_binlogs(prepare_sqls, values);
        unsafe {
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[0].column_values[0], -1);
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[1].column_values[0], 127);
        }
    }

    #[test]
    #[serial]
    fn test_smallint() {
        TestCommon::before_dml();
        let prepare_sqls = vec![TestCommon::get_create_table_sql_with_one_field(
            "SMALLINT".to_string(),
        )];
        let values = vec!["(-32768),(32767)".to_string()];
        TestCommon::execute_insert_sqls_and_get_binlogs(prepare_sqls, values);
        unsafe {
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[0].column_values[0], -32768);
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[1].column_values[0], 32767);
        }
    }

    #[test]
    #[serial]
    fn test_smallint_unsigned() {
        TestCommon::before_dml();
        let prepare_sqls = vec![TestCommon::get_create_table_sql_with_one_field(
            "SMALLINT UNSIGNED".to_string(),
        )];
        let values = vec!["(65535),(32767)".to_string()];
        TestCommon::execute_insert_sqls_and_get_binlogs(prepare_sqls, values);
        unsafe {
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[0].column_values[0], -1);
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[1].column_values[0], 32767);
        }
    }

    #[test]
    #[serial]
    fn test_mediumint() {
        TestCommon::before_dml();
        let prepare_sqls = vec![TestCommon::get_create_table_sql_with_one_field(
            "MEDIUMINT".to_string(),
        )];
        let values = vec!["(-8388608),(8388607)".to_string()];
        TestCommon::execute_insert_sqls_and_get_binlogs(prepare_sqls, values);
        unsafe {
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[0].column_values[0], -8388608);
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[1].column_values[0], 8388607);
        }
    }

    #[test]
    #[serial]
    fn test_mediumint_unsigned() {
        TestCommon::before_dml();
        let prepare_sqls = vec![TestCommon::get_create_table_sql_with_one_field(
            "MEDIUMINT UNSIGNED".to_string(),
        )];
        let values = vec!["(16777215),(8388607)".to_string()];
        TestCommon::execute_insert_sqls_and_get_binlogs(prepare_sqls, values);
        unsafe {
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[0].column_values[0], -1);
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[1].column_values[0], 8388607);
        }
    }

    #[test]
    #[serial]
    fn test_int() {
        TestCommon::before_dml();
        let prepare_sqls = vec![TestCommon::get_create_table_sql_with_one_field(
            "INT".to_string(),
        )];
        let values = vec!["(-2147483648),(2147483647)".to_string()];
        TestCommon::execute_insert_sqls_and_get_binlogs(prepare_sqls, values);
        unsafe {
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[0].column_values[0], -2147483648);
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[1].column_values[0], 2147483647);
        }
    }

    #[test]
    #[serial]
    fn test_int_unsigned() {
        TestCommon::before_dml();
        let prepare_sqls = vec![TestCommon::get_create_table_sql_with_one_field(
            "INT UNSIGNED".to_string(),
        )];
        let values = vec!["(4294967295),(2147483647)".to_string()];
        TestCommon::execute_insert_sqls_and_get_binlogs(prepare_sqls, values);
        unsafe {
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[0].column_values[0], -1);
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[1].column_values[0], 2147483647);
        }
    }

    #[test]
    #[serial]
    fn test_bigint() {
        TestCommon::before_dml();
        let prepare_sqls = vec![TestCommon::get_create_table_sql_with_one_field(
            "BIGINT".to_string(),
        )];
        let values = vec!["(-9223372036854775808),(9223372036854775807)".to_string()];
        TestCommon::execute_insert_sqls_and_get_binlogs(prepare_sqls, values);
        unsafe {
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[0].column_values[0], -9223372036854775808);
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[1].column_values[0], 9223372036854775807);
        }
    }

    #[test]
    #[serial]
    fn test_bigint_unsigned() {
        TestCommon::before_dml();
        let prepare_sqls = vec![TestCommon::get_create_table_sql_with_one_field(
            "BIGINT UNSIGNED".to_string(),
        )];
        let values = vec!["(18446744073709551615),(9223372036854775807)".to_string()];
        TestCommon::execute_insert_sqls_and_get_binlogs(prepare_sqls, values);
        unsafe {
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[0].column_values[0], -1);
            Assert::assert_numeric_eq(&I_EVENTS[0].rows[1].column_values[0], 9223372036854775807);
        }
    }

    #[test]
    #[serial]
    fn test_float() {
        TestCommon::before_dml();
        let prepare_sqls = vec![TestCommon::get_create_table_sql_with_one_field(
            "FLOAT(10,5)".to_string(),
        )];
        let values = vec!["(1234.12345)".to_string()];
        TestCommon::execute_insert_sqls_and_get_binlogs(prepare_sqls, values);
        unsafe {
            Assert::assert_float_eq(&I_EVENTS[0].rows[0].column_values[0], 1234.12345);
        }
    }

    #[test]
    #[serial]
    fn test_double() {
        TestCommon::before_dml();
        let prepare_sqls = vec![TestCommon::get_create_table_sql_with_one_field(
            "DOUBLE(20, 10)".to_string(),
        )];
        let values = vec!["(1234567890.0123456789)".to_string()];
        TestCommon::execute_insert_sqls_and_get_binlogs(prepare_sqls, values);
        unsafe {
            Assert::assert_double_eq(&I_EVENTS[0].rows[0].column_values[0], 1234567890.0123456789);
        }
    }
}
