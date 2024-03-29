#[cfg(test)]
mod test {

    use mysql_binlog_connector_rust::column::column_value::ColumnValue;
    use serial_test::serial;

    use crate::runner::{assert::test::Assert, mock::test::Mock, test_runner::test::TestRunner};

    #[test]
    #[serial]
    fn test_insert_multiple_rows() {
        let prepare_sqls = vec![
            Mock::default_create_sql(),
            "SET @@session.time_zone='UTC'".to_string(),
        ];

        let values = Mock::default_insert_values();
        let test_sqls = vec![
            Mock::insert_sql(&values[0..2]),
            Mock::insert_sql(&values[2..3]),
        ];

        let mut runner = TestRunner::new();
        runner.execute_sqls_and_get_binlogs(&prepare_sqls, &test_sqls);

        assert_eq!(runner.insert_events.len(), 2);
        assert_eq!(runner.insert_events[0].rows.len(), 2);
        assert_eq!(runner.insert_events[1].rows.len(), 1);

        Mock::default_check_values(
            &runner.insert_events[0].rows[0],
            0,
            1,
            2,
            3,
            4,
            5,
            "123456.1234".to_string(),
            1234.12,
            12345.123,
            3,
            "2022-01-02 03:04:05.123456".to_string(),
            "03:04:05.123456".to_string(),
            "2022-01-02".to_string(),
            2022,
            1641092645123456,
            vec![97u8, 98],
            vec![99u8, 100],
            vec![101u8, 102],
            vec![103u8, 104],
            vec![105u8, 106],
            vec![107u8, 108],
            vec![109u8, 110],
            vec![111u8, 112],
            vec![113u8, 114],
            vec![115u8, 116],
            vec![117u8, 118],
            vec![119u8, 120],
            1,
            1,
        );

        Mock::default_check_values(
            &runner.insert_events[0].rows[1],
            1,
            10,
            20,
            30,
            40,
            50,
            "654321.4321".to_string(),
            4321.21,
            54321.321,
            4,
            "2021-02-01 04:05:06.654321".to_string(),
            "04:05:06.654321".to_string(),
            "2012-02-01".to_string(),
            2021,
            1612152306654321,
            vec![49u8],
            vec![50u8],
            vec![51u8],
            vec![52u8],
            vec![53u8],
            vec![54u8],
            vec![55u8],
            vec![56u8],
            vec![57u8],
            vec![49u8, 48],
            vec![49u8, 49],
            vec![49u8, 50],
            2,
            2,
        );

        Mock::default_check_values(
            &runner.insert_events[1].rows[0],
            2,
            6,
            7,
            8,
            9,
            10,
            "234561.2341".to_string(),
            2341.12,
            23451.231,
            5,
            "2020-03-04 05:06:07.234561".to_string(),
            "05:06:07.234561".to_string(),
            "2022-05-06".to_string(),
            2020,
            1583298367234561,
            vec![97u8],
            vec![98u8],
            vec![99u8],
            vec![100u8],
            vec![101u8],
            vec![102u8],
            vec![103u8],
            vec![104u8],
            vec![105u8],
            vec![106u8],
            vec![107u8],
            vec![108u8],
            3,
            4,
        );
    }

    #[test]
    #[serial]
    fn test_insert_partial_null_fields() {
        let prepare_sqls = vec![
            Mock::default_create_sql(),
            "SET @@session.time_zone='UTC'".to_string(),
        ];

        let values = Mock::default_insert_values();
        let insert_sqls = vec![Mock::insert_sql(&values[3..4])];

        let mut runner = TestRunner::new();
        runner.execute_sqls_and_get_binlogs(&prepare_sqls, &insert_sqls);

        let column_values = &runner.insert_events[0].rows[0].column_values;
        Assert::assert_numeric_eq(&column_values[0], 3);
        // NULL fields
        for i in 0..13 {
            assert_eq!(column_values[2 * i + 2], ColumnValue::None);
        }
        // non-Null fields

        Assert::assert_numeric_eq(&column_values[1], 11 as i128);
        Assert::assert_numeric_eq(&column_values[3], 3 as i128);
        Assert::assert_numeric_eq(&column_values[5], 5 as i128);
        Assert::assert_float_eq(&column_values[7], 1234.12);
        Assert::assert_numeric_eq(&column_values[9], 3 as i128);
        Assert::assert_string_eq(&column_values[11], "03:04:05.123456".to_string());
        Assert::assert_numeric_eq(&column_values[13], 2022 as i128);
        Assert::assert_bytes_eq(&column_values[15], vec![97u8, 98]);
        Assert::assert_bytes_eq(&column_values[17], vec![101u8, 102]);
        Assert::assert_bytes_eq(&column_values[19], vec![105u8, 106]);
        Assert::assert_bytes_eq(&column_values[21], vec![109u8, 110]);
        Assert::assert_bytes_eq(&column_values[23], vec![113u8, 114]);
        Assert::assert_bytes_eq(&column_values[25], vec![117u8, 118]);
        Assert::assert_unsigned_numeric_eq(&column_values[27], 1 as u64);
    }

    #[test]
    #[serial]
    fn test_insert_all_null_fields() {
        let prepare_sqls = vec![
            Mock::default_create_sql(),
            "SET @@session.time_zone='UTC'".to_string(),
        ];

        let values = Mock::default_insert_values();
        let insert_sqls = vec![Mock::insert_sql(&values[4..5])];

        let mut runner = TestRunner::new();
        runner.execute_sqls_and_get_binlogs(&prepare_sqls, &insert_sqls);

        let column_values = &runner.insert_events[0].rows[0].column_values;
        Assert::assert_numeric_eq(&column_values[0], 4);
        for i in 1..28 {
            assert_eq!(column_values[i], ColumnValue::None);
        }
    }
}
