#[cfg(test)]
mod test {

    use mysql_binlog_connector_rust::column::column_value::ColumnValue;
    use serial_test::serial;

    use crate::runner::{assert::test::Assert, mock::test::Mock, test_runner::test::TestRunner};

    #[test]
    #[serial]
    fn test_update_multiple_rows() {
        let prepare_sqls = vec![
            Mock::default_create_sql(),
            "SET @@session.time_zone='UTC'".to_string(),
        ];

        // insert
        let col_names = Mock::default_col_names();
        let values = Mock::default_insert_values();
        let insert_sqls = vec![Mock::insert_sql(&values)];

        let mut runner = TestRunner::new();
        runner.execute_sqls_and_get_binlogs(&prepare_sqls, &insert_sqls);

        // update
        let update_sqls = vec![
            Mock::update_sql("pk", values[0][0], &col_names[1..], &values[1][1..]),
            Mock::update_sql("pk", values[1][0], &col_names[1..], &values[2][1..]),
            Mock::update_sql("pk", values[2][0], &col_names[1..], &values[3][1..]),
            Mock::update_sql("pk", values[3][0], &col_names[1..], &values[4][1..]),
        ];
        runner.execute_sqls_and_get_binlogs(
            &vec!["SET @@session.time_zone='UTC'".to_string()],
            &update_sqls,
        );

        assert_eq!(runner.update_events.len(), 4);

        // Verify table map events are generated for update operations
        assert!(!runner.table_map_events.is_empty(), "Should have table map events for update operations");
        
        // Verify update events reference the correct table ID
        let table_event = runner.table_map_events.iter()
            .find(|event| event.database_name == runner.default_db && event.table_name == runner.default_tb)
            .expect("Should find table map event for default test table");
        
        for update_event in &runner.update_events {
            assert_eq!(update_event.table_id, table_event.table_id, "Update events should reference the correct table");
        }

        // row 0, before
        Mock::default_check_values(
            &runner.update_events[0].rows[0].0,
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

        // row 0, after
        Mock::default_check_values(
            &runner.update_events[0].rows[0].1,
            0,
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

        // row 1, before
        Mock::default_check_values(
            &runner.update_events[1].rows[0].0,
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

        // row 1, after
        Mock::default_check_values(
            &runner.update_events[1].rows[0].1,
            1,
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

        // row 2, before
        Mock::default_check_values(
            &runner.update_events[2].rows[0].0,
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

        // row 2, after
        Assert::assert_numeric_eq(&runner.update_events[2].rows[0].1.column_values[0], 2);
        // NULL fields
        for i in 0..13 {
            assert_eq!(
                runner.update_events[2].rows[0].1.column_values[2 * i + 2],
                ColumnValue::None
            );
        }
        // non-Null fields
        let row_2_after = &runner.update_events[2].rows[0].1.column_values;
        Assert::assert_numeric_eq(&row_2_after[1], 11 as i128);
        Assert::assert_numeric_eq(&row_2_after[3], 3 as i128);
        Assert::assert_numeric_eq(&row_2_after[5], 5 as i128);
        Assert::assert_float_eq(&row_2_after[7], 1234.12);
        Assert::assert_numeric_eq(&row_2_after[9], 3 as i128);
        Assert::assert_string_eq(&row_2_after[11], "03:04:05.123456".to_string());
        Assert::assert_numeric_eq(&row_2_after[13], 2022 as i128);
        Assert::assert_bytes_eq(&row_2_after[15], vec![97u8, 98]);
        Assert::assert_bytes_eq(&row_2_after[17], vec![101u8, 102]);
        Assert::assert_bytes_eq(&row_2_after[19], vec![105u8, 106]);
        Assert::assert_bytes_eq(&row_2_after[21], vec![109u8, 110]);
        Assert::assert_bytes_eq(&row_2_after[23], vec![113u8, 114]);
        Assert::assert_bytes_eq(&row_2_after[25], vec![117u8, 118]);
        Assert::assert_unsigned_numeric_eq(&row_2_after[27], 1 as u64);

        // row 3, before
        Assert::assert_numeric_eq(&runner.update_events[3].rows[0].0.column_values[0], 3);
        // NULL fields
        for i in 0..13 {
            assert_eq!(
                runner.update_events[3].rows[0].0.column_values[2 * i + 2],
                ColumnValue::None
            );
        }
        // non-Null fields
        let row_3_before = &runner.update_events[3].rows[0].0.column_values;
        Assert::assert_numeric_eq(&row_3_before[1], 11 as i128);
        Assert::assert_numeric_eq(&row_3_before[3], 3 as i128);
        Assert::assert_numeric_eq(&row_3_before[5], 5 as i128);
        Assert::assert_float_eq(&row_3_before[7], 1234.12);
        Assert::assert_numeric_eq(&row_3_before[9], 3 as i128);
        Assert::assert_string_eq(&row_3_before[11], "03:04:05.123456".to_string());
        Assert::assert_numeric_eq(&row_3_before[13], 2022 as i128);
        Assert::assert_bytes_eq(&row_3_before[15], vec![97u8, 98]);
        Assert::assert_bytes_eq(&row_3_before[17], vec![101u8, 102]);
        Assert::assert_bytes_eq(&row_3_before[19], vec![105u8, 106]);
        Assert::assert_bytes_eq(&row_3_before[21], vec![109u8, 110]);
        Assert::assert_bytes_eq(&row_3_before[23], vec![113u8, 114]);
        Assert::assert_bytes_eq(&row_3_before[25], vec![117u8, 118]);
        Assert::assert_unsigned_numeric_eq(&row_3_before[27], 1 as u64);

        // row 3, after
        for i in 1..28 {
            assert_eq!(
                runner.update_events[3].rows[0].1.column_values[i],
                ColumnValue::None
            );
        }
    }
}
