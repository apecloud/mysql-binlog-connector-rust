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

        // Verify table map events are generated for DML operations
        assert!(!runner.table_map_events.is_empty(), "Should have table map events for DML operations");
        
        // Verify table map events reference the correct table
        let table_event = runner.table_map_events.iter()
            .find(|event| event.database_name == runner.default_db && event.table_name == runner.default_tb)
            .expect("Should find table map event for default test table");
        
        // Verify insert events reference the same table ID
        assert_eq!(runner.insert_events[0].table_id, table_event.table_id, "Insert events should reference the correct table");
        assert_eq!(runner.insert_events[1].table_id, table_event.table_id, "Insert events should reference the correct table");

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

    #[test]
    #[serial]
    fn test_insert_with_table_map_metadata() {
        let prepare_sqls = vec![
            "DROP DATABASE IF EXISTS test_table_map_metadata".to_string(),
            "CREATE DATABASE test_table_map_metadata".to_string(),
        ];

        let test_sqls = vec![
            // Create a table with various column types that will generate metadata
            r#"CREATE TABLE test_table_map_metadata.metadata_test (
                id INT PRIMARY KEY,
                name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                age TINYINT UNSIGNED,
                score DECIMAL(10,2) SIGNED,
                status ENUM('active', 'inactive', 'pending'),
                permissions SET('read', 'write', 'execute'),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"#.to_string(),
            // Insert data to trigger table map events
            "INSERT INTO test_table_map_metadata.metadata_test (id, name, age, score, status, permissions) VALUES (1, 'John Doe', 25, 95.50, 'active', 'read,write')".to_string(),
        ];

        let mut runner = TestRunner::new();
        runner.execute_sqls_and_get_binlogs(&prepare_sqls, &test_sqls);

        // Verify we got both insert events and table map events
        assert!(!runner.insert_events.is_empty(), "Should have received WriteRowsEvent");
        assert!(!runner.table_map_events.is_empty(), "Should have received TableMapEvent");

        // Find the table map event for our test table
        let table_event = runner.table_map_events.iter()
            .find(|event| event.database_name == "test_table_map_metadata" && event.table_name == "metadata_test")
            .expect("Should find TableMapEvent for our test table");

        // Verify basic table info
        assert_eq!(table_event.database_name, "test_table_map_metadata");
        assert_eq!(table_event.table_name, "metadata_test");

        // Verify we have metadata
        assert!(table_event.table_metadata.is_some(), "Table should have metadata");
        let metadata = table_event.table_metadata.as_ref().unwrap();

        // Verify we have the expected number of columns
        assert_eq!(metadata.columns.len(), 7, "Should have 7 columns");

        // Verify column names if available
        if let Some(ref column_name) = metadata.columns[0].column_name {
            assert_eq!(column_name, "id");
        }
        if let Some(ref column_name) = metadata.columns[1].column_name {
            assert_eq!(column_name, "name");
        }
        if let Some(ref column_name) = metadata.columns[2].column_name {
            assert_eq!(column_name, "age");
        }

        // Verify signedness metadata is present for numeric columns
        assert!(metadata.columns[0].is_signed.is_some(), "INT column should have signedness metadata");
        assert!(metadata.columns[2].is_signed.is_some(), "TINYINT column should have signedness metadata");
        assert!(metadata.columns[3].is_signed.is_some(), "DECIMAL column should have signedness metadata");

        // Verify charset metadata exists for string columns (when available)
        if let Some(charset_collation) = metadata.columns[1].charset_collation {
            assert!(charset_collation > 0, "String column should have valid charset collation");
        }

        // Verify the corresponding insert event references the same table
        let insert_event = &runner.insert_events[0];
        assert_eq!(insert_event.table_id, table_event.table_id, "Insert event should reference the same table as table map event");
        
        // Verify we have the inserted row data
        assert_eq!(insert_event.rows.len(), 1, "Should have one inserted row");
        let row = &insert_event.rows[0];
        Assert::assert_numeric_eq(&row.column_values[0], 1); // id = 1
    }
}
