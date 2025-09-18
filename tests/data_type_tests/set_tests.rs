#[cfg(test)]
mod test {
    use std::vec;

    use mysql_binlog_connector_rust::column::column_value::ColumnValue;
    use serial_test::serial;

    use crate::runner::test_runner::test::TestRunner;

    #[test]
    #[serial]
    fn test_set_name() {
        // refer: https://dev.mysql.com/doc/refman/8.0/en/set.html
        // A SET column can have a maximum of 64 distinct members.
        let col_type =
            "SET('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18')";
        let values = vec![
            "'1'", "'2'", "'3'", "'4'", "'5'", "'6'", "'7'", "'8'", "'9'", "'10'", "'11'", "'12'",
            "'13'", "'14'", "'15'", "'16'", "'17'", "'18'",
        ];
        let check_values = vec![
            1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536,
            131072,
        ];
        run_and_check(col_type, &values, &check_values);
    }

    #[test]
    #[serial]
    fn test_set_ordinal() {
        let col_type = "SET('1', '2', '3', '4', '5')";
        let values = vec!["1", "2", "3", "4", "5"];
        let check_values = vec![1, 2, 3, 4, 5];
        run_and_check(col_type, &values, &check_values);
    }

    #[test]
    #[serial]
    fn test_set_metadata_parsing() {
        let prepare_sqls = vec![
            "DROP DATABASE IF EXISTS test_set_metadata".to_string(),
            "CREATE DATABASE test_set_metadata".to_string(),
        ];

        let test_sqls = vec![
            // Create a table with SET columns that will generate metadata
            r#"CREATE TABLE test_set_metadata.set_metadata_test (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                permissions SET('read', 'write', 'execute', 'delete'),
                priority INT,
                created_by VARCHAR(50),
                flags SET('urgent', 'priority', 'confidential'),
                updated_at DATETIME
            )"#.to_string(),
            // Insert data to trigger table map events
            "INSERT INTO test_set_metadata.set_metadata_test (id, name, permissions, priority, created_by, flags, updated_at) VALUES (1, 'Test Task', 'read,write', 10, 'admin', 'urgent', '2023-01-01 10:00:00')".to_string(),
        ];

        let mut runner = TestRunner::new();
        runner.execute_sqls_and_get_binlogs(&prepare_sqls, &test_sqls);

        // Verify we got both insert events and table map events
        assert!(
            !runner.insert_events.is_empty(),
            "Should have received WriteRowsEvent"
        );
        assert!(
            !runner.table_map_events.is_empty(),
            "Should have received TableMapEvent"
        );

        // Find the table map event for our test table
        let table_event = runner
            .table_map_events
            .iter()
            .find(|event| {
                event.database_name == "test_set_metadata"
                    && event.table_name == "set_metadata_test"
            })
            .expect("Should find TableMapEvent for set test table");

        // Verify we have metadata
        assert!(
            table_event.table_metadata.is_some(),
            "Table should have metadata"
        );
        let metadata = table_event.table_metadata.as_ref().unwrap();

        // Verify we have the expected number of columns
        assert_eq!(metadata.columns.len(), 7, "Should have 7 columns");

        // Verify that set metadata is parsed and applied to the correct SET columns
        // Column 0: id (INT) - should not have set values
        // Column 1: name (VARCHAR) - should not have set values
        // Column 2: permissions (SET) - should have the permissions set values
        // Column 3: priority (INT) - should not have set values
        // Column 4: created_by (VARCHAR) - should not have set values
        // Column 5: flags (SET) - should have the flags set values
        // Column 6: updated_at (DATETIME) - should not have set values

        // Check that set string values are applied to the correct columns
        assert!(
            metadata.columns[0].set_string_values.is_none(),
            "Column 0 (id) should not have set string values"
        );
        assert!(
            metadata.columns[1].set_string_values.is_none(),
            "Column 1 (name) should not have set string values"
        );
        assert!(
            metadata.columns[2].set_string_values.is_some(),
            "Column 2 (permissions) should have set string values"
        );
        assert!(
            metadata.columns[3].set_string_values.is_none(),
            "Column 3 (priority) should not have set string values"
        );
        assert!(
            metadata.columns[4].set_string_values.is_none(),
            "Column 4 (created_by) should not have set string values"
        );
        assert!(
            metadata.columns[5].set_string_values.is_some(),
            "Column 5 (flags) should have set string values"
        );
        assert!(
            metadata.columns[6].set_string_values.is_none(),
            "Column 6 (updated_at) should not have set string values"
        );

        // Verify the permissions set values are in column 2
        let permissions_set_values = metadata.columns[2].set_string_values.as_ref().unwrap();
        assert_eq!(
            permissions_set_values.len(),
            4,
            "Permissions set should have 4 values"
        );
        assert_eq!(permissions_set_values[0], "read");
        assert_eq!(permissions_set_values[1], "write");
        assert_eq!(permissions_set_values[2], "execute");
        assert_eq!(permissions_set_values[3], "delete");

        // Verify the flags set values are in column 5
        let flags_set_values = metadata.columns[5].set_string_values.as_ref().unwrap();
        assert_eq!(flags_set_values.len(), 3, "Flags set should have 3 values");
        assert_eq!(flags_set_values[0], "urgent");
        assert_eq!(flags_set_values[1], "priority");
        assert_eq!(flags_set_values[2], "confidential");

        // Verify the insert event data
        let insert_event = &runner.insert_events[0];
        let row = &insert_event.rows[0];
        // 'read,write' = bit 0 (read) + bit 1 (write) = 1 + 2 = 3
        assert_eq!(
            row.column_values[2],
            ColumnValue::Set(3),
            "Should have 'read,write' set value (bits 0+1 = 3)"
        );
        // 'urgent' = bit 0 = 1
        assert_eq!(
            row.column_values[5],
            ColumnValue::Set(1),
            "Should have 'urgent' set value (bit 0 = 1)"
        );
    }

    fn run_and_check(col_type: &str, values: &Vec<&str>, check_values: &Vec<u64>) {
        let runner = TestRunner::run_one_col_test(col_type, values, &vec![]);
        for i in 0..check_values.len() {
            assert_eq!(
                runner.insert_events[0].rows[i].column_values[0],
                ColumnValue::Set(check_values[i]),
            );
        }
    }
}
