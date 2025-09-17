#[cfg(test)]
mod test {
    use std::vec;

    use mysql_binlog_connector_rust::column::column_value::ColumnValue;
    use serial_test::serial;

    use crate::runner::test_runner::test::TestRunner;

    #[test]
    #[serial]
    fn test_enum() {
        // refer: https://dev.mysql.com/doc/refman/8.0/en/enum.html
        // An ENUM column can have a maximum of 65,535 distinct elements.
        let col_type = "ENUM('x-small', 'small', 'medium', 'large', 'x-large')";
        let values = vec!["'x-small'", "'small'", "'medium'", "'large'", "'x-large'"];
        let check_values = vec![1, 2, 3, 4, 5];
        run_and_check(col_type, &values, &check_values);
    }

    #[test]
    #[serial]
    fn test_enum_metadata_parsing() {
        let prepare_sqls = vec![
            "DROP DATABASE IF EXISTS test_enum_metadata".to_string(),
            "CREATE DATABASE test_enum_metadata".to_string(),
        ];

        let test_sqls = vec![
            // Create a table with ENUM columns that will generate metadata
            r#"CREATE TABLE test_enum_metadata.enum_metadata_test (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                size ENUM('x-small', 'small', 'medium', 'large', 'x-large'),
                price DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status ENUM('active', 'inactive', 'pending', 'archived'),
                description TEXT
            )"#.to_string(),
            // Insert data to trigger table map events
            "INSERT INTO test_enum_metadata.enum_metadata_test (id, name, size, price, status, description) VALUES (1, 'Test Item', 'small', 19.99, 'active', 'A test description')".to_string(),
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
                event.database_name == "test_enum_metadata"
                    && event.table_name == "enum_metadata_test"
            })
            .expect("Should find TableMapEvent for enum test table");

        // Verify we have metadata
        assert!(
            table_event.table_metadata.is_some(),
            "Table should have metadata"
        );
        let metadata = table_event.table_metadata.as_ref().unwrap();

        // Verify we have the expected number of columns
        assert_eq!(metadata.columns.len(), 7, "Should have 7 columns");

        // Verify that enum metadata is parsed and applied to the correct ENUM columns
        // Column 0: id (INT) - should not have enum values
        // Column 1: name (VARCHAR) - should not have enum values
        // Column 2: size (ENUM) - should have the size enum values
        // Column 3: price (DECIMAL) - should not have enum values
        // Column 4: created_at (TIMESTAMP) - should not have enum values
        // Column 5: status (ENUM) - should have the status enum values
        // Column 6: description (TEXT) - should not have enum values

        // Check that enum string values are applied to the correct columns
        assert!(
            metadata.columns[0].enum_string_values.is_none(),
            "Column 0 (id) should not have enum string values"
        );
        assert!(
            metadata.columns[1].enum_string_values.is_none(),
            "Column 1 (name) should not have enum string values"
        );
        assert!(
            metadata.columns[2].enum_string_values.is_some(),
            "Column 2 (size) should have enum string values"
        );
        assert!(
            metadata.columns[3].enum_string_values.is_none(),
            "Column 3 (price) should not have enum string values"
        );
        assert!(
            metadata.columns[4].enum_string_values.is_none(),
            "Column 4 (created_at) should not have enum string values"
        );
        assert!(
            metadata.columns[5].enum_string_values.is_some(),
            "Column 5 (status) should have enum string values"
        );
        assert!(
            metadata.columns[6].enum_string_values.is_none(),
            "Column 6 (description) should not have enum string values"
        );

        // Verify the size enum values are in column 2
        let size_enum_values = metadata.columns[2].enum_string_values.as_ref().unwrap();
        assert_eq!(size_enum_values.len(), 5, "Size enum should have 5 values");
        assert_eq!(size_enum_values[0], "x-small");
        assert_eq!(size_enum_values[1], "small");
        assert_eq!(size_enum_values[2], "medium");
        assert_eq!(size_enum_values[3], "large");
        assert_eq!(size_enum_values[4], "x-large");

        // Verify the status enum values are in column 5
        let status_enum_values = metadata.columns[5].enum_string_values.as_ref().unwrap();
        assert_eq!(
            status_enum_values.len(),
            4,
            "Status enum should have 4 values"
        );
        assert_eq!(status_enum_values[0], "active");
        assert_eq!(status_enum_values[1], "inactive");
        assert_eq!(status_enum_values[2], "pending");
        assert_eq!(status_enum_values[3], "archived");

        // Verify the insert event data
        let insert_event = &runner.insert_events[0];
        let row = &insert_event.rows[0];
        assert_eq!(
            row.column_values[2],
            ColumnValue::Enum(2),
            "Should have 'small' enum value (index 2)"
        );
        assert_eq!(
            row.column_values[5],
            ColumnValue::Enum(1),
            "Should have 'active' enum value (index 1)"
        );
    }

    fn run_and_check(col_type: &str, values: &Vec<&str>, check_values: &Vec<u32>) {
        let runner = TestRunner::run_one_col_test(col_type, values, &vec![]);
        for i in 0..check_values.len() {
            assert_eq!(
                runner.insert_events[0].rows[i].column_values[0],
                ColumnValue::Enum(check_values[i]),
            );
        }
    }
}
