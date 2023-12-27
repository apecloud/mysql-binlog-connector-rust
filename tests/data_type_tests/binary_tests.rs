#[cfg(test)]
mod test {

    use mysql_binlog_connector_rust::column::column_type::ColumnType;
    use serial_test::serial;

    use crate::data_type_tests::bytes_test_util::BytesTestUtil;

    #[test]
    #[serial]
    fn test_binary_255() {
        let (values, check_values) = BytesTestUtil::generate_visible_char_values();
        BytesTestUtil::run_and_check("BINARY(255)", ColumnType::String, &values, &check_values);

        let (values, check_values) = BytesTestUtil::generate_trailing_space_values(true);
        BytesTestUtil::run_and_check("BINARY(255)", ColumnType::String, &values, &check_values);

        let (values, check_values) = BytesTestUtil::generate_trailing_nul_values(false);
        BytesTestUtil::run_and_check("BINARY(255)", ColumnType::String, &values, &check_values);
    }

    #[test]
    #[serial]
    fn test_varbinary_255() {
        let (values, check_values) = BytesTestUtil::generate_visible_char_values();
        BytesTestUtil::run_and_check(
            "VARBINARY(255)",
            ColumnType::VarChar,
            &values,
            &check_values,
        );

        let (values, check_values) = BytesTestUtil::generate_trailing_space_values(true);
        BytesTestUtil::run_and_check(
            "VARBINARY(255)",
            ColumnType::VarChar,
            &values,
            &check_values,
        );

        let (values, check_values) = BytesTestUtil::generate_trailing_nul_values(true);
        BytesTestUtil::run_and_check(
            "VARBINARY(255)",
            ColumnType::VarChar,
            &values,
            &check_values,
        );
    }
}
