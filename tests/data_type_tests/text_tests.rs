#[cfg(test)]
mod test {

    use mysql_binlog_connector_rust::column::column_type::ColumnType;
    use serial_test::serial;

    use crate::data_type_tests::bytes_test_util::BytesTestUtil;

    #[test]
    #[serial]
    fn test_tinytext() {
        run_and_check("TINYTEXT");
    }

    #[test]
    #[serial]
    fn test_text() {
        run_and_check("TEXT");
    }

    #[test]
    #[serial]
    fn test_mediumtext() {
        run_and_check("MEDIUMTEXT");
    }

    #[test]
    #[serial]
    fn test_longtext() {
        run_and_check("LONGTEXT");
    }

    fn run_and_check(mysql_column_type: &str) {
        let (values, check_values) = BytesTestUtil::generate_visible_char_values();
        BytesTestUtil::run_and_check(mysql_column_type, ColumnType::Blob, &values, &check_values);

        let (values, check_values) = BytesTestUtil::generate_trailing_space_values(true);
        BytesTestUtil::run_and_check(mysql_column_type, ColumnType::Blob, &values, &check_values);

        let (values, check_values) = BytesTestUtil::generate_trailing_nul_values(true);
        BytesTestUtil::run_and_check(mysql_column_type, ColumnType::Blob, &values, &check_values);
    }
}
