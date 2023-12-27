#[cfg(test)]
mod test {

    use mysql_binlog_connector_rust::column::column_type::ColumnType;
    use serial_test::serial;

    use crate::data_type_tests::bytes_test_util::BytesTestUtil;

    #[test]
    #[serial]
    fn test_tinyblob() {
        run_and_check("TINYBLOB");
    }

    #[test]
    #[serial]
    fn test_blob() {
        run_and_check("BLOB");
    }

    #[test]
    #[serial]
    fn test_mediumblob() {
        run_and_check("MEDIUMBLOB");
    }

    #[test]
    #[serial]
    fn test_longblob() {
        run_and_check("LONGBLOB");
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
