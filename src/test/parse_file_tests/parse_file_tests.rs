mod test {
    use std::{collections::HashMap, env, fs::File};

    use crate::{binlog_error::BinlogError, binlog_parser::BinlogParser};

    #[test]
    fn test_bad_magic() {
        let mut file = open_file("/non-binlog");
        let mut parser = BinlogParser {
            checksum_length: 0,
            table_map_event_by_table_id: HashMap::new(),
        };

        let res = parser.check_magic(&mut file);
        assert!(res.is_err());
        match res.err().unwrap() {
            BinlogError::ReadBinlogError { error } => {
                assert_eq!(error, "bad magic")
            }

            _ => {
                assert!(false)
            }
        }
    }

    #[test]
    fn test_parse_57_binlog() {
        test_parse_binlog("/mysql-bin.000057", 8);
    }

    #[test]
    fn test_parse_80_binlog() {
        test_parse_binlog("/mysql-bin.000080", 37);
    }

    fn open_file(file_name: &str) -> File {
        let current_dir = env::current_dir();
        let file_path = format!(
            "{}{}{}",
            current_dir.unwrap().display(),
            "/src/test/parse_file_tests/",
            file_name
        );
        File::open(file_path).unwrap()
    }

    fn test_parse_binlog(file_name: &str, expect_event_count: i32) {
        let mut file = open_file(file_name);
        let mut parser = BinlogParser {
            checksum_length: 4,
            table_map_event_by_table_id: HashMap::new(),
        };

        assert!(parser.check_magic(&mut file).is_ok());

        let mut count = 0;
        while let Ok(_) = parser.next(&mut file) {
            count += 1;
        }
        assert_eq!(count, expect_event_count);
    }
}
