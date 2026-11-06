#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        env,
        fs::File,
        io::{ErrorKind, Seek},
    };

    use mysql_binlog_connector_rust::{
        binlog_error::BinlogError,
        binlog_parser::BinlogParser,
        column::column_value::ColumnValue,
        event::{event_data::EventData, event_header::EventHeader},
    };

    const UUID_84: &str = "8f555992-d370-11f1-a239-0242ac110002";
    const LONG_TAG: &str = "abcdefghijklmnopqrstuvwxyz_12345";

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
            BinlogError::UnexpectedData(error) => {
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

    #[test]
    fn test_parse_56_binlog() {
        let events = read_events("/mysql-bin.000056");
        assert_eq!(
            events.iter().map(|e| e.0.event_type).collect::<Vec<_>>(),
            [15, 35, 33, 2, 19, 30, 16, 4]
        );
        assert_gtids(
            &events,
            "17e55caf-d371-11f1-b04b-0242ac110003",
            &[(33, 1, "2")],
        );
        assert_eq!(events[2].0.event_length, 19 + 25 + 4);
        assert_previous(&events, "17e55caf-d371-11f1-b04b-0242ac110003:1-1");
        assert_transactions(&events, &[2]);
    }

    #[test]
    fn test_parse_84_tagged_binlog() {
        let events = read_events("/mysql-bin.000084");
        let mut types = vec![15, 35];
        for gtid_type in [33, 42, 42, 33, 42] {
            types.extend([gtid_type, 2, 19, 30, 16]);
        }
        types.push(4);
        assert_eq!(
            events.iter().map(|e| e.0.event_type).collect::<Vec<_>>(),
            types
        );
        assert_gtids(
            &events,
            UUID_84,
            &[
                (33, 0, "1"),
                (42, 0, "admin:1"),
                (42, 0, "biz:1"),
                (33, 0, "2"),
                (42, 0, &format!("{}:1", LONG_TAG)),
            ],
        );
        assert_previous(&events, "");
        assert_transactions(&events, &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_parse_84_tagged_history_and_compression() {
        let events = read_events("/mysql-bin.000084-compressed");
        assert_eq!(
            events.iter().map(|e| e.0.event_type).collect::<Vec<_>>(),
            [15, 35, 42, 40, 33, 2, 19, 30, 16, 4]
        );
        assert_gtids(&events, UUID_84, &[(42, 0, "admin:2"), (33, 0, "3")]);
        assert_previous(
            &events,
            &format!(
                "{0}:1-2,{0}:{1}:1-1,{0}:admin:1-1,{0}:biz:1-1",
                UUID_84, LONG_TAG
            ),
        );
        assert_transactions(&events, &[6, 7]);
        match &events[3].1 {
            EventData::TransactionPayload(payload) => {
                assert_eq!(
                    payload
                        .uncompressed_events
                        .iter()
                        .map(|e| e.0.event_type)
                        .collect::<Vec<_>>(),
                    [2, 19, 30, 16]
                );
                assert_eq!(payload.uncompressed_size, 195);
            }
            other => panic!("expected compressed transaction, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_84_tagged_without_checksum() {
        let events = read_events("/mysql-bin.000084-no-checksum");
        assert_eq!(
            events.iter().map(|e| e.0.event_type).collect::<Vec<_>>(),
            [15, 35, 42, 2, 19, 30, 16, 33, 2, 19, 30, 16, 4]
        );
        assert_gtids(&events, UUID_84, &[(42, 0, "biz:2"), (33, 0, "4")]);
        assert_previous(
            &events,
            &format!(
                "{0}:1-3,{0}:{1}:1-1,{0}:admin:1-2,{0}:biz:1-1",
                UUID_84, LONG_TAG
            ),
        );
        assert_transactions(&events, &[8, 9]);
    }

    fn assert_gtids(events: &[(EventHeader, EventData)], uuid: &str, expected: &[(u8, u8, &str)]) {
        let gtids: Vec<_> = events
            .iter()
            .filter_map(|(header, data)| match data {
                EventData::Gtid(event) => {
                    Some((header.event_type, event.flags, event.gtid.clone()))
                }
                _ => None,
            })
            .collect();
        let expected: Vec<_> = expected
            .iter()
            .map(|(kind, flags, suffix)| (*kind, *flags, format!("{}:{}", uuid, suffix)))
            .collect();
        assert_eq!(gtids, expected);
    }

    fn assert_previous(events: &[(EventHeader, EventData)], expected: &str) {
        match &events[1].1 {
            EventData::PreviousGtids(event) => assert_eq!(event.gtid_set, expected),
            other => panic!("expected PreviousGtids, got {:?}", other),
        }
    }

    fn assert_transactions(events: &[(EventHeader, EventData)], expected_ids: &[i32]) {
        let mut ids = Vec::new();
        let mut queries = 0;
        let mut commits = 0;
        for (_, event) in events {
            match event {
                EventData::TransactionPayload(payload) => {
                    assert_transactions(&payload.uncompressed_events, &expected_ids[..1]);
                    ids.push(expected_ids[0]);
                    queries += 1;
                    commits += 1;
                }
                EventData::Query(query) => {
                    assert_eq!(query.query, "BEGIN");
                    queries += 1;
                }
                EventData::TableMap(table) => {
                    assert_eq!(table.database_name, "tagged_gtid_test");
                    assert_eq!(table.table_name, "events");
                }
                EventData::WriteRows(event) => {
                    assert_eq!(event.rows.len(), 1);
                    match event.rows[0].column_values.as_slice() {
                        [ColumnValue::Long(id), ColumnValue::Long(value)] => {
                            assert_eq!(*value, id * 10);
                            ids.push(*id);
                        }
                        other => panic!("unexpected row {:?}", other),
                    }
                }
                EventData::Xid(_) => commits += 1,
                _ => {}
            }
        }
        assert_eq!(ids, expected_ids);
        assert_eq!(queries, expected_ids.len());
        assert_eq!(commits, expected_ids.len());
    }

    fn read_events(file_name: &str) -> Vec<(EventHeader, EventData)> {
        let mut file = open_file(file_name);
        let mut parser = BinlogParser {
            checksum_length: 4,
            table_map_event_by_table_id: HashMap::new(),
        };
        parser.check_magic(&mut file).unwrap();
        let length = file.metadata().unwrap().len();
        let mut events = Vec::new();
        while file.stream_position().unwrap() < length {
            // Parse errors must not be mistaken for EOF.
            events.push(parser.next(&mut file).unwrap());
        }
        assert_eq!(file.stream_position().unwrap(), length);
        assert!(
            matches!(parser.next(&mut file), Err(BinlogError::IoError(error)) if error.kind() == ErrorKind::UnexpectedEof)
        );
        events
    }

    fn open_file(file_name: &str) -> File {
        let current_dir = env::current_dir();
        let file_path = format!(
            "{}/{}/{}",
            current_dir.unwrap().display(),
            "tests/parse_file_tests",
            file_name
        );
        File::open(file_path).unwrap()
    }

    fn test_parse_binlog(file_name: &str, expect_event_count: i32) {
        let events = read_events(file_name);
        assert_eq!(events.len(), expect_event_count as usize);
        if file_name.ends_with("000057") {
            assert_gtids(
                &events,
                "76f3e7be-6720-11ed-9cad-0242ac110002",
                &[(33, 1, "11"), (33, 0, "12"), (33, 0, "13")],
            );
        } else {
            assert_gtids(
                &events,
                "58cf6502-63db-11ed-8079-0242ac110002",
                &[
                    (33, 0, "53"),
                    (33, 0, "54"),
                    (33, 0, "55"),
                    (33, 0, "56"),
                    (33, 1, "57"),
                    (33, 1, "58"),
                    (33, 1, "59"),
                    (33, 1, "60"),
                    (33, 1, "61"),
                    (33, 0, "62"),
                ],
            );
        }
    }
}
