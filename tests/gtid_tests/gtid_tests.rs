#[cfg(test)]
mod test {
    use async_std::task::block_on;
    use serial_test::serial;
    use std::time::Duration;

    use mysql_binlog_connector_rust::command::{
        authenticator::Authenticator, command_util::CommandUtil, gtid_set::GtidSet,
    };
    use mysql_binlog_connector_rust::{
        binlog_client::{BinlogClient, StartPosition},
        binlog_stream::BinlogStream,
        column::column_value::ColumnValue,
        event::event_data::EventData,
    };

    use crate::runner::{assert::test::Assert, test_runner::test::TestRunner};

    #[test]
    #[serial]
    #[ignore = "requires an isolated MySQL 8.4 with GTID enabled and permission to set GTID_NEXT"]
    fn test_mysql84_tagged_gtid_transactions() {
        let mut runner = TestRunner::new();
        // A fresh UUID makes the explicit GTIDs repeatable without resetting server history.
        let uuid = block_on(async {
            let mut authenticator = Authenticator::new(&runner.db_url, 60, None).unwrap();
            let mut channel = authenticator.connect().await.unwrap();
            let result = CommandUtil::execute_query(&mut channel, "SELECT UUID()")
                .await
                .unwrap();
            result[0].values[0].clone()
        });
        let prepare_sqls = vec![format!(
            "CREATE TABLE {}.{} (id INT PRIMARY KEY, value INT)",
            runner.default_db, runner.default_tb
        )];
        let suffixes = [
            "1",
            "admin:1",
            "biz:1",
            "2",
            "abcdefghijklmnopqrstuvwxyz_12345:1",
        ];
        let mut test_sqls = Vec::new();
        for (index, suffix) in suffixes.iter().enumerate() {
            test_sqls.push(format!(
                "SET SESSION binlog_transaction_compression={}",
                if index == 1 { "ON" } else { "OFF" }
            ));
            test_sqls.push(format!("SET GTID_NEXT='{}:{}'", uuid, suffix));
            test_sqls.push(format!(
                "INSERT INTO {}.{} VALUES ({}, {})",
                runner.default_db,
                runner.default_tb,
                index + 1,
                (index + 1) * 10
            ));
        }
        test_sqls.push("SET GTID_NEXT='AUTOMATIC'".to_string());

        runner.execute_sqls_and_get_binlogs(&prepare_sqls, &test_sqls);

        assert_eq!(runner.gtid_events.len(), suffixes.len());
        assert_eq!(runner.insert_events.len(), suffixes.len());
        for (index, suffix) in suffixes.iter().enumerate() {
            assert_eq!(
                runner.gtid_events[index].gtid,
                format!("{}:{}", uuid, suffix)
            );
            assert_eq!(runner.gtid_events[index].flags, 0);
            assert_eq!(runner.insert_events[index].rows.len(), 1);
            let row = &runner.insert_events[index].rows[0];
            Assert::assert_numeric_eq(&row.column_values[0], (index + 1) as i128);
            Assert::assert_numeric_eq(&row.column_values[1], ((index + 1) * 10) as i128);
        }
    }

    #[test]
    #[serial]
    #[ignore = "requires an isolated MySQL 8.4 with tagged GTID and FLUSH BINARY LOGS privileges"]
    fn test_mysql84_tagged_gtid_resume() {
        let runner = TestRunner::new();
        // Optionally use a separate read-only replication account.
        let replica_url =
            std::env::var("MYSQL84_REPLICA_URL").unwrap_or_else(|_| runner.db_url.clone());
        block_on(async {
            let mut authenticator = Authenticator::new(&runner.db_url, 10, None).unwrap();
            let mut writer = authenticator.connect().await.unwrap();
            CommandUtil::execute_sql(
                &mut writer,
                &format!(
                    "CREATE TABLE {}.{} (id INT PRIMARY KEY, value INT)",
                    runner.default_db, runner.default_tb
                ),
            )
            .await
            .unwrap();
            let result = CommandUtil::execute_query(&mut writer, "SELECT UUID()")
                .await
                .unwrap();
            let uuid = &result[0].values[0];
            for (id, suffix) in [(1, "1"), (2, "admin:1")] {
                CommandUtil::execute_sql(
                    &mut writer,
                    &format!("SET GTID_NEXT='{}:{}'", uuid, suffix),
                )
                .await
                .unwrap();
                CommandUtil::execute_sql(
                    &mut writer,
                    &format!(
                        "INSERT INTO {}.{} VALUES ({}, {})",
                        runner.default_db,
                        runner.default_tb,
                        id,
                        id * 10
                    ),
                )
                .await
                .unwrap();
            }
            CommandUtil::execute_sql(&mut writer, "SET GTID_NEXT='AUTOMATIC'")
                .await
                .unwrap();
            let (_, _, checkpoint) = CommandUtil::fetch_binlog_info(&mut writer).await.unwrap();
            assert!(checkpoint.contains(":admin:1"));
            CommandUtil::execute_sql(&mut writer, "FLUSH BINARY LOGS")
                .await
                .unwrap();
            for (id, suffix) in [(3, "biz:1"), (4, "admin:2"), (5, "2")] {
                CommandUtil::execute_sql(
                    &mut writer,
                    &format!(
                        "SET SESSION binlog_transaction_compression={}",
                        if id == 4 { "ON" } else { "OFF" }
                    ),
                )
                .await
                .unwrap();
                CommandUtil::execute_sql(
                    &mut writer,
                    &format!("SET GTID_NEXT='{}:{}'", uuid, suffix),
                )
                .await
                .unwrap();
                CommandUtil::execute_sql(
                    &mut writer,
                    &format!(
                        "INSERT INTO {}.{} VALUES ({}, {})",
                        runner.default_db,
                        runner.default_tb,
                        id,
                        id * 10
                    ),
                )
                .await
                .unwrap();
            }
            CommandUtil::execute_sql(&mut writer, "SET GTID_NEXT='AUTOMATIC'")
                .await
                .unwrap();
            let (_, _, latest_checkpoint) =
                CommandUtil::fetch_binlog_info(&mut writer).await.unwrap();

            let mut consumed_checkpoint = GtidSet::new(&checkpoint).unwrap();
            let mut stream = BinlogClient::new(
                &replica_url,
                runner.server_id,
                StartPosition::Gtid(checkpoint),
            )
            .connect()
            .await
            .unwrap();
            // biz:1 must still arrive even though admin:1 was in the checkpoint.
            assert_gtid_stream(
                &mut stream,
                uuid,
                &[("biz:1", 3), ("admin:2", 4), ("2", 5)],
                &mut consumed_checkpoint,
            )
            .await;
            assert_eq!(
                consumed_checkpoint,
                GtidSet::new(&latest_checkpoint).unwrap()
            );
            stream.close().await.unwrap();

            // Gtid("") fetches a mixed set from the server and must start after it.
            let mut client = BinlogClient::new(
                &replica_url,
                runner.server_id + 1,
                StartPosition::Gtid(String::new()),
            );
            let mut stream = client.connect().await.unwrap();
            assert_eq!(client.gtid_set, latest_checkpoint);
            let mut automatic_checkpoint = GtidSet::new(&client.gtid_set).unwrap();
            CommandUtil::execute_sql(&mut writer, &format!("SET GTID_NEXT='{}:biz:2'", uuid))
                .await
                .unwrap();
            CommandUtil::execute_sql(
                &mut writer,
                &format!(
                    "INSERT INTO {}.{} VALUES (6, 60)",
                    runner.default_db, runner.default_tb
                ),
            )
            .await
            .unwrap();
            CommandUtil::execute_sql(&mut writer, "SET GTID_NEXT='AUTOMATIC'")
                .await
                .unwrap();
            assert_gtid_stream(
                &mut stream,
                uuid,
                &[("biz:2", 6)],
                &mut automatic_checkpoint,
            )
            .await;
            stream.close().await.unwrap();

            let mut stream = BinlogClient::new(
                &replica_url,
                runner.server_id + 2,
                // Restore from the checkpoint accumulated by GtidSet::add at commit.
                StartPosition::Gtid(consumed_checkpoint.to_string()),
            )
            .connect()
            .await
            .unwrap();
            assert_gtid_stream(&mut stream, uuid, &[("biz:2", 6)], &mut consumed_checkpoint).await;
            let (_, _, server_checkpoint) =
                CommandUtil::fetch_binlog_info(&mut writer).await.unwrap();
            assert_eq!(consumed_checkpoint, automatic_checkpoint);
            assert_eq!(
                consumed_checkpoint,
                GtidSet::new(&server_checkpoint).unwrap()
            );
            stream.close().await.unwrap();
        });
    }

    async fn assert_gtid_stream(
        stream: &mut BinlogStream,
        uuid: &str,
        expected: &[(&str, i32)],
        checkpoint: &mut GtidSet,
    ) {
        let mut gtids = Vec::new();
        let mut rows = Vec::new();
        let mut commits = 0;
        let mut pending_gtid = None;
        while commits < expected.len() {
            let (_, event) = async_std::future::timeout(Duration::from_secs(10), stream.read())
                .await
                .unwrap()
                .unwrap();
            let mut events = vec![event];
            while let Some(event) = events.pop() {
                match event {
                    EventData::Gtid(event) => {
                        assert_eq!(event.flags, 0);
                        pending_gtid = Some(event.gtid.clone());
                        gtids.push(event.gtid);
                    }
                    EventData::TransactionPayload(payload) => events.extend(
                        payload
                            .uncompressed_events
                            .into_iter()
                            .rev()
                            .map(|(_, event)| event),
                    ),
                    EventData::WriteRows(event) => {
                        assert_eq!(event.rows.len(), 1);
                        match event.rows[0].column_values.as_slice() {
                            [ColumnValue::Long(id), ColumnValue::Long(value)] => {
                                rows.push((*id, *value))
                            }
                            other => panic!("unexpected row {:?}", other),
                        }
                    }
                    EventData::Xid(_) => {
                        assert!(checkpoint
                            .add(&pending_gtid.take().expect("GTID before commit"))
                            .unwrap());
                        commits += 1;
                    }
                    _ => {}
                }
            }
        }
        assert_eq!(
            gtids,
            expected
                .iter()
                .map(|(suffix, _)| format!("{}:{}", uuid, suffix))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            rows,
            expected
                .iter()
                .map(|(_, id)| (*id, id * 10))
                .collect::<Vec<_>>()
        );
    }
}
