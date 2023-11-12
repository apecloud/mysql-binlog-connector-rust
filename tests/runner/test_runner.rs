pub(crate) mod test {
    use std::{
        sync::{Arc, Mutex},
        thread,
        time::Duration,
    };

    use async_std::task::block_on;
    use mysql_binlog_connector_rust::{
        binlog_client::BinlogClient,
        binlog_error::BinlogError,
        command::{authenticator::Authenticator, command_util::CommandUtil},
        event::{
            delete_rows_event::DeleteRowsEvent, event_data::EventData, query_event::QueryEvent,
            update_rows_event::UpdateRowsEvent, write_rows_event::WriteRowsEvent,
        },
    };

    use crate::runner::{env::test::Env, mock::test::Mock};

    pub struct TestRunner {
        pub insert_events: Vec<WriteRowsEvent>,
        pub update_events: Vec<UpdateRowsEvent>,
        pub delete_events: Vec<DeleteRowsEvent>,
        pub query_events: Vec<QueryEvent>,
        pub binlog_parse_millis: u64,
        pub db_url: String,
        pub server_id: u64,
        pub default_db: String,
        pub default_tb: String,
    }

    #[allow(dead_code)]
    impl TestRunner {
        pub fn new() -> TestRunner {
            // load environment variables
            let env = Env::load_vars();
            let runner = TestRunner {
                insert_events: Vec::new(),
                update_events: Vec::new(),
                delete_events: Vec::new(),
                query_events: Vec::new(),
                db_url: env.get(Env::DB_URL).unwrap().to_string(),
                default_db: env.get(Env::DEFAULT_DB).unwrap().to_string(),
                default_tb: env.get(Env::DEFAULT_TB).unwrap().to_string(),
                server_id: env
                    .get(Env::SERVER_ID)
                    .unwrap()
                    .to_string()
                    .parse::<u64>()
                    .unwrap(),
                binlog_parse_millis: env
                    .get(Env::BINLOG_PARSE_MILLIS)
                    .unwrap()
                    .to_string()
                    .parse::<u64>()
                    .unwrap(),
            };

            // run init sqls to prepare test dabase
            let prepare_sqls = vec![
                "DROP DATABASE IF EXISTS ".to_string() + &runner.default_db,
                "CREATE DATABASE ".to_string() + &runner.default_db,
            ];
            let test_sqls = vec![];
            let _ = block_on(runner.execute_sqls(&prepare_sqls, &test_sqls));

            runner
        }

        pub fn run_one_col_test(
            col_type: &str,
            values: &[&str],
            prepare_sqls: &[&str],
        ) -> TestRunner {
            let mut runner = TestRunner::new();
            let create_sql = Mock::one_col_create_sql(col_type);
            let insert_sql = Mock::one_col_insert_sql(values);

            let mut prepare_sqls: Vec<String> =
                prepare_sqls.into_iter().map(|i| i.to_string()).collect();
            prepare_sqls.push(create_sql);

            runner.execute_sqls_and_get_binlogs(&prepare_sqls, &vec![insert_sql]);
            runner
        }

        pub fn execute_sqls_and_get_binlogs(
            &mut self,
            prepare_sqls: &Vec<String>,
            test_sqls: &Vec<String>,
        ) {
            block_on(self.execute_sqls_and_get_binlogs_internal(&prepare_sqls, &test_sqls));
        }

        async fn execute_sqls_and_get_binlogs_internal(
            &mut self,
            prepare_sqls: &Vec<String>,
            test_sqls: &Vec<String>,
        ) {
            // execute sqls, binlog_position will start from the first test sql, prepare sqls will be ignored.
            let (binlog_filename, binlog_position) =
                self.execute_sqls(prepare_sqls, test_sqls).await.unwrap();

            // parse binlogs
            let client = BinlogClient {
                url: self.db_url.clone(),
                binlog_filename,
                binlog_position,
                server_id: self.server_id,
            };

            let all_events = Arc::new(Mutex::new(Vec::new()));
            let all_events_clone = all_events.clone();
            let parse_binlogs = |mut client: BinlogClient, events: Arc<Mutex<Vec<EventData>>>| async move {
                let mut stream = client.connect().await.unwrap();
                loop {
                    let result = stream.read().await;
                    if let Err(_error) = result {
                        break;
                    } else {
                        events.lock().unwrap().push(result.unwrap().1);
                    }
                }
            };
            thread::spawn(move || block_on(parse_binlogs(client, all_events_clone)));

            // wait for binlog parsing
            async_std::task::sleep(Duration::from_millis(self.binlog_parse_millis)).await;

            for data in all_events.lock().unwrap().to_vec() {
                match data {
                    EventData::WriteRows(event) => {
                        self.insert_events.push(event);
                    }
                    // mysql8.0 with binlog transaction compression
                    EventData::TransactionPayload(event) => {
                        for (_header, data) in event.uncompressed_events {
                            match data {
                                EventData::WriteRows(event) => {
                                    self.insert_events.push(event);
                                }
                                EventData::UpdateRows(event) => {
                                    self.update_events.push(event);
                                }
                                EventData::DeleteRows(event) => {
                                    self.delete_events.push(event);
                                }
                                _ => {}
                            }
                        }
                    }
                    EventData::UpdateRows(event) => {
                        self.update_events.push(event);
                    }
                    EventData::DeleteRows(event) => {
                        self.delete_events.push(event);
                    }
                    EventData::Query(event) => {
                        self.query_events.push(event);
                    }
                    _ => {}
                }
            }
        }

        async fn execute_sqls(
            &self,
            prepare_sqls: &Vec<String>,
            test_sqls: &Vec<String>,
        ) -> Result<(String, u32), BinlogError> {
            let mut authenticator = Authenticator::new(&self.db_url)?;
            let mut channel = authenticator.connect().await?;

            for sql in prepare_sqls {
                println!("execute prepare sql: {}", sql);
                CommandUtil::execute_sql(&mut channel, &sql).await?;
            }

            // get current binlog info
            let (binlog_filename, binlog_position) =
                CommandUtil::fetch_binlog_info(&mut channel).await?;

            for sql in test_sqls {
                println!("execute test sql: {}", sql);
                CommandUtil::execute_sql(&mut channel, &sql).await?;
            }

            Ok((binlog_filename, binlog_position))
        }
    }

    impl Default for TestRunner {
        fn default() -> Self {
            Self::new()
        }
    }
}
