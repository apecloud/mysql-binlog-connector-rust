pub mod test {
    use std::{
        env,
        sync::{Arc, Mutex},
        thread,
        time::Duration,
    };

    use async_std::task::block_on;

    use crate::{
        binlog_client::BinlogClient,
        binlog_error::BinlogError,
        command::command_util::CommandUtil,
        event::{
            delete_rows_event::DeleteRowsEvent, event_data::EventData, query_event::QueryEvent,
            update_rows_event::UpdateRowsEvent, write_rows_event::WriteRowsEvent,
        },
    };

    pub struct TestRunner {
        pub insert_events: Vec<WriteRowsEvent>,
        pub update_events: Vec<UpdateRowsEvent>,
        pub delete_events: Vec<DeleteRowsEvent>,
        pub query_events: Vec<QueryEvent>,
        pub binlog_parse_millis: u64,
        pub host: String,
        pub port: String,
        pub user_name: String,
        pub password: String,
        pub schema: String,
        pub server_id: u64,
        pub default_db: String,
        pub default_tb: String,
    }

    #[allow(dead_code)]
    impl TestRunner {
        pub fn new() -> TestRunner {
            // load environment variables
            let env_path = env::current_dir().unwrap().join("src/test/.env");
            dotenv::from_path(env_path).unwrap();
            let host = env::var("host").unwrap();
            let port = env::var("port").unwrap();
            let user_name = env::var("user_name").unwrap();
            let password = env::var("password").unwrap();
            let schema = env::var("schema").unwrap();
            let server_id: u64 = env::var("server_id").unwrap().parse().unwrap();
            let default_db = env::var("default_db").unwrap();
            let default_tb = env::var("default_tb").unwrap();

            let runner = TestRunner {
                insert_events: Vec::new(),
                update_events: Vec::new(),
                delete_events: Vec::new(),
                query_events: Vec::new(),
                binlog_parse_millis: 100,
                host,
                port,
                user_name,
                password,
                schema,
                server_id,
                default_db,
                default_tb,
            };

            // run init sqls to prepare test dabase
            let prepare_sqls = vec![
                "DROP DATABASE IF EXISTS ".to_string() + &runner.default_db,
                "CREATE DATABASE ".to_string() + &runner.default_db,
            ];
            let test_sqls = vec![];
            let _ = block_on(runner.execute_sqls(prepare_sqls, test_sqls));

            runner
        }

        pub fn get_create_table_sql_with_one_field(&self, field_type: String) -> String {
            format!(
                "CREATE TABLE {}.{} (f_0 {})",
                self.default_db, self.default_tb, field_type
            )
        }

        pub fn execute_insert_sqls_and_get_binlogs(
            &mut self,
            prepare_sqls: Vec<String>,
            values: Vec<String>,
        ) {
            let insert_sql = format!(
                "INSERT INTO {}.{} VALUES ",
                self.default_db, self.default_tb
            );
            let mut test_sqls = Vec::with_capacity(values.capacity());
            for v in values {
                test_sqls.push(insert_sql.clone() + v.as_str());
            }

            let events = block_on(self.execute_sqls_and_get_binlogs(prepare_sqls, test_sqls));
            for data in events {
                match data {
                    EventData::WriteRows(event) => {
                        self.insert_events.push(event);
                    }
                    _ => {}
                }
            }
        }

        pub fn execute_delete_sqls_and_get_binlogs(
            &mut self,
            prepare_sqls: Vec<String>,
            f_0_values: Vec<String>,
        ) {
            let mut test_sqls = Vec::with_capacity(f_0_values.capacity());
            if f_0_values.is_empty() {
                test_sqls.push(format!(
                    "DELETE FROM {}.{}",
                    self.default_db, self.default_tb
                ));
            } else {
                let delete_sql = format!(
                    "DELETE FROM {}.{} WHERE f_0 in ",
                    self.default_db, self.default_tb
                );
                for v in f_0_values {
                    test_sqls.push(delete_sql.clone() + v.as_str());
                }
            }

            let events = block_on(self.execute_sqls_and_get_binlogs(prepare_sqls, test_sqls));
            for data in events {
                match data {
                    EventData::DeleteRows(event) => {
                        self.delete_events.push(event);
                    }
                    _ => {}
                }
            }
        }

        pub fn execute_update_sqls_and_get_binlogs(
            &mut self,
            prepare_sqls: Vec<String>,
            test_sqls: Vec<String>,
        ) {
            let events = block_on(self.execute_sqls_and_get_binlogs(prepare_sqls, test_sqls));
            for data in events {
                match data {
                    EventData::UpdateRows(event) => {
                        self.update_events.push(event);
                    }
                    _ => {}
                }
            }
        }

        pub fn execute_ddl_sqls_and_get_binlogs(
            &mut self,
            prepare_sqls: Vec<String>,
            test_sqls: Vec<String>,
        ) {
            let events = block_on(self.execute_sqls_and_get_binlogs(prepare_sqls, test_sqls));
            for data in events {
                match data {
                    EventData::Query(event) => {
                        self.query_events.push(event);
                    }
                    _ => {}
                }
            }
        }

        async fn execute_sqls_and_get_binlogs(
            &mut self,
            prepare_sqls: Vec<String>,
            test_sqls: Vec<String>,
        ) -> Vec<EventData> {
            // execute sqls, binlog_position will start from the first test sql, prepare sqls will be ignored.
            let (binlog_filename, binlog_position) =
                self.execute_sqls(prepare_sqls, test_sqls).await.unwrap();

            // parse binlogs
            let client = BinlogClient {
                hostname: self.host.clone(),
                port: self.port.clone(),
                username: self.user_name.clone(),
                password: self.password.clone(),
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
            let results = all_events.lock().unwrap().to_vec();
            results
        }

        async fn execute_sqls(
            &self,
            prepare_sqls: Vec<String>,
            test_sqls: Vec<String>,
        ) -> Result<(String, u64), BinlogError> {
            let mut channel = CommandUtil::connect_and_authenticate(
                self.host.clone(),
                self.port.clone(),
                self.user_name.clone(),
                self.password.clone(),
                self.schema.clone(),
            )
            .await?;

            for sql in prepare_sqls {
                CommandUtil::execute_sql(&mut channel, sql).await?;
            }

            // get current binlog info
            let (binlog_filename, binlog_position) =
                CommandUtil::fetch_binlog_info(&mut channel).await?;

            for sql in test_sqls {
                CommandUtil::execute_sql(&mut channel, sql.to_string()).await?;
            }

            Ok((binlog_filename, binlog_position))
        }
    }
}
