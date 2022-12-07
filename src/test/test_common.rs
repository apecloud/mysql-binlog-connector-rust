pub mod test {
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread,
        time::Duration,
    };

    use crate::{
        binlog_client::BinlogClient,
        binlog_error::BinlogError,
        command::command_util::CommandUtil,
        event::{
            delete_rows_event::DeleteRowsEvent, event_data::EventData, event_header::EventHeader,
            query_event::QueryEvent, update_rows_event::UpdateRowsEvent,
            write_rows_event::WriteRowsEvent,
        },
    };

    const HOSTNAME: &str = "127.0.0.1";
    const PORT: &str = "3307";
    const USERNAME: &str = "root";
    const PASSWORD: &str = "123456";
    const SCHEMA: &str = "";
    const SERVER_ID: u64 = 200;

    pub const DB: &str = "db_test";
    pub const TB: &str = "tb_test";

    pub static mut I_EVENTS: Vec<WriteRowsEvent> = Vec::new();
    pub static mut U_EVENTS: Vec<UpdateRowsEvent> = Vec::new();
    pub static mut D_EVENTS: Vec<DeleteRowsEvent> = Vec::new();
    pub static mut Q_EVENTS: Vec<QueryEvent> = Vec::new();
    pub static mut BINLOG_PARSING_MILLIS: u64 = 100;

    pub struct TestCommon {}

    #[allow(dead_code)]
    impl TestCommon {
        pub fn before_dml() {
            Self::clear_events();
            let prepare_sqls = vec![
                "DROP DATABASE IF EXISTS ".to_string() + DB,
                "CREATE DATABASE ".to_string() + DB,
            ];
            let test_sqls = vec![];
            let (_, _) = Self::execute_sqls(prepare_sqls, test_sqls).unwrap();
        }

        pub fn clear_events() {
            unsafe {
                I_EVENTS.clear();
                U_EVENTS.clear();
                D_EVENTS.clear();
                Q_EVENTS.clear();
            }
        }

        fn on_insert_event(_header: EventHeader, data: EventData) {
            unsafe {
                match data {
                    EventData::WriteRows(event) => {
                        I_EVENTS.push(event);
                    }
                    _ => {}
                }
            }
        }

        fn on_delete_event(_header: EventHeader, data: EventData) {
            unsafe {
                match data {
                    EventData::DeleteRows(event) => {
                        D_EVENTS.push(event);
                    }
                    _ => {}
                }
            }
        }

        fn on_update_event(_header: EventHeader, data: EventData) {
            unsafe {
                match data {
                    EventData::UpdateRows(event) => {
                        U_EVENTS.push(event);
                    }
                    _ => {}
                }
            }
        }

        fn on_ddl_event(_header: EventHeader, data: EventData) {
            unsafe {
                match data {
                    EventData::Query(event) => {
                        Q_EVENTS.push(event);
                    }
                    _ => {}
                }
            }
        }

        pub fn get_create_table_sql_with_one_field(field_type: String) -> String {
            format!("CREATE TABLE {}.{} (f_0 {})", DB, TB, field_type)
        }

        pub fn execute_insert_sqls_and_get_binlogs(prepare_sqls: Vec<String>, values: Vec<String>) {
            let insert_sql = format!("INSERT INTO {}.{} VALUES ", DB, TB);
            let mut test_sqls = Vec::with_capacity(values.capacity());
            for v in values {
                test_sqls.push(insert_sql.clone() + v.as_str());
            }
            Self::execute_sqls_and_get_binlogs(prepare_sqls, test_sqls, Self::on_insert_event);
        }

        pub fn execute_delete_sqls_and_get_binlogs(
            prepare_sqls: Vec<String>,
            f_0_values: Vec<String>,
        ) {
            let mut test_sqls = Vec::with_capacity(f_0_values.capacity());
            if f_0_values.is_empty() {
                test_sqls.push(format!("DELETE FROM {}.{}", DB, TB));
            } else {
                let delete_sql = format!("DELETE FROM {}.{} WHERE f_0 in ", DB, TB);
                for v in f_0_values {
                    test_sqls.push(delete_sql.clone() + v.as_str());
                }
            }
            Self::execute_sqls_and_get_binlogs(prepare_sqls, test_sqls, Self::on_delete_event);
        }

        pub fn execute_update_sqls_and_get_binlogs(
            prepare_sqls: Vec<String>,
            test_sqls: Vec<String>,
        ) {
            Self::execute_sqls_and_get_binlogs(prepare_sqls, test_sqls, Self::on_update_event);
        }

        pub fn execute_ddl_sqls_and_get_binlogs(prepare_sqls: Vec<String>, test_sqls: Vec<String>) {
            Self::execute_sqls_and_get_binlogs(prepare_sqls, test_sqls, Self::on_ddl_event);
        }

        fn execute_sqls_and_get_binlogs(
            prepare_sqls: Vec<String>,
            test_sqls: Vec<String>,
            on_event: fn(EventHeader, EventData),
        ) {
            // execute sqls, binlog_position will start from the first test sql, prepare sqls will be ignored.
            let (binlog_filename, binlog_position) =
                Self::execute_sqls(prepare_sqls, test_sqls).unwrap();

            // parse binlogs
            let shut_down = Arc::new(AtomicBool::new(false));
            let mut client = BinlogClient {
                hostname: HOSTNAME.to_string(),
                port: PORT.to_string(),
                username: USERNAME.to_string(),
                password: PASSWORD.to_string(),
                binlog_filename,
                binlog_position,
                server_id: SERVER_ID,
                shut_down: shut_down.clone(),
                on_event,
            };

            thread::spawn(move || {
                let _ = client.connect();
            });

            // wait for binlog parsing
            unsafe {
                thread::sleep(Duration::from_millis(BINLOG_PARSING_MILLIS));
            }
            shut_down.store(true, Ordering::Relaxed);
        }

        fn execute_sqls(
            prepare_sqls: Vec<String>,
            test_sqls: Vec<String>,
        ) -> Result<(String, u64), BinlogError> {
            let mut channel = CommandUtil::connect_and_authenticate(
                HOSTNAME.to_string(),
                PORT.to_string(),
                USERNAME.to_string(),
                PASSWORD.to_string(),
                SCHEMA.to_string(),
            )
            .unwrap();

            for sql in prepare_sqls {
                CommandUtil::execute_sql(&mut channel, sql).unwrap();
            }

            // get current binlog info
            let (binlog_filename, binlog_position) =
                CommandUtil::fetch_binlog_info(&mut channel).unwrap();

            for sql in test_sqls {
                CommandUtil::execute_sql(&mut channel, sql.to_string()).unwrap();
            }

            Ok((binlog_filename, binlog_position))
        }
    }
}
