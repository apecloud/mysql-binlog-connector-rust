use std::env;

use futures::executor::block_on;
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient,
    column::{column_value::ColumnValue, json::json_binary::JsonBinary},
    event::{event_data::EventData, row_event::RowEvent},
};

fn main() {
    let env_path = env::current_dir().unwrap().join("example/src/.env");
    dotenv::from_path(env_path).unwrap();
    let db_url = env::var("db_url").unwrap();
    let server_id: u64 = env::var("server_id").unwrap().parse().unwrap();
    let binlog_filename = env::var("binlog_filename").unwrap();
    let binlog_position: u32 = env::var("binlog_position").unwrap().parse().unwrap();

    block_on(start_client(
        db_url,
        server_id,
        binlog_filename,
        binlog_position,
    ));
}

async fn start_client(url: String, server_id: u64, binlog_filename: String, binlog_position: u32) {
    let mut client = BinlogClient {
        url,
        binlog_filename,
        binlog_position,
        server_id,
    };

    let mut stream = client.connect().await.unwrap();

    loop {
        let (header, data) = stream.read().await.unwrap();
        println!("header: {:?}", header);
        println!("data: {:?}", data);
        println!("");
    }
}

fn parse_json_columns(data: EventData) {
    let parse_row = |row: RowEvent| {
        for column_value in row.column_values {
            if let ColumnValue::Json(bytes) = column_value {
                println!(
                    "json column: {}",
                    JsonBinary::parse_as_string(&bytes).unwrap()
                )
            }
        }
    };

    match data {
        EventData::WriteRows(event) => {
            for row in event.rows {
                parse_row(row)
            }
        }
        EventData::DeleteRows(event) => {
            for row in event.rows {
                parse_row(row)
            }
        }
        EventData::UpdateRows(event) => {
            for (before, after) in event.rows {
                parse_row(before);
                parse_row(after);
            }
        }
        _ => {}
    }
}
