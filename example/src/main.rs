use std::{collections::HashMap, env, fs::File};

use futures::executor::block_on;
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient,
    binlog_parser::BinlogParser,
    column::{column_value::ColumnValue, json::json_binary::JsonBinary},
    event::{event_data::EventData, row_event::RowEvent},
};

fn main() {
    // example 1: dump and parse binlogs from mysql
    block_on(dump_and_parse())

    // example 2: parse mysql binlog file
    // block_on(parse_file())
}

async fn dump_and_parse() {
    let env_path = env::current_dir().unwrap().join("example/src/.env");
    dotenv::from_path(env_path).unwrap();
    let url = env::var("db_url").unwrap();
    let server_id: u64 = env::var("server_id").unwrap().parse().unwrap();
    let binlog_filename = env::var("binlog_filename").unwrap();
    let binlog_position: u32 = env::var("binlog_position").unwrap().parse().unwrap();

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
        println!();
    }
}

#[allow(dead_code)]
async fn parse_file() {
    let file_path = "path-to-binlog-file";
    let mut file = File::open(file_path).unwrap();

    let mut parser = BinlogParser {
        checksum_length: 4,
        table_map_event_by_table_id: HashMap::new(),
    };

    assert!(parser.check_magic(&mut file).is_ok());
    while let Ok((header, data)) = parser.next(&mut file) {
        println!("header: {:?}", header);
        println!("data: {:?}", data);
        println!();
    }
}

#[allow(dead_code)]
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
