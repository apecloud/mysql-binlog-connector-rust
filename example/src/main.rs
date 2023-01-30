use std::env;

use futures::executor::block_on;
use mysql_binlog_connector_rust::binlog_client::BinlogClient;

fn main() {
    let env_path = env::current_dir().unwrap().join("example/src/.env");
    dotenv::from_path(env_path).unwrap();
    let db_url = env::var("db_url").unwrap();
    let server_id: u64 = env::var("server_id").unwrap().parse().unwrap();
    let binlog_filename = env::var("binlog_filename").unwrap();
    let binlog_position: u64 = env::var("binlog_position").unwrap().parse().unwrap();

    block_on(start_client(
        db_url,
        server_id,
        binlog_filename,
        binlog_position,
    ));
}

async fn start_client(url: String, server_id: u64, binlog_filename: String, binlog_position: u64) {
    let mut client = BinlogClient {
        url,
        binlog_filename,
        binlog_position,
        server_id,
    };

    let mut stream = client.connect().await.unwrap();

    loop {
        let (_header, data) = stream.read().await.unwrap();
        println!("{}", format!("recevied data: {:?}", data));
    }
}
