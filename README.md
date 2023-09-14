# mysql-binlog-connector-rust

## Overview

## Quick start
### Run tests

setup env: 
- run an empty mysql server with following configs: 
```
hostname = 127.0.0.1
port = 3307
username = root
password = 123456
```
- you may easily start a docker container with:
```
docker run -d --name mysql57 \
--platform linux/x86_64 \
-it --restart=always \
-v [path-to-your-local-folder-to-store-mysql-data]:/var/lib/mysql \
-p 3307:3306 \
-e MYSQL_ROOT_PASSWORD="123456" \
mysql:5.7.40 \
--lower_case_table_names=1 \
--character-set-server=utf8 \
--collation-server=utf8_general_ci \
--datadir=/var/lib/mysql \
--user=mysql \
--server_id=1 \
--log_bin=/var/lib/mysql/mysql-bin.log \
--max_binlog_size=100M \
--gtid_mode=ON \
--enforce_gtid_consistency=ON \
--binlog_format=ROW 
```

run tests: 
- you may need to modify tests/.env to suit your test environment
```
cargo test --package mysql-binlog-connector-rust --test integration_test
```

## Example
```rust
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
        let (_header, data) = stream.read().await.unwrap();
        println!("recevied data: {:?}", data);
    }
}
```

## Parse json field 
```rust
fn parse_json_as_string(column_value: &ColumnValue) -> Result<String, BinlogError> {
    match column_value {
        ColumnValue::Json(bytes) => JsonBinary::parse_as_string(bytes),
        _ => Err(BinlogError::ParseJsonError(
            "column value is not json".into(),
        )),
    }
}
```
