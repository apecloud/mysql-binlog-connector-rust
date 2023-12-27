# mysql-binlog-connector-rust

## Overview
- A simple but strong lib to parse mysql Row Based Replication Events in RUST with async IO.

### Supported mysql versions
- mysql 5.6 (tested in mysql:5.6.51)
- mysql 5.7 (tested in mysql:5.7.40)
- mysql 8.0 (tested in mysql:8.0.31)

### Supported event types
- FORMAT_DESCRIPTION_EVENT
- ROTATE_EVENT
- PREVIOUS_GTIDS_LOG_EVENT
- GTID_LOG_EVENT
- QUERY_EVENT
- XID_EVENT
- XA_PREPARE_LOG_EVENT
- TRANSACTION_PAYLOAD_EVENT
- ROWS_QUERY_LOG_EVENT
- TABLE_MAP_EVENT
- WRITE_ROWS_EVENT_V1
- WRITE_ROWS_EVENT
- UPDATE_ROWS_EVENT_V1
- UPDATE_ROWS_EVENT
- DELETE_ROWS_EVENT_V1
- DELETE_ROWS_EVENT

- for more details, refer to: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication_binlog_event.html

### Mapping between mysql columns and rust types
| mysql column type | binlog column type(raw) | binlog column type(parsed from binlog column meta) | rust type       |
| :---------------- | :---------------------- | :------------------------------------------------- | :---------      |
| BIT | MYSQL_TYPE_BIT = 16 | ColumnType::Bit | ColumnValue::Bit(u64) |
| TINYINT [UNSIGNED] | MYSQL_TYPE_TINY = 1 | ColumnType::Tiny | ColumnValue::Tiny(i8) |
| SMALLINT [UNSIGNED] | MYSQL_TYPE_SHORT = 2 | ColumnType::Short | ColumnValue::Short(i16) |
| MEDIUMINT [UNSIGNED] | MYSQL_TYPE_INT24 = 9 | ColumnType::Int24 | ColumnValue::Long(i32) |
| INT [UNSIGNED] | MYSQL_TYPE_LONG = 3 | ColumnType::Long | ColumnValue::Long(i32) |
| BIGINT [UNSIGNED] | MYSQL_TYPE_LONGLONG = 8 | ColumnType::LongLong | ColumnValue::LongLong(i64) |
| FLOAT | MYSQL_TYPE_FLOAT = 4 | ColumnType::Float | ColumnValue::Float(f32) |
| DOUBLE | MYSQL_TYPE_DOUBLE = 5 | ColumnType::Double | ColumnValue::Double(f64) |
| DECIMAL | MYSQL_TYPE_NEWDECIMAL = 246 | ColumnType::NewDecimal | ColumnValue::Decimal(String) |
| DATE | MYSQL_TYPE_DATE = 10 | ColumnType::Date | ColumnValue::Date(String) |
| TIME | MYSQL_TYPE_TIME2 = 19 | ColumnType::Time2 | ColumnValue::Time(String) |
| TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 = 17 | ColumnType::TimeStamp2 | ColumnValue::Timestamp(i64) |
| DATETIME | MYSQL_TYPE_DATETIME2 = 18 | ColumnType::DateTime2 | ColumnValue::DateTime(String) |
| YEAR | MYSQL_TYPE_YEAR = 13 | ColumnType::Year | ColumnValue::Year(u16) |
| CHAR | MYSQL_TYPE_STRING = 254 | ColumnType::String | ColumnValue::String(Vec&lt;u8&gt;) |
| VARCHAR | MYSQL_TYPE_VARCHAR = 15 | ColumnType::VarChar | ColumnValue::String(Vec&lt;u8&gt;) |
| BINARY | MYSQL_TYPE_STRING = 254 | ColumnType::String | ColumnValue::String(Vec&lt;u8&gt;) |
| VARBINARY | MYSQL_TYPE_VARCHAR = 15 | ColumnType::VarChar | ColumnValue::String(Vec&lt;u8&gt;) |
| ENUM | MYSQL_TYPE_STRING = 254 | ColumnType::Enum | ColumnValue::Enum(u32) |
| SET | MYSQL_TYPE_STRING = 254 | ColumnType::Set | ColumnValue::Set(u64) |
| TINYTEXT TEXT MEDIUMTEXT LONGTEXT TINYBLOB BLOB MEDIUMBLOB LONGBLOB | MYSQL_TYPE_BLOB = 252 | ColumnType::Blob | ColumnValue::Blob(Vec&lt;u8&gt;) |
| GEOMETRY | MYSQL_TYPE_GEOMETRY = 255 | ColumnType::Geometry | ColumnValue::Blob(Vec&lt;u8&gt;) |
| JSON | MYSQL_TYPE_JSON = 245 | ColumnType::Json | ColumnValue::Json(Vec&lt;u8&gt;) |

- for CHAR / VARCHAR columns, since binlog contains no charset information, we just get raw bytes and store them in ColumnValue::String(Vec&lt;u8&gt;) objects, you may need to convert them into strings based on column metadatas for further usage.
- for UNSIGNED numeric columns, since binlog contains no unsigned flags, we just parse them as signed numerics, you may need to convert them into unsigned values based on column metadatas for further usage.
- for JSON columns, we get raw bytes and store them in ColumnValue::Json(Vec&lt;u8&gt;) objects, we also provide a default deserializer "JsonBinary" to parse them into strings, find example later in this doc.

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

## Parse json column to string
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
