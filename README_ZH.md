[English](README.md) | 中文

# mysql-binlog-connector-rust

## 概览
- 使用异步 IO 拉取并解析 mysql binlog（binlog_format=ROW）

### 支持的 mysql 版本
- mysql 5.6 (tested in mysql:5.6.51)
- mysql 5.7 (tested in mysql:5.7.40)
- mysql 8.0 (tested in mysql:8.0.31)

### 支持的复制模式
- binlog-file-position-based replication
- gtid-based replication

### 支持的事件类型
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

- 更多细节, 参考: [mysql doc](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication_binlog_event.html)

### mysql 数据类型和 rust 数据类型映射
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

- 对于 CHAR / VARCHAR 列，由于 binlog 不包含字符集信息，我们只获取二进制数据并存储在 ColumnValue::String(Vec&lt;u8&gt;) 对象中，用户需根据列的元数据进行转换。
- 对于 UNSIGNED 数字列，由于 binlog 不包含符号标志，我们只将其解析为有符号数字，用户需根据列的元数据进行转换。
- 对于 JSON 列，我们只获取二进制数据并将其存储在 ColumnValue::Json(Vec&lt;u8&gt;) 对象中，同时我们还提供一个的默认解析器 JsonBinary 将其解析为字符串，本文后续有相应示例。


## 快速开始
### 运行测试用例
- docker 启动 mysql 5.7，开启 binlog，关闭 binlog-transaction-compression
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

- docker 启动 mysql 8.0，开启 binlog，打开 binlog-transaction-compression
```
docker run -d --name mysql80 \
--platform linux/x86_64 \
-it  --restart=always \
-p 3308:3306 -e MYSQL_ROOT_PASSWORD="123456" \
 mysql:8.0.31 \
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
 --binlog_format=ROW \
 --binlog-transaction-compression \
 --binlog_rows_query_log_events=ON \
 --default_authentication_plugin=mysql_native_password \
 --default_time_zone="+08:00"
```

- 更新 tests/.env 中的配置
```
db_url=mysql://root:123456@127.0.0.1:3307
server_id=200
default_db="db_test"
default_tb="tb_test"
binlog_parse_millis=100
```

- 运行测试
```
cargo test --package mysql-binlog-connector-rust --test integration_test
```
- 每个测试用例会：
- &nbsp; &nbsp; &nbsp; &nbsp; 执行 sql 并生成 binlog
- &nbsp; &nbsp; &nbsp; &nbsp; 获取 binlog 并解析
- &nbsp; &nbsp; &nbsp; &nbsp; 等待 binlog_parse_millis 以将所有 binlog 解析完毕
- 对于大事务，可能需要增大 binlog_parse_millis

## 用例
```rust
fn main() {
    block_on(dump_and_parse())
}

async fn dump_and_parse() {
    let env_path = env::current_dir().unwrap().join("example/src/.env");
    dotenv::from_path(env_path).unwrap();
    let url = env::var("db_url").unwrap();
    let server_id: u64 = env::var("server_id").unwrap().parse().unwrap();
    let binlog_filename = env::var("binlog_filename").unwrap();
    let binlog_position: u32 = env::var("binlog_position").unwrap().parse().unwrap();
    let gtid_enabled: bool = env::var("gtid_enabled").unwrap().parse().unwrap();
    let gtid_set = env::var("gtid_set").unwrap();

    let mut client = BinlogClient {
        url,
        binlog_filename,
        binlog_position,
        server_id,
        gtid_enabled,
        gtid_set,
    };

    let mut stream = client.connect().await.unwrap();

    loop {
        let (header, data) = stream.read().await.unwrap();
        println!("header: {:?}", header);
        println!("data: {:?}", data);
        println!();
    }
}
```

### 用例 1: 解析关闭 binlog-transaction-compression 的事务
- 执行 sql 
```sql
flush logs;

SET autocommit=0; 
CREATE DATABASE test_db;
USE test_db;
CREATE TABLE test_tb(id INT, value INT);
INSERT INTO test_tb VALUES(1,1),(2,2),(3,3),(4,4);
UPDATE test_tb SET value=3 WHERE id in(1,2);
DELETE FROM test_tb WHERE id in (1,2);
TRUNCATE TABLE test_tb;
DROP TABLE test_tb;
commit;
```

- 查看 binlog 事件
```sql
mysql> show binary logs;
+------------------+-----------+
| Log_name         | File_size |
+------------------+-----------+
| mysql-bin.000050 |      1255 |

mysql> show binlog events in 'mysql-bin.000050';
+------------------+------+----------------+-----------+-------------+------------------------------------------------------------------------+
| Log_name         | Pos  | Event_type     | Server_id | End_log_pos | Info                                                                   |
+------------------+------+----------------+-----------+-------------+------------------------------------------------------------------------+
| mysql-bin.000050 |    4 | Format_desc    |         1 |         123 | Server ver: 5.7.40-log, Binlog ver: 4                                  |
| mysql-bin.000050 |  123 | Previous_gtids |         1 |         194 | 50dc6874-13d3-11ee-a17a-0242ac110002:1-176027                          |
| mysql-bin.000050 |  194 | Gtid           |         1 |         259 | SET @@SESSION.GTID_NEXT= '50dc6874-13d3-11ee-a17a-0242ac110002:176028' |
| mysql-bin.000050 |  259 | Query          |         1 |         378 | use `test_db`; CREATE TABLE test_tb(id INT, value INT)                 |
| mysql-bin.000050 |  378 | Gtid           |         1 |         443 | SET @@SESSION.GTID_NEXT= '50dc6874-13d3-11ee-a17a-0242ac110002:176029' |
| mysql-bin.000050 |  443 | Query          |         1 |         518 | BEGIN                                                                  |
| mysql-bin.000050 |  518 | Table_map      |         1 |         572 | table_id: 12832 (test_db.test_tb)                                      |
| mysql-bin.000050 |  572 | Write_rows     |         1 |         643 | table_id: 12832 flags: STMT_END_F                                      |
| mysql-bin.000050 |  643 | Table_map      |         1 |         697 | table_id: 12832 (test_db.test_tb)                                      |
| mysql-bin.000050 |  697 | Update_rows    |         1 |         769 | table_id: 12832 flags: STMT_END_F                                      |
| mysql-bin.000050 |  769 | Table_map      |         1 |         823 | table_id: 12832 (test_db.test_tb)                                      |
| mysql-bin.000050 |  823 | Delete_rows    |         1 |         876 | table_id: 12832 flags: STMT_END_F                                      |
| mysql-bin.000050 |  876 | Xid            |         1 |         907 | COMMIT /* xid=13739 */                                                 |
| mysql-bin.000050 |  907 | Gtid           |         1 |         972 | SET @@SESSION.GTID_NEXT= '50dc6874-13d3-11ee-a17a-0242ac110002:176030' |
| mysql-bin.000050 |  972 | Query          |         1 |        1064 | use `test_db`; TRUNCATE TABLE test_tb                                  |
| mysql-bin.000050 | 1064 | Gtid           |         1 |        1129 | SET @@SESSION.GTID_NEXT= '50dc6874-13d3-11ee-a17a-0242ac110002:176031' |
| mysql-bin.000050 | 1129 | Query          |         1 |        1255 | use `test_db`; DROP TABLE `test_tb` /* generated by server */          |
+------------------+------+----------------+-----------+-------------+------------------------------------------------------------------------+
```

- 解析出的 binlog
```
header: EventHeader { timestamp: 0, event_type: 4, server_id: 1, event_length: 47, next_event_position: 0, event_flags: 32 }
data: Rotate(RotateEvent { binlog_filename: "mysql-bin.000050", binlog_position: 194 })

header: EventHeader { timestamp: 1704443761, event_type: 15, server_id: 1, event_length: 119, next_event_position: 0, event_flags: 0 }
data: FormatDescription(FormatDescriptionEvent { binlog_version: 4, server_version: "5.7.40-log\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0", create_timestamp: 0, header_length: 19, checksum_type: CRC32 })

header: EventHeader { timestamp: 1704443769, event_type: 33, server_id: 1, event_length: 65, next_event_position: 259, event_flags: 0 }
data: Gtid(GtidEvent { flags: 1, gtid: "50dc6874-13d3-11ee-a17a-0242ac110002:176028" })

header: EventHeader { timestamp: 1704443769, event_type: 2, server_id: 1, event_length: 119, next_event_position: 378, event_flags: 0 }
data: Query(QueryEvent { thread_id: 493, exec_time: 0, error_code: 0, schema: "test_db", query: "CREATE TABLE test_tb(id INT, value INT)" })

header: EventHeader { timestamp: 1704443769, event_type: 33, server_id: 1, event_length: 65, next_event_position: 443, event_flags: 0 }
data: Gtid(GtidEvent { flags: 0, gtid: "50dc6874-13d3-11ee-a17a-0242ac110002:176029" })

header: EventHeader { timestamp: 1704443769, event_type: 2, server_id: 1, event_length: 75, next_event_position: 518, event_flags: 8 }
data: Query(QueryEvent { thread_id: 493, exec_time: 0, error_code: 0, schema: "test_db", query: "BEGIN" })

header: EventHeader { timestamp: 1704443769, event_type: 19, server_id: 1, event_length: 54, next_event_position: 572, event_flags: 0 }
data: TableMap(TableMapEvent { table_id: 12832, database_name: "test_db", table_name: "test_tb", column_types: [3, 3], column_metas: [0, 0], null_bits: [true, true] })

header: EventHeader { timestamp: 1704443769, event_type: 30, server_id: 1, event_length: 71, next_event_position: 643, event_flags: 0 }
data: WriteRows(WriteRowsEvent { table_id: 12832, included_columns: [true, true], rows: [RowEvent { column_values: [Long(1), Long(1)] }, RowEvent { column_values: [Long(2), Long(2)] }, RowEvent { column_values: [Long(3), Long(3)] }, RowEvent { column_values: [Long(4), Long(4)] }] })

header: EventHeader { timestamp: 1704443769, event_type: 19, server_id: 1, event_length: 54, next_event_position: 697, event_flags: 0 }
data: TableMap(TableMapEvent { table_id: 12832, database_name: "test_db", table_name: "test_tb", column_types: [3, 3], column_metas: [0, 0], null_bits: [true, true] })

header: EventHeader { timestamp: 1704443769, event_type: 31, server_id: 1, event_length: 72, next_event_position: 769, event_flags: 0 }
data: UpdateRows(UpdateRowsEvent { table_id: 12832, included_columns_before: [true, true], included_columns_after: [true, true], rows: [(RowEvent { column_values: [Long(1), Long(1)] }, RowEvent { column_values: [Long(1), Long(3)] }), (RowEvent { column_values: [Long(2), Long(2)] }, RowEvent { column_values: [Long(2), Long(3)] })] })

header: EventHeader { timestamp: 1704443769, event_type: 19, server_id: 1, event_length: 54, next_event_position: 823, event_flags: 0 }
data: TableMap(TableMapEvent { table_id: 12832, database_name: "test_db", table_name: "test_tb", column_types: [3, 3], column_metas: [0, 0], null_bits: [true, true] })

header: EventHeader { timestamp: 1704443769, event_type: 32, server_id: 1, event_length: 53, next_event_position: 876, event_flags: 0 }
data: DeleteRows(DeleteRowsEvent { table_id: 12832, included_columns: [true, true], rows: [RowEvent { column_values: [Long(1), Long(3)] }, RowEvent { column_values: [Long(2), Long(3)] }] })

header: EventHeader { timestamp: 1704443769, event_type: 16, server_id: 1, event_length: 31, next_event_position: 907, event_flags: 0 }
data: Xid(XidEvent { xid: 13739 })

header: EventHeader { timestamp: 1704443769, event_type: 33, server_id: 1, event_length: 65, next_event_position: 972, event_flags: 0 }
data: Gtid(GtidEvent { flags: 1, gtid: "50dc6874-13d3-11ee-a17a-0242ac110002:176030" })

header: EventHeader { timestamp: 1704443769, event_type: 2, server_id: 1, event_length: 92, next_event_position: 1064, event_flags: 0 }
data: Query(QueryEvent { thread_id: 493, exec_time: 0, error_code: 0, schema: "test_db", query: "TRUNCATE TABLE test_tb" })

header: EventHeader { timestamp: 1704443769, event_type: 33, server_id: 1, event_length: 65, next_event_position: 1129, event_flags: 0 }
data: Gtid(GtidEvent { flags: 1, gtid: "50dc6874-13d3-11ee-a17a-0242ac110002:176031" })

header: EventHeader { timestamp: 1704443769, event_type: 2, server_id: 1, event_length: 126, next_event_position: 1255, event_flags: 4 }
data: Query(QueryEvent { thread_id: 493, exec_time: 0, error_code: 0, schema: "test_db", query: "DROP TABLE `test_tb` /* generated by server */" })
```

### 用例 2: 解析开启 binlog-transaction-compression 的 binlog
- 执行 sql 
```sql
flush logs;

SET autocommit=0; 
CREATE DATABASE test_db;
USE test_db;
CREATE TABLE test_tb(id INT, value INT);
INSERT INTO test_tb VALUES(1,1),(2,2),(3,3),(4,4);
UPDATE test_tb SET value=3 WHERE id in(1,2);
DELETE FROM test_tb WHERE id in (1,2);
TRUNCATE TABLE test_tb;
DROP TABLE test_tb;
commit;
```

- 查看 binlog 事件
```sql
mysql> show binary logs;
+------------------+-----------+-----------+
| Log_name         | File_size | Encrypted |
+------------------+-----------+-----------+
| mysql-bin.000033 |      1429 | No        |

mysql> show binlog events in 'mysql-bin.000033';
+------------------+------+---------------------+-----------+-------------+----------------------------------------------------------------------------+
| Log_name         | Pos  | Event_type          | Server_id | End_log_pos | Info                                                                       |
+------------------+------+---------------------+-----------+-------------+----------------------------------------------------------------------------+
| mysql-bin.000033 |    4 | Format_desc         |         1 |         126 | Server ver: 8.0.31, Binlog ver: 4                                          |
| mysql-bin.000033 |  126 | Previous_gtids      |         1 |         197 | 36682cf3-a048-11ed-b4b3-0242ac110004:1-125753                              |
| mysql-bin.000033 |  197 | Gtid                |         1 |         274 | SET @@SESSION.GTID_NEXT= '36682cf3-a048-11ed-b4b3-0242ac110004:125754'     |
| mysql-bin.000033 |  274 | Query               |         1 |         391 | CREATE DATABASE test_db /* xid=5 */                                        |
| mysql-bin.000033 |  391 | Gtid                |         1 |         468 | SET @@SESSION.GTID_NEXT= '36682cf3-a048-11ed-b4b3-0242ac110004:125755'     |
| mysql-bin.000033 |  468 | Query               |         1 |         601 | use `test_db`; CREATE TABLE test_tb(id INT, value INT) /* xid=10 */        |
| mysql-bin.000033 |  601 | Gtid                |         1 |         680 | SET @@SESSION.GTID_NEXT= '36682cf3-a048-11ed-b4b3-0242ac110004:125756'     |
| mysql-bin.000033 |  680 | Transaction_payload |         1 |        1033 | compression='ZSTD', decompressed_size=633 bytes                            |
| mysql-bin.000033 | 1033 | Query               |         1 |        1033 | BEGIN                                                                      |
| mysql-bin.000033 | 1033 | Rows_query          |         1 |        1033 | # INSERT INTO test_tb VALUES(1,1),(2,2),(3,3),(4,4)                        |
| mysql-bin.000033 | 1033 | Table_map           |         1 |        1033 | table_id: 90 (test_db.test_tb)                                             |
| mysql-bin.000033 | 1033 | Write_rows          |         1 |        1033 | table_id: 90 flags: STMT_END_F                                             |
| mysql-bin.000033 | 1033 | Rows_query          |         1 |        1033 | # UPDATE test_tb SET value=3 WHERE id in(1,2)                              |
| mysql-bin.000033 | 1033 | Table_map           |         1 |        1033 | table_id: 90 (test_db.test_tb)                                             |
| mysql-bin.000033 | 1033 | Update_rows         |         1 |        1033 | table_id: 90 flags: STMT_END_F                                             |
| mysql-bin.000033 | 1033 | Rows_query          |         1 |        1033 | # DELETE FROM test_tb WHERE id in (1,2)                                    |
| mysql-bin.000033 | 1033 | Table_map           |         1 |        1033 | table_id: 90 (test_db.test_tb)                                             |
| mysql-bin.000033 | 1033 | Delete_rows         |         1 |        1033 | table_id: 90 flags: STMT_END_F                                             |
| mysql-bin.000033 | 1033 | Xid                 |         1 |        1033 | COMMIT /* xid=11 */                                                        |
| mysql-bin.000033 | 1033 | Gtid                |         1 |        1110 | SET @@SESSION.GTID_NEXT= '36682cf3-a048-11ed-b4b3-0242ac110004:125757'     |
| mysql-bin.000033 | 1110 | Query               |         1 |        1214 | use `test_db`; TRUNCATE TABLE test_tb /* xid=14 */                         |
| mysql-bin.000033 | 1214 | Gtid                |         1 |        1291 | SET @@SESSION.GTID_NEXT= '36682cf3-a048-11ed-b4b3-0242ac110004:125758'     |
| mysql-bin.000033 | 1291 | Query               |         1 |        1429 | use `test_db`; DROP TABLE `test_tb` /* generated by server */ /* xid=15 */ |
+------------------+------+---------------------+-----------+-------------+----------------------------------------------------------------------------+
```

- 解析出的 binlog
```
header: EventHeader { timestamp: 1704445709, event_type: 15, server_id: 1, event_length: 122, next_event_position: 126, event_flags: 0 }
data: FormatDescription(FormatDescriptionEvent { binlog_version: 4, server_version: "8.0.31\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0", create_timestamp: 0, header_length: 19, checksum_type: CRC32 })

header: EventHeader { timestamp: 1704445709, event_type: 35, server_id: 1, event_length: 71, next_event_position: 197, event_flags: 128 }
data: PreviousGtids(PreviousGtidsEvent { gtid_set: "36682cf3-a048-11ed-b4b3-0242ac110004:1-125753" })

header: EventHeader { timestamp: 1704445716, event_type: 33, server_id: 1, event_length: 77, next_event_position: 274, event_flags: 0 }
data: Gtid(GtidEvent { flags: 1, gtid: "36682cf3-a048-11ed-b4b3-0242ac110004:125754" })

header: EventHeader { timestamp: 1704445716, event_type: 2, server_id: 1, event_length: 117, next_event_position: 391, event_flags: 8 }
data: Query(QueryEvent { thread_id: 8, exec_time: 0, error_code: 0, schema: "test_db", query: "CREATE DATABASE test_db" })

header: EventHeader { timestamp: 1704445716, event_type: 33, server_id: 1, event_length: 77, next_event_position: 468, event_flags: 0 }
data: Gtid(GtidEvent { flags: 1, gtid: "36682cf3-a048-11ed-b4b3-0242ac110004:125755" })

header: EventHeader { timestamp: 1704445716, event_type: 2, server_id: 1, event_length: 133, next_event_position: 601, event_flags: 0 }
data: Query(QueryEvent { thread_id: 8, exec_time: 0, error_code: 0, schema: "test_db", query: "CREATE TABLE test_tb(id INT, value INT)" })

header: EventHeader { timestamp: 1704445717, event_type: 33, server_id: 1, event_length: 79, next_event_position: 680, event_flags: 0 }
data: Gtid(GtidEvent { flags: 0, gtid: "36682cf3-a048-11ed-b4b3-0242ac110004:125756" })

header: EventHeader { timestamp: 1704445717, event_type: 40, server_id: 1, event_length: 353, next_event_position: 1033, event_flags: 0 }
data: TransactionPayload(TransactionPayloadEvent { uncompressed_size: 633, uncompressed_events: [(EventHeader { timestamp: 1704445716, event_type: 2, server_id: 1, event_length: 74, next_event_position: 0, event_flags: 8 }, Query(QueryEvent { thread_id: 8, exec_time: 1, error_code: 0, schema: "test_db", query: "BEGIN" })), (EventHeader { timestamp: 1704445716, event_type: 29, server_id: 1, event_length: 69, next_event_position: 0, event_flags: 128 }, RowsQuery(RowsQueryEvent { query: "INSERT INTO test_tb VALUES(1,1),(2,2),(3,3),(4,4)" })), (EventHeader { timestamp: 1704445716, event_type: 19, server_id: 1, event_length: 53, next_event_position: 0, event_flags: 0 }, TableMap(TableMapEvent { table_id: 90, database_name: "test_db", table_name: "test_tb", column_types: [3, 3], column_metas: [0, 0], null_bits: [true, true] })), (EventHeader { timestamp: 1704445716, event_type: 30, server_id: 1, event_length: 67, next_event_position: 0, event_flags: 0 }, WriteRows(WriteRowsEvent { table_id: 90, included_columns: [true, true], rows: [RowEvent { column_values: [Long(1), Long(1)] }, RowEvent { column_values: [Long(2), Long(2)] }, RowEvent { column_values: [Long(3), Long(3)] }, RowEvent { column_values: [Long(4), Long(4)] }] })), (EventHeader { timestamp: 1704445717, event_type: 29, server_id: 1, event_length: 63, next_event_position: 0, event_flags: 128 }, RowsQuery(RowsQueryEvent { query: "UPDATE test_tb SET value=3 WHERE id in(1,2)" })), (EventHeader { timestamp: 1704445717, event_type: 19, server_id: 1, event_length: 53, next_event_position: 0, event_flags: 0 }, TableMap(TableMapEvent { table_id: 90, database_name: "test_db", table_name: "test_tb", column_types: [3, 3], column_metas: [0, 0], null_bits: [true, true] })), (EventHeader { timestamp: 1704445717, event_type: 31, server_id: 1, event_length: 68, next_event_position: 0, event_flags: 0 }, UpdateRows(UpdateRowsEvent { table_id: 90, included_columns_before: [true, true], included_columns_after: [true, true], rows: [(RowEvent { column_values: [Long(1), Long(1)] }, RowEvent { column_values: [Long(1), Long(3)] }), (RowEvent { column_values: [Long(2), Long(2)] }, RowEvent { column_values: [Long(2), Long(3)] })] })), (EventHeader { timestamp: 1704445717, event_type: 29, server_id: 1, event_length: 57, next_event_position: 0, event_flags: 128 }, RowsQuery(RowsQueryEvent { query: "DELETE FROM test_tb WHERE id in (1,2)" })), (EventHeader { timestamp: 1704445717, event_type: 19, server_id: 1, event_length: 53, next_event_position: 0, event_flags: 0 }, TableMap(TableMapEvent { table_id: 90, database_name: "test_db", table_name: "test_tb", column_types: [3, 3], column_metas: [0, 0], null_bits: [true, true] })), (EventHeader { timestamp: 1704445717, event_type: 32, server_id: 1, event_length: 49, next_event_position: 0, event_flags: 0 }, DeleteRows(DeleteRowsEvent { table_id: 90, included_columns: [true, true], rows: [RowEvent { column_values: [Long(1), Long(3)] }, RowEvent { column_values: [Long(2), Long(3)] }] })), (EventHeader { timestamp: 1704445717, event_type: 16, server_id: 1, event_length: 27, next_event_position: 0, event_flags: 0 }, Xid(XidEvent { xid: 11 }))] })

header: EventHeader { timestamp: 1704445717, event_type: 33, server_id: 1, event_length: 77, next_event_position: 1110, event_flags: 0 }
data: Gtid(GtidEvent { flags: 1, gtid: "36682cf3-a048-11ed-b4b3-0242ac110004:125757" })

header: EventHeader { timestamp: 1704445717, event_type: 2, server_id: 1, event_length: 104, next_event_position: 1214, event_flags: 0 }
data: Query(QueryEvent { thread_id: 8, exec_time: 0, error_code: 0, schema: "test_db", query: "TRUNCATE TABLE test_tb" })

header: EventHeader { timestamp: 1704445717, event_type: 33, server_id: 1, event_length: 77, next_event_position: 1291, event_flags: 0 }
data: Gtid(GtidEvent { flags: 1, gtid: "36682cf3-a048-11ed-b4b3-0242ac110004:125758" })

header: EventHeader { timestamp: 1704445717, event_type: 2, server_id: 1, event_length: 138, next_event_position: 1429, event_flags: 4 }
data: Query(QueryEvent { thread_id: 8, exec_time: 0, error_code: 0, schema: "test_db", query: "DROP TABLE `test_tb` /* generated by server */" })
```

### 用例 3: 将 json 字段解析成 string
```rust
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
```

- 执行 sql 
```sql

CREATE TABLE test_db_1.json_test(id INT AUTO_INCREMENT, json_col JSON, PRIMARY KEY(id));

SET autocommit=0;
INSERT INTO test_db_1.json_test VALUES (NULL, '{"k.1":1,"k.0":0,"k.-1":-1,"k.true":true,"k.false":false,"k.null":null,"k.string":"string","k.true_false":[true,false],"k.32767":32767,"k.32768":32768,"k.-32768":-32768,"k.-32769":-32769,"k.2147483647":2147483647,"k.2147483648":2147483648,"k.-2147483648":-2147483648,"k.-2147483649":-2147483649,"k.18446744073709551615":18446744073709551615,"k.18446744073709551616":18446744073709551616,"k.3.14":3.14,"k.{}":{},"k.[]":[]}');
INSERT INTO test_db_1.json_test VALUES (NULL, '{"中文":"😀"}');
commit;
```

- 解析出的 json 字段
```
json column: {"k.0":0,"k.1":1,"k.-1":-1,"k.[]":[],"k.{}":{},"k.3.14":3.14,"k.null":null,"k.true":true,"k.32767":32767,"k.32768":32768,"k.false":false,"k.-32768":-32768,"k.-32769":-32769,"k.string":"string","k.2147483647":2147483647,"k.2147483648":2147483648,"k.true_false":[true,false],"k.-2147483648":-2147483648,"k.-2147483649":-2147483649,"k.18446744073709551615":18446744073709551615,"k.18446744073709551616":18446744073709552000}

json column: {"中文":"😀"}
```

### 用例 4: 解析 binlog 文件
```rust
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
        println!("");
    }
}
```