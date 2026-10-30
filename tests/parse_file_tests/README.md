# GTID binlog fixtures

`mysql-bin.000057` and `mysql-bin.000080` are existing fixtures. Their embedded
server versions are 8.0.31 and 5.7.40-log respectively, despite their filenames.
Both are included in the compatibility checks. The new files
below were generated with official Docker images on 2026-10-29, copied after
rotation, and checked with the matching server's `SHOW BINLOG EVENTS` and
`mysqlbinlog --base64-output=DECODE-ROWS -vv` (5.6.51 and 8.4.13).

| File | Server | Checksum | Expected contents |
| --- | --- | --- | --- |
| `mysql-bin.000056` | 5.6.51-log | CRC32 | Old PreviousGtids `:1`, 25-byte GTID body `:2`, row `(2,20)`, commit |
| `mysql-bin.000084` | 8.4.13 | CRC32 | Empty old PreviousGtids, GTIDs `:1`, `:admin:1`, `:biz:1`, `:2`, `:abcdefghijklmnopqrstuvwxyz_12345:1`; rows 1–5 |
| `mysql-bin.000084-compressed` | 8.4.13 | CRC32 | Mixed tagged/untagged PreviousGtids; `:admin:2` with compressed row 6 and commit; old GTID `:3` with row 7 |
| `mysql-bin.000084-no-checksum` | 8.4.13 | NONE | Mixed PreviousGtids; `:biz:2`, old GTID `:4`; rows 8–9 |

8.4 UUID: `8f555992-d370-11f1-a239-0242ac110002` (includes high-bit bytes).
5.6 UUID: `17e55caf-d371-11f1-b04b-0242ac110003`.
The 5.6 GTID has flags 1; the 8.4 DML GTIDs have flags 0. Every row is
`(id, id * 10)` in `tagged_gtid_test.events`.
PreviousGtids display keeps this library's existing closed interval format,
including `1-1` for singleton intervals and separate entries for each TSID.

## Reproduce in disposable containers

```sh
docker run -d --name tagged84 -e MYSQL_ALLOW_EMPTY_PASSWORD=yes mysql:8.4 \
  --server-id=84 --log-bin=mysql-bin --gtid-mode=ON \
  --enforce-gtid-consistency=ON --binlog-format=ROW
docker run -d --platform linux/amd64 --name tagged56 \
  -e MYSQL_ALLOW_EMPTY_PASSWORD=yes mysql:5.6.51 \
  --server-id=56 --log-bin=mysql-bin --log-slave-updates=ON \
  --gtid-mode=ON --enforce-gtid-consistency=ON --binlog-format=ROW
```

The 8.4 image used here was
`mysql@sha256:a0225d1ea6631c8bc2385dc4c2faba17bbf1834ecf369e1659a9a382bd8d5dd7`.
Run this setup using the container's root account:

```sql
SET SQL_LOG_BIN=0;
CREATE DATABASE tagged_gtid_test;
CREATE TABLE tagged_gtid_test.events (id INT PRIMARY KEY, value INT);
SET SQL_LOG_BIN=1;
```

For 8.4:

```sql
RESET BINARY LOGS AND GTIDS;
SET GTID_NEXT='AUTOMATIC';
INSERT INTO tagged_gtid_test.events VALUES (1,10);
SET GTID_NEXT='AUTOMATIC:admin';
INSERT INTO tagged_gtid_test.events VALUES (2,20);
SET GTID_NEXT='AUTOMATIC:biz';
INSERT INTO tagged_gtid_test.events VALUES (3,30);
SET GTID_NEXT='AUTOMATIC';
INSERT INTO tagged_gtid_test.events VALUES (4,40);
SET GTID_NEXT='AUTOMATIC:abcdefghijklmnopqrstuvwxyz_12345';
INSERT INTO tagged_gtid_test.events VALUES (5,50);
FLUSH BINARY LOGS;
SET GTID_NEXT='AUTOMATIC:admin';
SET SESSION binlog_transaction_compression=ON;
INSERT INTO tagged_gtid_test.events VALUES (6,60);
SET GTID_NEXT='AUTOMATIC';
SET SESSION binlog_transaction_compression=OFF;
INSERT INTO tagged_gtid_test.events VALUES (7,70);
SET GLOBAL binlog_checksum='NONE';
SET GTID_NEXT='AUTOMATIC:biz';
INSERT INTO tagged_gtid_test.events VALUES (8,80);
SET GTID_NEXT='AUTOMATIC';
INSERT INTO tagged_gtid_test.events VALUES (9,90);
FLUSH BINARY LOGS;
```

Copy files 1, 2, and 3 as the three 8.4 fixtures. Changing checksum rotates the
log automatically. The minimal Docker image omits `mysqlbinlog`; the official
`mysql-community-client-8.4.13-1.el9.aarch64.rpm` contains the matching binary.

For 5.6, run `RESET MASTER`, insert `(1,10)`, `FLUSH LOGS`, insert `(2,20)`,
then `FLUSH LOGS` again. Copy file 2 as `mysql-bin.000056`.
Fresh containers have different UUIDs; update expected UUIDs if regenerating.

## Run tests

```sh
cargo test --offline --lib
cargo test --offline --test integration_test parse_file_tests
```

The library test also compares every decoded event from `next` and
`next_slice` across all six files. File tests assert normal EOF separately from
parse errors, exact GTIDs/flags, transaction contents, compression and history.
Small golden-byte unit cases cover empty tags, omitted suffixes, unknown
ignorable/non-ignorable fields, integer boundaries, truncation and bad counts
or intervals. They use the MySQL 8.4 serialization format, not an encoder in
this crate.

`gtid_tests::gtid_tests::test::test_mysql84_tagged_gtid_transactions` is an
opt-in online test using the existing `TestRunner` and `tests/.env` conventions.
It drops/recreates the configured test database. Point `db_url`, `default_db`,
`default_tb`, `server_id`, and `binlog_parse_millis` at a disposable 8.4 instance
with GTID enabled, replication privileges, and permission to set tagged
`GTID_NEXT`, then run:

```sh
cargo test --offline --test integration_test test_mysql84_tagged_gtid_transactions -- --ignored
```

The additional `test_mysql84_tagged_gtid_resume` test verifies explicit mixed
checkpoints across rotation, same-number transactions under different tags,
compressed transactions, `StartPosition::Gtid("")` with tagged server history,
and reconnecting from a checkpoint accumulated through `GtidSet::add` at each
transaction commit. The accumulated collection is independently compared with
the server's executed set, then serialized and reparsed for recovery.
It also requires `FLUSH BINARY LOGS`
privileges. `MYSQL84_REPLICA_URL` may point to a separate read-only replication
account; SQL writes still use `db_url` in the configured test database.

```sh
cargo test --offline --test integration_test test_mysql84_tagged_gtid_resume -- --ignored
```

Request-encoding unit tests compare the mixed checkpoint bytes with the real
8.4 PreviousGtids fixture, assert the legacy request bytes, and cover tag case
normalization, repeated TSIDs, interval merging, and malformed tagged inputs.
Collection tests also cover `add` deduplication and interval bridging, per-tag
containment, `put_uuid_set`, repeated source entries, and string round trips.

Protocol references: [tagged event fields](https://github.com/mysql/mysql-server/blob/mysql-8.4.0/libs/mysql/binlog/event/control_events.h),
[serialization](https://github.com/mysql/mysql-server/blob/mysql-8.4.0/libs/mysql/serialization/serializer_default_impl.hpp),
[PreviousGtids set encoding](https://github.com/mysql/mysql-server/blob/mysql-8.4.0/sql/rpl_gtid_set.cc).
