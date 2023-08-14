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
- cargo test --package mysql-binlog-connector-rust --test integration_test