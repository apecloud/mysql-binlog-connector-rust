// refer: https://github.com/osheroff/mysql-binlog-connector-java/blob/master/src/test/java/com/github/shyiko/mysql/binlog/event/deserialization/json/JsonBinaryValueIntegrationTest.java
#[cfg(test)]
mod test {

    use mysql_binlog_connector_rust::{
        binlog_error::BinlogError,
        column::{column_value::ColumnValue, json::json_binary::JsonBinary},
    };
    use serial_test::serial;

    use crate::runner::{assert::test::Assert, mock::test::Mock, test_runner::test::TestRunner};

    #[test]
    #[serial]
    fn test_json_basic() {
        let values = vec![
            // json with basic types, array, and nested document
            r#"{
                "literal1": true,
                "i16": 4,
                "i32": 2147483647,
                "int64": 4294967295,
                "double": 1.0001,
                "string": "abc",
                "time": "2022-01-01 12:34:56.000000",
                "array": [1, 2, {
                    "i16": 4,
                    "array": [false, true, "abcd"]
                }],
                "small_document": {
                    "i16": 4,
                    "array": [false, true, 3],
                    "small_document": {
                        "i16": 4,
                        "i32": 2147483647,
                        "int64": 4294967295
                    }
                }
            }"#,
            // json array
            r#"[{
                "i16": 4,
                "small_document": {
                    "i16": 4,
                    "i32": 2147483647,
                    "int64": 4294967295
                }
            }, {
                "i16": 4,
                "array": [false, true, "abcd"]
            }, "abc", 10, null, true, false]"#,
        ];

        run_json_test(&values, &values, true);
    }

    #[test]
    #[serial]
    fn test_mysql8_json_set_partial_update_with_holes() {
        let mut runner = TestRunner::new();
        let tb = format!("{}.{}", runner.default_db, runner.default_tb);
        let prepare_sqls = vec![format!("create table {} (j JSON)", tb)];

        let json_value = "{\"age\":22,\"addr\":{\"code\":100,\"detail\":{\"ab\":\"970785C8-C299\"}},\"name\":\"Alice\"}".to_string();
        let test_sqls = vec![
            format!("insert into {} values ('{}')", tb, json_value),
            format!(
                "update {} set j = JSON_SET(j, '$.addr.detail.ab', '970785C8')",
                tb
            ),
        ];

        runner.execute_sqls_and_get_binlogs(&prepare_sqls, &test_sqls);

        Assert::assert_json_string_eq(
            &json_value,
            &parse_json_as_string(&runner.insert_events[0].rows[0].column_values[0]).unwrap(),
        );
        Assert::assert_json_string_eq(
            &json_value.replace("970785C8-C299", "970785C8"),
            &parse_json_as_string(&runner.update_events[0].rows[0].1.column_values[0]).unwrap(),
        );
    }

    #[test]
    #[serial]
    fn test_mysql8_json_remove_partial_update_with_holes() {
        let mut runner = TestRunner::new();
        let tb = format!("{}.{}", runner.default_db, runner.default_tb);
        let prepare_sqls = vec![format!("create table {} (j JSON)", tb)];

        let json_value = "{\"age\":22,\"addr\":{\"code\":100,\"detail\":{\"ab\":\"970785C8-C299\"}},\"name\":\"Alice\"}".to_string();
        let test_sqls = vec![
            format!("insert into {} values ('{}')", tb, json_value),
            format!("update {} set j = JSON_REMOVE(j, '$.addr.detail.ab')", tb),
        ];

        runner.execute_sqls_and_get_binlogs(&prepare_sqls, &test_sqls);

        Assert::assert_json_string_eq(
            &json_value,
            &parse_json_as_string(&runner.insert_events[0].rows[0].column_values[0]).unwrap(),
        );
        Assert::assert_json_string_eq(
            &json_value.replace("\"ab\":\"970785C8-C299\"", ""),
            &parse_json_as_string(&runner.update_events[0].rows[0].1.column_values[0]).unwrap(),
        );
    }

    #[test]
    #[serial]
    fn test_mysql8_json_remove_partial_update_with_holes_and_sparse_keys() {
        let mut runner = TestRunner::new();
        let tb = format!("{}.{}", runner.default_db, runner.default_tb);
        let prepare_sqls = vec![format!("create table {} (j JSON)", tb)];

        let json_value = "{\"17fc9889474028063990914001f6854f6b8b5784\":\"test_field_for_remove_fields_behaviour_2\",\"1f3a2ea5bc1f60258df20521bee9ac636df69a3a\":{\"currency\":\"USD\"},\"4f4d99a438f334d7dbf83a1816015b361b848b3b\":{\"currency\":\"USD\"},\"9021162291be72f5a8025480f44bf44d5d81d07c\":\"test_field_for_remove_fields_behaviour_3_will_be_removed\",\"9b0ed11532efea688fdf12b28f142b9eb08a80c5\":{\"currency\":\"USD\"},\"e65ad0762c259b05b4866f7249eabecabadbe577\":\"test_field_for_remove_fields_behaviour_1_updated\",\"ff2c07edcaa3e987c23fb5cc4fe860bb52becf00\":{\"currency\":\"USD\"}}".to_string();
        let test_sqls = vec![
            format!("insert into {} values ('{}')", tb, json_value),
            format!(
                "update {} set j = JSON_REMOVE(j, '$.\"17fc9889474028063990914001f6854f6b8b5784\"')",
                tb
            ),
        ];

        runner.execute_sqls_and_get_binlogs(&prepare_sqls, &test_sqls);

        Assert::assert_json_string_eq(
            &json_value,
            &parse_json_as_string(&runner.insert_events[0].rows[0].column_values[0]).unwrap(),
        );
        Assert::assert_json_string_eq(
            &json_value.replace("\"17fc9889474028063990914001f6854f6b8b5784\":\"test_field_for_remove_fields_behaviour_2\",", ""),
            &parse_json_as_string(&runner.update_events[0].rows[0].1.column_values[0]).unwrap(),
        );
    }

    #[test]
    #[serial]
    fn test_mysql8_json_replace_partial_update_with_holes() {
        let mut runner = TestRunner::new();
        let tb = format!("{}.{}", runner.default_db, runner.default_tb);
        let prepare_sqls = vec![format!("create table {} (j JSON)", tb)];

        let json_value = "{\"age\":22,\"addr\":{\"code\":100,\"detail\":{\"ab\":\"970785C8-C299\"}},\"name\":\"Alice\"}".to_string();
        let test_sqls = vec![
            format!("insert into {} values ('{}')", tb, json_value),
            format!(
                "update {} set j = JSON_REPLACE(j, '$.addr.detail.ab', '9707')",
                tb
            ),
        ];

        runner.execute_sqls_and_get_binlogs(&prepare_sqls, &test_sqls);

        Assert::assert_json_string_eq(
            &json_value,
            &parse_json_as_string(&runner.insert_events[0].rows[0].column_values[0]).unwrap(),
        );
        Assert::assert_json_string_eq(
            &json_value.replace("970785C8-C299", "9707"),
            &parse_json_as_string(&runner.update_events[0].rows[0].1.column_values[0]).unwrap(),
        );
    }

    #[test]
    #[serial]
    fn test_mysql8_json_remove_array_value() {
        let mut runner = TestRunner::new();
        let tb = format!("{}.{}", runner.default_db, runner.default_tb);
        let prepare_sqls = vec![format!("create table {} (j JSON)", tb)];

        let json_value = "[\"foo\",\"bar\",\"baz\"]".to_string();
        let test_sqls = vec![
            format!("insert into {} values ('{}')", tb, json_value),
            format!("update {} set j = JSON_REMOVE(j, '$[1]')", tb),
        ];

        runner.execute_sqls_and_get_binlogs(&prepare_sqls, &test_sqls);

        Assert::assert_json_string_eq(
            &json_value,
            &parse_json_as_string(&runner.insert_events[0].rows[0].column_values[0]).unwrap(),
        );
        Assert::assert_json_string_eq(
            "[\"foo\",\"baz\"]",
            &parse_json_as_string(&runner.update_events[0].rows[0].1.column_values[0]).unwrap(),
        );
    }

    #[test]
    #[serial]
    fn test_value_boundaries_are_honored() {
        let mut runner = TestRunner::new();
        let tb = format!("{}.{}", runner.default_db, runner.default_tb);

        let prepare_sqls = vec![format!(
            "create table {} (h varchar(255), j JSON, k varchar(255))",
            tb
        )];
        let values = vec!["'sponge'", "'{}'", "'bob'"];
        let insert_sql = Mock::insert_sql(&vec![values]);

        runner.execute_sqls_and_get_binlogs(&prepare_sqls, &vec![insert_sql]);

        Assert::assert_bytes_eq(
            &runner.insert_events[0].rows[0].column_values[0],
            "sponge".as_bytes().to_vec(),
        );
        Assert::assert_bytes_eq(
            &runner.insert_events[0].rows[0].column_values[2],
            "bob".as_bytes().to_vec(),
        );
        Assert::assert_json_string_eq(
            "{}",
            &parse_json_as_string(&runner.insert_events[0].rows[0].column_values[1]).unwrap(),
        );
    }

    #[test]
    #[serial]
    fn test_null() {
        let origin_values = vec!["null"];
        let runner = run_json_test(&origin_values, &vec![], false);
        assert_eq!(
            runner.insert_events[0].rows[0].column_values[0],
            ColumnValue::None
        );
    }

    #[test]
    #[serial]
    fn test_unicode_support() {
        let origin_values = vec!["{\"key\":\"éééàààà\"}"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_json_object() {
        let value = "{".to_string()
            + "\"k.1\":1,"
            + "\"k.0\":0,"
            + "\"k.-1\":-1,"
            + "\"k.true\":true,"
            + "\"k.false\":false,"
            + "\"k.null\":null,"
            + "\"k.string\":\"string\","
            + "\"k.true_false\":[true,false],"
            + "\"k.32767\":32767,"
            + "\"k.32768\":32768,"
            + "\"k.-32768\":-32768,"
            + "\"k.-32769\":-32769,"
            + "\"k.2147483647\":2147483647,"
            + "\"k.2147483648\":2147483648,"
            + "\"k.-2147483648\":-2147483648,"
            + "\"k.-2147483649\":-2147483649,"
            + "\"k.18446744073709551615\":18446744073709551615,"
            + "\"k.18446744073709551616\":18446744073709551616,"
            + "\"k.3.14\":3.14,"
            + "\"k.{}\":{},"
            + "\"k.[]\":[]"
            + "}";
        let origin_values = vec![value.as_str()];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_json_object_larger_than_64k() {
        let mut sb = String::new();
        for i in 0..64 * 1024 {
            sb.push_str(&format!("\"g.{}\":{},", i, i));
        }

        let long_string = "{".to_string()
            + sb.as_str()
            + "\"k.1\":1,"
            + "\"k.0\":0,"
            + "\"k.-1\":-1,"
            + "\"k.true\":true,"
            + "\"k.false\":false,"
            + "\"k.null\":null,"
            + "\"k.string\":\"string\","
            + "\"k.true_false\":[true,false],"
            + "\"k.32767\":32767,"
            + "\"k.32768\":32768,"
            + "\"k.-32768\":-32768,"
            + "\"k.-32769\":-32769,"
            + "\"k.2147483647\":2147483647,"
            + "\"k.2147483648\":2147483648,"
            + "\"k.-2147483648\":-2147483648,"
            + "\"k.-2147483649\":-2147483649,"
            + "\"k.18446744073709551615\":18446744073709551615,"
            + "\"k.18446744073709551616\":18446744073709551616,"
            + "\"k.3.14\":3.14,"
            + "\"k.{}\":{},"
            + "\"k.[]\":[]"
            + "}";

        let origin_values = vec![long_string.as_str()];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_json_object_nested() {
        let origin_values = vec![r#"{"a":{"b":{"c":"d","e":["f","g"]}}}"#];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_json_with_empty_key() {
        let origin_values = vec![r#"{"bitrate":{"":0}}"#];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_json_array() {
        let origin_values = vec![
            r#"[
            -1,
            0,
            1,
            true,
            false,
            null,
            "string",
            [true,false],
            32767,
            32768,
            -32768,
            -32769,
            2147483647,
            2147483648,
            -2147483648,
            -2147483649,
            18446744073709551615,
            18446744073709551616,
            3.14,
            {},
            []
        ]"#,
        ];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_json_array_nested() {
        let origin_values = vec!["[-1,[\"b\",[\"c\"]],1]"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_scalar_string() {
        let mut long_string = String::from("");
        for _i in 0..65 {
            long_string.push_str("LONG");
        }
        long_string = format!("\"{}\"", long_string);
        let origin_values = vec!["\"scalar string\"", &long_string];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_scalar_boolean_true() {
        let origin_values = vec!["true"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_scalar_boolean_false() {
        let origin_values = vec!["false"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_scalar_null() {
        let origin_values = vec!["null"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_scalar_negative_integer() {
        let origin_values = vec!["-1"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_scalar_positive_integer() {
        let origin_values = vec!["1"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_scalar_max_positive_int16() {
        let origin_values = vec!["32767"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_scalar_int32() {
        let origin_values = vec!["32768"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_scalar_min_negative_int16() {
        let origin_values = vec!["-32768"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_scalar_negative_int32() {
        let origin_values = vec!["-32769"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_scalar_max_positive_int32() {
        let origin_values = vec!["2147483647"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_scalar_positive_int64() {
        let origin_values = vec!["2147483648"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_scalar_min_negative_int32() {
        let origin_values = vec!["-2147483648"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_scalar_negative_int64() {
        let origin_values = vec!["-2147483649"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_scalar_uint64() {
        let origin_values = vec!["18446744073709551615"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_scalar_uint64_overflow() {
        let origin_values = vec!["18446744073709551616"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_scalar_float() {
        let origin_values = vec!["3.14"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_empty_object() {
        let origin_values = vec!["{}"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_empty_array() {
        let origin_values = vec!["[]"];
        run_json_test(&origin_values, &origin_values, true);
    }

    #[test]
    #[serial]
    fn test_scalar_datetime() {
        let origin_values = vec!["CAST(CAST('2015-01-15 23:24:25' AS DATETIME) AS JSON)"];
        let expected_values = vec!["\"2015-01-15 23:24:25\""];
        run_json_test(&origin_values, &expected_values, false);
    }

    #[test]
    #[serial]
    fn test_scalar_time() {
        let origin_values = vec![
            "CAST(CAST('23:24:25' AS TIME) AS JSON)",
            "CAST(CAST('23:24:25.12' AS TIME(3)) AS JSON)",
            "CAST(CAST('23:24:25.0237' AS TIME(3)) AS JSON)",
        ];
        let expected_values = vec!["\"23:24:25\"", "\"23:24:25.12\"", "\"23:24:25.024\""];
        run_json_test(&origin_values, &expected_values, false);
    }

    #[test]
    #[serial]
    fn test_scalar_timestamp() {
        let origin_values = vec![
            "CAST(TIMESTAMP'2015-01-15 23:24:25' AS JSON)",
            "CAST(TIMESTAMP'2015-01-15 23:24:25.12' AS JSON)",
            "CAST(TIMESTAMP'2015-01-15 23:24:25.0237' AS JSON)",
            "CAST(UNIX_TIMESTAMP(CONVERT_TZ('2015-01-15 23:24:25','GMT',@@session.time_zone)) AS JSON)",
        ];
        let expected_values = vec![
            "\"2015-01-15 23:24:25\"",
            "\"2015-01-15 23:24:25.12\"",
            "\"2015-01-15 23:24:25.0237\"",
            "1421364265",
        ];
        run_json_test(&origin_values, &expected_values, false);
    }

    #[test]
    #[serial]
    fn test_scalar_geometry() {
        let origin_values = vec!["CAST(ST_GeomFromText('POINT(1 1)') AS JSON)"];
        let expected_values = vec!["{\"type\":\"Point\",\"coordinates\":[1,1]}"];
        run_json_test(&origin_values, &expected_values, false);
    }

    #[test]
    #[serial]
    fn test_scalar_string_with_charset_conversion() {
        let origin_values = vec!["CAST('[]' AS CHAR CHARACTER SET 'ascii')"];
        let expected_values = vec!["[]"];
        run_json_test(&origin_values, &expected_values, false);
    }

    #[test]
    #[serial]
    fn test_scalar_binary_as_base64() {
        let origin_values = vec!["CAST(x'cafe' AS JSON)", "CAST(x'cafebabe' AS JSON)"];
        let expected_values = vec![r#""yv4=""#, r#""yv66vg==""#];
        run_json_test(&origin_values, &expected_values, false);
    }

    #[test]
    #[serial]
    fn test_scalar_decimal() {
        let origin_values = vec![
            "CAST(CAST(\"212765.700000000010000\" AS DECIMAL(21,15)) AS JSON)",
            "CAST(CAST(\"111111.11111110000001\" AS DECIMAL(24,17)) AS JSON)",
        ];
        run_json_test(
            &origin_values,
            &vec!["212765.700000000010000", "111111.11111110000001"],
            false,
        );
    }

    fn run_json_test(
        origin_values: &Vec<&str>,
        expected_values: &Vec<&str>,
        quote: bool,
    ) -> TestRunner {
        let values = origin_values
            .clone()
            .into_iter()
            .map(|v| {
                if quote {
                    format!("('{}')", v)
                } else {
                    format!("({})", v)
                }
            })
            .collect::<Vec<String>>();

        let mut str_values = vec![];
        for i in values.iter() {
            str_values.push(i.as_str());
        }

        let runner = TestRunner::run_one_col_test("JSON", &str_values, &vec![]);

        if !expected_values.is_empty() {
            let mut binlog_values = Vec::new();
            for i in 0..origin_values.len() {
                if let Ok(json_string) =
                    parse_json_as_string(&runner.insert_events[0].rows[i].column_values[0])
                {
                    binlog_values.push(json_string);
                } else {
                    assert!(false, "expect json");
                }
            }

            for i in 0..origin_values.len() {
                Assert::assert_json_string_eq(&expected_values[i], &binlog_values[i]);
            }
        }

        runner
    }

    fn parse_json_as_string(column_value: &ColumnValue) -> Result<String, BinlogError> {
        match column_value {
            ColumnValue::Json(bytes) => JsonBinary::parse_as_string(bytes),
            _ => Err(BinlogError::ParseJsonError(
                "column value is not json".into(),
            )),
        }
    }
}
