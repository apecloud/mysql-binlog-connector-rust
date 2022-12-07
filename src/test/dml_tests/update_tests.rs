mod test {

    use serial_test::serial;

    use crate::test::{
        dml_tests::dml_test_common::test::DmlTestCommon,
        test_common::test::{TestCommon, U_EVENTS},
    };

    #[test]
    #[serial]
    fn test_update_multiple_rows() {
        TestCommon::before_dml();
        let prepare_sqls = vec![DmlTestCommon::get_create_table_sql_with_all_types()];
        let values = DmlTestCommon::generate_basic_dml_test_data();

        let insert_test_values = vec![
            "(".to_string() + &values[0].join(",") + ")",
            "(".to_string() + &values[1].join(",") + ")",
            "(".to_string() + &values[2].join(",") + ")",
            "(".to_string() + &values[3].join(",") + ")",
            "(".to_string() + &values[4].join(",") + ")",
        ];
        TestCommon::execute_insert_sqls_and_get_binlogs(prepare_sqls, insert_test_values);

        let update_test_sqls = vec![
            DmlTestCommon::get_update_table_sql_with_all_types(
                values[0][0].clone(),
                values[1].clone(),
            ),
            DmlTestCommon::get_update_table_sql_with_all_types(
                values[1][0].clone(),
                values[2].clone(),
            ),
            DmlTestCommon::get_update_table_sql_with_all_types(
                values[2][0].clone(),
                values[3].clone(),
            ),
            DmlTestCommon::get_update_table_sql_with_all_types(
                values[3][0].clone(),
                values[4].clone(),
            ),
        ];
        TestCommon::execute_update_sqls_and_get_binlogs(vec![], update_test_sqls);

        unsafe {
            assert_eq!(U_EVENTS.len(), 4);

            DmlTestCommon::check_values(
                &U_EVENTS[0].rows[0].0,
                1,
                2,
                3,
                4,
                5,
                "123456.1234".to_string(),
                1234.12,
                12345.123,
                vec![true, true, false],
                "2022-01-02 03:04:05.123456".to_string(),
                "03:04:05.123456".to_string(),
                "2022-01-02".to_string(),
                2022,
                1641092645123456,
                vec![97u8, 98],
                vec![99u8, 100],
                vec![101u8, 102],
                vec![103u8, 104],
                vec![105u8, 106],
                vec![107u8, 108],
                vec![109u8, 110],
                vec![111u8, 112],
                vec![113u8, 114],
                vec![115u8, 116],
                vec![117u8, 118],
                vec![119u8, 120],
                1,
                1,
            );

            DmlTestCommon::check_values(
                &U_EVENTS[0].rows[0].1,
                1,
                20,
                30,
                40,
                50,
                "654321.4321".to_string(),
                4321.21,
                54321.321,
                vec![false, false, true],
                "2021-02-01 04:05:06.654321".to_string(),
                "04:05:06.654321".to_string(),
                "2012-02-01".to_string(),
                2021,
                1612152306654321,
                vec![49u8],
                vec![50u8],
                vec![51u8],
                vec![52u8],
                vec![53u8],
                vec![54u8],
                vec![55u8],
                vec![56u8],
                vec![57u8],
                vec![49u8, 48],
                vec![49u8, 49],
                vec![49u8, 50],
                2,
                2,
            );

            DmlTestCommon::check_values(
                &U_EVENTS[1].rows[0].0,
                10,
                20,
                30,
                40,
                50,
                "654321.4321".to_string(),
                4321.21,
                54321.321,
                vec![false, false, true],
                "2021-02-01 04:05:06.654321".to_string(),
                "04:05:06.654321".to_string(),
                "2012-02-01".to_string(),
                2021,
                1612152306654321,
                vec![49u8],
                vec![50u8],
                vec![51u8],
                vec![52u8],
                vec![53u8],
                vec![54u8],
                vec![55u8],
                vec![56u8],
                vec![57u8],
                vec![49u8, 48],
                vec![49u8, 49],
                vec![49u8, 50],
                2,
                2,
            );

            DmlTestCommon::check_values(
                &U_EVENTS[1].rows[0].1,
                10,
                7,
                8,
                9,
                10,
                "234561.2341".to_string(),
                2341.12,
                23451.231,
                vec![true, false, true],
                "2020-03-04 05:06:07.234561".to_string(),
                "05:06:07.234561".to_string(),
                "2022-05-06".to_string(),
                2020,
                1583298367234561,
                vec![97u8],
                vec![98u8],
                vec![99u8],
                vec![100u8],
                vec![101u8],
                vec![102u8],
                vec![103u8],
                vec![104u8],
                vec![105u8],
                vec![106u8],
                vec![107u8],
                vec![108u8],
                3,
                4,
            );

            DmlTestCommon::check_values(
                &U_EVENTS[2].rows[0].0,
                6,
                7,
                8,
                9,
                10,
                "234561.2341".to_string(),
                2341.12,
                23451.231,
                vec![true, false, true],
                "2020-03-04 05:06:07.234561".to_string(),
                "05:06:07.234561".to_string(),
                "2022-05-06".to_string(),
                2020,
                1583298367234561,
                vec![97u8],
                vec![98u8],
                vec![99u8],
                vec![100u8],
                vec![101u8],
                vec![102u8],
                vec![103u8],
                vec![104u8],
                vec![105u8],
                vec![106u8],
                vec![107u8],
                vec![108u8],
                3,
                4,
            );

            // NULL fields
            for i in 0..13 {
                assert!(&U_EVENTS[2].rows[0].1.column_values[2 * i + 1].is_none());
            }
            // non-Null fields
            for i in 1..13 {
                assert!(&U_EVENTS[2].rows[0].1.column_values[2 * i].is_some());
            }
            DmlTestCommon::check_values(
                &U_EVENTS[2].rows[0].1,
                6,
                2,
                3,
                4,
                5,
                "123456.1234".to_string(),
                1234.12,
                12345.123,
                vec![true, true, false],
                "2022-01-02 03:04:05.123456".to_string(),
                "03:04:05.123456".to_string(),
                "2022-01-02".to_string(),
                2022,
                1641092645123456,
                vec![97u8, 98],
                vec![99u8, 100],
                vec![101u8, 102],
                vec![103u8, 104],
                vec![105u8, 106],
                vec![107u8, 108],
                vec![109u8, 110],
                vec![111u8, 112],
                vec![113u8, 114],
                vec![115u8, 116],
                vec![117u8, 118],
                vec![119u8, 120],
                1,
                1,
            );

            // NULL fields
            for i in 0..13 {
                assert!(&U_EVENTS[3].rows[0].0.column_values[2 * i + 1].is_none());
            }
            // non-Null fields
            for i in 0..13 {
                assert!(&U_EVENTS[3].rows[0].0.column_values[2 * i].is_some());
            }
            DmlTestCommon::check_values(
                &U_EVENTS[3].rows[0].0,
                11,
                2,
                3,
                4,
                5,
                "123456.1234".to_string(),
                1234.12,
                12345.123,
                vec![true, true, false],
                "2022-01-02 03:04:05.123456".to_string(),
                "03:04:05.123456".to_string(),
                "2022-01-02".to_string(),
                2022,
                1641092645123456,
                vec![97u8, 98],
                vec![99u8, 100],
                vec![101u8, 102],
                vec![103u8, 104],
                vec![105u8, 106],
                vec![107u8, 108],
                vec![109u8, 110],
                vec![111u8, 112],
                vec![113u8, 114],
                vec![115u8, 116],
                vec![117u8, 118],
                vec![119u8, 120],
                1,
                1,
            );

            for i in 1..27 {
                assert!(&U_EVENTS[3].rows[0].1.column_values[i].is_none());
            }
        }
    }
}
