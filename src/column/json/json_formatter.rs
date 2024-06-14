use crate::column::column_type::ColumnType;

// refer: https://github.com/osheroff/mysql-binlog-connector-java/blob/master/src/main/java/com/github/shyiko/mysql/binlog/event/deserialization/json/JsonFormatter.java
pub trait JsonFormatter {
    fn begin_object(&mut self, num_elements: u32);

    fn begin_array(&mut self, num_elements: u32);

    fn end_object(&mut self);

    fn end_array(&mut self);

    fn name(&mut self, name: &str);

    fn value_string(&mut self, value: &str);

    fn value_int(&mut self, value: i32);

    fn value_long(&mut self, value: i64);

    fn value_double(&mut self, value: f64);

    fn value_big_int(&mut self, value: i128);

    fn value_decimal(&mut self, value: &str);

    fn value_bool(&mut self, value: bool);

    fn value_null(&mut self);

    fn value_year(&mut self, year: i32);

    fn value_date(&mut self, year: i32, month: i32, day: i32);

    #[allow(clippy::too_many_arguments)]
    fn value_datetime(
        &mut self,
        year: i32,
        month: i32,
        day: i32,
        hour: i32,
        min: i32,
        sec: i32,
        micro_seconds: i32,
    );

    fn value_time(&mut self, hour: i32, min: i32, sec: i32, micro_seconds: i32);

    fn value_timestamp(&mut self, seconds_past_epoch: i64, micro_seconds: i32);

    fn value_opaque(&mut self, column_type: &ColumnType, value: &[u8]);

    fn next_entry(&mut self);
}
