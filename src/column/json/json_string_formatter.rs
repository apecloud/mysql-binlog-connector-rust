use super::json_formatter::JsonFormatter;
use crate::column::column_type::ColumnType;
use lazy_static::lazy_static;
use openssl::base64;

// refer: https://github.com/osheroff/mysql-binlog-connector-java/blob/master/src/main/java/com/github/shyiko/mysql/binlog/event/deserialization/json/JsonStringFormatter.java
#[derive(Default)]
pub struct JsonStringFormatter {
    sb: String,
}

const ESCAPE_GENERIC: i32 = -1;

lazy_static! {
    static ref ESCAPES: [i32; 128] = {
        let mut escape = [0; 128];
        #[allow(clippy::needless_range_loop)]
        for i in 0..32 {
            escape[i] = ESCAPE_GENERIC;
        }
        escape[b'"' as usize] = b'"' as i32;
        escape[b'\\' as usize] = b'\\' as i32;
        escape[0x08] = b'b' as i32;
        escape[0x09] = b't' as i32;
        escape[0x0C] = b'f' as i32;
        escape[0x0A] = b'n' as i32;
        escape[0x0D] = b'r' as i32;
        escape
    };
}

const HEX_CODES: [char; 16] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F',
];

impl JsonStringFormatter {
    pub fn get_string(&self) -> String {
        self.sb.clone()
    }

    fn append_string(&mut self, original: &str) {
        for c in original.chars() {
            let ch = c as usize;
            if ch >= ESCAPES.len() || ESCAPES[ch] == 0 {
                self.sb.push(c);
                continue;
            }

            let escape = ESCAPES[ch];
            if escape > 0 {
                // 2-char escape, fine
                self.sb.push('\\');
                self.sb.push(escape as u8 as char);
            } else {
                self.unicode_escape(ch as i32);
            }
        }
    }

    fn unicode_escape(&mut self, char_to_escape: i32) {
        let mut char_to_escape = char_to_escape;
        self.sb.push('\\');
        self.sb.push('u');
        if char_to_escape > 0xFF {
            let hi = (char_to_escape >> 8) & 0xFF;
            self.sb.push(HEX_CODES[(hi >> 4) as usize]);
            self.sb.push(HEX_CODES[(hi & 0xF) as usize]);
            char_to_escape &= 0xFF;
        } else {
            self.sb.push('0');
            self.sb.push('0');
        }
        // We know it's a control char, so only the last 2 chars are non-0
        self.sb.push(HEX_CODES[(char_to_escape >> 4) as usize]);
        self.sb.push(HEX_CODES[(char_to_escape & 0xF) as usize]);
    }

    fn append_two_digit_unsigned_int(&mut self, value: i32) {
        if value < 10 {
            self.sb.push('0');
        }
        self.sb.push_str(&value.to_string());
    }

    fn append_four_digit_unsigned_int(&mut self, value: i32) {
        if value < 10 {
            self.sb.push_str("000");
        } else if value < 100 {
            self.sb.push_str("00");
        } else if value < 1000 {
            self.sb.push('0');
        }
        self.sb.push_str(&value.to_string());
    }

    pub fn append_six_digit_unsigned_int(&mut self, mut value: i32, trim_trailing_zeros: bool) {
        if value < 10 {
            self.sb.push_str("00000");
        } else if value < 100 {
            self.sb.push_str("0000");
        } else if value < 1000 {
            self.sb.push_str("000");
        } else if value < 10000 {
            self.sb.push_str("00");
        } else if value < 100000 {
            self.sb.push('0');
        };

        if trim_trailing_zeros {
            // Remove any trailing 0's ...
            for _ in 0..6 {
                if value % 10 == 0 {
                    value /= 10;
                }
            }
            self.sb.push_str(&value.to_string());
        }
    }

    fn append_date(&mut self, year: i32, month: i32, day: i32) {
        let mut year = year;
        if year < 0 {
            self.sb.push('-');
            year = year.abs();
        }
        self.append_four_digit_unsigned_int(year);
        self.sb.push('-');
        self.append_two_digit_unsigned_int(month);
        self.sb.push('-');
        self.append_two_digit_unsigned_int(day);
    }

    fn append_time(&mut self, hour: i32, min: i32, sec: i32, micro_seconds: i32) {
        self.append_two_digit_unsigned_int(hour);
        self.sb.push(':');
        self.append_two_digit_unsigned_int(min);
        self.sb.push(':');
        self.append_two_digit_unsigned_int(sec);
        if micro_seconds != 0 {
            self.sb.push('.');
            self.append_six_digit_unsigned_int(micro_seconds, true);
        }
    }
}

impl JsonFormatter for JsonStringFormatter {
    fn begin_object(&mut self, _num_elements: u32) {
        self.sb.push('{');
    }

    fn end_object(&mut self) {
        self.sb.push('}');
    }

    fn begin_array(&mut self, _num_elements: u32) {
        self.sb.push('[');
    }

    fn end_array(&mut self) {
        self.sb.push(']');
    }

    fn name(&mut self, name: &str) {
        self.sb.push('"');
        self.append_string(name);
        self.sb.push_str(r#"":"#);
    }

    fn value_string(&mut self, value: &str) {
        self.sb.push('"');
        self.append_string(value);
        self.sb.push('"');
    }

    fn value_int(&mut self, value: i32) {
        self.sb.push_str(&value.to_string());
    }

    fn value_long(&mut self, value: i64) {
        self.sb.push_str(&value.to_string());
    }

    fn value_double(&mut self, value: f64) {
        self.sb.push_str(&value.to_string());
    }

    fn value_big_int(&mut self, value: i128) {
        self.sb.push_str(&value.to_string());
    }

    fn value_decimal(&mut self, value: &str) {
        self.sb.push_str(value);
    }

    fn value_bool(&mut self, value: bool) {
        self.sb.push_str(&value.to_string());
    }

    fn value_null(&mut self) {
        self.sb.push_str("null");
    }

    fn value_year(&mut self, year: i32) {
        self.sb.push_str(&year.to_string())
    }

    fn value_date(&mut self, year: i32, month: i32, day: i32) {
        self.sb.push('"');
        self.append_date(year, month, day);
        self.sb.push('"');
    }

    fn value_datetime(
        &mut self,
        year: i32,
        month: i32,
        day: i32,
        hour: i32,
        min: i32,
        sec: i32,
        micro_seconds: i32,
    ) {
        self.sb.push('"');
        self.append_date(year, month, day);
        self.sb.push(' ');
        self.append_time(hour, min, sec, micro_seconds);
        self.sb.push('"');
    }

    fn value_time(&mut self, hour: i32, min: i32, sec: i32, micro_seconds: i32) {
        self.sb.push('"');

        let mut hour = hour;
        if hour < 0 {
            self.sb.push('-');
            hour = hour.abs();
        }

        self.append_time(hour, min, sec, micro_seconds);
        self.sb.push('"');
    }

    fn value_timestamp(&mut self, seconds_past_epoch: i64, micro_seconds: i32) {
        self.sb.push_str(&seconds_past_epoch.to_string());
        self.append_six_digit_unsigned_int(micro_seconds, false);
    }

    fn value_opaque(&mut self, _column_type: &ColumnType, value: &[u8]) {
        self.sb.push('"');
        self.sb.push_str(&base64::encode_block(value));
        self.sb.push('"');
    }

    fn next_entry(&mut self) {
        self.sb.push(',');
    }
}
