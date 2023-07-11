use serde::{Deserialize, Serialize};

// refer: https://github.com/osheroff/mysql-binlog-connector-java/blob/master/src/main/java/com/github/shyiko/mysql/binlog/event/deserialization/json/ValueType.java
#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) enum ValueType {
    SmallDocument = 0x00,
    LargeDocument = 0x01,
    SmallArray = 0x02,
    LargeArray = 0x03,
    Literal = 0x04,
    Int16 = 0x05,
    Uint16 = 0x06,
    Int32 = 0x07,
    Uint32 = 0x08,
    Int64 = 0x09,
    Uint64 = 0x0a,
    Double = 0x0b,
    String = 0x0c,
    Custom = 0x0f,
}

impl ValueType {
    pub fn by_code(code: u8) -> Option<ValueType> {
        match code {
            0x00 => Some(ValueType::SmallDocument),
            0x01 => Some(ValueType::LargeDocument),
            0x02 => Some(ValueType::SmallArray),
            0x03 => Some(ValueType::LargeArray),
            0x04 => Some(ValueType::Literal),
            0x05 => Some(ValueType::Int16),
            0x06 => Some(ValueType::Uint16),
            0x07 => Some(ValueType::Int32),
            0x08 => Some(ValueType::Uint32),
            0x09 => Some(ValueType::Int64),
            0x0a => Some(ValueType::Uint64),
            0x0b => Some(ValueType::Double),
            0x0c => Some(ValueType::String),
            0x0f => Some(ValueType::Custom),
            _ => None,
        }
    }
}
