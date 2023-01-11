use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum ChecksumType {
    None,
    CRC32,
}

impl ChecksumType {
    pub fn from_code(code: u8) -> Self {
        match code {
            0x01 => ChecksumType::CRC32,
            _ => ChecksumType::None,
        }
    }

    pub fn from_name(name: &str) -> Self {
        match name {
            "CRC32" => ChecksumType::CRC32,
            _ => ChecksumType::None,
        }
    }

    pub fn get_length(&self) -> u8 {
        match self {
            ChecksumType::CRC32 => 4,
            _ => 0,
        }
    }
}
