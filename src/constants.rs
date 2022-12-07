#![allow(dead_code)]
pub struct ClientCapabilities {}

impl ClientCapabilities {
    pub const LONG_PASSWORD: u32 = 1;
    pub const FOUND_ROWS: u32 = 1 << 1;
    pub const LONG_FLAG: u32 = 1 << 2;
    pub const CONNECT_WITH_DB: u32 = 1 << 3;
    pub const NO_SCHEMA: u32 = 1 << 4;
    pub const COMPRESS: u32 = 1 << 5;
    pub const ODBC: u32 = 1 << 6;
    pub const LOCAL_FILES: u32 = 1 << 7;
    pub const IGNORE_SPACE: u32 = 1 << 8;
    pub const PROTOCOL_41: u32 = 1 << 9;
    pub const INTERACTIVE: u32 = 1 << 10;
    pub const SSL: u32 = 1 << 11;
    pub const IGNORE_SIGPIPE: u32 = 1 << 12;
    pub const TRANSACTIONS: u32 = 1 << 13;
    pub const RESERVED: u32 = 1 << 14;
    pub const SECURE_CONNECTION: u32 = 1 << 15;
    pub const MULTI_STATEMENTS: u32 = 1 << 16;
    pub const MULTI_RESULTS: u32 = 1 << 17;
    pub const PS_MULTI_RESULTS: u32 = 1 << 18;
    pub const PLUGIN_AUTH: u32 = 1 << 19;
    pub const PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 1 << 21;
    pub const SSL_VERIFY_SERVER_CERT: u32 = 1 << 30;
    pub const REMEMBER_OPTIONS: u32 = 1 << 31;
}

pub struct MysqlRespCode {}

impl MysqlRespCode {
    pub const OK: u8 = 0x00;
    pub const ERROR: u8 = 0xFF;
    pub const EOF: u8 = 0xFE;
    pub const AUTH_PLUGIN_SWITCH: u8 = 0xFE;
}

pub const EVENT_HEADER_LENGTH: usize = 19;
pub const NULL_TERMINATOR: u8 = 0;
