use thiserror::Error;

#[derive(Error, Debug)]
pub enum BinlogError {
    #[error("unsupported column type: {0}")]
    UnsupportedColumnType(String),

    #[error("unexpected binlog data: {0}")]
    UnexpectedData(String),

    #[error("connect error: {0}")]
    ConnectError(String),

    #[error("fmt error: {0}")]
    FmtError(#[from] std::fmt::Error),

    #[error("parse int error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("parse utf8 error: {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),

    #[error("parse url error: {0}")]
    ParseUrlError(#[from] url::ParseError),

    #[error("parse json error: {0}")]
    ParseJsonError(String),

    #[error("invalid gtid: {0}")]
    InvalidGtid(String),
}
