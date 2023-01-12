#[derive(Debug)]
pub enum BinlogError {
    UnsupportedColumnType { error: String },

    ReadBinlogError { error: String },

    MysqlError { error: String },

    FmtError { error: std::fmt::Error },

    ParseIntError { error: std::num::ParseIntError },

    IoError { error: std::io::Error },

    FromUtf8Error { error: std::string::FromUtf8Error },

    ParseError { error: url::ParseError },
}

impl From<std::io::Error> for BinlogError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError { error: err }
    }
}

impl From<std::string::FromUtf8Error> for BinlogError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Self::FromUtf8Error { error: err }
    }
}

impl From<std::fmt::Error> for BinlogError {
    fn from(err: std::fmt::Error) -> Self {
        Self::FmtError { error: err }
    }
}

impl From<std::num::ParseIntError> for BinlogError {
    fn from(err: std::num::ParseIntError) -> Self {
        Self::ParseIntError { error: err }
    }
}

impl From<url::ParseError> for BinlogError {
    fn from(err: url::ParseError) -> Self {
        Self::ParseError { error: err }
    }
}
