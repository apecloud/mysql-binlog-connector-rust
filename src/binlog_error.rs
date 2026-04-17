use thiserror::Error;

#[derive(Error, Debug)]
pub enum BinlogError {
    #[error("unsupported column type: {0}")]
    UnsupportedColumnType(String),

    #[error("unexpected binlog data: {0}")]
    UnexpectedData(String),

    #[error("connect error: {0}")]
    ConnectError(String),

    #[error("network timeout: {0}")]
    NetworkTimeout(String),

    #[error("connection closed: {0}")]
    ConnectionClosed(String),

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

impl BinlogError {
    pub fn network_timeout(message: impl Into<String>) -> Self {
        Self::NetworkTimeout(message.into())
    }

    pub fn connection_closed(message: impl Into<String>) -> Self {
        Self::ConnectionClosed(message.into())
    }

    pub fn is_retryable_network_error(&self) -> bool {
        match self {
            Self::NetworkTimeout(_) | Self::ConnectionClosed(_) => true,
            Self::IoError(err) => matches!(
                err.kind(),
                std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::WouldBlock
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::NotConnected
            ),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BinlogError;
    use std::io::{Error, ErrorKind};

    #[test]
    fn retryable_network_error_detection_accepts_io_disconnects() {
        assert!(
            BinlogError::IoError(Error::new(ErrorKind::UnexpectedEof, "eof"))
                .is_retryable_network_error()
        );
        assert!(
            BinlogError::IoError(Error::new(ErrorKind::ConnectionReset, "rst"))
                .is_retryable_network_error()
        );
    }

    #[test]
    fn retryable_network_error_detection_accepts_structured_network_errors() {
        assert!(
            BinlogError::network_timeout("connection timed out").is_retryable_network_error()
        );
        assert!(
            BinlogError::connection_closed("connection closed by peer")
                .is_retryable_network_error()
        );
    }

    #[test]
    fn retryable_network_error_detection_rejects_protocol_errors() {
        assert!(
            !BinlogError::UnexpectedData("bad packet header".to_string())
                .is_retryable_network_error()
        );
        assert!(
            !BinlogError::ConnectError("connect mysql failed".to_string()).is_retryable_network_error()
        );
    }
}
