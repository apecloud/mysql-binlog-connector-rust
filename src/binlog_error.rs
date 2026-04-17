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

impl BinlogError {
    pub fn is_retryable_network_error(&self) -> bool {
        match self {
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
            Self::UnexpectedData(message) | Self::ConnectError(message) => {
                let message = message.to_ascii_lowercase();
                [
                    "timeout",
                    "timed out",
                    "unexpected eof",
                    "end of file",
                    "connection reset",
                    "connection aborted",
                    "connection refused",
                    "broken pipe",
                    "connection closed",
                    "closed by peer",
                    "not connected",
                ]
                .iter()
                .any(|pattern| message.contains(pattern))
            }
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
    fn retryable_network_error_detection_accepts_timeout_messages() {
        assert!(
            BinlogError::ConnectError("connection timed out".to_string())
                .is_retryable_network_error()
        );
        assert!(
            BinlogError::UnexpectedData("connection closed by peer".to_string())
                .is_retryable_network_error()
        );
    }

    #[test]
    fn retryable_network_error_detection_rejects_protocol_errors() {
        assert!(
            !BinlogError::UnexpectedData("bad packet header".to_string())
                .is_retryable_network_error()
        );
    }
}
