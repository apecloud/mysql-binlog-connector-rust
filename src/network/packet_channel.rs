#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(windows)]
use std::os::windows::io::AsRawSocket;

use std::{
    io::{Cursor, Write},
    time::Duration,
};

use async_std::{future::timeout, net::TcpStream, prelude::*};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use log::trace;

use crate::binlog_error::BinlogError;

const MAX_PACKET_LENGTH: usize = 16777215;

pub struct PacketChannel {
    stream: TcpStream,
    timeout_secs: u64,
}

pub struct KeepAliveConfig {
    pub keepidle_secs: u64,
    pub keepintvl_secs: u64,
}

impl PacketChannel {
    pub async fn new(
        ip: &str,
        port: &str,
        timeout_secs: u64,
        keepalive_config: &Option<KeepAliveConfig>,
    ) -> Result<Self, BinlogError> {
        let addr = format!("{}:{}", ip, port);
        let stream =
            match timeout(Duration::from_secs(timeout_secs), TcpStream::connect(&addr)).await {
                Ok(Ok(stream)) => stream,
                Ok(Err(e)) => return Err(BinlogError::from(e)),
                Err(_) => {
                    return Err(BinlogError::ConnectError(format!(
                        "Connection timeout after {} seconds while connecting to {}",
                        timeout_secs, addr
                    )))
                }
            };

        if let Some(config) = keepalive_config {
            Self::configure_keepalive(&stream, config)?;
        }

        Ok(Self {
            stream,
            timeout_secs,
        })
    }

    /// Configure TCP keepalive settings for the stream
    /// This is safe because:
    /// 1. We only borrow the stream temporarily
    /// 2. set_tcp_keepalive is a fast syscall (setsockopt) that doesn't block
    /// 3. Keepalive is handled by the kernel, doesn't affect async operations
    fn configure_keepalive(
        stream: &TcpStream,
        config: &KeepAliveConfig,
    ) -> Result<(), BinlogError> {
        if config.keepidle_secs == 0 || config.keepintvl_secs == 0 {
            return Ok(());
        }

        #[cfg(unix)]
        {
            use socket2::{SockRef, TcpKeepalive};
            use std::os::unix::io::BorrowedFd;

            let raw_fd = stream.as_raw_fd();
            let borrowed_fd = unsafe { BorrowedFd::borrow_raw(raw_fd) };
            let socket_ref = SockRef::from(&borrowed_fd);

            let keepalive = TcpKeepalive::new()
                .with_time(Duration::from_secs(config.keepidle_secs))
                .with_interval(Duration::from_secs(config.keepintvl_secs));

            socket_ref
                .set_tcp_keepalive(&keepalive)
                .map_err(BinlogError::IoError)?;
        }

        #[cfg(windows)]
        {
            use socket2::{SockRef, TcpKeepalive};
            use std::os::windows::io::BorrowedSocket;

            let raw_socket = stream.as_raw_socket();
            let borrowed_socket = unsafe { BorrowedSocket::borrow_raw(raw_socket) };
            let socket_ref = SockRef::from(&borrowed_socket);

            let keepalive = TcpKeepalive::new()
                .with_time(Duration::from_secs(config.keepidle_secs))
                .with_interval(Duration::from_secs(config.keepintvl_secs));

            socket_ref
                .set_tcp_keepalive(&keepalive)
                .map_err(|e| BinlogError::IoError(e))?;
        }

        Ok(())
    }

    pub async fn close(&self) -> Result<(), BinlogError> {
        self.stream.shutdown(std::net::Shutdown::Both)?;
        Ok(())
    }

    pub async fn write(&mut self, buf: &[u8], sequence: u8) -> Result<(), BinlogError> {
        let mut wtr = Vec::new();
        wtr.write_u24::<LittleEndian>(buf.len() as u32)?;
        wtr.write_u8(sequence)?;
        Write::write(&mut wtr, buf)?;
        match timeout(
            Duration::from_secs(self.timeout_secs),
            self.stream.write_all(&wtr),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(BinlogError::from(e)),
            Err(_) => {
                return Err(BinlogError::UnexpectedData(format!(
                    "Write binlog timeout after {}s while sending packet",
                    self.timeout_secs
                )));
            }
        }
        Ok(())
    }

    async fn read_packet_info(&mut self) -> Result<(usize, u8), BinlogError> {
        let mut buf = vec![0u8; 4];
        match timeout(
            Duration::from_secs(self.timeout_secs),
            self.stream.read_exact(&mut buf),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(BinlogError::from(e)),
            Err(_) => {
                return Err(BinlogError::UnexpectedData(format!(
                    "Read binlog header timeout after {}s while waiting for packet header",
                    self.timeout_secs
                )));
            }
        }
        let mut rdr = Cursor::new(buf);
        let length = rdr.read_u24::<LittleEndian>()? as usize;
        let sequence = rdr.read_u8()?;
        Ok((length, sequence))
    }

    pub async fn read_with_sequece(&mut self) -> Result<(Vec<u8>, u8), BinlogError> {
        let (length, sequence) = self.read_packet_info().await?;
        let buf = if length == MAX_PACKET_LENGTH {
            let mut all_buf = self.read_exact(length).await?;
            loop {
                let (chunk_length, _) = self.read_packet_info().await?;
                let mut chunk_buf = self.read_exact(chunk_length).await?;
                all_buf.append(&mut chunk_buf);
                if chunk_length != MAX_PACKET_LENGTH {
                    break;
                }
            }
            trace!("Received big binlog data, full length: {}", all_buf.len());
            all_buf
        } else {
            self.read_exact(length).await?
        };
        Ok((buf, sequence))
    }

    pub async fn read(&mut self) -> Result<Vec<u8>, BinlogError> {
        let (buf, _sequence) = Self::read_with_sequece(self).await?;
        Ok(buf)
    }

    async fn read_exact(&mut self, length: usize) -> Result<Vec<u8>, BinlogError> {
        let mut buf = vec![0u8; length];
        let mut read_count = 0;

        while read_count < length {
            match timeout(
                Duration::from_secs(self.timeout_secs),
                self.stream.read(&mut buf[read_count..]),
            )
            .await
            {
                Ok(Ok(n)) => {
                    if n == 0 {
                        // read() returning 0 means the peer has closed the connection (TCP FIN).
                        // This is an unrecoverable EOF — retrying will never yield new data.
                        return Err(BinlogError::IoError(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            format!(
                                "Connection closed by peer. Expected data length: {}, read so far: {}",
                                length, read_count
                            ),
                        )));
                    }
                    read_count += n;
                    trace!(
                        "Stream reading binlog data, Expected data length: {}, read so far: {}",
                        length,
                        read_count
                    );
                }
                Ok(Err(e)) => {
                    return Err(BinlogError::from(e));
                }
                Err(_) => {
                    return Err(BinlogError::UnexpectedData(format!(
                        "Read binlog timeout, expect data length: {}, read so far: {}",
                        length, read_count
                    )));
                }
            }
        }
        Ok(buf)
    }
}
