#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(windows)]
use std::os::windows::io::AsRawSocket;

use std::{
    io::{Cursor, Write},
    time::Duration,
};
#[cfg(feature = "rustls")]
use std::net::IpAddr;

use async_std::{
    future::timeout,
    io::{ReadExt as AsyncStdReadExt, WriteExt as AsyncStdWriteExt},
    net::TcpStream,
};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use log::{trace, warn};

use crate::binlog_error::BinlogError;

#[cfg(feature = "rustls")]
use futures::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(feature = "rustls")]
use futures_rustls::client::TlsStream;
#[cfg(feature = "rustls")]
use futures_rustls::TlsConnector;
#[cfg(feature = "rustls")]
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
#[cfg(feature = "rustls")]
use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    ClientConfig, DigitallySignedStruct, SignatureScheme,
};
#[cfg(feature = "rustls")]
use std::sync::Arc;

const MAX_PACKET_LENGTH: usize = 16777215;

enum ChannelStream {
    Plain(TcpStream),
    #[cfg(feature = "rustls")]
    Tls(Box<TlsStream<TcpStream>>),
}

pub struct PacketChannel {
    stream: Option<ChannelStream>,
    timeout_secs: u64,
}

pub struct KeepAliveConfig {
    pub keepidle_secs: u64,
    pub keepintvl_secs: u64,
}

#[cfg(feature = "rustls")]
#[derive(Debug)]
struct NoCertificateVerification;

#[cfg(feature = "rustls")]
impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
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
            stream: Some(ChannelStream::Plain(stream)),
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
                .map_err(BinlogError::IoError)?;
        }

        Ok(())
    }

    pub fn is_secure_transport(&self) -> bool {
        match self.stream.as_ref() {
            Some(ChannelStream::Plain(_)) => false,
            #[cfg(feature = "rustls")]
            Some(ChannelStream::Tls(_)) => true,
            None => false,
        }
    }

    #[cfg(feature = "rustls")]
    pub async fn upgrade_to_tls(&mut self, host: &str) -> Result<(), BinlogError> {
        let plain_stream = match self.stream.take() {
            Some(ChannelStream::Plain(stream)) => stream,
            Some(ChannelStream::Tls(stream)) => {
                self.stream = Some(ChannelStream::Tls(stream));
                return Ok(());
            }
            None => {
                return Err(BinlogError::ConnectError(
                    "cannot upgrade a disconnected channel to tls".into(),
                ))
            }
        };

        let server_name = Self::build_server_name(host)?;
        let config = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCertificateVerification))
            .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(config));
        let tls_stream = connector
            .connect(server_name, plain_stream)
            .await
            .map_err(|e| BinlogError::ConnectError(format!("tls handshake failed: {}", e)))?;

        self.stream = Some(ChannelStream::Tls(Box::new(tls_stream)));
        Ok(())
    }

    #[cfg(feature = "rustls")]
    fn build_server_name(host: &str) -> Result<ServerName<'static>, BinlogError> {
        if let Ok(ip_addr) = host.parse::<IpAddr>() {
            return Ok(ServerName::IpAddress(ip_addr.into()));
        }

        ServerName::try_from(host.to_string())
            .map_err(|_| BinlogError::ConnectError(format!("invalid tls server name: {}", host)))
    }

    #[cfg(not(feature = "rustls"))]
    pub async fn upgrade_to_tls(&mut self, _host: &str) -> Result<(), BinlogError> {
        Err(BinlogError::ConnectError(
            "TLS support is unavailable because the 'rustls' feature is disabled".into(),
        ))
    }

    pub async fn close(&mut self) -> Result<(), BinlogError> {
        match self.stream.as_mut() {
            Some(ChannelStream::Plain(stream)) => {
                stream.shutdown(std::net::Shutdown::Both)?;
            }
            #[cfg(feature = "rustls")]
            Some(ChannelStream::Tls(stream)) => {
                AsyncWriteExt::close(stream.as_mut()).await?;
            }
            None => {}
        }
        Ok(())
    }

    pub async fn write(&mut self, buf: &[u8], sequence: u8) -> Result<(), BinlogError> {
        let mut wtr = Vec::new();
        wtr.write_u24::<LittleEndian>(buf.len() as u32)?;
        wtr.write_u8(sequence)?;
        Write::write(&mut wtr, buf)?;
        self.write_all(&wtr).await?;
        Ok(())
    }

    async fn read_packet_info(&mut self) -> Result<(usize, u8), BinlogError> {
        let mut buf = vec![0u8; 4];
        match timeout(
            Duration::from_secs(self.timeout_secs),
            self.read_exact_into(&mut buf),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
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
        self.read_loop(&mut buf).await?;
        Ok(buf)
    }

    async fn read_exact_into(&mut self, buf: &mut [u8]) -> Result<(), BinlogError> {
        self.read_loop(buf).await
    }

    async fn read_loop(&mut self, buf: &mut [u8]) -> Result<(), BinlogError> {
        let length = buf.len();
        let wait_data_millis = 10;
        let max_zero_reads = self.timeout_secs * 1000 / wait_data_millis;
        let mut read_count = 0;
        let mut zero_reads = 0;

        while read_count < length {
            match timeout(
                Duration::from_secs(self.timeout_secs),
                self.read_once(&mut buf[read_count..]),
            )
            .await
            {
                Ok(Ok(n)) => {
                    if n == 0 {
                        zero_reads += 1;
                        if zero_reads >= max_zero_reads {
                            return Err(BinlogError::UnexpectedData(format!(
                                "Too many zero-length reads. Expected data length: {}, read so far: {}",
                                length, read_count
                            )));
                        }
                        warn!(
                            "Stream reading binlog returns zero-length data, Expected data length: {}, read so far: {}",
                            length, read_count
                        );
                        async_std::task::sleep(Duration::from_millis(wait_data_millis)).await;
                        continue;
                    }
                    zero_reads = 0;
                    read_count += n;
                    trace!(
                        "Stream reading binlog data, Expected data length: {}, read so far: {}",
                        length,
                        read_count
                    );
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(BinlogError::UnexpectedData(format!(
                        "Read binlog timeout, expect data length: {}, read so far: {}",
                        length, read_count
                    )));
                }
            }
        }
        Ok(())
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), BinlogError> {
        match &mut self.stream {
            Some(ChannelStream::Plain(stream)) => {
                AsyncStdWriteExt::write_all(stream, buf).await?;
                AsyncStdWriteExt::flush(stream).await?;
            }
            #[cfg(feature = "rustls")]
            Some(ChannelStream::Tls(stream)) => {
                AsyncWriteExt::write_all(stream.as_mut(), buf).await?;
                AsyncWriteExt::flush(stream.as_mut()).await?;
            }
            None => {
                return Err(BinlogError::ConnectError(
                    "channel stream is unavailable".into(),
                ))
            }
        }
        Ok(())
    }

    async fn read_once(&mut self, buf: &mut [u8]) -> Result<usize, BinlogError> {
        let read = match self.stream.as_mut() {
            Some(ChannelStream::Plain(stream)) => AsyncStdReadExt::read(stream, buf).await?,
            #[cfg(feature = "rustls")]
            Some(ChannelStream::Tls(stream)) => AsyncReadExt::read(stream.as_mut(), buf).await?,
            None => {
                return Err(BinlogError::ConnectError(
                    "channel stream is unavailable".into(),
                ))
            }
        };
        Ok(read)
    }
}

#[cfg(all(test, feature = "rustls"))]
mod tests {
    use rustls::pki_types::ServerName;

    use super::PacketChannel;

    #[test]
    fn build_server_name_accepts_ipv4_literals() {
        let server_name = PacketChannel::build_server_name("127.0.0.1").unwrap();
        assert!(matches!(server_name, ServerName::IpAddress(_)));
    }

    #[test]
    fn build_server_name_accepts_dns_names() {
        let server_name = PacketChannel::build_server_name("mysql.example.com").unwrap();
        assert!(matches!(server_name, ServerName::DnsName(_)));
    }
}
