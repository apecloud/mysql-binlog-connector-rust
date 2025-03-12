use std::{
    io::{Cursor, Write},
    time::Duration,
};

use async_std::prelude::*;
use async_std::{future::timeout, net::TcpStream};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use log::{debug, warn};

use crate::binlog_error::BinlogError;

pub struct PacketChannel {
    stream: TcpStream,
    timeout_secs: u64,
}

impl PacketChannel {
    pub async fn new(ip: &str, port: &str, timeout_secs: u64) -> Result<Self, BinlogError> {
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
        Ok(Self {
            stream,
            timeout_secs,
        })
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
        self.stream.write_all(&wtr).await?;
        Ok(())
    }

    pub async fn read_with_sequece(&mut self) -> Result<(Vec<u8>, u8), BinlogError> {
        let buf = self.read_exact(4).await?;
        let mut rdr = Cursor::new(buf);
        let length = rdr.read_u24::<LittleEndian>()? as usize;
        let sequence = rdr.read_u8()?;

        let buf = self.read_exact(length).await?;
        Ok((buf, sequence))
    }

    pub async fn read(&mut self) -> Result<Vec<u8>, BinlogError> {
        let (buf, _sequence) = Self::read_with_sequece(self).await?;
        Ok(buf)
    }

    async fn read_exact(&mut self, length: usize) -> Result<Vec<u8>, BinlogError> {
        let mut buf = vec![0u8; length];
        // keep reading data until the complete packet is received
        // MySQL protocol packets may require multiple reads for complete reception
        let wait_data_millis = 10;
        let max_zero_reads = self.timeout_secs * 1000 / wait_data_millis;
        let mut read_count = 0;
        let mut zero_reads = 0;

        while read_count < length {
            match timeout(
                Duration::from_secs(self.timeout_secs),
                self.stream.read(&mut buf[read_count..]),
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
                    debug!(
                        "Stream reading binlog data, Expected data length: {}, read so far: {}",
                        length, read_count
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
