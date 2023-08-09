use std::io::{Cursor, Write};

use async_std::net::TcpStream;
use async_std::prelude::*;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::binlog_error::BinlogError;

pub struct PacketChannel {
    stream: TcpStream,
}

impl PacketChannel {
    pub async fn new(ip: &str, port: &str) -> Result<Self, BinlogError> {
        let stream = TcpStream::connect(format!("{}:{}", ip, port)).await?;
        Ok(Self { stream })
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
        let mut buf = vec![0u8; 4];
        self.stream.read_exact(&mut buf).await?;

        let mut rdr = Cursor::new(buf);
        let length = rdr.read_u24::<LittleEndian>()? as usize;
        let sequence = rdr.read_u8()?;

        let mut buf = vec![0u8; length];
        self.stream.read_exact(&mut buf).await?;
        Ok((buf, sequence))
    }

    pub async fn read(&mut self) -> Result<Vec<u8>, BinlogError> {
        let (buf, _sequence) = Self::read_with_sequece(self).await?;
        Ok(buf)
    }
}
