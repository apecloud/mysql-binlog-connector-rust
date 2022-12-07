use std::{
    io::{Read, Write},
    net::TcpStream,
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::binlog_error::BinlogError;

pub struct PacketChannel {
    stream: TcpStream,
}

impl PacketChannel {
    pub fn new(ip: String, port: String) -> Result<Self, BinlogError> {
        let stream = TcpStream::connect(format!("{}:{}", ip, port))?;
        Ok(Self { stream })
    }

    pub fn write(&mut self, buf: &[u8], sequence: u8) -> Result<(), BinlogError> {
        self.stream.write_u24::<LittleEndian>(buf.len() as u32)?;
        self.stream.write_u8(sequence)?;
        self.stream.write(&buf)?;
        Ok(())
    }

    pub fn read_with_sequece(&mut self) -> Result<(Vec<u8>, u8), BinlogError> {
        let length = self.stream.read_u24::<LittleEndian>()? as usize;
        let sequence = self.stream.read_u8()?;
        let mut buf = vec![0u8; length];
        self.stream.read_exact(&mut buf)?;
        Ok((buf, sequence))
    }

    pub fn read(&mut self) -> Result<Vec<u8>, BinlogError> {
        let (buf, _sequence) = Self::read_with_sequece(self)?;
        Ok(buf)
    }
}
