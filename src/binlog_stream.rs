use std::io::Cursor;

use byteorder::ReadBytesExt;

use crate::{
    binlog_error::BinlogError,
    binlog_parser::BinlogParser,
    command::command_util::CommandUtil,
    constants::MysqlRespCode,
    event::{event_data::EventData, event_header::EventHeader},
    network::packet_channel::PacketChannel,
};

pub struct BinlogStream {
    pub channel: PacketChannel,
    pub parser: BinlogParser,
}

impl BinlogStream {
    pub async fn read(&mut self) -> Result<(EventHeader, EventData), BinlogError> {
        let buf = self.channel.read().await?;
        let mut cursor = Cursor::new(&buf);

        if cursor.read_u8()? == MysqlRespCode::ERROR {
            CommandUtil::parse_result(&buf)?;
        }

        // parse events, execute the callback
        self.parser.next(&mut cursor)
    }

    pub async fn close(&mut self) -> Result<(), BinlogError> {
        self.channel.close().await?;
        Ok(())
    }
}
