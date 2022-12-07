use std::{
    collections::HashMap,
    io::Cursor,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use byteorder::ReadBytesExt;

use crate::{
    binlog_error::BinlogError,
    binlog_parser::BinlogParser,
    command::command_util::CommandUtil,
    constants::MysqlRespCode,
    event::{event_data::EventData, event_header::EventHeader},
    network::{error_packet::ErrorPacket, packet_channel::PacketChannel},
};

pub struct BinlogClient {
    pub hostname: String,
    pub port: String,
    pub username: String,
    pub password: String,
    pub binlog_filename: String,
    pub binlog_position: u64,
    pub server_id: u64,
    pub shut_down: Arc<AtomicBool>,
    pub on_event: fn(EventHeader, EventData),
}

const MIN_BINLOG_POSITION: u64 = 4;

impl BinlogClient {
    pub fn connect(&mut self) -> Result<(), BinlogError> {
        // init connect
        let mut channel = CommandUtil::connect_and_authenticate(
            self.hostname.clone(),
            self.port.clone(),
            self.username.clone(),
            self.password.clone(),
            "".to_string(),
        )?;

        // fetch binlog info
        if self.binlog_filename.is_empty() {
            let (binlog_filename, binlog_position) = CommandUtil::fetch_binlog_info(&mut channel)?;
            self.binlog_filename = binlog_filename;
            self.binlog_position = binlog_position;
        }

        if self.binlog_position < MIN_BINLOG_POSITION {
            self.binlog_position = MIN_BINLOG_POSITION;
        }

        // fetch binlog checksum
        let binlog_checksum = CommandUtil::fetch_binlog_checksum(&mut channel)?;

        // setup connection
        CommandUtil::setup_binlog_connection(&mut channel)?;

        // dump binlog
        CommandUtil::dump_binlog(
            &mut channel,
            self.binlog_filename.clone(),
            self.binlog_position,
            self.server_id,
        )?;

        // list for binlog
        self.listen_binlog(&mut channel, binlog_checksum.get_length())?;

        Ok(())
    }

    fn listen_binlog(
        &self,
        channel: &mut PacketChannel,
        checksum_length: u8,
    ) -> Result<(), BinlogError> {
        let mut parser = BinlogParser {
            checksum_length,
            table_map_event_by_table_id: HashMap::new(),
        };

        while !self.shut_down.load(Ordering::Relaxed) {
            let buf = channel.read()?;
            let mut cursor = Cursor::new(buf.clone());

            if cursor.read_u8()? == MysqlRespCode::ERROR {
                let _error_packet = ErrorPacket::new(&buf)?;
                return Err(BinlogError::ReadBinlogError {
                    error: "read binlog failed, mysql response code: ERROR".to_string(),
                });
            }

            // parse events, execute the callback
            let event = parser.next(&mut cursor)?;
            (self.on_event)(event.0, event.1);
        }

        Ok(())
    }
}
