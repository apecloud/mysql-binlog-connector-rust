use std::collections::HashMap;

use crate::{
    binlog_error::BinlogError, binlog_parser::BinlogParser, binlog_stream::BinlogStream,
    command::command_util::CommandUtil,
};

pub struct BinlogClient {
    pub hostname: String,
    pub port: String,
    pub username: String,
    pub password: String,
    pub binlog_filename: String,
    pub binlog_position: u64,
    pub server_id: u64,
}

const MIN_BINLOG_POSITION: u64 = 4;

impl BinlogClient {
    pub async fn connect(&mut self) -> Result<BinlogStream, BinlogError> {
        // init connect
        let mut channel = CommandUtil::connect_and_authenticate(
            self.hostname.clone(),
            self.port.clone(),
            self.username.clone(),
            self.password.clone(),
            "".to_string(),
        )
        .await?;

        // fetch binlog info
        if self.binlog_filename.is_empty() {
            let (binlog_filename, binlog_position) =
                CommandUtil::fetch_binlog_info(&mut channel).await?;
            self.binlog_filename = binlog_filename;
            self.binlog_position = binlog_position;
        }

        if self.binlog_position < MIN_BINLOG_POSITION {
            self.binlog_position = MIN_BINLOG_POSITION;
        }

        // fetch binlog checksum
        let binlog_checksum = CommandUtil::fetch_binlog_checksum(&mut channel).await?;

        // setup connection
        CommandUtil::setup_binlog_connection(&mut channel).await?;

        // dump binlog
        CommandUtil::dump_binlog(
            &mut channel,
            self.binlog_filename.clone(),
            self.binlog_position,
            self.server_id,
        )
        .await?;

        // list for binlog
        let parser = BinlogParser {
            checksum_length: binlog_checksum.get_length(),
            table_map_event_by_table_id: HashMap::new(),
        };

        Ok(BinlogStream { channel, parser })
    }
}
