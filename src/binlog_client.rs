use std::collections::HashMap;

use crate::{
    binlog_error::BinlogError,
    binlog_parser::BinlogParser,
    binlog_stream::BinlogStream,
    command::{authenticator::Authenticator, command_util::CommandUtil},
};

#[derive(Default)]
pub struct BinlogClient {
    pub url: String,
    pub binlog_filename: String,
    pub binlog_position: u32,
    pub server_id: u64,
    pub gtid_enabled: bool,
    pub gtid_set: String,
    pub heartbeat_interval_secs: u64,
}

const MIN_BINLOG_POSITION: u32 = 4;

impl BinlogClient {
    pub async fn connect(&mut self) -> Result<BinlogStream, BinlogError> {
        // init connect
        let mut authenticator = Authenticator::new(&self.url)?;
        let mut channel = authenticator.connect().await?;

        if self.gtid_enabled {
            if self.gtid_set.is_empty() {
                let (_, _, gtid_set) = CommandUtil::fetch_binlog_info(&mut channel).await?;
                self.gtid_set = gtid_set;
            }
        } else {
            // fetch binlog info
            if self.binlog_filename.is_empty() {
                let (binlog_filename, binlog_position, _) =
                    CommandUtil::fetch_binlog_info(&mut channel).await?;
                self.binlog_filename = binlog_filename;
                self.binlog_position = binlog_position;
            }

            if self.binlog_position < MIN_BINLOG_POSITION {
                self.binlog_position = MIN_BINLOG_POSITION;
            }
        }

        // fetch binlog checksum
        let binlog_checksum = CommandUtil::fetch_binlog_checksum(&mut channel).await?;

        // setup connection
        CommandUtil::setup_binlog_connection(&mut channel).await?;

        if self.heartbeat_interval_secs > 0 {
            CommandUtil::enable_heartbeat(&mut channel, self.heartbeat_interval_secs).await?;
        }

        // dump binlog
        CommandUtil::dump_binlog(&mut channel, self).await?;

        // list for binlog
        let parser = BinlogParser {
            checksum_length: binlog_checksum.get_length(),
            table_map_event_by_table_id: HashMap::new(),
        };

        Ok(BinlogStream { channel, parser })
    }
}
