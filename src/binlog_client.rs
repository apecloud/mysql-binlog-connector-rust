use std::{collections::HashMap, time::Duration};

use crate::{
    binlog_error::BinlogError,
    binlog_parser::BinlogParser,
    binlog_stream::BinlogStream,
    command::{authenticator::Authenticator, command_util::CommandUtil},
    network::packet_channel::KeepAliveConfig,
};

pub enum StartPosition {
    BinlogPosition(String, u32),
    Gtid(String),
    Latest,
}

#[derive(Default)]
pub struct BinlogClient {
    /// MySQL server connection URL in format "mysql://user:password@host:port"
    pub url: String,
    /// Name of the binlog file to start replication from, e.g. "mysql-bin.000001"
    /// Only used when gtid_enabled is false
    pub binlog_filename: String,
    /// Position in the binlog file to start replication from
    pub binlog_position: u32,
    /// Unique identifier for this replication client
    /// Must be different from other clients connected to the same MySQL server
    pub server_id: u64,
    /// Whether to enable GTID mode for replication
    pub gtid_enabled: bool,
    /// GTID set in format "uuid:1-100,uuid2:1-200"
    /// Only used when gtid_enabled is true
    pub gtid_set: String,
    /// Heartbeat interval in seconds
    /// Server will send a heartbeat event if no binlog events are received within this interval
    /// If heartbeat_interval_secs=0, server won't send heartbeat events
    pub heartbeat_interval_secs: u64,
    /// Network operation timeout in seconds
    /// Maximum wait time for operations like connection establishment and data reading
    /// If timeout_secs=0, the default value(60) will be used
    pub timeout_secs: u64,

    /// TCP keepalive idle time in seconds
    /// The time period after which the first keepalive packet is sent if no data has been exchanged between the two endpoints
    /// If keepalive_idle_secs=0, TCP keepalive will not be enabled
    pub keepalive_idle_secs: u64,
    /// TCP keepalive interval time in seconds
    /// The time period between keepalive packets if the connection is still active
    /// If keepalive_interval_secs=0, TCP keepalive will not be enabled
    pub keepalive_interval_secs: u64,
}

const MIN_BINLOG_POSITION: u32 = 4;

impl BinlogClient {
    pub fn new(url: &str, server_id: u64, position: StartPosition) -> Self {
        let mut client = Self {
            url: url.to_string(),
            server_id,
            timeout_secs: 60,
            ..Default::default()
        };
        match position {
            StartPosition::BinlogPosition(binlog_filename, binlog_position) => {
                client.binlog_filename = binlog_filename.to_string();
                client.binlog_position = binlog_position;
            }
            StartPosition::Gtid(gtid_set) => {
                client.gtid_set = gtid_set.to_string();
                client.gtid_enabled = true;
            }
            StartPosition::Latest => {}
        }
        client
    }

    pub fn with_master_heartbeat(self, heartbeat_interval: Duration) -> Self {
        Self {
            heartbeat_interval_secs: heartbeat_interval.as_secs(),
            ..self
        }
    }

    pub fn with_read_timeout(self, timeout: Duration) -> Self {
        Self {
            timeout_secs: timeout.as_secs(),
            ..self
        }
    }

    pub fn with_keepalive(self, keepalive_idle: Duration, keepalive_interval: Duration) -> Self {
        Self {
            keepalive_idle_secs: keepalive_idle.as_secs(),
            keepalive_interval_secs: keepalive_interval.as_secs(),
            ..self
        }
    }

    pub async fn connect(&mut self) -> Result<BinlogStream, BinlogError> {
        // init connect
        let timeout_secs = if self.timeout_secs > 0 {
            self.timeout_secs
        } else {
            60
        };
        let mut authenticator =
            Authenticator::new(&self.url, timeout_secs, self.build_keepalive_config())?;
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

    fn build_keepalive_config(&self) -> Option<KeepAliveConfig> {
        if self.keepalive_idle_secs == 0 || self.keepalive_interval_secs == 0 {
            return None;
        }

        Some(KeepAliveConfig {
            keepidle_secs: self.keepalive_idle_secs,
            keepintvl_secs: self.keepalive_interval_secs,
        })
    }
}
