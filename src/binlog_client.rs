use std::{collections::HashMap, time::Duration};

use crate::{
    binlog_error::BinlogError,
    binlog_parser::BinlogParser,
    binlog_stream::BinlogStream,
    command::{
        authenticator::Authenticator,
        command_util::{CommandUtil, DumpBinlogOptions},
    },
    network::packet_channel::KeepAliveConfig,
};
use async_std::task::sleep;
use log::warn;

#[derive(Debug, Clone)]
pub enum StartPosition {
    BinlogPosition(String, u32),
    Gtid(String),
    Latest,
}

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub max_attempts: Option<usize>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 1_000,
            max_backoff_ms: 30_000,
            max_attempts: Some(1),
        }
    }
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
    /// Retry policy for building a binlog session, including TCP connect/auth/setup/dump phases.
    pub retry_config: RetryConfig,
}

const MIN_BINLOG_POSITION: u32 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedStartPosition {
    gtid_enabled: bool,
    gtid_set: String,
    binlog_filename: String,
    binlog_position: u32,
}

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
        let resolved_start = self.resolve_start_position(&mut channel).await?;

        // fetch binlog checksum
        let binlog_checksum = CommandUtil::fetch_binlog_checksum(&mut channel).await?;

        // setup connection
        CommandUtil::setup_binlog_connection(&mut channel).await?;

        if self.heartbeat_interval_secs > 0 {
            CommandUtil::enable_heartbeat(&mut channel, self.heartbeat_interval_secs).await?;
        }

        // dump binlog
        CommandUtil::dump_binlog(
            &mut channel,
            DumpBinlogOptions {
                server_id: self.server_id,
                gtid_enabled: resolved_start.gtid_enabled,
                gtid_set: &resolved_start.gtid_set,
                binlog_filename: &resolved_start.binlog_filename,
                binlog_position: resolved_start.binlog_position,
            },
        )
        .await?;

        self.apply_resolved_start_position(&resolved_start);

        // list for binlog
        let parser = BinlogParser {
            checksum_length: binlog_checksum.get_length(),
            table_map_event_by_table_id: HashMap::new(),
        };

        Ok(BinlogStream { channel, parser })
    }

    pub async fn connect_with_retry(&mut self) -> Result<BinlogStream, BinlogError> {
        let max_attempts = self.retry_config.max_attempts.unwrap_or(usize::MAX);
        let mut attempt = 0usize;

        loop {
            attempt += 1;

            match self.connect().await {
                Ok(stream) => return Ok(stream),
                Err(err) if err.is_retryable_network_error() && attempt < max_attempts => {
                    let backoff_ms = self.compute_backoff_ms(attempt);
                    warn!(
                        "Binlog connect attempt {} failed with retryable error: {}. Retrying in {} ms",
                        attempt, err, backoff_ms
                    );
                    sleep(Duration::from_millis(backoff_ms)).await;
                }
                Err(err) => return Err(err),
            }
        }
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

    fn compute_backoff_ms(&self, attempt: usize) -> u64 {
        let exp = (attempt.saturating_sub(1) as u32).min(16);
        let multiplier = 2u64.pow(exp);
        let base = self
            .retry_config
            .initial_backoff_ms
            .saturating_mul(multiplier);
        base.min(self.retry_config.max_backoff_ms.max(1))
    }

    async fn resolve_start_position(
        &self,
        channel: &mut crate::network::packet_channel::PacketChannel,
    ) -> Result<ResolvedStartPosition, BinlogError> {
        if self.gtid_enabled {
            let gtid_set = if self.gtid_set.is_empty() {
                let (_, _, gtid_set) = CommandUtil::fetch_binlog_info(channel).await?;
                gtid_set
            } else {
                self.gtid_set.clone()
            };

            return Ok(ResolvedStartPosition {
                gtid_enabled: true,
                gtid_set,
                binlog_filename: String::new(),
                binlog_position: 0,
            });
        }

        let (binlog_filename, binlog_position) = if self.binlog_filename.is_empty() {
            let (binlog_filename, binlog_position, _) =
                CommandUtil::fetch_binlog_info(channel).await?;
            (binlog_filename, binlog_position)
        } else {
            (self.binlog_filename.clone(), self.binlog_position)
        };

        Ok(ResolvedStartPosition {
            gtid_enabled: false,
            gtid_set: String::new(),
            binlog_filename,
            binlog_position: binlog_position.max(MIN_BINLOG_POSITION),
        })
    }

    fn apply_resolved_start_position(&mut self, resolved_start: &ResolvedStartPosition) {
        self.gtid_enabled = resolved_start.gtid_enabled;
        self.gtid_set = resolved_start.gtid_set.clone();
        self.binlog_filename = resolved_start.binlog_filename.clone();
        self.binlog_position = resolved_start.binlog_position;
    }
}

#[cfg(test)]
mod tests {
    use super::{BinlogClient, ResolvedStartPosition, RetryConfig, StartPosition};

    #[test]
    fn compute_backoff_caps_at_max() {
        let client = BinlogClient {
            retry_config: RetryConfig {
                initial_backoff_ms: 100,
                max_backoff_ms: 1_000,
                max_attempts: None,
            },
            ..Default::default()
        };

        assert_eq!(client.compute_backoff_ms(1), 100);
        assert_eq!(client.compute_backoff_ms(2), 200);
        assert_eq!(client.compute_backoff_ms(8), 1_000);
    }

    #[test]
    fn apply_resolved_start_position_updates_client_after_success() {
        let mut client =
            BinlogClient::new("mysql://root:root@127.0.0.1:3306", 1, StartPosition::Latest);
        let resolved = ResolvedStartPosition {
            gtid_enabled: false,
            gtid_set: String::new(),
            binlog_filename: "mysql-bin.000123".to_string(),
            binlog_position: 456,
        };

        client.apply_resolved_start_position(&resolved);

        assert!(!client.gtid_enabled);
        assert_eq!(client.binlog_filename, "mysql-bin.000123");
        assert_eq!(client.binlog_position, 456);
    }

    #[test]
    fn applying_resolved_position_is_explicit_not_implicit() {
        let client =
            BinlogClient::new("mysql://root:root@127.0.0.1:3306", 1, StartPosition::Latest);

        assert!(!client.gtid_enabled);
        assert!(client.gtid_set.is_empty());
        assert!(client.binlog_filename.is_empty());
        assert_eq!(client.binlog_position, 0);
    }
}
