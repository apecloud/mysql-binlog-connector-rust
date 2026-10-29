use crate::{
    binlog_client::BinlogClient,
    binlog_error::BinlogError,
    constants::MysqlRespCode,
    event::checksum_type::ChecksumType,
    network::{
        error_packet::ErrorPacket, packet_channel::PacketChannel,
        result_set_row_packet::ResultSetRowPacket,
    },
};

use super::{
    dump_binlog_command::DumpBinlogCommand, dump_binlog_gtid_command::DumpBinlogGtidCommand,
    gtid_set::GtidSet, query_command::QueryCommand,
};

pub struct CommandUtil {}

impl CommandUtil {
    pub async fn execute_query(
        channel: &mut PacketChannel,
        sql: &str,
    ) -> Result<Vec<ResultSetRowPacket>, BinlogError> {
        Self::execute_sql(channel, sql).await?;
        // read to EOF
        while channel.read().await?[0] != MysqlRespCode::EOF {}
        // get result sets
        let mut result_sets = Vec::new();

        let mut buf = channel.read().await?;
        while buf[0] != MysqlRespCode::EOF {
            Self::check_error_packet(&buf)?;
            let result_set = ResultSetRowPacket::new(&buf)?;
            result_sets.push(result_set);
            buf = channel.read().await?;
        }

        Ok(result_sets)
    }

    pub async fn execute_sql(channel: &mut PacketChannel, sql: &str) -> Result<(), BinlogError> {
        let mut command = QueryCommand {
            sql: sql.to_string(),
        };

        // send the query command, sequence for non-authenticate commands are always 0
        channel.write(&command.to_bytes()?, 0).await?;

        // read the response packet
        let buf = channel.read().await?;
        Self::check_error_packet(&buf)
    }

    pub async fn fetch_binlog_info(
        channel: &mut PacketChannel,
    ) -> Result<(String, u32, String), BinlogError> {
        let server_version = channel.server_version().ok_or_else(|| {
            BinlogError::ConnectError("missing server version in mysql greeting".into())
        })?;
        let query = Self::binlog_status_query(server_version)?;
        let result_sets = Self::execute_query(channel, query).await?;
        let row = result_sets
            .first()
            .filter(|row| row.values.len() >= 5)
            .ok_or_else(|| {
                BinlogError::ConnectError("failed to fetch binlog filename and position".into())
            })?;
        let binlog_filename = row.values[0].clone();
        let binlog_position = row.values[1].clone().parse::<u32>()?;
        let gtid_set = row.values[4].clone();
        Ok((binlog_filename, binlog_position, gtid_set))
    }

    fn binlog_status_query(server_version: &str) -> Result<&'static str, BinlogError> {
        let mut parts = server_version.split('.');
        let major = parts.next().and_then(|part| part.parse::<u16>().ok());
        let minor = parts.next().and_then(|part| part.parse::<u16>().ok());
        match (major, minor) {
            // SHOW MASTER STATUS was removed in MySQL 8.4.
            (Some(major), Some(minor)) if (major, minor) >= (8, 4) => Ok("show binary log status"),
            (Some(_), Some(_)) => Ok("show master status"),
            _ => Err(BinlogError::ConnectError(format!(
                "invalid mysql server version: {}",
                server_version
            ))),
        }
    }

    pub async fn fetch_binlog_checksum(
        channel: &mut PacketChannel,
    ) -> Result<ChecksumType, BinlogError> {
        let result_set_rows =
            Self::execute_query(channel, "select @@global.binlog_checksum").await?;
        let mut checksum_name = "";
        if !result_set_rows.is_empty() {
            checksum_name = result_set_rows[0].values[0].as_str();
        }
        Ok(ChecksumType::from_name(checksum_name))
    }

    pub async fn setup_binlog_connection(channel: &mut PacketChannel) -> Result<(), BinlogError> {
        let mut command = QueryCommand {
            sql: "set @master_binlog_checksum= @@global.binlog_checksum".to_string(),
        };
        channel.write(&command.to_bytes()?, 0).await?;
        let buf = channel.read().await?;
        Self::check_error_packet(&buf)
    }

    pub async fn enable_heartbeat(
        channel: &mut PacketChannel,
        heartbeat_interval_secs: u64,
    ) -> Result<(), BinlogError> {
        let mut command = QueryCommand {
            sql: format!(
                "set @master_heartbeat_period={}",
                heartbeat_interval_secs * 1_000_000_000
            ),
        };
        channel.write(&command.to_bytes()?, 0).await?;
        let buf = channel.read().await?;
        Self::check_error_packet(&buf)
    }

    pub async fn dump_binlog(
        channel: &mut PacketChannel,
        client: &BinlogClient,
    ) -> Result<(), BinlogError> {
        let buf = if client.gtid_enabled {
            let mut command = DumpBinlogGtidCommand {
                server_id: client.server_id,
                gtid_set: GtidSet::new(&client.gtid_set)?,
            };
            command.to_bytes()?
        } else {
            let mut command = DumpBinlogCommand {
                binlog_filename: client.binlog_filename.clone(),
                binlog_position: client.binlog_position,
                server_id: client.server_id,
            };
            command.to_bytes()?
        };
        channel.write(&buf, 0).await
    }

    pub fn parse_result(buf: &Vec<u8>) -> Result<(), BinlogError> {
        match buf[0] {
            MysqlRespCode::OK => Ok(()),

            MysqlRespCode::ERROR => Self::check_error_packet(buf),

            _ => Err(BinlogError::ConnectError("connect mysql failed".into())),
        }
    }

    pub fn check_error_packet(buf: &Vec<u8>) -> Result<(), BinlogError> {
        if buf[0] == MysqlRespCode::ERROR {
            let error_packet = ErrorPacket::new(buf)?;
            return Err(BinlogError::ConnectError(format!(
                "connect mysql failed: {}",
                error_packet.error_message
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::CommandUtil;

    #[test]
    fn selects_binlog_status_query_by_numeric_version() {
        for version in ["5.6.51", "5.7.44-log", "8.0.33", "8.3.0"] {
            assert_eq!(
                CommandUtil::binlog_status_query(version).unwrap(),
                "show master status"
            );
        }
        for version in ["8.4.0", "8.4.13-commercial", "8.10.0", "9.0.0"] {
            assert_eq!(
                CommandUtil::binlog_status_query(version).unwrap(),
                "show binary log status"
            );
        }
        for version in ["", "unknown", "8", "8.x.0"] {
            assert!(CommandUtil::binlog_status_query(version).is_err());
        }
    }
}
