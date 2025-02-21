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
        let result_sets = Self::execute_query(channel, "show master status").await?;
        if result_sets.is_empty() {
            return Err(BinlogError::ConnectError(
                "failed to fetch binlog filename and position".into(),
            ));
        }
        let binlog_filename = result_sets[0].values[0].clone();
        let binlog_position = result_sets[0].values[1].clone().parse::<u32>()?;
        let gtid_set = result_sets[0].values[4].clone();
        Ok((binlog_filename, binlog_position, gtid_set))
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
                heartbeat_interval_secs * 1000_000_000
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
