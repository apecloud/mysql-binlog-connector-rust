use async_recursion::async_recursion;
use percent_encoding::percent_decode_str;
use url::Url;

use crate::{
    binlog_error::BinlogError,
    constants::MysqlRespCode,
    network::{
        auth_plugin_switch_packet::AuthPluginSwitchPacket, greeting_packet::GreetingPacket,
        packet_channel::PacketChannel,
    },
};

use super::{
    auth_native_password_command::AuthNativePasswordCommand, auth_plugin::AuthPlugin,
    auth_sha2_password_command::AuthSha2PasswordCommand,
    auth_sha2_rsa_password_command::AuthSha2RsaPasswordCommand, command_util::CommandUtil,
};

pub struct Authenticator {
    host: String,
    port: String,
    username: String,
    password: String,
    schema: String,
    scramble: String,
    collation: u8,
    timeout_secs: u64,
}

impl Authenticator {
    pub fn new(url: &str, timeout_secs: u64) -> Result<Self, BinlogError> {
        // url example: mysql://root:123456@127.0.0.1:3307/test_db?ssl-mode=disabled
        let url_info = Url::parse(url)?;
        let host = url_info.host_str().unwrap_or("");
        let port = format!("{}", url_info.port().unwrap_or(3306));
        let username = url_info.username();
        let password = url_info.password().unwrap_or("");
        let mut schema = "";
        let pathes = url_info.path_segments().map(|c| c.collect::<Vec<_>>());
        if let Some(vec) = pathes {
            if !vec.is_empty() {
                schema = vec[0];
            }
        }

        Ok(Self {
            host: percent_decode_str(host).decode_utf8_lossy().to_string(),
            port,
            username: percent_decode_str(username).decode_utf8_lossy().to_string(),
            password: percent_decode_str(password).decode_utf8_lossy().to_string(),
            schema: percent_decode_str(schema).decode_utf8_lossy().to_string(),
            scramble: String::new(),
            collation: 0,
            timeout_secs,
        })
    }

    pub async fn connect(&mut self) -> Result<PacketChannel, BinlogError> {
        // connect to hostname:port
        let mut channel = PacketChannel::new(&self.host, &self.port, self.timeout_secs).await?;

        // read and parse greeting packet
        let (greeting_buf, sequence) = channel.read_with_sequece().await?;
        let greeting_packet = GreetingPacket::new(greeting_buf)?;

        self.collation = greeting_packet.server_collation;
        self.scramble = greeting_packet.scramble;

        // authenticate
        self.authenticate(
            &mut channel,
            &greeting_packet.plugin_provided_data,
            sequence,
        )
        .await?;

        Ok(channel)
    }

    async fn authenticate(
        &mut self,
        channel: &mut PacketChannel,
        auth_plugin_name: &str,
        sequence: u8,
    ) -> Result<(), BinlogError> {
        let command_buf = match AuthPlugin::from_name(auth_plugin_name) {
            AuthPlugin::MySqlNativePassword => AuthNativePasswordCommand {
                schema: self.schema.clone(),
                username: self.username.clone(),
                password: self.password.clone(),
                scramble: self.scramble.clone(),
                collation: self.collation,
            }
            .to_bytes()?,

            AuthPlugin::CachingSha2Password => AuthSha2PasswordCommand {
                schema: self.schema.clone(),
                username: self.username.clone(),
                password: self.password.clone(),
                scramble: self.scramble.clone(),
                collation: self.collation,
            }
            .to_bytes()?,

            AuthPlugin::Unsupported => {
                return Err(BinlogError::ConnectError("unsupported auth plugin".into()));
            }
        };

        channel.write(&command_buf, sequence + 1).await?;
        let (auth_res, sequence) = channel.read_with_sequece().await?;
        self.handle_auth_result(channel, auth_plugin_name, sequence, &auth_res)
            .await
    }

    async fn handle_auth_result(
        &mut self,
        channel: &mut PacketChannel,
        auth_plugin_name: &str,
        sequence: u8,
        auth_res: &Vec<u8>,
    ) -> Result<(), BinlogError> {
        // parse result
        match auth_res[0] {
            MysqlRespCode::OK => return Ok(()),

            MysqlRespCode::ERROR => return CommandUtil::check_error_packet(auth_res),

            MysqlRespCode::AUTH_PLUGIN_SWITCH => {
                return self
                    .handle_auth_plugin_switch(channel, sequence, auth_res)
                    .await;
            }

            _ => match AuthPlugin::from_name(auth_plugin_name) {
                AuthPlugin::MySqlNativePassword => {
                    return Err(BinlogError::ConnectError(format!(
                        "unexpected auth result for mysql_native_password: {}",
                        auth_res[0]
                    )));
                }

                AuthPlugin::CachingSha2Password => {
                    return self
                        .handle_sha2_auth_result(channel, sequence, auth_res)
                        .await;
                }

                // won't happen
                _ => {}
            },
        };

        Ok(())
    }

    #[async_recursion]
    async fn handle_auth_plugin_switch(
        &mut self,
        channel: &mut PacketChannel,
        sequence: u8,
        auth_res: &Vec<u8>,
    ) -> Result<(), BinlogError> {
        let switch_packet = AuthPluginSwitchPacket::new(auth_res)?;
        let auth_plugin_name = &switch_packet.auth_plugin_name;
        self.scramble = switch_packet.scramble;

        let encrypted_password = match AuthPlugin::from_name(auth_plugin_name) {
            AuthPlugin::CachingSha2Password => AuthSha2PasswordCommand {
                schema: self.schema.clone(),
                username: self.username.clone(),
                password: self.password.clone(),
                scramble: self.scramble.clone(),
                collation: self.collation,
            }
            .encrypted_password()?,

            AuthPlugin::MySqlNativePassword => AuthNativePasswordCommand {
                schema: self.schema.clone(),
                username: self.username.clone(),
                password: self.password.clone(),
                scramble: self.scramble.clone(),
                collation: self.collation,
            }
            .encrypted_password()?,

            _ => {
                return Err(BinlogError::ConnectError(format!(
                    "unexpected auth plugin for auth plugin switch: {}",
                    auth_plugin_name
                )));
            }
        };

        channel.write(&encrypted_password, sequence + 1).await?;
        let (encrypted_auth_res, sequence) = channel.read_with_sequece().await?;
        self.handle_auth_result(channel, auth_plugin_name, sequence, &encrypted_auth_res)
            .await
    }

    async fn handle_sha2_auth_result(
        &self,
        channel: &mut PacketChannel,
        sequence: u8,
        auth_res: &[u8],
    ) -> Result<(), BinlogError> {
        // buf[0] is the length of buf, always 1
        match auth_res[1] {
            0x03 => Ok(()),

            0x04 => self.sha2_rsa_authenticate(channel, sequence).await,

            _ => Err(BinlogError::ConnectError(format!(
                "unexpected auth result for caching_sha2_password: {}",
                auth_res[1]
            ))),
        }
    }

    async fn sha2_rsa_authenticate(
        &self,
        channel: &mut PacketChannel,
        sequence: u8,
    ) -> Result<(), BinlogError> {
        // refer: https://mariadb.com/kb/en/caching_sha2_password-authentication-plugin/
        // try to get RSA key from server
        channel.write(&[0x02], sequence + 1).await?;
        let (rsa_res, sequence) = channel.read_with_sequece().await?;
        match rsa_res[0] {
            0x01 => {
                // try sha2 authentication with rsa
                let mut command = AuthSha2RsaPasswordCommand {
                    rsa_res: rsa_res[1..].to_vec(),
                    password: self.password.clone(),
                    scramble: self.scramble.clone(),
                };
                channel.write(&command.to_bytes()?, sequence + 1).await?;

                let (auth_res, _) = channel.read_with_sequece().await?;
                CommandUtil::parse_result(&auth_res)
            }

            _ => Err(BinlogError::ConnectError(format!(
                "failed to get RSA key from server for caching_sha2_password: {}",
                rsa_res[0]
            ))),
        }
    }
}
