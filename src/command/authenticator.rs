use async_recursion::async_recursion;
use openssl::rsa::Rsa;
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
}

impl Authenticator {
    pub fn new(url: &str) -> Result<Self, BinlogError> {
        // url example: mysql://root:123456@127.0.0.1:3307/test_db?ssl-mode=disabled
        let url_info = Url::parse(url)?;
        let host = url_info.host_str().unwrap().to_string();
        let port = format!("{}", url_info.port().unwrap());
        let username = url_info.username().to_string();
        let password = url_info.password().unwrap().to_string();
        let mut schema = "".to_string();
        let pathes = url_info.path_segments().map(|c| c.collect::<Vec<_>>());
        if let Some(vec) = pathes {
            if vec.len() > 0 {
                schema = vec[0].to_string();
            }
        }

        Ok(Self {
            host,
            port,
            username,
            password,
            schema,
            scramble: "".to_string(),
            collation: 0,
        })
    }

    pub async fn connect(&mut self) -> Result<PacketChannel, BinlogError> {
        // connect to hostname:port
        let mut channel = PacketChannel::new(&self.host, &self.port).await?;

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
                return Err(BinlogError::MysqlError {
                    error: "unsupported auth plugin".to_string(),
                });
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

            MysqlRespCode::ERROR => return CommandUtil::check_error_packet(&auth_res),

            MysqlRespCode::AUTH_PLUGIN_SWITCH => {
                return self
                    .handle_auth_plugin_switch(channel, sequence, &auth_res)
                    .await;
            }

            _ => match AuthPlugin::from_name(auth_plugin_name) {
                AuthPlugin::MySqlNativePassword => {
                    return Err(BinlogError::MysqlError {
                        error: format!(
                            "unexpected authentication result for mysql_native_password: {}",
                            auth_res[0]
                        ),
                    });
                }

                AuthPlugin::CachingSha2Password => {
                    return self
                        .handle_sha2_auth_result(channel, sequence, &auth_res)
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
        let switch_packet = AuthPluginSwitchPacket::new(&auth_res)?;
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
                return Err(BinlogError::MysqlError {
                    error: format!(
                        "unexpected auth plugin for auth plugin switch: {}",
                        auth_plugin_name
                    ),
                });
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
        auth_res: &Vec<u8>,
    ) -> Result<(), BinlogError> {
        // buf[0] is the length of buf, always 1
        match auth_res[1] {
            0x03 => Ok(()),

            0x04 => self.sha2_rsa_authenticate(channel, sequence).await,

            _ => Err(BinlogError::MysqlError {
                error: format!(
                    "unexpected authentication result for caching_sha2_password: {}",
                    auth_res[1]
                ),
            }),
        }
    }

    async fn sha2_rsa_authenticate(
        &self,
        channel: &mut PacketChannel,
        sequence: u8,
    ) -> Result<(), BinlogError> {
        // refer: https://mariadb.com/kb/en/caching_sha2_password-authentication-plugin/
        // try to get RSA key from server
        channel.write(&vec![0x02], sequence + 1).await?;
        let (rsa_res, sequence) = channel.read_with_sequece().await?;
        match rsa_res[0] {
            0x01 => {
                let rsa_key = Rsa::public_key_from_pem(&rsa_res[1..])?;

                // try sha2 authentication with rsa
                let mut command = AuthSha2RsaPasswordCommand {
                    rsa_key,
                    password: self.password.clone(),
                    scramble: self.scramble.clone(),
                };
                channel.write(&command.to_bytes()?, sequence + 1).await?;

                let (auth_res, _) = channel.read_with_sequece().await?;
                return CommandUtil::parse_result(&auth_res);
            }

            _ => {
                return Err(BinlogError::MysqlError {
                    error: format!(
                        "failed to get RSA key from server for caching_sha2_password: {}",
                        rsa_res[0]
                    ),
                })
            }
        }
    }
}
