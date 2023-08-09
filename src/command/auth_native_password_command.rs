use std::io::Write;

use byteorder::{LittleEndian, WriteBytesExt};
use sha1::{Digest, Sha1};

use crate::{binlog_error::BinlogError, constants::ClientCapabilities, ext::buf_ext::BufExt};

use super::auth_plugin::AuthPlugin;

pub struct AuthNativePasswordCommand {
    pub schema: String,
    pub username: String,
    pub password: String,
    pub scramble: String,
    pub collation: u8,
}

impl AuthNativePasswordCommand {
    pub fn encrypted_password(&mut self) -> Result<Vec<u8>, BinlogError> {
        let encrypted_password = Self::encrypt_password_sha1(&self.password, &self.scramble)?;
        Ok(encrypted_password)
    }

    pub fn to_bytes(&mut self) -> Result<Vec<u8>, BinlogError> {
        let mut buf = Vec::new();

        let mut client_capabilities = ClientCapabilities::LONG_FLAG
            | ClientCapabilities::PROTOCOL_41
            | ClientCapabilities::SECURE_CONNECTION
            | ClientCapabilities::PLUGIN_AUTH;
        if !self.schema.is_empty() {
            client_capabilities |= ClientCapabilities::CONNECT_WITH_DB;
        }
        buf.write_u32::<LittleEndian>(client_capabilities)?;

        // maximum packet length
        buf.write_u32::<LittleEndian>(0)?;
        buf.write_u8(self.collation)?;

        // reserved bytes
        for _ in 0..23 {
            buf.write_u8(0)?;
        }

        buf.write_null_terminated_string(&self.username)?;

        // encrypted password
        let encrypted_password = Self::encrypt_password_sha1(&self.password, &self.scramble)?;
        buf.write_u8(encrypted_password.len() as u8)?;
        buf.write_all(&encrypted_password)?;

        if !self.schema.is_empty() {
            buf.write_null_terminated_string(&self.schema)?;
        }

        buf.write_null_terminated_string(AuthPlugin::MySqlNativePassword.to_str())?;
        Ok(buf)
    }

    fn encrypt_password_sha1(password: &str, scramble: &str) -> Result<Vec<u8>, BinlogError> {
        let mut hash1 = Self::hash_sha1(password.as_bytes());
        let scramble_concat_hash1 =
            [scramble.as_bytes().to_vec(), Self::hash_sha1(&hash1)].concat();
        let hash2 = Self::hash_sha1(&scramble_concat_hash1);
        Ok(hash1.xor(hash2))
    }

    fn hash_sha1(value: &[u8]) -> Vec<u8> {
        let mut hasher = Sha1::new();
        hasher.update(value);
        hasher.finalize().as_slice().to_vec()
    }
}
