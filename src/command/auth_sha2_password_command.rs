use std::io::Write;

use byteorder::{LittleEndian, WriteBytesExt};
use sha1::Digest;
use sha2::Sha256;

use crate::{binlog_error::BinlogError, constants::ClientCapabilities, ext::buf_ext::BufExt};

use super::auth_plugin::AuthPlugin;

pub struct AuthSha2PasswordCommand {
    pub schema: String,
    pub username: String,
    pub password: String,
    pub scramble: String,
    pub collation: u8,
}

impl AuthSha2PasswordCommand {
    pub fn encrypted_password(&mut self) -> Result<Vec<u8>, BinlogError> {
        let encrypted_password = Self::encrypt_password_sha256(&self.password, &self.scramble)?;
        Ok(encrypted_password)
    }

    pub fn to_bytes(&mut self) -> Result<Vec<u8>, BinlogError> {
        let mut buf = Vec::new();

        let mut client_capabilities = ClientCapabilities::LONG_FLAG
            | ClientCapabilities::PROTOCOL_41
            | ClientCapabilities::SECURE_CONNECTION
            | ClientCapabilities::PLUGIN_AUTH
            | ClientCapabilities::PLUGIN_AUTH_LENENC_CLIENT_DATA;
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
        let encrypted_password = Self::encrypt_password_sha256(&self.password, &self.scramble)?;
        buf.write_u8(encrypted_password.len() as u8)?;
        buf.write_all(&encrypted_password)?;

        if !self.schema.is_empty() {
            buf.write_null_terminated_string(&self.schema)?;
        }

        buf.write_null_terminated_string(AuthPlugin::CachingSha2Password.to_str())?;
        Ok(buf)
    }

    pub fn encrypt_password_sha256(password: &str, scramble: &str) -> Result<Vec<u8>, BinlogError> {
        let mut hash1 = Self::hash_sha256(password.as_bytes());
        let hash2 = Self::hash_sha256(&hash1[0..]);

        let mut hasher = Sha256::new();
        hasher.update(hash2);
        hasher.update(scramble.as_bytes());
        let hash3 = hasher.finalize().as_slice().to_vec();

        Ok(hash1.xor(hash3))
    }

    fn hash_sha256(value: &[u8]) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(value);
        hasher.finalize().as_slice().to_vec()
    }
}
