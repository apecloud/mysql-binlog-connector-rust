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
    pub client_capabilities: u32,
}

impl AuthNativePasswordCommand {
    /// Authentication response shared by the handshake and auth plugin switch.
    pub fn encrypted_password(&mut self) -> Result<Vec<u8>, BinlogError> {
        if self.password.is_empty() {
            return Ok(Vec::new());
        }
        let encrypted_password = Self::encrypt_password_sha1(&self.password, &self.scramble)?;
        Ok(encrypted_password)
    }

    pub fn to_bytes(&mut self) -> Result<Vec<u8>, BinlogError> {
        let mut buf = Vec::new();

        let client_capabilities = self.client_capabilities | ClientCapabilities::PLUGIN_AUTH;
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
        let encrypted_password = self.encrypted_password()?;
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

#[cfg(test)]
mod tests {
    use super::AuthNativePasswordCommand;
    use crate::constants::ClientCapabilities;

    #[test]
    fn encodes_password_responses_for_handshake_and_switch() {
        // Non-empty response cross-checked against the Java connector with this nonce.
        for (password, expected) in [
            ("", vec![]),
            (
                "example",
                vec![
                    0x33, 0x4d, 0xaf, 0x2a, 0x09, 0x6a, 0x6d, 0x91, 0x76, 0xed, 0x74, 0x9a, 0xd6,
                    0x98, 0x54, 0x65, 0xdd, 0xc9, 0xea, 0x8d,
                ],
            ),
        ] {
            let mut command = AuthNativePasswordCommand {
                schema: "test".into(),
                username: "user".into(),
                password: password.into(),
                scramble: "12345678901234567890".into(),
                collation: 45,
                client_capabilities: ClientCapabilities::SECURE_CONNECTION
                    | ClientCapabilities::CONNECT_WITH_DB,
            };
            assert_eq!(command.encrypted_password().unwrap(), expected);

            let mut expected_tail = vec![expected.len() as u8];
            expected_tail.extend_from_slice(&expected);
            expected_tail.extend_from_slice(b"test\0mysql_native_password\0");
            let packet = command.to_bytes().unwrap();
            assert_eq!(&packet[32 + b"user\0".len()..], expected_tail);
        }
    }
}
