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
    pub client_capabilities: u32,
}

impl AuthSha2PasswordCommand {
    /// Authentication response shared by the handshake and auth plugin switch.
    pub fn encrypted_password(&mut self) -> Result<Vec<u8>, BinlogError> {
        if self.password.is_empty() {
            return Ok(vec![0]);
        }
        let encrypted_password = Self::encrypt_password_sha256(&self.password, &self.scramble)?;
        Ok(encrypted_password)
    }

    pub fn to_bytes(&mut self) -> Result<Vec<u8>, BinlogError> {
        let mut buf = Vec::new();

        // Use the Protocol::HandshakeResponse41 secure-connection auth format.
        // We intentionally do not advertise PLUGIN_AUTH_LENENC_CLIENT_DATA because
        // this command writes the auth response with a 1-byte length prefix.
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

#[cfg(test)]
mod tests {
    use super::AuthSha2PasswordCommand;
    use crate::constants::ClientCapabilities;

    #[test]
    fn encodes_password_responses_for_handshake_and_switch() {
        // Non-empty response independently calculated from the SHA256 scramble formula.
        for (password, expected) in [
            ("", vec![0]),
            (
                "example",
                vec![
                    0x00, 0x39, 0xd2, 0xfa, 0xb1, 0x1d, 0xbb, 0x6c, 0x95, 0x4e, 0xb5, 0x8d, 0x0b,
                    0x35, 0x7a, 0xe6, 0xe9, 0x76, 0x1b, 0x0f, 0x1e, 0x73, 0x7a, 0xe8, 0x88, 0x5c,
                    0x4b, 0x4f, 0xde, 0x55, 0xec, 0xde,
                ],
            ),
        ] {
            let mut command = AuthSha2PasswordCommand {
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
            expected_tail.extend_from_slice(b"test\0caching_sha2_password\0");
            let packet = command.to_bytes().unwrap();
            assert_eq!(&packet[32 + b"user\0".len()..], expected_tail);
        }
    }
}
