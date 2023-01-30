use openssl::{
    pkey::Public,
    rsa::{Padding, Rsa},
};

use crate::{binlog_error::BinlogError, constants, ext::buf_ext::BufExt};

pub struct AuthSha2RsaPasswordCommand {
    pub rsa_key: Rsa<Public>,
    pub password: String,
    pub scramble: String,
}

impl AuthSha2RsaPasswordCommand {
    pub fn to_bytes(&mut self) -> Result<Vec<u8>, BinlogError> {
        let mut password_buf = self.password.as_bytes().to_vec();
        password_buf.push(constants::NULL_TERMINATOR);
        let encrypted_password = password_buf.xor(self.scramble.as_bytes().to_vec());

        let mut res = vec![0u8; self.rsa_key.size() as usize];
        self.rsa_key
            .public_encrypt(&encrypted_password, &mut res, Padding::PKCS1_OAEP)?;
        Ok(res)
    }
}
