use crate::{binlog_error::BinlogError, constants, ext::buf_ext::BufExt};

pub struct AuthSha2RsaPasswordCommand {
    pub rsa_res: Vec<u8>,
    pub password: String,
    pub scramble: String,
}

impl AuthSha2RsaPasswordCommand {
    pub fn to_bytes(&mut self) -> Result<Vec<u8>, BinlogError> {
        let mut password_buf = self.password.as_bytes().to_vec();
        password_buf.push(constants::NULL_TERMINATOR);
        let encrypted_password = password_buf.xor(self.scramble.as_bytes().to_vec());

        Ok(mysql_common::crypto::encrypt(
            &encrypted_password,
            self.rsa_res.as_slice(),
        ))
    }
}
