use byteorder::{LittleEndian, WriteBytesExt};

use crate::{binlog_error::BinlogError, constants::ClientCapabilities};

pub struct SSLRequestCommand {
    pub client_capabilities: u32,
    pub collation: u8,
}

impl SSLRequestCommand {
    pub fn to_bytes(&self) -> Result<Vec<u8>, BinlogError> {
        let mut buf = Vec::new();
        let client_capabilities = self.client_capabilities | ClientCapabilities::SSL;

        buf.write_u32::<LittleEndian>(client_capabilities)?;
        buf.write_u32::<LittleEndian>(0)?;
        buf.write_u8(self.collation)?;

        for _ in 0..23 {
            buf.write_u8(0)?;
        }

        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use crate::constants::ClientCapabilities;

    use super::SSLRequestCommand;

    #[test]
    fn ssl_request_sets_ssl_capability() {
        let bytes = SSLRequestCommand {
            client_capabilities: ClientCapabilities::LONG_FLAG
                | ClientCapabilities::PROTOCOL_41
                | ClientCapabilities::SECURE_CONNECTION,
            collation: 45,
        }
        .to_bytes()
        .unwrap();

        assert_eq!(bytes.len(), 32);
        assert_eq!(
            u32::from_le_bytes(bytes[0..4].try_into().unwrap()) & ClientCapabilities::SSL,
            ClientCapabilities::SSL
        );
        assert_eq!(bytes[8], 45);
    }
}
