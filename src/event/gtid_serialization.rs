use std::io::{Cursor, Read};

use byteorder::ReadBytesExt;

use crate::binlog_error::BinlogError;

// MySQL serialization varints, not the connection protocol's packed numbers.
// https://github.com/mysql/mysql-server/blob/mysql-8.4.0/libs/mysql/serialization/variable_length_integers.h
pub(super) fn read_unsigned(cursor: &mut Cursor<impl AsRef<[u8]>>) -> Result<u64, BinlogError> {
    let first = cursor.read_u8()?;
    let extra = first.trailing_ones() as usize;
    let mut bytes = [0; 8];
    cursor.read_exact(&mut bytes[..extra])?;
    let high = u64::from_le_bytes(bytes);
    Ok(if extra == 8 {
        high
    } else {
        (u64::from(first) >> (extra + 1)) | (high << (7 - extra))
    })
}

pub(super) fn read_signed(cursor: &mut Cursor<impl AsRef<[u8]>>) -> Result<i64, BinlogError> {
    // Signed values use MySQL's zigzag mapping over the unsigned varint:
    // https://github.com/mysql/mysql-server/blob/mysql-8.4.0/libs/mysql/serialization/variable_length_integers.h#L174-L188
    let value = read_unsigned(cursor)?;
    Ok((value >> 1) as i64 ^ -((value & 1) as i64))
}

pub(super) fn read_tag(cursor: &mut Cursor<impl AsRef<[u8]>>) -> Result<String, BinlogError> {
    // Bounded strings contain a serialized byte length followed by string bytes:
    // https://github.com/mysql/mysql-server/blob/mysql-8.4.0/libs/mysql/serialization/primitive_type_codec.h#L159-L175
    let length = read_unsigned(cursor)?;
    if length > 32 {
        return Err(BinlogError::UnexpectedData(
            "GTID tag exceeds 32 bytes".into(),
        ));
    }
    let mut bytes = vec![0; length as usize];
    cursor.read_exact(&mut bytes)?;
    Ok(String::from_utf8(bytes)?)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_serialization_integer_boundaries() {
        for (bytes, expected) in [
            (vec![0], 0),
            (vec![0xfe], 127),
            (vec![1, 2], 128),
            (vec![0xfd, 0xff], 16383),
            (vec![3, 0, 2], 16384),
            (
                vec![0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
                (1 << 56) - 1,
            ),
            (vec![0xff, 0, 0, 0, 0, 0, 0, 0, 1], 1 << 56),
            (vec![0xff; 9], u64::MAX),
        ] {
            assert_eq!(read_unsigned(&mut Cursor::new(&bytes)).unwrap(), expected);
            for end in 0..bytes.len() {
                assert!(read_unsigned(&mut Cursor::new(&bytes[..end])).is_err());
            }
        }
        assert_eq!(read_signed(&mut Cursor::new([2])).unwrap(), -1);
        assert_eq!(read_signed(&mut Cursor::new([4])).unwrap(), 1);
        assert_eq!(read_signed(&mut Cursor::new([0xff; 9])).unwrap(), i64::MIN);
    }
}
