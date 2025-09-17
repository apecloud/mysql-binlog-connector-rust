use std::io::{Cursor, Read};

use byteorder::ReadBytesExt;
use serde::{Deserialize, Serialize};

use crate::{
    binlog_error::BinlogError, column::column_type::ColumnType, ext::cursor_ext::CursorExt,
};

use super::{default_charset::DefaultCharset, metadata_type::MetadataType};

/// Contains metadata for a single table column.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ColumnMetadata {
    /// Column name.
    pub column_name: Option<String>,

    /// Signedness of numeric column.
    pub is_signed: Option<bool>,

    /// Charset collation for character column.
    pub charset_collation: Option<u32>,

    /// String values for ENUM column.
    pub enum_string_values: Option<Vec<String>>,

    /// String values for SET column.
    pub set_string_values: Option<Vec<String>>,

    /// Real type for geometry column.
    pub geometry_type: Option<u32>,

    /// Whether this column is a simple primary key.
    pub is_simple_primary_key: Option<bool>,

    /// Primary key prefix length if this is a prefixed primary key.
    pub primary_key_prefix: Option<u32>,

    /// Charset collation for ENUM/SET column.
    pub enum_and_set_charset_collation: Option<u32>,

    /// Column visibility (for MySQL 8.0+ invisible columns).
    pub is_visible: Option<bool>,
}

/// Contains metadata for table columns.
/// Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TableMetadata {
    /// Default charset for character columns.
    pub default_charset: Option<DefaultCharset>,

    /// Default charset for ENUM and SET columns.
    pub enum_and_set_default_charset: Option<DefaultCharset>,

    /// Per-column metadata.
    pub columns: Vec<ColumnMetadata>,
}

impl ColumnMetadata {
    pub fn new() -> Self {
        Self {
            column_name: None,
            is_signed: None,
            charset_collation: None,
            enum_string_values: None,
            set_string_values: None,
            geometry_type: None,
            is_simple_primary_key: None,
            primary_key_prefix: None,
            enum_and_set_charset_collation: None,
            is_visible: None,
        }
    }
}

impl TableMetadata {
    pub fn parse(
        cursor: &mut Cursor<&Vec<u8>>,
        column_types: &[u8],
        column_metas: &[u16],
    ) -> Result<Self, BinlogError> {
        // Initialize columns with default values
        let mut columns: Vec<ColumnMetadata> = (0..column_types.len())
            .map(|_| ColumnMetadata::new())
            .collect();

        let mut default_charset = None;
        let mut enum_and_set_default_charset = None;

        while cursor.available() > 0 {
            let metadata_type = MetadataType::from_code(cursor.read_u8()?)?;
            let metadata_length = cursor.read_packed_number()?;

            let mut metadata = vec![0u8; metadata_length];
            cursor.read_exact(&mut metadata)?;

            let mut buffer = Cursor::new(&metadata);
            match metadata_type {
                MetadataType::Signedness => {
                    let signedness_values = read_signedness_bitmap(&mut buffer, column_types)?;
                    apply_signedness_to_columns(&mut columns, column_types, &signedness_values);
                }
                MetadataType::DefaultCharset => {
                    default_charset = Some(parse_default_charset(&mut buffer)?);
                }
                MetadataType::ColumnCharset => {
                    parse_column_charsets(&mut columns, &mut buffer)?;
                }
                MetadataType::ColumnName => {
                    parse_column_names(&mut columns, &mut buffer)?;
                }
                MetadataType::SetStrValue => {
                    parse_set_string_values(&mut columns, &mut buffer, column_types, column_metas)?;
                }
                MetadataType::EnumStrValue => {
                    parse_enum_string_values(
                        &mut columns,
                        &mut buffer,
                        column_types,
                        column_metas,
                    )?;
                }
                MetadataType::GeometryType => {
                    parse_geometry_types(&mut columns, &mut buffer)?;
                }
                MetadataType::SimplePrimaryKey => {
                    parse_simple_primary_keys(&mut columns, &mut buffer)?;
                }
                MetadataType::PrimaryKeyWithPrefix => {
                    parse_primary_key_prefixes(&mut columns, &mut buffer)?;
                }
                MetadataType::EnumAndSetDefaultCharset => {
                    enum_and_set_default_charset = Some(parse_default_charset(&mut buffer)?);
                }
                MetadataType::EnumAndSetColumnCharset => {
                    parse_enum_set_charsets(&mut columns, &mut buffer)?;
                }
                MetadataType::ColumnVisibility => {
                    let visibility = read_bitmap_reverted(&mut buffer, column_types.len())?;
                    apply_column_visibility(&mut columns, &visibility);
                }
            }
        }

        Ok(Self {
            default_charset,
            enum_and_set_default_charset,
            columns,
        })
    }
}

fn parse_default_charset(cursor: &mut Cursor<&Vec<u8>>) -> Result<DefaultCharset, BinlogError> {
    let default_collation = cursor.read_packed_number()?;
    let mut charset_collations = Vec::new();
    while cursor.available() > 0 {
        let key = cursor.read_packed_number()? as u32;
        let value = cursor.read_packed_number()? as u32;
        charset_collations.push((key, value));
    }
    Ok(DefaultCharset::new(
        default_collation as u32,
        charset_collations,
    ))
}

pub(crate) fn read_bitmap_reverted(
    cursor: &mut Cursor<&Vec<u8>>,
    bits_number: usize,
) -> Result<Vec<bool>, BinlogError> {
    let mut result = vec![false; bits_number];
    let bytes_number = (bits_number + 7) / 8;
    for i in 0..bytes_number {
        let value = cursor.read_u8()?;
        for y in 0..8 {
            let index = (i << 3) + y;
            if index == bits_number {
                break;
            }

            // The difference from read_bits is that bits are reverted
            result[index] = (value & (1 << (7 - y))) > 0;
        }
    }
    Ok(result)
}

// Helper functions for applying metadata to columns

fn read_signedness_bitmap(
    cursor: &mut Cursor<&Vec<u8>>,
    column_types: &[u8],
) -> Result<Vec<bool>, BinlogError> {
    let count = get_numeric_column_count(column_types)?;
    read_bitmap_reverted(cursor, count)
}

fn apply_signedness_to_columns(
    columns: &mut [ColumnMetadata],
    column_types: &[u8],
    signedness_values: &[bool],
) {
    let mut signedness_index = 0;
    for (i, &column_type_code) in column_types.iter().enumerate() {
        let column_type = ColumnType::from_code(column_type_code);
        if is_numeric_type(column_type) {
            if signedness_index < signedness_values.len() {
                columns[i].is_signed = Some(signedness_values[signedness_index]);
                signedness_index += 1;
            }
        }
    }
}

fn parse_column_charsets(
    columns: &mut [ColumnMetadata],
    cursor: &mut Cursor<&Vec<u8>>,
) -> Result<(), BinlogError> {
    let mut index = 0;
    while cursor.available() > 0 && index < columns.len() {
        let charset = cursor.read_packed_number()? as u32;
        columns[index].charset_collation = Some(charset);
        index += 1;
    }
    Ok(())
}

fn parse_column_names(
    columns: &mut [ColumnMetadata],
    cursor: &mut Cursor<&Vec<u8>>,
) -> Result<(), BinlogError> {
    let mut index = 0;
    while cursor.available() > 0 && index < columns.len() {
        let length = cursor.read_packed_number()?;
        let name = cursor.read_string(length)?;
        columns[index].column_name = Some(name);
        index += 1;
    }
    Ok(())
}

fn parse_set_string_values(
    columns: &mut [ColumnMetadata],
    cursor: &mut Cursor<&Vec<u8>>,
    column_types: &[u8],
    column_metas: &[u16],
) -> Result<(), BinlogError> {
    let mut set_column_index = 0;

    while cursor.available() > 0 {
        let length = cursor.read_packed_number()?;
        let mut values = Vec::new();
        for _ in 0..length {
            let str_length = cursor.read_packed_number()?;
            let value = cursor.read_string(str_length)?;
            values.push(value);
        }

        // Find the set_column_index-th SET column and apply values to it
        // SetStrValue metadata is provided in the order of columns that are actual SETs
        let mut current_set_index = 0;
        for i in 0..column_types.len() {
            if is_set_column(column_types[i], column_metas[i]) {
                if current_set_index == set_column_index {
                    columns[i].set_string_values = Some(values);
                    break;
                }
                current_set_index += 1;
            }
        }
        set_column_index += 1;
    }
    Ok(())
}

fn parse_enum_string_values(
    columns: &mut [ColumnMetadata],
    cursor: &mut Cursor<&Vec<u8>>,
    column_types: &[u8],
    column_metas: &[u16],
) -> Result<(), BinlogError> {
    let mut enum_column_index = 0;

    while cursor.available() > 0 {
        let length = cursor.read_packed_number()?;
        let mut values = Vec::new();
        for _ in 0..length {
            let str_length = cursor.read_packed_number()?;
            let value = cursor.read_string(str_length)?;
            values.push(value);
        }

        // Find the enum_column_index-th ENUM column and apply values to it
        // EnumStrValue metadata is provided in the order of columns that are actual ENUMs
        let mut current_enum_index = 0;
        for i in 0..column_types.len() {
            if is_enum_column(column_types[i], column_metas[i]) {
                if current_enum_index == enum_column_index {
                    columns[i].enum_string_values = Some(values);
                    break;
                }
                current_enum_index += 1;
            }
        }
        enum_column_index += 1;
    }
    Ok(())
}

fn parse_geometry_types(
    columns: &mut [ColumnMetadata],
    cursor: &mut Cursor<&Vec<u8>>,
) -> Result<(), BinlogError> {
    let mut index = 0;
    while cursor.available() > 0 && index < columns.len() {
        let geometry_type = cursor.read_packed_number()? as u32;
        columns[index].geometry_type = Some(geometry_type);
        index += 1;
    }
    Ok(())
}

fn parse_simple_primary_keys(
    columns: &mut [ColumnMetadata],
    cursor: &mut Cursor<&Vec<u8>>,
) -> Result<(), BinlogError> {
    while cursor.available() > 0 {
        let pk_column = cursor.read_packed_number()?;
        if let Some(column) = columns.get_mut(pk_column) {
            column.is_simple_primary_key = Some(true);
        }
    }
    Ok(())
}

fn parse_primary_key_prefixes(
    columns: &mut [ColumnMetadata],
    cursor: &mut Cursor<&Vec<u8>>,
) -> Result<(), BinlogError> {
    while cursor.available() > 0 {
        let column_index = cursor.read_packed_number()?;
        let prefix_length = cursor.read_packed_number()? as u32;
        if let Some(column) = columns.get_mut(column_index) {
            column.primary_key_prefix = Some(prefix_length);
        }
    }
    Ok(())
}

fn parse_enum_set_charsets(
    columns: &mut [ColumnMetadata],
    cursor: &mut Cursor<&Vec<u8>>,
) -> Result<(), BinlogError> {
    let mut index = 0;
    while cursor.available() > 0 && index < columns.len() {
        let charset = cursor.read_packed_number()? as u32;
        columns[index].enum_and_set_charset_collation = Some(charset);
        index += 1;
    }
    Ok(())
}

fn apply_column_visibility(columns: &mut [ColumnMetadata], visibility: &[bool]) {
    for (i, &is_visible) in visibility.iter().enumerate().take(columns.len()) {
        columns[i].is_visible = Some(is_visible);
    }
}

fn is_numeric_type(column_type: ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::Tiny
            | ColumnType::Short
            | ColumnType::Int24
            | ColumnType::Long
            | ColumnType::LongLong
            | ColumnType::Float
            | ColumnType::Double
            | ColumnType::NewDecimal
    )
}

fn is_enum_column(column_type_code: u8, column_meta: u16) -> bool {
    if column_type_code == ColumnType::String as u8 {
        if let Ok((real_column_type, _)) =
            ColumnType::parse_string_column_meta(column_meta, column_type_code)
        {
            return real_column_type == ColumnType::Enum as u8;
        }
    }
    false
}

fn is_set_column(column_type_code: u8, column_meta: u16) -> bool {
    if column_type_code == ColumnType::String as u8 {
        if let Ok((real_column_type, _)) =
            ColumnType::parse_string_column_meta(column_meta, column_type_code)
        {
            return real_column_type == ColumnType::Set as u8;
        }
    }
    false
}

pub(crate) fn get_numeric_column_count(column_types: &[u8]) -> Result<usize, BinlogError> {
    let mut count = 0;
    for &column_type_code in column_types {
        let column_type = ColumnType::from_code(column_type_code);
        if is_numeric_type(column_type) {
            count += 1;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn create_test_column_types() -> Vec<u8> {
        vec![
            1,   // MYSQL_TYPE_TINY (numeric, index 0)
            3,   // MYSQL_TYPE_LONG (numeric, index 1)
            4,   // MYSQL_TYPE_FLOAT (numeric, index 2)
            5,   // MYSQL_TYPE_DOUBLE (numeric, index 3)
            246, // MYSQL_TYPE_NEWDECIMAL (numeric, index 4)
            253, // MYSQL_TYPE_VAR_STRING (non-numeric, index 5)
            254, // MYSQL_TYPE_STRING (non-numeric, index 6)
        ]
    }

    fn create_test_column_metas() -> Vec<u16> {
        vec![
            0, // TINY - no metadata
            0, // LONG - no metadata
            0, // FLOAT - no metadata
            0, // DOUBLE - no metadata
            0, // NEWDECIMAL - no metadata
            0, // VAR_STRING - no metadata
            0, // STRING - no metadata
        ]
    }

    #[test]
    fn test_get_numeric_column_count() {
        let column_types = create_test_column_types();
        let count = get_numeric_column_count(&column_types).unwrap();
        // Should count TINY, LONG, FLOAT, DOUBLE, NEWDECIMAL = 5 numeric columns
        assert_eq!(count, 5);
    }

    #[test]
    fn test_parse_signedness_metadata() {
        // Create test data for signedness metadata
        let test_data = vec![
            1,          // MetadataType::Signedness
            1,          // Length of metadata (1 byte)
            0b11010000, // Signedness bitmap: bits 7,6,4 are set (reverted), meaning numeric columns 0,1,3 are signed
        ];

        let column_types = create_test_column_types();
        let column_metas = create_test_column_metas();
        let mut cursor = Cursor::new(&test_data);
        let result = TableMetadata::parse(&mut cursor, &column_types, &column_metas).unwrap();

        assert_eq!(result.columns.len(), 7); // Total columns

        // Check signedness for numeric columns only
        assert_eq!(result.columns[0].is_signed, Some(true)); // TINY - bit 7 -> signed
        assert_eq!(result.columns[1].is_signed, Some(true)); // LONG - bit 6 -> signed
        assert_eq!(result.columns[2].is_signed, Some(false)); // FLOAT - bit 5 -> unsigned
        assert_eq!(result.columns[3].is_signed, Some(true)); // DOUBLE - bit 4 -> signed
        assert_eq!(result.columns[4].is_signed, Some(false)); // NEWDECIMAL - bit 3 -> unsigned

        // Non-numeric columns should not have signedness
        assert_eq!(result.columns[5].is_signed, None); // VAR_STRING
        assert_eq!(result.columns[6].is_signed, None); // STRING
    }

    #[test]
    fn test_parse_column_names_metadata() {
        // Create test data for column names metadata
        let test_data = vec![
            4, // MetadataType::ColumnName
            3, // Length of metadata (1 byte length + 2 bytes content)
            2, b'i', b'd', // First column name: "id"
        ];

        let column_types = create_test_column_types();
        let column_metas = create_test_column_metas();
        let mut cursor = Cursor::new(&test_data);
        let result = TableMetadata::parse(&mut cursor, &column_types, &column_metas).unwrap();

        assert_eq!(result.columns.len(), 7);

        // Only first column should have a name
        assert_eq!(result.columns[0].column_name, Some("id".to_string()));

        // Other columns should not have names
        for i in 1..7 {
            assert_eq!(result.columns[i].column_name, None);
        }
    }

    #[test]
    fn test_parse_default_charset_metadata() {
        // Create test data for default charset metadata
        let test_data = vec![
            2,  // MetadataType::DefaultCharset
            1,  // Length of metadata (just default collation)
            33, // Default collation (utf8_general_ci = 33)
        ];

        let column_types = create_test_column_types();
        let column_metas = create_test_column_metas();
        let mut cursor = Cursor::new(&test_data);
        let result = TableMetadata::parse(&mut cursor, &column_types, &column_metas).unwrap();

        assert!(result.default_charset.is_some());
        let default_charset = result.default_charset.unwrap();
        assert_eq!(default_charset.default_charset_collation, 33);
        assert_eq!(default_charset.charset_collations.len(), 0);
    }

    #[test]
    fn test_parse_enum_string_values_metadata() {
        // Create test data for ENUM string values
        let test_data = vec![
            6, // MetadataType::EnumStrValue
            7, // Length of metadata (1 + 1 + 5 = 7)
            1, // Number of values for first enum
            5, b's', b'm', b'a', b'l', b'l', // "small"
        ];

        // Create column types with an actual ENUM column (String type with ENUM metadata)
        let column_types = vec![
            1,   // MYSQL_TYPE_TINY (numeric, index 0)
            254, // MYSQL_TYPE_STRING - this will be decoded as ENUM (index 1)
            3,   // MYSQL_TYPE_LONG (numeric, index 2)
        ];

        // Create column metadata with ENUM encoding for the String column
        // For ENUM: high byte = 247 (ColumnType::Enum), low byte = number of values
        let column_metas = vec![
            0,              // TINY - no metadata
            (247 << 8) | 1, // STRING with ENUM metadata (247 = ENUM type, 1 value)
            0,              // LONG - no metadata
        ];

        let mut cursor = Cursor::new(&test_data);
        let result = TableMetadata::parse(&mut cursor, &column_types, &column_metas).unwrap();

        assert_eq!(result.columns.len(), 3);

        // First column (TINY) should not have enum values
        assert!(result.columns[0].enum_string_values.is_none());

        // Second column (ENUM) should have enum values
        assert!(result.columns[1].enum_string_values.is_some());
        let enum_values = result.columns[1].enum_string_values.as_ref().unwrap();
        assert_eq!(enum_values.len(), 1);
        assert_eq!(enum_values[0], "small");

        // Third column (LONG) should not have enum values
        assert!(result.columns[2].enum_string_values.is_none());
    }

    #[test]
    fn test_parse_multiple_metadata_types() {
        // Create test data with multiple metadata types
        let test_data = vec![
            // Signedness metadata
            1,          // MetadataType::Signedness
            1,          // Length
            0b10100000, // Bitmap (bit 7 and 5 set)
            // Column names metadata
            4, // MetadataType::ColumnName
            3, // Length (1 byte length + 2 bytes content)
            2, b'i', b'd', // "id"
        ];

        let column_types = create_test_column_types();
        let column_metas = create_test_column_metas();
        let mut cursor = Cursor::new(&test_data);
        let result = TableMetadata::parse(&mut cursor, &column_types, &column_metas).unwrap();

        assert_eq!(result.columns.len(), 7);

        // Check signedness was applied
        assert_eq!(result.columns[0].is_signed, Some(true)); // TINY - bit 7 set
        assert_eq!(result.columns[1].is_signed, Some(false)); // LONG - bit 6 not set
        assert_eq!(result.columns[2].is_signed, Some(true)); // FLOAT - bit 5 set

        // Check column name was applied
        assert_eq!(result.columns[0].column_name, Some("id".to_string()));
        assert_eq!(result.columns[1].column_name, None);
    }

    #[test]
    fn test_parse_empty_metadata() {
        let test_data = vec![];
        let column_types = create_test_column_types();
        let column_metas = create_test_column_metas();
        let mut cursor = Cursor::new(&test_data);
        let result = TableMetadata::parse(&mut cursor, &column_types, &column_metas).unwrap();

        // Should have all columns initialized but with no metadata
        assert_eq!(result.columns.len(), 7);
        assert!(result.default_charset.is_none());
        assert!(result.enum_and_set_default_charset.is_none());

        // All column metadata should be None
        for column in &result.columns {
            assert_eq!(column.column_name, None);
            assert_eq!(column.is_signed, None);
            assert_eq!(column.charset_collation, None);
            assert_eq!(column.enum_string_values, None);
            assert_eq!(column.set_string_values, None);
            assert_eq!(column.geometry_type, None);
            assert_eq!(column.is_simple_primary_key, None);
            assert_eq!(column.primary_key_prefix, None);
            assert_eq!(column.enum_and_set_charset_collation, None);
            assert_eq!(column.is_visible, None);
        }
    }

    #[test]
    fn test_read_bitmap_reverted() {
        // Test the bitmap reading with reverted bits
        let test_data = vec![0b11010000]; // Binary: 11010000
        let mut cursor = Cursor::new(&test_data);
        let result = read_bitmap_reverted(&mut cursor, 5).unwrap();

        assert_eq!(result.len(), 5);
        assert_eq!(result[0], true); // bit 7
        assert_eq!(result[1], true); // bit 6
        assert_eq!(result[2], false); // bit 5
        assert_eq!(result[3], true); // bit 4
        assert_eq!(result[4], false); // bit 3
    }

    #[test]
    fn test_column_metadata_creation() {
        let column = ColumnMetadata::new();
        assert_eq!(column.column_name, None);
        assert_eq!(column.is_signed, None);
        assert_eq!(column.charset_collation, None);
        assert_eq!(column.enum_string_values, None);
        assert_eq!(column.set_string_values, None);
        assert_eq!(column.geometry_type, None);
        assert_eq!(column.is_simple_primary_key, None);
        assert_eq!(column.primary_key_prefix, None);
        assert_eq!(column.enum_and_set_charset_collation, None);
        assert_eq!(column.is_visible, None);
    }
}
