use serde::{Deserialize, Serialize};

/// Represents charsets of character columns.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DefaultCharset {
    /// Gets the most used charset collation.
    pub default_charset_collation: u32,

    /// Gets ColumnIndex-Charset map for columns that don't use the default charset.
    pub charset_collations: Vec<(u32, u32)>,
}

impl DefaultCharset {
    pub fn new(default_charset_collation: u32, charset_collations: Vec<(u32, u32)>) -> Self {
        Self {
            default_charset_collation,
            charset_collations,
        }
    }
}
