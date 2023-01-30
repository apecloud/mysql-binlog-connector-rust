use core::str;

#[derive(PartialEq, Clone)]
pub enum AuthPlugin {
    Unsupported,
    MySqlNativePassword,
    CachingSha2Password,
}

impl AuthPlugin {
    pub fn to_str(&self) -> &str {
        match self {
            AuthPlugin::MySqlNativePassword => "mysql_native_password",
            AuthPlugin::CachingSha2Password => "caching_sha2_password",
            _ => "unsupported",
        }
    }

    pub fn from_name(name: &str) -> Self {
        match name.to_lowercase().as_str() {
            "mysql_native_password" => AuthPlugin::MySqlNativePassword,
            "caching_sha2_password" => AuthPlugin::CachingSha2Password,
            _ => AuthPlugin::Unsupported,
        }
    }
}
