pub(crate) mod test {
    use std::{collections::HashMap, env};

    pub struct Env {}

    impl Env {
        const ENV_FILE: &str = "tests/.env";

        pub const DB_URL: &str = "db_url";
        pub const SERVER_ID: &str = "server_id";
        pub const DEFAULT_DB: &str = "default_db";
        pub const DEFAULT_TB: &str = "default_tb";
        pub const BINLOG_PARSE_MILLIS: &str = "binlog_parse_millis";

        pub fn load_vars() -> HashMap<String, String> {
            let env_path = env::current_dir().unwrap().join(Self::ENV_FILE);
            dotenv::from_path(env_path).unwrap();

            let mut vars = HashMap::new();
            vars.insert(Self::DB_URL.into(), env::var(Self::DB_URL).unwrap());
            vars.insert(Self::SERVER_ID.into(), env::var(Self::SERVER_ID).unwrap());
            vars.insert(Self::DEFAULT_DB.into(), env::var(Self::DEFAULT_DB).unwrap());
            vars.insert(Self::DEFAULT_TB.into(), env::var(Self::DEFAULT_TB).unwrap());
            vars.insert(
                Self::BINLOG_PARSE_MILLIS.into(),
                env::var(Self::BINLOG_PARSE_MILLIS).unwrap(),
            );
            vars
        }
    }
}
