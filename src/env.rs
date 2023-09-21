//! Fns to read variables from the environment more conveniently and help other functions figure
//! out what environment they're running in.

use std::env;

use lazy_static::lazy_static;
use tracing::{debug, warn};

const SECRET_LOG_BLACKLIST: [&str; 1] = ["S3_SECRET_ACCESS_KEY"];

lazy_static! {
    pub static ref ENV_CONFIG: EnvConfig = get_env_config();
}

fn obfuscate_if_secret(blacklist: &[&str], key: &str, value: &str) -> String {
    if blacklist.contains(&key) {
        let mut last_four = value.to_string();
        last_four.drain(0..value.len().saturating_sub(4));
        format!("****{last_four}")
    } else {
        value.to_string()
    }
}

/// Get an environment variable, encoding found or missing as Option, and panic otherwise.
pub fn get_env_var(key: &str) -> Option<String> {
    let var = match env::var(key) {
        Err(env::VarError::NotPresent) => None,
        Err(e) => panic!("{e}"),
        Ok(var) => Some(var),
    };

    if let Some(ref existing_var) = var {
        let output = obfuscate_if_secret(&SECRET_LOG_BLACKLIST, key, existing_var);
        debug!("env var {key}: {output}");
    } else {
        debug!("env var {key} requested but not found")
    };

    var
}

/// Get an environment variable we can't run without.
pub fn get_env_var_unsafe(key: &str) -> String {
    get_env_var(key).unwrap_or_else(|| panic!("{key} should be in env"))
}

/// Some things do not need to be configurable explicitly but do differ between environments. This
/// enum helps create those distinctions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Env {
    Dev,
    Prod,
    Stag,
}

pub fn get_env() -> Env {
    let env_str = get_env_var("ENV");
    match env_str {
        None => {
            warn!("no ENV in env, assuming Dev");
            Env::Dev
        }
        Some(str) => match str.as_ref() {
            "dev" => Env::Dev,
            "development" => Env::Dev,
            "stag" => Env::Stag,
            "staging" => Env::Stag,
            "prod" => Env::Prod,
            "production" => Env::Prod,
            _ => {
                panic!("ENV present: {str}, but not one of dev, stag, prod, panicking!")
            }
        },
    }
}

pub fn get_env_bool(key: &str) -> bool {
    get_env_var(key).map_or(false, |var| var.to_lowercase() == "true")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Network {
    Mainnet,
    Goerli,
}

pub fn get_network() -> Network {
    let network_str = get_env_var("NETWORK");
    match network_str {
        None => {
            warn!("no NETWORK in env, assuming Mainnet");
            Network::Mainnet
        }
        Some(str) => match str.to_lowercase().as_ref() {
            "mainnet" => Network::Mainnet,
            "goerli" => Network::Goerli,
            _ => panic!("NETWORK present: {str}, but not one of [mainnet, goerli], panicking!"),
        },
    }
}

#[derive(Debug, Clone)]
pub struct EnvConfig {
    pub env: Env,
    pub log_perf: bool,
    pub network: Network,
    pub pod_name: Option<String>,
    pub redis_uri: String,
    pub s3_bucket: String,
    pub use_local_store: bool,
}

fn get_env_config() -> EnvConfig {
    EnvConfig {
        env: get_env(),
        log_perf: get_env_bool("LOG_PERF"),
        network: get_network(),
        pod_name: get_env_var("POD_NAME"),
        redis_uri: get_env_var_unsafe("REDIS_URI"),
        s3_bucket: get_env_var("S3_BUCKET").unwrap_or("block-submission-archive-dev".to_string()),
        use_local_store: get_env_bool("USE_LOCAL_STORE"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn test_get_env_var_unsafe_panics() {
        get_env_var_unsafe("DOESNT_EXIST");
    }

    #[test]
    fn test_get_env_var_unsafe() {
        let test_key = "TEST_KEY_UNSAFE";
        let test_value = "my-env-value";
        std::env::set_var(test_key, test_value);
        assert_eq!(get_env_var_unsafe(test_key), test_value);
    }

    #[test]
    fn test_get_env_var_safe_some() {
        let test_key = "TEST_KEY_SAFE_SOME";
        let test_value = "my-env-value";
        std::env::set_var(test_key, test_value);
        assert_eq!(get_env_var(test_key), Some(test_value.to_string()));
    }

    #[test]
    fn test_get_env_var_safe_none() {
        let key = get_env_var("DOESNT_EXIST");
        assert!(key.is_none());
    }

    #[test]
    fn test_get_env_bool_not_there() {
        let flag = get_env_bool("DOESNT_EXIST");
        assert!(!flag);
    }

    #[test]
    fn test_get_env_bool_true() {
        let test_key = "TEST_KEY_BOOL_TRUE";
        let test_value = "true";
        std::env::set_var(test_key, test_value);
        assert!(get_env_bool(test_key));
    }

    #[test]
    fn test_get_env_bool_true_upper() {
        let test_key = "TEST_KEY_BOOL_TRUE2";
        let test_value = "TRUE";
        std::env::set_var(test_key, test_value);
        assert!(get_env_bool(test_key));
    }

    #[test]
    fn test_get_env_bool_false() {
        let test_key = "TEST_KEY_BOOL_FALSE";
        let test_value = "false";
        std::env::set_var(test_key, test_value);
        assert!(!get_env_bool(test_key));
    }

    #[test]
    fn test_get_env() {
        std::env::set_var("ENV", "dev");
        assert_eq!(get_env(), Env::Dev);

        std::env::set_var("ENV", "development");
        assert_eq!(get_env(), Env::Dev);

        std::env::set_var("ENV", "stag");
        assert_eq!(get_env(), Env::Stag);

        std::env::set_var("ENV", "staging");
        assert_eq!(get_env(), Env::Stag);

        std::env::set_var("ENV", "prod");
        assert_eq!(get_env(), Env::Prod);

        std::env::set_var("ENV", "production");
        assert_eq!(get_env(), Env::Prod);

        std::env::remove_var("ENV");
        assert_eq!(get_env(), Env::Dev);
    }

    #[test]
    #[should_panic]
    #[ignore = "this test breaks ENV for parallel tests"]
    fn test_get_env_panics() {
        std::env::set_var("ENV", "invalid_env");
        get_env();
    }

    #[test]
    fn test_obfuscate_if_secret() {
        let secret_key = "SECRET_KEY";
        let blacklist = vec![secret_key];
        assert_eq!(
            obfuscate_if_secret(&blacklist, secret_key, "my_secret_value"),
            "****alue"
        );

        let normal_key = "NORMAL_KEY";
        assert_eq!(
            obfuscate_if_secret(&blacklist, normal_key, "my_normal_value"),
            "my_normal_value"
        );
    }

    #[test]
    fn test_get_network() {
        std::env::set_var("NETWORK", "mainnet");
        assert_eq!(get_network(), Network::Mainnet);

        std::env::set_var("NETWORK", "goerli");
        assert_eq!(get_network(), Network::Goerli);

        std::env::set_var("NETWORK", "Mainnet");
        assert_eq!(get_network(), Network::Mainnet);

        std::env::set_var("NETWORK", "Goerli");
        assert_eq!(get_network(), Network::Goerli);

        std::env::remove_var("NETWORK");
        assert_eq!(get_network(), Network::Mainnet);
    }

    #[test]
    #[ignore = "this test breaks NETWORK for parallel tests"]
    fn test_get_network_panics() {
        std::env::set_var("NETWORK", "invalid_network");
        get_network();
    }
}
