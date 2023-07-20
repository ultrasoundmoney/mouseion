//! Fns to read variables from the environment more conveniently and help other functions figure
//! out what environment they're running in.

use std::env;

use tracing::{debug, warn};

const SECRET_LOG_BLACKLIST: [&str; 0] = [];

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
        Err(err) => panic!("{err}"),
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

#[cfg(test)]
/// Get an environment variable we can't run without.
pub fn get_env_var_unsafe(key: &str) -> String {
    get_env_var(key).unwrap_or_else(|| panic!("{key} should be in env"))
}

/// Some things are different between environments. Urls we contact, timeouts we use, data we have.
/// This enum is the main way to create these branches in our logic.
#[derive(Debug, PartialEq, Eq)]
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

#[cfg(test)]
pub fn get_env_bool(key: &str) -> bool {
    let flag = get_env_var(key).map_or(false, |var| var.to_lowercase() == "true");
    debug!("env flag {key}: {flag}");
    flag
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn get_env_var_unsafe_panics_test() {
        get_env_var_unsafe("DOESNT_EXIST");
    }

    #[test]
    fn get_env_var_unsafe_test() {
        let test_key = "TEST_KEY_UNSAFE";
        let test_value = "my-env-value";
        std::env::set_var(test_key, test_value);
        assert_eq!(get_env_var_unsafe(test_key), test_value);
    }

    #[test]
    fn get_env_var_safe_some_test() {
        let test_key = "TEST_KEY_SAFE_SOME";
        let test_value = "my-env-value";
        std::env::set_var(test_key, test_value);
        assert_eq!(get_env_var(test_key), Some(test_value.to_string()));
    }

    #[test]
    fn get_env_var_safe_none_test() {
        let key = get_env_var("DOESNT_EXIST");
        assert!(key.is_none());
    }

    #[test]
    fn get_env_bool_not_there_test() {
        let flag = get_env_bool("DOESNT_EXIST");
        assert!(!flag);
    }

    #[test]
    fn get_env_bool_true_test() {
        let test_key = "TEST_KEY_BOOL_TRUE";
        let test_value = "true";
        std::env::set_var(test_key, test_value);
        assert!(get_env_bool(test_key));
    }

    #[test]
    fn get_env_bool_true_upper_test() {
        let test_key = "TEST_KEY_BOOL_TRUE2";
        let test_value = "TRUE";
        std::env::set_var(test_key, test_value);
        assert!(get_env_bool(test_key));
    }

    #[test]
    fn get_env_bool_false_test() {
        let test_key = "TEST_KEY_BOOL_FALSE";
        let test_value = "false";
        std::env::set_var(test_key, test_value);
        assert!(!get_env_bool(test_key));
    }

    #[test]
    fn get_env_test() {
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

        std::env::set_var("ENV", "invalid_env");
        let result = std::panic::catch_unwind(|| get_env());
        assert!(result.is_err());
    }

    #[test]
    fn obfuscate_if_secret_test() {
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
}
