use std::collections::HashMap;

use fred::{
    prelude::{RedisError, RedisErrorKind},
    types::RedisValue,
};

pub fn into_redis_parse_err(err: impl std::fmt::Display) -> RedisError {
    RedisError::new(RedisErrorKind::Parse, err.to_string())
}

pub fn parse_string_optional(
    map: &'_ mut HashMap<String, RedisValue>,
    key: &str,
) -> Result<Option<String>, RedisError> {
    map.remove(key)
        .map(|rv| {
            rv.as_string()
                .ok_or_else(|| into_redis_parse_err(format!("failed to parse {} as string", key)))
        })
        .transpose()
}

pub fn parse_string_required(
    map: &'_ mut HashMap<String, RedisValue>,
    key: &str,
) -> Result<String, RedisError> {
    let v = map
        .remove(key)
        .ok_or_else(|| into_redis_parse_err(format!("expected {} in block submission", key)))?
        .as_string()
        .ok_or_else(|| {
            into_redis_parse_err(format!(
                "failed to parse {} as string, str: {}",
                key,
                map.get(key).unwrap().as_str().unwrap()
            ))
        })?;

    Ok(v)
}

pub fn parse_u64_optional(
    map: &'_ mut HashMap<String, RedisValue>,
    key: &str,
) -> Result<Option<u64>, RedisError> {
    map.remove(key)
        .map(|rv| {
            rv.as_u64().ok_or_else(|| {
                into_redis_parse_err(format!(
                    "failed to parse {} as u64, str: {}",
                    key,
                    rv.as_str().unwrap()
                ))
            })
        })
        .transpose()
}

pub fn parse_u64_required(
    map: &'_ mut HashMap<String, RedisValue>,
    key: &str,
) -> Result<u64, RedisError> {
    let v = map
        .remove(key)
        .ok_or_else(|| into_redis_parse_err(format!("expected {} in block submission", key)))?
        .as_u64()
        .ok_or_else(|| {
            into_redis_parse_err(format!(
                "failed to parse {} as u64, str: {}",
                key,
                map.get(key).unwrap().as_str().unwrap()
            ))
        })?;

    Ok(v)
}

pub fn parse_bool_optional(
    map: &'_ mut HashMap<String, RedisValue>,
    key: &str,
) -> Result<Option<bool>, RedisError> {
    map.remove(key)
        .map(|rv| {
            rv.as_bool().ok_or_else(|| {
                into_redis_parse_err(format!(
                    "failed to parse {} as bool, str: {}",
                    key,
                    rv.as_str().unwrap()
                ))
            })
        })
        .transpose()
}

pub fn parse_bool_required(
    map: &'_ mut HashMap<String, RedisValue>,
    key: &str,
) -> Result<bool, RedisError> {
    let v = map
        .remove(key)
        .ok_or_else(|| into_redis_parse_err(format!("expected {} in block submission", key)))?
        .as_bool()
        .ok_or_else(|| {
            into_redis_parse_err(format!(
                "failed to parse {} as bool, str: {}",
                key,
                map.get(key).unwrap().as_str().unwrap()
            ))
        })?;

    Ok(v)
}
