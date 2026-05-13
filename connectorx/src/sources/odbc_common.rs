use std::collections::{BTreeSet, HashSet};

use anyhow::{anyhow, Result};
use url::Url;

pub(crate) const REPLACE_INVALID_UTF16_PARAM: &str = "replace_invalid_utf16";
pub(crate) const REPLACE_INVALID_UTF8_PARAM: &str = "replace_invalid_utf8";
pub(crate) const MAX_CONNECTIONS_PARAM: &str = "max_connections";
pub(crate) const LOGIN_TIMEOUT_SECS_PARAM: &str = "login_timeout_secs";
pub(crate) const QUERY_TIMEOUT_SECS_PARAM: &str = "query_timeout_secs";

pub(crate) fn is_raw_odbc_conn_string(conn: &str) -> bool {
    let lower = conn.trim_start().to_ascii_lowercase();
    ["driver=", "dsn=", "filedsn=", "database="]
        .iter()
        .any(|prefix| lower.starts_with(prefix))
}

#[cfg(any(feature = "src_odbc", feature = "src_db2", feature = "src_sybase"))]
pub(crate) fn is_connector_option_key(key: &str) -> bool {
    key.eq_ignore_ascii_case("cxprotocol")
        || key.eq_ignore_ascii_case(REPLACE_INVALID_UTF16_PARAM)
        || key.eq_ignore_ascii_case(REPLACE_INVALID_UTF8_PARAM)
        || key.eq_ignore_ascii_case(MAX_CONNECTIONS_PARAM)
        || key.eq_ignore_ascii_case(LOGIN_TIMEOUT_SECS_PARAM)
        || key.eq_ignore_ascii_case(QUERY_TIMEOUT_SECS_PARAM)
}

pub(crate) fn connection_query_pairs(conn: &str) -> Result<Option<Vec<(String, String)>>> {
    if is_raw_odbc_conn_string(conn) {
        return Ok(None);
    }

    let url = Url::parse(conn)?;
    Ok(Some(url_query_pairs(&url)?))
}

pub(crate) fn url_query_pairs(url: &Url) -> Result<Vec<(String, String)>> {
    let mut seen = HashSet::new();
    let mut duplicates = BTreeSet::new();
    let mut params = Vec::new();

    for (key, value) in url.query_pairs() {
        let key = key.into_owned();
        let normalized_key = key.to_ascii_lowercase();
        if !seen.insert(normalized_key.clone()) {
            duplicates.insert(normalized_key);
        }
        params.push((key, value.into_owned()));
    }

    if !duplicates.is_empty() {
        let duplicated_keys = duplicates.into_iter().collect::<Vec<_>>().join(", ");
        return Err(anyhow!(
            "duplicate ODBC URL query parameter(s): {duplicated_keys}"
        ));
    }

    Ok(params)
}

pub(crate) fn param_value<'a>(params: &'a [(String, String)], key: &str) -> Option<&'a str> {
    params
        .iter()
        .find(|(param_key, _)| param_key.eq_ignore_ascii_case(key))
        .map(|(_, value)| value.as_str())
}

pub(crate) fn param_bool_param(params: &[(String, String)], key: &str) -> Result<Option<bool>> {
    param_value(params, key)
        .map(|value| parse_bool_param(key, value))
        .transpose()
}

fn parse_bool_param(key: &str, value: &str) -> Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(anyhow!(
            "{key} must be a boolean value (true/false, 1/0, yes/no, or on/off)"
        )),
    }
}

pub(crate) fn param_usize_param(params: &[(String, String)], key: &str) -> Result<Option<usize>> {
    param_value(params, key)
        .map(|value| parse_usize_param(key, value))
        .transpose()
}

pub(crate) fn param_u32_param(params: &[(String, String)], key: &str) -> Result<Option<u32>> {
    param_value(params, key)
        .map(|value| parse_u32_param(key, value))
        .transpose()
}

fn parse_usize_param(key: &str, value: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .map_err(|_| anyhow!("{key} must be a positive integer"))
        .and_then(|value| {
            if value == 0 {
                Err(anyhow!("{key} must be at least 1"))
            } else {
                Ok(value)
            }
        })
}

fn parse_u32_param(key: &str, value: &str) -> Result<u32> {
    value
        .parse::<u32>()
        .map_err(|_| anyhow!("{key} must be a positive integer up to {}", u32::MAX))
        .and_then(|value| {
            if value == 0 {
                Err(anyhow!("{key} must be at least 1"))
            } else {
                Ok(value)
            }
        })
}

pub(crate) fn odbc_conn_value(value: &str) -> String {
    format!("{{{}}}", value.replace('}', "}}"))
}

pub(crate) fn odbc_conn_value_if_needed(value: &str) -> String {
    if value.is_empty()
        || value.trim() != value
        || value
            .bytes()
            .any(|byte| matches!(byte, b';' | b'{' | b'}' | b'=') || byte.is_ascii_whitespace())
    {
        odbc_conn_value(value)
    } else {
        value.to_string()
    }
}

#[cfg(any(feature = "src_odbc", feature = "src_db2", feature = "src_sybase"))]
pub(crate) fn is_valid_odbc_key(key: &str) -> bool {
    !key.is_empty()
        && key.trim() == key
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b' '))
}

#[cfg(feature = "src_odbc")]
pub(crate) fn push_odbc_pair(conn: &mut String, key: &str, value: &str) {
    conn.push_str(key);
    conn.push('=');
    conn.push_str(&generic_odbc_conn_value(value));
    conn.push(';');
}

#[cfg(feature = "src_odbc")]
fn generic_odbc_conn_value(value: &str) -> String {
    odbc_conn_value_if_needed(value)
}
