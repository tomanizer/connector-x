use anyhow::{anyhow, Result};
use url::Url;

pub(crate) const REPLACE_INVALID_UTF16_PARAM: &str = "replace_invalid_utf16";
pub(crate) const MAX_CONNECTIONS_PARAM: &str = "max_connections";

pub(crate) fn is_raw_odbc_conn_string(conn: &str) -> bool {
    let lower = conn.trim_start().to_ascii_lowercase();
    ["driver=", "dsn=", "filedsn=", "database="]
        .iter()
        .any(|prefix| lower.starts_with(prefix))
}

#[cfg(any(feature = "src_odbc", feature = "src_db2"))]
pub(crate) fn is_connector_option_key(key: &str) -> bool {
    key.eq_ignore_ascii_case("cxprotocol")
        || key.eq_ignore_ascii_case(REPLACE_INVALID_UTF16_PARAM)
        || key.eq_ignore_ascii_case(MAX_CONNECTIONS_PARAM)
}

pub(crate) fn connection_bool_param(conn: &str, key: &str) -> Result<Option<bool>> {
    if is_raw_odbc_conn_string(conn) {
        return Ok(None);
    }

    let url = Url::parse(conn)?;
    url_bool_param(&url, key)
}

pub(crate) fn connection_usize_param(conn: &str, key: &str) -> Result<Option<usize>> {
    if is_raw_odbc_conn_string(conn) {
        return Ok(None);
    }

    let url = Url::parse(conn)?;
    url_usize_param(&url, key)
}

pub(crate) fn url_bool_param(url: &Url, key: &str) -> Result<Option<bool>> {
    url.query_pairs()
        .find(|(param_key, _)| param_key.eq_ignore_ascii_case(key))
        .map(|(_, value)| parse_bool_param(key, &value))
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

pub(crate) fn url_usize_param(url: &Url, key: &str) -> Result<Option<usize>> {
    url.query_pairs()
        .find(|(param_key, _)| param_key.eq_ignore_ascii_case(key))
        .map(|(_, value)| parse_usize_param(key, &value))
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

pub(crate) fn odbc_conn_value(value: &str) -> String {
    format!("{{{}}}", value.replace('}', "}}"))
}

#[cfg(any(feature = "src_odbc", feature = "src_db2"))]
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
