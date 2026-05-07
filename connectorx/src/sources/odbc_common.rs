pub(crate) fn is_raw_odbc_conn_string(conn: &str) -> bool {
    let lower = conn.trim_start().to_ascii_lowercase();
    ["driver=", "dsn=", "filedsn=", "database="]
        .iter()
        .any(|prefix| lower.starts_with(prefix))
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
