//! Source implementation for generic ODBC.

mod errors;
mod typesystem;

pub use self::errors::OdbcSourceError;
pub use self::typesystem::OdbcTypeSystem;

use self::typesystem::ODBC_UNKNOWN_TYPE_FALLBACK_ENV;
use crate::{
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{
        odbc_common::{
            connection_bool_param, connection_u32_param, connection_usize_param,
            is_connector_option_key, is_raw_odbc_conn_string, is_valid_odbc_key, push_odbc_pair,
            url_bool_param, url_usize_param, LOGIN_TIMEOUT_SECS_PARAM, MAX_CONNECTIONS_PARAM,
            QUERY_TIMEOUT_SECS_PARAM, REPLACE_INVALID_UTF16_PARAM,
        },
        odbc_core::{self, OdbcCoreError, OdbcExecutionOptions, OdbcTypePolicy},
        Source, SourcePartition,
    },
    sql::{count_query, CXQuery},
};
#[cfg(feature = "dst_arrow")]
use crate::{destinations::arrow::ArrowDestination, errors::OutResult};
use anyhow::anyhow;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use fehler::{throw, throws};
use odbc_api::{
    buffers::{BufferDesc, ColumnarAnyBuffer},
    Cursor,
};
use rust_decimal::Decimal;
use sqlparser::dialect::GenericDialect;
use std::sync::Arc;
use url::Url;
use urlencoding::decode;

const ODBC_DEFAULT_BATCH_SIZE: usize = 1024;
const ODBC_DEFAULT_MAX_STR_LEN: usize = 1024;

pub type OdbcSourceParser = odbc_core::OdbcParser<OdbcTypeSystem, OdbcSourceError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OdbcOptions {
    pub batch_size: usize,
    pub max_str_len: usize,
    pub max_connections: Option<usize>,
    pub login_timeout_secs: Option<u32>,
    pub query_timeout_secs: Option<usize>,
    pub unknown_type_fallback_to_varchar: bool,
    pub replace_invalid_utf16: bool,
}

impl OdbcOptions {
    pub fn from_env() -> Self {
        Self {
            batch_size: odbc_core::env_usize("ODBC_BATCH_SIZE").unwrap_or(ODBC_DEFAULT_BATCH_SIZE),
            max_str_len: odbc_core::env_usize(OdbcTypeSystem::max_str_len_env())
                .unwrap_or(ODBC_DEFAULT_MAX_STR_LEN),
            max_connections: odbc_core::env_usize("ODBC_MAX_CONNECTIONS"),
            login_timeout_secs: odbc_core::env_u32("ODBC_LOGIN_TIMEOUT_SECS"),
            query_timeout_secs: odbc_core::env_usize("ODBC_QUERY_TIMEOUT_SECS"),
            unknown_type_fallback_to_varchar: odbc_core::env_bool(ODBC_UNKNOWN_TYPE_FALLBACK_ENV)
                .unwrap_or(false),
            replace_invalid_utf16: false,
        }
    }
}

impl Default for OdbcOptions {
    fn default() -> Self {
        Self {
            batch_size: ODBC_DEFAULT_BATCH_SIZE,
            max_str_len: ODBC_DEFAULT_MAX_STR_LEN,
            max_connections: None,
            login_timeout_secs: None,
            query_timeout_secs: None,
            unknown_type_fallback_to_varchar: false,
            replace_invalid_utf16: false,
        }
    }
}

pub struct OdbcSource {
    conn: String,
    origin_query: Option<String>,
    queries: Vec<CXQuery<String>>,
    names: Vec<String>,
    schema: Vec<OdbcTypeSystem>,
    column_buffer_max_lens: Vec<usize>,
    batch_size: usize,
    max_str_len: usize,
    connection_limiter: Arc<odbc_core::OdbcConnectionLimiter>,
    execution_options: OdbcExecutionOptions,
    unknown_type_fallback_to_varchar: bool,
    replace_invalid_utf16: bool,
}

impl OdbcSource {
    #[throws(OdbcSourceError)]
    pub fn new(conn: &str, nconn: usize) -> Self {
        Self::with_options(conn, nconn, OdbcOptions::from_env())?
    }

    #[throws(OdbcSourceError)]
    pub fn with_options(conn: &str, nconn: usize, options: OdbcOptions) -> Self {
        let replace_invalid_utf16 = connection_bool_param(conn, REPLACE_INVALID_UTF16_PARAM)?
            .unwrap_or(options.replace_invalid_utf16);
        let max_connections =
            connection_usize_param(conn, MAX_CONNECTIONS_PARAM)?.or(options.max_connections);
        let connection_limiter = odbc_core::connection_limiter(max_connections, nconn)?;
        let execution_options = odbc_execution_options(conn, options)?;
        Self {
            conn: odbc_conn_string(conn)?,
            origin_query: None,
            queries: vec![],
            names: vec![],
            schema: vec![],
            column_buffer_max_lens: vec![],
            batch_size: options.batch_size,
            max_str_len: options.max_str_len,
            connection_limiter,
            execution_options,
            unknown_type_fallback_to_varchar: options.unknown_type_fallback_to_varchar,
            replace_invalid_utf16,
        }
    }

    #[throws(OdbcSourceError)]
    fn execute_query(
        conn: &str,
        query: &str,
        execution_options: OdbcExecutionOptions,
    ) -> odbc_core::OdbcCursor {
        odbc_core::execute_query::<OdbcSourceError>(
            OdbcTypeSystem::source_name(),
            conn,
            query,
            execution_options,
        )?
    }
}

impl Source for OdbcSource
where
    OdbcSourcePartition: SourcePartition<TypeSystem = OdbcTypeSystem, Error = OdbcSourceError>,
{
    const DATA_ORDERS: &'static [DataOrder] = &[DataOrder::RowMajor];
    type Partition = OdbcSourcePartition;
    type TypeSystem = OdbcTypeSystem;
    type Error = OdbcSourceError;

    #[throws(OdbcSourceError)]
    fn set_data_order(&mut self, data_order: DataOrder) {
        if !matches!(data_order, DataOrder::RowMajor) {
            throw!(ConnectorXError::UnsupportedDataOrder(data_order));
        }
    }

    fn set_queries<Q: ToString>(&mut self, queries: &[CXQuery<Q>]) {
        self.queries = queries.iter().map(|q| q.map(Q::to_string)).collect();
    }

    fn set_origin_query(&mut self, query: Option<String>) {
        self.origin_query = query;
    }

    #[throws(OdbcSourceError)]
    fn fetch_metadata(&mut self) {
        assert!(!self.queries.is_empty());

        let first_query = self.queries[0].to_string();
        let unknown_type_fallback_to_varchar = self.unknown_type_fallback_to_varchar;
        let (names, schema, column_buffer_max_lens) =
            odbc_core::fetch_metadata::<OdbcTypeSystem, OdbcSourceError, _>(
                OdbcTypeSystem::source_name(),
                &self.conn,
                &first_query,
                self.max_str_len,
                &self.connection_limiter,
                self.execution_options,
                |data_type, nullability, column_name| {
                    OdbcTypeSystem::from_odbc(
                        data_type,
                        nullability,
                        column_name,
                        unknown_type_fallback_to_varchar,
                    )
                    .map_err(Into::into)
                },
            )?;
        self.names = names;
        self.schema = schema;
        self.column_buffer_max_lens = column_buffer_max_lens;
    }

    #[throws(OdbcSourceError)]
    fn result_rows(&mut self) -> Option<usize> {
        match &self.origin_query {
            Some(q) => Some(odbc_core::fetch_count::<OdbcSourceError, _>(
                OdbcTypeSystem::source_name(),
                &self.conn,
                q,
                &GenericDialect {},
                &self.connection_limiter,
                self.execution_options,
            )?),
            None => None,
        }
    }

    fn names(&self) -> Vec<String> {
        self.names.clone()
    }

    fn schema(&self) -> Vec<Self::TypeSystem> {
        self.schema.clone()
    }

    #[throws(OdbcSourceError)]
    fn partition(self) -> Vec<Self::Partition> {
        self.queries
            .iter()
            .map(|query| {
                OdbcSourcePartition::new(
                    self.conn.clone(),
                    query,
                    &self.names,
                    &self.schema,
                    &self.column_buffer_max_lens,
                    self.batch_size,
                    Arc::clone(&self.connection_limiter),
                    self.execution_options,
                    self.replace_invalid_utf16,
                )
            })
            .collect()
    }
}

pub struct OdbcSourcePartition {
    conn: String,
    query: CXQuery<String>,
    names: Arc<[String]>,
    schema: Arc<[OdbcTypeSystem]>,
    column_buffer_max_lens: Vec<usize>,
    nrows: usize,
    ncols: usize,
    batch_size: usize,
    connection_limiter: Arc<odbc_core::OdbcConnectionLimiter>,
    execution_options: OdbcExecutionOptions,
    replace_invalid_utf16: bool,
}

impl OdbcSourcePartition {
    pub(crate) fn new(
        conn: String,
        query: &CXQuery<String>,
        names: &[String],
        schema: &[OdbcTypeSystem],
        column_buffer_max_lens: &[usize],
        batch_size: usize,
        connection_limiter: Arc<odbc_core::OdbcConnectionLimiter>,
        execution_options: OdbcExecutionOptions,
        replace_invalid_utf16: bool,
    ) -> Self {
        Self {
            conn,
            query: query.clone(),
            names: names.to_vec().into(),
            schema: schema.to_vec().into(),
            column_buffer_max_lens: column_buffer_max_lens.to_vec(),
            nrows: 0,
            ncols: schema.len(),
            batch_size,
            connection_limiter,
            execution_options,
            replace_invalid_utf16,
        }
    }
}

impl SourcePartition for OdbcSourcePartition {
    type TypeSystem = OdbcTypeSystem;
    type Parser<'a> = OdbcSourceParser;
    type Error = OdbcSourceError;

    #[throws(OdbcSourceError)]
    fn result_rows(&mut self) {
        let cquery = count_query(&self.query, &GenericDialect {})?;
        self.nrows = odbc_core::fetch_count_query::<OdbcSourceError>(
            OdbcTypeSystem::source_name(),
            &self.conn,
            cquery.as_str(),
            &self.connection_limiter,
            self.execution_options,
        )?;
    }

    #[throws(OdbcSourceError)]
    fn parser(&mut self) -> Self::Parser<'_> {
        let connection_permit = self.connection_limiter.acquire();
        let cursor =
            OdbcSource::execute_query(&self.conn, self.query.as_str(), self.execution_options)?;
        let buffer = ColumnarAnyBuffer::try_from_descs(
            self.batch_size,
            self.schema
                .iter()
                .zip(&self.column_buffer_max_lens)
                .map(|(ty, max_len)| ty.buffer_desc(*max_len)),
        )?;
        let cursor = cursor.bind_buffer(buffer)?;
        OdbcSourceParser::new(
            cursor,
            Arc::clone(&self.names),
            Arc::clone(&self.schema),
            self.replace_invalid_utf16,
            connection_permit,
        )
    }

    fn nrows(&self) -> usize {
        self.nrows
    }

    fn ncols(&self) -> usize {
        self.ncols
    }
}

#[cfg(feature = "dst_arrow")]
pub(crate) fn odbc_get_arrow(
    conn: &Url,
    origin_query: Option<String>,
    queries: &[CXQuery<String>],
) -> OutResult<ArrowDestination> {
    let options = OdbcOptions::from_env();
    let conn_str = odbc_conn_string(&conn[..])?;
    let unknown_type_fallback_to_varchar = options.unknown_type_fallback_to_varchar;
    let replace_invalid_utf16 =
        url_bool_param(conn, REPLACE_INVALID_UTF16_PARAM)?.unwrap_or(options.replace_invalid_utf16);
    let max_connections = url_usize_param(conn, MAX_CONNECTIONS_PARAM)?.or(options.max_connections);
    let connection_limiter = odbc_core::connection_limiter(max_connections, queries.len())?;
    let execution_options = odbc_execution_options(conn.as_str(), options)?;
    Ok(odbc_core::odbc_get_arrow_impl::<
        OdbcTypeSystem,
        OdbcSourceError,
    >(
        &conn_str,
        origin_query,
        queries,
        options.max_str_len,
        options.batch_size,
        connection_limiter,
        execution_options,
        replace_invalid_utf16,
        move |data_type, nullability, column_name| {
            OdbcTypeSystem::from_odbc(
                data_type,
                nullability,
                column_name,
                unknown_type_fallback_to_varchar,
            )
            .map_err(Into::into)
        },
    )?)
}

odbc_core::impl_odbc_arrow_policy!(OdbcTypeSystem);

impl OdbcCoreError for OdbcSourceError {
    fn get_nrows_failed() -> Self {
        Self::GetNRowsFailed
    }

    fn no_result_set(query: String) -> Self {
        Self::NoResultSet(query)
    }

    fn parse_value(value: String, ty: &'static str) -> Self {
        Self::ParseValue { value, ty }
    }

    fn connection_timeout(source_name: &'static str, timeout_secs: u32, cause: String) -> Self {
        Self::ConnectionTimeout {
            source_name,
            timeout_secs,
            cause,
        }
    }

    fn query_timeout(
        source_name: &'static str,
        query: String,
        timeout_secs: usize,
        cause: String,
    ) -> Self {
        Self::QueryTimeout {
            source_name,
            query,
            timeout_secs,
            cause,
        }
    }

    fn invalid_partition_bound(
        source_name: &'static str,
        column_name: &str,
        bound_name: &'static str,
        value: String,
        reason: &'static str,
    ) -> Self {
        Self::InvalidPartitionBound {
            source_name,
            column_name: column_name.to_string(),
            bound_name,
            value,
            reason,
        }
    }

    fn invalid_utf16(
        source_name: &'static str,
        column_name: Option<&str>,
        row_index: usize,
        byte_offset: usize,
        surrogate: u16,
    ) -> Self {
        Self::InvalidUtf16 {
            source_name,
            column_name: column_name.unwrap_or("<unknown>").to_string(),
            row_index,
            byte_offset,
            surrogate,
        }
    }
}

impl OdbcTypePolicy for OdbcTypeSystem {
    fn source_name() -> &'static str {
        "Odbc"
    }

    fn max_str_len_env() -> &'static str {
        "ODBC_MAX_STR_LEN"
    }

    fn nullable(self) -> bool {
        match self {
            OdbcTypeSystem::TinyInt(nullable)
            | OdbcTypeSystem::SmallInt(nullable)
            | OdbcTypeSystem::Int(nullable)
            | OdbcTypeSystem::BigInt(nullable)
            | OdbcTypeSystem::Real(nullable)
            | OdbcTypeSystem::Double(nullable)
            | OdbcTypeSystem::Numeric(nullable, ..)
            | OdbcTypeSystem::Decimal(nullable, ..)
            | OdbcTypeSystem::Bit(nullable)
            | OdbcTypeSystem::Char(nullable)
            | OdbcTypeSystem::Varchar(nullable)
            | OdbcTypeSystem::Text(nullable)
            | OdbcTypeSystem::Binary(nullable)
            | OdbcTypeSystem::Date(nullable)
            | OdbcTypeSystem::Time(nullable)
            | OdbcTypeSystem::Timestamp(nullable) => nullable,
        }
    }

    fn buffer_desc(self, max_str_len: usize) -> BufferDesc {
        let nullable = self.nullable();
        match self {
            OdbcTypeSystem::TinyInt(_) => BufferDesc::U8 { nullable },
            OdbcTypeSystem::SmallInt(_) => BufferDesc::I16 { nullable },
            OdbcTypeSystem::Int(_) => BufferDesc::I32 { nullable },
            OdbcTypeSystem::BigInt(_) => BufferDesc::I64 { nullable },
            OdbcTypeSystem::Real(_) => BufferDesc::F32 { nullable },
            OdbcTypeSystem::Double(_) => BufferDesc::F64 { nullable },
            OdbcTypeSystem::Bit(_) => BufferDesc::Bit { nullable },
            OdbcTypeSystem::Numeric(..)
            | OdbcTypeSystem::Decimal(..)
            | OdbcTypeSystem::Char(_)
            | OdbcTypeSystem::Varchar(_)
            | OdbcTypeSystem::Text(_) => BufferDesc::Text { max_str_len },
            OdbcTypeSystem::Binary(_) => BufferDesc::Binary {
                max_bytes: max_str_len,
            },
            OdbcTypeSystem::Date(_) => BufferDesc::Date { nullable },
            OdbcTypeSystem::Time(_) => BufferDesc::Time { nullable },
            OdbcTypeSystem::Timestamp(_) => BufferDesc::Timestamp { nullable },
        }
    }
}

odbc_core::impl_parse_from_bytes!(
    OdbcSourceParser,
    OdbcSourceError,
    Decimal,
    "Decimal",
    parse_decimal
);
odbc_core::impl_parse_from_cell!(OdbcSourceParser, OdbcSourceError, u8, "u8", cell_u8);
odbc_core::impl_parse_from_cell!(OdbcSourceParser, OdbcSourceError, i16, "i16", cell_i16);
odbc_core::impl_parse_from_cell!(OdbcSourceParser, OdbcSourceError, i32, "i32", cell_i32);
odbc_core::impl_parse_from_cell!(OdbcSourceParser, OdbcSourceError, i64, "i64", cell_i64);
odbc_core::impl_parse_from_cell!(OdbcSourceParser, OdbcSourceError, f32, "f32", cell_f32);
odbc_core::impl_parse_from_cell!(OdbcSourceParser, OdbcSourceError, f64, "f64", cell_f64);
odbc_core::impl_parse_from_cell!(
    OdbcSourceParser,
    OdbcSourceError,
    NaiveDate,
    "NaiveDate",
    cell_date
);
odbc_core::impl_parse_from_cell!(
    OdbcSourceParser,
    OdbcSourceError,
    NaiveTime,
    "NaiveTime",
    cell_time
);
odbc_core::impl_parse_from_cell!(
    OdbcSourceParser,
    OdbcSourceError,
    NaiveDateTime,
    "NaiveDateTime",
    cell_timestamp
);
odbc_core::impl_bool_produce!(OdbcSourceParser, OdbcSourceError);
odbc_core::impl_string_produce!(OdbcSourceParser, OdbcSourceError);
odbc_core::impl_bytes_clone_produce!(OdbcSourceParser, OdbcSourceError);

pub(crate) fn fetch_i64_pair(
    conn: &str,
    query: &str,
    column_name: &str,
    execution_options: OdbcExecutionOptions,
) -> Result<(i64, i64), OdbcSourceError> {
    odbc_core::fetch_i64_pair::<OdbcSourceError>(
        conn,
        query,
        OdbcTypeSystem::source_name(),
        column_name,
        execution_options,
    )
}

#[throws(OdbcSourceError)]
pub(crate) fn odbc_execution_options(conn: &str, options: OdbcOptions) -> OdbcExecutionOptions {
    OdbcExecutionOptions::new(
        connection_u32_param(conn, LOGIN_TIMEOUT_SECS_PARAM)?.or(options.login_timeout_secs),
        connection_usize_param(conn, QUERY_TIMEOUT_SECS_PARAM)?.or(options.query_timeout_secs),
    )?
}

#[throws(OdbcSourceError)]
pub fn odbc_conn_string(conn: &str) -> String {
    if is_raw_odbc_conn_string(conn) {
        return conn.to_string();
    }

    let url = Url::parse(conn)?;
    let params = url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect::<Vec<_>>();

    if let Some(raw_conn) = param_value(&params, "odbc_connect") {
        if !is_raw_odbc_conn_string(raw_conn) {
            throw!(anyhow!(
                "odbc_connect must contain a raw ODBC connection string starting with Driver=, DSN=, FileDSN=, or Database="
            ));
        }
        return raw_conn.to_string();
    }

    let driver = param_value(&params, "driver");
    let dsn = param_value(&params, "dsn");
    if driver.is_none() && dsn.is_none() {
        throw!(anyhow!(
            "ODBC URLs require either a driver= or dsn= query parameter; raw ODBC connection strings are also supported"
        ));
    }

    let database = decode(url.path().trim_start_matches('/'))?.into_owned();
    let username = decode(url.username())?.into_owned();
    let password = decode(url.password().unwrap_or(""))?.into_owned();
    let server_key = param_value(&params, "server_key").unwrap_or("Server");
    if !is_valid_odbc_key(server_key) {
        throw!(anyhow!(
            "invalid ODBC connection-string key: {server_key:?}"
        ));
    }

    let mut ret = String::new();
    if let Some(dsn) = dsn {
        push_odbc_pair(&mut ret, "DSN", dsn);
    } else if let Some(driver) = driver {
        push_odbc_pair(&mut ret, "Driver", driver);
    }
    if let Some(host) = url.host_str() {
        push_odbc_pair(&mut ret, server_key, decode(host)?.as_ref());
    }
    if let Some(port) = url.port() {
        ret.push_str(&format!("Port={};", port));
    }
    if !database.is_empty() {
        push_odbc_pair(&mut ret, "Database", &database);
    }
    if !username.is_empty() {
        push_odbc_pair(&mut ret, "UID", &username);
    }
    if !password.is_empty() {
        push_odbc_pair(&mut ret, "PWD", &password);
    }
    for (key, value) in &params {
        if !is_connector_option_key(key)
            && !matches!(
                key.to_ascii_lowercase().as_str(),
                "driver" | "dsn" | "server_key"
            )
        {
            if !is_valid_odbc_key(key) {
                throw!(anyhow!("invalid ODBC connection-string key: {key:?}"));
            }
            push_odbc_pair(&mut ret, key, value);
        }
    }
    ret
}

fn param_value<'a>(params: &'a [(String, String)], key: &str) -> Option<&'a str> {
    params
        .iter()
        .find(|(param_key, _)| param_key.eq_ignore_ascii_case(key))
        .map(|(_, value)| value.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_options_keeps_per_source_limits() {
        let conn = "Driver={SQLite3};Database=:memory:;";
        let small = OdbcSource::with_options(
            conn,
            1,
            OdbcOptions {
                batch_size: 2,
                max_str_len: 8,
                max_connections: Some(1),
                login_timeout_secs: Some(5),
                query_timeout_secs: Some(30),
                unknown_type_fallback_to_varchar: true,
                replace_invalid_utf16: true,
            },
        )
        .unwrap();
        let large = OdbcSource::with_options(
            conn,
            1,
            OdbcOptions {
                batch_size: 32,
                max_str_len: 4096,
                max_connections: Some(4),
                login_timeout_secs: None,
                query_timeout_secs: None,
                unknown_type_fallback_to_varchar: false,
                replace_invalid_utf16: false,
            },
        )
        .unwrap();

        assert_eq!(small.batch_size, 2);
        assert_eq!(small.max_str_len, 8);
        assert_eq!(small.connection_limiter.max_connections(), 1);
        assert_eq!(small.execution_options.login_timeout_secs, Some(5));
        assert_eq!(small.execution_options.query_timeout_secs, Some(30));
        assert!(small.unknown_type_fallback_to_varchar);
        assert!(small.replace_invalid_utf16);
        assert_eq!(large.batch_size, 32);
        assert_eq!(large.max_str_len, 4096);
        assert_eq!(large.connection_limiter.max_connections(), 4);
        assert_eq!(large.execution_options, OdbcExecutionOptions::default());
        assert!(!large.unknown_type_fallback_to_varchar);
        assert!(!large.replace_invalid_utf16);
    }

    #[test]
    fn default_options_match_previous_defaults() {
        assert_eq!(
            OdbcOptions::default(),
            OdbcOptions {
                batch_size: ODBC_DEFAULT_BATCH_SIZE,
                max_str_len: ODBC_DEFAULT_MAX_STR_LEN,
                max_connections: None,
                login_timeout_secs: None,
                query_timeout_secs: None,
                unknown_type_fallback_to_varchar: false,
                replace_invalid_utf16: false,
            }
        );
    }

    #[test]
    fn replace_invalid_utf16_url_option_is_connector_only() {
        let conn = "odbc://example.com/db?driver=PostgreSQL&replace_invalid_utf16=true&max_connections=3&login_timeout_secs=5&query_timeout_secs=30";
        assert_eq!(
            odbc_conn_string(conn).unwrap(),
            "Driver=PostgreSQL;Server=example.com;Database=db;"
        );

        let source = OdbcSource::with_options(conn, 1, OdbcOptions::default()).unwrap();
        assert!(source.replace_invalid_utf16);
        assert_eq!(source.connection_limiter.max_connections(), 3);
        assert_eq!(source.execution_options.login_timeout_secs, Some(5));
        assert_eq!(source.execution_options.query_timeout_secs, Some(30));
    }
}
