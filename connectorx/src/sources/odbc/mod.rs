//! Source implementation for generic ODBC.

mod errors;
mod typesystem;

pub use self::errors::OdbcSourceError;
pub use self::typesystem::OdbcTypeSystem;
pub use crate::sources::odbc_core::OdbcLobStrategy;

use self::typesystem::ODBC_UNKNOWN_TYPE_FALLBACK_ENV;
#[cfg(feature = "dst_arrow")]
use crate::{
    arrow_batch_iter::RecordBatchIterator, destinations::arrow::ArrowDestination, errors::OutResult,
};
use crate::{
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{
        odbc_common::{
            connection_query_pairs, is_connector_option_key, is_raw_odbc_conn_string,
            is_valid_odbc_key, param_value, push_odbc_pair, url_query_pairs,
        },
        odbc_core::{self, OdbcCoreError, OdbcExecutionOptions, OdbcTypePolicy},
        Source, SourcePartition,
    },
    sql::CXQuery,
};
use anyhow::anyhow;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use fehler::{throw, throws};
use odbc_api::buffers::BufferDesc;
use rust_decimal::Decimal;
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
    pub lob_strategy: OdbcLobStrategy,
    pub unknown_type_fallback_to_varchar: bool,
    pub replace_invalid_utf16: bool,
    pub replace_invalid_utf8: bool,
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
            lob_strategy: odbc_core::env_lob_strategy("ODBC_LOB_STRATEGY").unwrap_or_default(),
            unknown_type_fallback_to_varchar: odbc_core::env_bool(ODBC_UNKNOWN_TYPE_FALLBACK_ENV)
                .unwrap_or(false),
            replace_invalid_utf16: false,
            replace_invalid_utf8: false,
        }
    }
}

odbc_core::impl_odbc_runtime_options!(OdbcOptions);

fn validate_odbc_options(options: &OdbcOptions) -> Result<(), anyhow::Error> {
    odbc_core::validate_batch_and_buffer_limits(
        OdbcTypeSystem::source_name(),
        "ODBC_BATCH_SIZE",
        options.batch_size,
        OdbcTypeSystem::max_str_len_env(),
        options.max_str_len,
    )
}

impl Default for OdbcOptions {
    fn default() -> Self {
        Self {
            batch_size: ODBC_DEFAULT_BATCH_SIZE,
            max_str_len: ODBC_DEFAULT_MAX_STR_LEN,
            max_connections: None,
            login_timeout_secs: None,
            query_timeout_secs: None,
            lob_strategy: OdbcLobStrategy::Bounded,
            unknown_type_fallback_to_varchar: false,
            replace_invalid_utf16: false,
            replace_invalid_utf8: false,
        }
    }
}

pub struct OdbcSource {
    state: odbc_core::OdbcSourceState<OdbcTypeSystem, OdbcSourceError>,
}

impl OdbcSource {
    #[throws(OdbcSourceError)]
    pub fn new(conn: &str, nconn: usize) -> Self {
        Self::with_options(conn, nconn, OdbcOptions::from_env())?
    }

    #[throws(OdbcSourceError)]
    pub fn with_options(conn: &str, nconn: usize, options: OdbcOptions) -> Self {
        validate_odbc_options(&options)?;
        let params = connection_query_pairs(conn)?;
        let params = params.as_deref();
        let runtime_options = odbc_core::resolve_runtime_options(params, &options, nconn)?;
        Self {
            state: odbc_core::OdbcSourceState::new(
                odbc_conn_string(conn)?,
                options.batch_size,
                options.max_str_len,
                options.unknown_type_fallback_to_varchar,
                runtime_options,
            ),
        }
    }
}

odbc_core::impl_odbc_source_partition_wrapper!(
    OdbcSourcePartition,
    OdbcTypeSystem,
    OdbcSourceError
);

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
        self.state.set_queries(queries);
    }

    fn set_origin_query(&mut self, query: Option<String>) {
        self.state.set_origin_query(query);
    }

    #[throws(OdbcSourceError)]
    fn fetch_metadata(&mut self) {
        let unknown_type_fallback_to_varchar = self.state.unknown_type_fallback_to_varchar;
        self.state
            .fetch_metadata(|data_type, nullability, column_name| {
                OdbcTypeSystem::from_odbc(
                    data_type,
                    nullability,
                    column_name,
                    unknown_type_fallback_to_varchar,
                )
                .map_err(Into::into)
            })?;
    }

    #[throws(OdbcSourceError)]
    fn result_rows(&mut self) -> Option<usize> {
        self.state.result_rows(odbc_core::OdbcSqlDialect::Generic)?
    }

    fn names(&self) -> Vec<String> {
        self.state.names()
    }

    fn schema(&self) -> Vec<Self::TypeSystem> {
        self.state.schema()
    }

    #[throws(OdbcSourceError)]
    fn partition(self) -> Vec<Self::Partition> {
        self.state
            .partition(odbc_core::OdbcSqlDialect::Generic)
            .into_iter()
            .map(OdbcSourcePartition::new)
            .collect()
    }
}

#[cfg(feature = "dst_arrow")]
pub(crate) fn odbc_get_arrow(
    conn: &Url,
    origin_query: Option<String>,
    queries: &[CXQuery<String>],
    pre_execution_queries: Option<&[String]>,
) -> OutResult<ArrowDestination> {
    let options = OdbcOptions::from_env();
    validate_odbc_options(&options)?;
    let params = url_query_pairs(conn)?;
    let conn_str = odbc_conn_string(&conn[..])?;
    let unknown_type_fallback_to_varchar = options.unknown_type_fallback_to_varchar;
    let runtime_options =
        odbc_core::resolve_runtime_options(Some(&params), &options, queries.len())?;
    let lob_strategy = odbc_core::lob_strategy_from_params(Some(&params), options.lob_strategy)?;
    Ok(odbc_core::odbc_get_arrow_impl::<
        OdbcTypeSystem,
        OdbcSourceError,
    >(
        &conn_str,
        origin_query,
        queries,
        options.max_str_len,
        options.batch_size,
        runtime_options.connection_limiter,
        runtime_options.execution_options,
        pre_execution_queries,
        lob_strategy,
        runtime_options.replace_invalid_utf16,
        runtime_options.replace_invalid_utf8,
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

#[cfg(feature = "dst_arrow")]
pub(crate) fn odbc_record_batch_iter(
    conn: &Url,
    origin_query: Option<String>,
    queries: &[CXQuery<String>],
    batch_size: usize,
    pre_execution_queries: Option<&[String]>,
) -> OutResult<Box<dyn RecordBatchIterator>> {
    let options = OdbcOptions::from_env();
    odbc_core::validate_batch_and_buffer_limits(
        OdbcTypeSystem::source_name(),
        "batch_size",
        batch_size,
        OdbcTypeSystem::max_str_len_env(),
        options.max_str_len,
    )?;
    let params = url_query_pairs(conn)?;
    let conn_str = odbc_conn_string(&conn[..])?;
    let unknown_type_fallback_to_varchar = options.unknown_type_fallback_to_varchar;
    let runtime_options =
        odbc_core::resolve_runtime_options(Some(&params), &options, queries.len())?;
    let lob_strategy = odbc_core::lob_strategy_from_params(Some(&params), options.lob_strategy)?;
    let iterator = odbc_core::odbc_record_batch_iter_impl::<OdbcTypeSystem, OdbcSourceError>(
        &conn_str,
        origin_query,
        queries,
        options.max_str_len,
        batch_size,
        runtime_options.connection_limiter,
        runtime_options.execution_options,
        pre_execution_queries,
        lob_strategy,
        runtime_options.replace_invalid_utf16,
        runtime_options.replace_invalid_utf8,
        move |data_type, nullability, column_name| {
            OdbcTypeSystem::from_odbc(
                data_type,
                nullability,
                column_name,
                unknown_type_fallback_to_varchar,
            )
            .map_err(Into::into)
        },
    )?;
    Ok(Box::new(iterator))
}

odbc_core::impl_odbc_arrow_policy!(OdbcTypeSystem, wide_text);

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

    fn invalid_utf8(
        source_name: &'static str,
        column_name: Option<&str>,
        row_index: usize,
        byte_offset: usize,
    ) -> Self {
        Self::InvalidUtf8 {
            source_name,
            column_name: column_name.unwrap_or("<unknown>").to_string(),
            row_index,
            byte_offset,
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
            | OdbcTypeSystem::WChar(nullable)
            | OdbcTypeSystem::WVarchar(nullable)
            | OdbcTypeSystem::WText(nullable)
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
            OdbcTypeSystem::WChar(_) | OdbcTypeSystem::WVarchar(_) | OdbcTypeSystem::WText(_) => {
                BufferDesc::WText { max_str_len }
            }
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
    let params = connection_query_pairs(conn)?;
    odbc_core::execution_options_from_params(params.as_deref(), &options)?
}

#[throws(OdbcSourceError)]
pub fn odbc_conn_string(conn: &str) -> String {
    if is_raw_odbc_conn_string(conn) {
        return conn.to_string();
    }

    let url = Url::parse(conn)?;
    let params = url_query_pairs(&url)?;

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
                lob_strategy: OdbcLobStrategy::Piecewise,
                unknown_type_fallback_to_varchar: true,
                replace_invalid_utf16: true,
                replace_invalid_utf8: true,
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
                lob_strategy: OdbcLobStrategy::Bounded,
                unknown_type_fallback_to_varchar: false,
                replace_invalid_utf16: false,
                replace_invalid_utf8: false,
            },
        )
        .unwrap();

        assert_eq!(small.state.batch_size, 2);
        assert_eq!(small.state.max_str_len, 8);
        assert_eq!(small.state.connection_limiter.max_connections(), 1);
        assert_eq!(small.state.execution_options.login_timeout_secs, Some(5));
        assert_eq!(small.state.execution_options.query_timeout_secs, Some(30));
        assert!(small.state.unknown_type_fallback_to_varchar);
        assert!(small.state.replace_invalid_utf16);
        assert!(small.state.replace_invalid_utf8);
        assert_eq!(large.state.batch_size, 32);
        assert_eq!(large.state.max_str_len, 4096);
        assert_eq!(large.state.connection_limiter.max_connections(), 4);
        assert_eq!(
            large.state.execution_options,
            OdbcExecutionOptions::default()
        );
        assert!(!large.state.unknown_type_fallback_to_varchar);
        assert!(!large.state.replace_invalid_utf16);
        assert!(!large.state.replace_invalid_utf8);
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
                lob_strategy: OdbcLobStrategy::Bounded,
                unknown_type_fallback_to_varchar: false,
                replace_invalid_utf16: false,
                replace_invalid_utf8: false,
            }
        );
    }

    #[test]
    fn rejects_oversized_batch_and_buffer_options() {
        let conn = "Driver={SQLite3};Database=:memory:;";
        let too_many_rows = match OdbcSource::with_options(
            conn,
            1,
            OdbcOptions {
                batch_size: odbc_core::MAX_BATCH_SIZE + 1,
                ..OdbcOptions::default()
            },
        ) {
            Ok(_) => panic!("expected oversized ODBC batch size to fail"),
            Err(err) => err.to_string(),
        };
        assert!(
            too_many_rows.contains("ODBC_BATCH_SIZE"),
            "{}",
            too_many_rows
        );

        let too_much_buffer = match OdbcSource::with_options(
            conn,
            1,
            OdbcOptions {
                max_str_len: odbc_core::MAX_STR_LEN + 1,
                ..OdbcOptions::default()
            },
        ) {
            Ok(_) => panic!("expected oversized ODBC max string length to fail"),
            Err(err) => err.to_string(),
        };
        assert!(
            too_much_buffer.contains("ODBC_MAX_STR_LEN"),
            "{}",
            too_much_buffer
        );
    }

    #[test]
    fn fetch_metadata_without_queries_returns_error() {
        let mut source = OdbcSource::with_options(
            "Driver={SQLite3};Database=:memory:;",
            1,
            OdbcOptions::default(),
        )
        .unwrap();
        let err = crate::sources::Source::fetch_metadata(&mut source)
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("Odbc metadata requires at least one query"),
            "{}",
            err
        );
    }

    #[test]
    fn replace_invalid_encoding_url_options_are_connector_only() {
        let conn = "odbc://example.com/db?driver=PostgreSQL&replace_invalid_utf16=true&replace_invalid_utf8=true&max_connections=3&login_timeout_secs=5&query_timeout_secs=30";
        assert_eq!(
            odbc_conn_string(conn).unwrap(),
            "Driver=PostgreSQL;Server=example.com;Database=db;"
        );

        let source = OdbcSource::with_options(conn, 1, OdbcOptions::default()).unwrap();
        assert!(source.state.replace_invalid_utf16);
        assert!(source.state.replace_invalid_utf8);
        assert_eq!(source.state.connection_limiter.max_connections(), 3);
        assert_eq!(source.state.execution_options.login_timeout_secs, Some(5));
        assert_eq!(source.state.execution_options.query_timeout_secs, Some(30));
    }
}
