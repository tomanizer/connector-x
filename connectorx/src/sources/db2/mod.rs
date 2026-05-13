//! Source implementation for IBM Db2 through ODBC.

mod errors;
mod profile;
mod typesystem;

pub use self::errors::Db2SourceError;
pub use self::profile::{
    diagnose_replication_key, key_constraint_catalog_query, replication_key_catalog_query,
    table_metadata_catalog_query, unique_index_catalog_query, Db2CatalogColumn, Db2KeyConstraint,
    Db2KeyConstraintKind, Db2PartitionHint, Db2Profile, Db2ProfileConfig,
    Db2ReplicationKeyDiagnostic, Db2ReplicationKeyEvidence, Db2ReplicationKeyUniqueness,
    Db2UniqueIndex,
};
pub use self::typesystem::Db2TypeSystem;

use self::profile::is_db2_profile_option_key;
use self::typesystem::DB2_UNKNOWN_TYPE_FALLBACK_ENV;
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
            is_valid_odbc_key, odbc_conn_value_if_needed, param_value, url_query_pairs,
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

const DB2_DEFAULT_BATCH_SIZE: usize = 1024;
const DB2_DEFAULT_MAX_STR_LEN: usize = 1024;

pub type Db2SourceParser = odbc_core::OdbcParser<Db2TypeSystem, Db2SourceError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Db2Options {
    pub batch_size: usize,
    pub max_str_len: usize,
    pub max_connections: Option<usize>,
    pub login_timeout_secs: Option<u32>,
    pub query_timeout_secs: Option<usize>,
    pub unknown_type_fallback_to_varchar: bool,
    pub replace_invalid_utf16: bool,
    pub replace_invalid_utf8: bool,
}

impl Db2Options {
    pub fn from_env() -> Self {
        Self {
            batch_size: odbc_core::env_usize("DB2_BATCH_SIZE").unwrap_or(DB2_DEFAULT_BATCH_SIZE),
            max_str_len: odbc_core::env_usize(Db2TypeSystem::max_str_len_env())
                .unwrap_or(DB2_DEFAULT_MAX_STR_LEN),
            max_connections: odbc_core::env_usize("DB2_MAX_CONNECTIONS"),
            login_timeout_secs: odbc_core::env_u32("DB2_LOGIN_TIMEOUT_SECS"),
            query_timeout_secs: odbc_core::env_usize("DB2_QUERY_TIMEOUT_SECS"),
            unknown_type_fallback_to_varchar: odbc_core::env_bool(DB2_UNKNOWN_TYPE_FALLBACK_ENV)
                .unwrap_or(false),
            replace_invalid_utf16: false,
            replace_invalid_utf8: false,
        }
    }
}

odbc_core::impl_odbc_runtime_options!(Db2Options);

fn validate_db2_options(options: &Db2Options) -> Result<(), anyhow::Error> {
    odbc_core::validate_batch_and_buffer_limits(
        Db2TypeSystem::source_name(),
        "DB2_BATCH_SIZE",
        options.batch_size,
        Db2TypeSystem::max_str_len_env(),
        options.max_str_len,
    )
}

impl Default for Db2Options {
    fn default() -> Self {
        Self {
            batch_size: DB2_DEFAULT_BATCH_SIZE,
            max_str_len: DB2_DEFAULT_MAX_STR_LEN,
            max_connections: None,
            login_timeout_secs: None,
            query_timeout_secs: None,
            unknown_type_fallback_to_varchar: false,
            replace_invalid_utf16: false,
            replace_invalid_utf8: false,
        }
    }
}

pub struct Db2Source {
    state: odbc_core::OdbcSourceState<Db2TypeSystem, Db2SourceError>,
    profile_config: Db2ProfileConfig,
}

impl Db2Source {
    #[throws(Db2SourceError)]
    pub fn new(conn: &str, nconn: usize) -> Self {
        Self::with_options(conn, nconn, Db2Options::from_env())?
    }

    #[throws(Db2SourceError)]
    pub fn with_options(conn: &str, nconn: usize, options: Db2Options) -> Self {
        validate_db2_options(&options)?;
        let params = connection_query_pairs(conn)?;
        let params = params.as_deref();
        let profile_config = Db2ProfileConfig::from_env_and_params(params)?;
        log_db2_profile_scope(&profile_config);
        let runtime_options = odbc_core::resolve_runtime_options(params, &options, nconn)?;
        Self {
            state: odbc_core::OdbcSourceState::new(
                db2_conn_string(conn)?,
                options.batch_size,
                options.max_str_len,
                options.unknown_type_fallback_to_varchar,
                runtime_options,
            ),
            profile_config,
        }
    }

    pub fn profile_config(&self) -> &Db2ProfileConfig {
        &self.profile_config
    }
}

odbc_core::impl_odbc_source_partition_wrapper!(Db2SourcePartition, Db2TypeSystem, Db2SourceError);

impl Source for Db2Source
where
    Db2SourcePartition: SourcePartition<TypeSystem = Db2TypeSystem, Error = Db2SourceError>,
{
    const DATA_ORDERS: &'static [DataOrder] = &[DataOrder::RowMajor];
    type Partition = Db2SourcePartition;
    type TypeSystem = Db2TypeSystem;
    type Error = Db2SourceError;

    #[throws(Db2SourceError)]
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

    #[throws(Db2SourceError)]
    fn fetch_metadata(&mut self) {
        let unknown_type_fallback_to_varchar = self.state.unknown_type_fallback_to_varchar;
        self.state
            .fetch_metadata(|data_type, nullability, column_name| {
                Db2TypeSystem::from_odbc(
                    data_type,
                    nullability,
                    column_name,
                    unknown_type_fallback_to_varchar,
                )
                .map_err(Into::into)
            })?;
    }

    #[throws(Db2SourceError)]
    fn result_rows(&mut self) -> Option<usize> {
        self.state.result_rows(odbc_core::OdbcSqlDialect::Generic)?
    }

    fn names(&self) -> Vec<String> {
        self.state.names()
    }

    fn schema(&self) -> Vec<Self::TypeSystem> {
        self.state.schema()
    }

    #[throws(Db2SourceError)]
    fn partition(self) -> Vec<Self::Partition> {
        self.state
            .partition(odbc_core::OdbcSqlDialect::Generic)
            .into_iter()
            .map(Db2SourcePartition::new)
            .collect()
    }
}

impl OdbcCoreError for Db2SourceError {
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

impl OdbcTypePolicy for Db2TypeSystem {
    fn source_name() -> &'static str {
        "Db2"
    }

    fn max_str_len_env() -> &'static str {
        "DB2_MAX_STR_LEN"
    }

    fn nullable(self) -> bool {
        match self {
            Db2TypeSystem::TinyInt(nullable)
            | Db2TypeSystem::SmallInt(nullable)
            | Db2TypeSystem::Int(nullable)
            | Db2TypeSystem::BigInt(nullable)
            | Db2TypeSystem::Real(nullable)
            | Db2TypeSystem::Double(nullable)
            | Db2TypeSystem::Numeric(nullable, ..)
            | Db2TypeSystem::Decimal(nullable, ..)
            | Db2TypeSystem::Bit(nullable)
            | Db2TypeSystem::Char(nullable)
            | Db2TypeSystem::Varchar(nullable)
            | Db2TypeSystem::Text(nullable)
            | Db2TypeSystem::Binary(nullable)
            | Db2TypeSystem::Date(nullable)
            | Db2TypeSystem::Time(nullable)
            | Db2TypeSystem::Timestamp(nullable) => nullable,
        }
    }

    fn buffer_desc(self, max_str_len: usize) -> BufferDesc {
        let nullable = self.nullable();
        match self {
            Db2TypeSystem::TinyInt(_) => BufferDesc::U8 { nullable },
            Db2TypeSystem::SmallInt(_) => BufferDesc::I16 { nullable },
            Db2TypeSystem::Int(_) => BufferDesc::I32 { nullable },
            Db2TypeSystem::BigInt(_) => BufferDesc::I64 { nullable },
            Db2TypeSystem::Real(_) => BufferDesc::F32 { nullable },
            Db2TypeSystem::Double(_) => BufferDesc::F64 { nullable },
            Db2TypeSystem::Bit(_) => BufferDesc::Bit { nullable },
            Db2TypeSystem::Numeric(..)
            | Db2TypeSystem::Decimal(..)
            | Db2TypeSystem::Char(_)
            | Db2TypeSystem::Varchar(_)
            | Db2TypeSystem::Text(_) => BufferDesc::Text { max_str_len },
            Db2TypeSystem::Binary(_) => BufferDesc::Binary {
                max_bytes: max_str_len,
            },
            Db2TypeSystem::Date(_) => BufferDesc::Date { nullable },
            Db2TypeSystem::Time(_) => BufferDesc::Time { nullable },
            Db2TypeSystem::Timestamp(_) => BufferDesc::Timestamp { nullable },
        }
    }
}

odbc_core::impl_parse_from_bytes!(
    Db2SourceParser,
    Db2SourceError,
    Decimal,
    "Decimal",
    parse_decimal
);
odbc_core::impl_parse_from_cell!(Db2SourceParser, Db2SourceError, u8, "u8", cell_u8);
odbc_core::impl_parse_from_cell!(Db2SourceParser, Db2SourceError, i16, "i16", cell_i16);
odbc_core::impl_parse_from_cell!(Db2SourceParser, Db2SourceError, i32, "i32", cell_i32);
odbc_core::impl_parse_from_cell!(Db2SourceParser, Db2SourceError, i64, "i64", cell_i64);
odbc_core::impl_parse_from_cell!(Db2SourceParser, Db2SourceError, f32, "f32", cell_f32);
odbc_core::impl_parse_from_cell!(Db2SourceParser, Db2SourceError, f64, "f64", cell_f64);
odbc_core::impl_parse_from_cell!(
    Db2SourceParser,
    Db2SourceError,
    NaiveDate,
    "NaiveDate",
    cell_date
);
odbc_core::impl_parse_from_cell!(
    Db2SourceParser,
    Db2SourceError,
    NaiveTime,
    "NaiveTime",
    cell_time
);
odbc_core::impl_parse_from_cell!(
    Db2SourceParser,
    Db2SourceError,
    NaiveDateTime,
    "NaiveDateTime",
    cell_timestamp
);
odbc_core::impl_bool_produce!(Db2SourceParser, Db2SourceError);
odbc_core::impl_string_produce!(Db2SourceParser, Db2SourceError);
odbc_core::impl_bytes_clone_produce!(Db2SourceParser, Db2SourceError);

pub(crate) fn fetch_i64_pair(
    conn: &str,
    query: &str,
    column_name: &str,
    execution_options: OdbcExecutionOptions,
) -> Result<(i64, i64), Db2SourceError> {
    odbc_core::fetch_i64_pair::<Db2SourceError>(
        conn,
        query,
        Db2TypeSystem::source_name(),
        column_name,
        execution_options,
    )
}

#[throws(Db2SourceError)]
pub(crate) fn db2_execution_options(conn: &str, options: Db2Options) -> OdbcExecutionOptions {
    let params = connection_query_pairs(conn)?;
    odbc_core::execution_options_from_params(params.as_deref(), &options)?
}

#[cfg(feature = "dst_arrow")]
pub(crate) fn db2_get_arrow(
    conn: &Url,
    origin_query: Option<String>,
    queries: &[CXQuery<String>],
    pre_execution_queries: Option<&[String]>,
) -> OutResult<ArrowDestination> {
    let options = Db2Options::from_env();
    validate_db2_options(&options)?;
    let params = url_query_pairs(conn)?;
    let conn_str = db2_conn_string(&conn[..])?;
    let unknown_type_fallback_to_varchar = options.unknown_type_fallback_to_varchar;
    let profile_config = Db2ProfileConfig::from_env_and_params(Some(&params))?;
    log_db2_profile_scope(&profile_config);
    let runtime_options =
        odbc_core::resolve_runtime_options(Some(&params), &options, queries.len())?;
    Ok(odbc_core::odbc_get_arrow_impl::<
        Db2TypeSystem,
        Db2SourceError,
    >(
        &conn_str,
        origin_query,
        queries,
        options.max_str_len,
        options.batch_size,
        runtime_options.connection_limiter,
        runtime_options.execution_options,
        pre_execution_queries,
        runtime_options.replace_invalid_utf16,
        runtime_options.replace_invalid_utf8,
        move |data_type, nullability, column_name| {
            Db2TypeSystem::from_odbc(
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
pub(crate) fn db2_record_batch_iter(
    conn: &Url,
    origin_query: Option<String>,
    queries: &[CXQuery<String>],
    batch_size: usize,
    pre_execution_queries: Option<&[String]>,
) -> OutResult<Box<dyn RecordBatchIterator>> {
    let options = Db2Options::from_env();
    odbc_core::validate_batch_and_buffer_limits(
        Db2TypeSystem::source_name(),
        "batch_size",
        batch_size,
        Db2TypeSystem::max_str_len_env(),
        options.max_str_len,
    )?;
    let params = url_query_pairs(conn)?;
    let conn_str = db2_conn_string(&conn[..])?;
    let unknown_type_fallback_to_varchar = options.unknown_type_fallback_to_varchar;
    let profile_config = Db2ProfileConfig::from_env_and_params(Some(&params))?;
    log_db2_profile_scope(&profile_config);
    let runtime_options =
        odbc_core::resolve_runtime_options(Some(&params), &options, queries.len())?;
    let iterator = odbc_core::odbc_record_batch_iter_impl::<Db2TypeSystem, Db2SourceError>(
        &conn_str,
        origin_query,
        queries,
        options.max_str_len,
        batch_size,
        runtime_options.connection_limiter,
        runtime_options.execution_options,
        pre_execution_queries,
        runtime_options.replace_invalid_utf16,
        runtime_options.replace_invalid_utf8,
        move |data_type, nullability, column_name| {
            Db2TypeSystem::from_odbc(
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

odbc_core::impl_odbc_arrow_policy!(Db2TypeSystem);

fn log_db2_profile_scope(profile_config: &Db2ProfileConfig) {
    if let Some(message) = profile_config.runtime_scope_message() {
        log::debug!("{message}");
    }
}

#[throws(Db2SourceError)]
pub fn db2_conn_string(conn: &str) -> String {
    if is_raw_odbc_conn_string(conn) {
        return conn.to_string();
    }

    let url = Url::parse(conn)?;
    let params = url_query_pairs(&url)?;

    let driver = param_value(&params, "driver").unwrap_or("IBM DB2 ODBC DRIVER");
    let host = decode(url.host_str().unwrap_or("localhost"))?.into_owned();
    let port = url.port().unwrap_or(50000);
    let database = decode(url.path().trim_start_matches('/'))?.into_owned();
    let username = decode(url.username())?.into_owned();
    let password = decode(url.password().unwrap_or(""))?.into_owned();
    let protocol = param_value(&params, "protocol").unwrap_or("TCPIP");

    let mut ret = format!(
        "Driver={};Hostname={};Port={};Protocol={};UID={};PWD={};",
        odbc_conn_value_if_needed(driver),
        odbc_conn_value_if_needed(&host),
        port,
        odbc_conn_value_if_needed(protocol),
        odbc_conn_value_if_needed(&username),
        odbc_conn_value_if_needed(&password)
    );
    if !database.is_empty() {
        ret.push_str(&format!(
            "Database={};",
            odbc_conn_value_if_needed(&database)
        ));
    }
    for (key, value) in &params {
        if !is_connector_option_key(key)
            && !is_db2_profile_option_key(key)
            && !key.eq_ignore_ascii_case("driver")
            && !key.eq_ignore_ascii_case("protocol")
        {
            if !is_valid_odbc_key(key) {
                throw!(anyhow!("invalid ODBC connection-string key: {key:?}"));
            }
            ret.push_str(&format!("{}={};", key, odbc_conn_value_if_needed(value)));
        }
    }
    ret
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_options_sets_instance_limits() {
        let source = Db2Source::with_options(
            "db2://db2inst1:password@127.0.0.1:50000/testdb?db2_profile=generic",
            1,
            Db2Options {
                batch_size: 7,
                max_str_len: 2048,
                max_connections: Some(2),
                login_timeout_secs: Some(5),
                query_timeout_secs: Some(30),
                unknown_type_fallback_to_varchar: true,
                replace_invalid_utf16: true,
                replace_invalid_utf8: true,
            },
        )
        .unwrap();

        assert_eq!(source.state.batch_size, 7);
        assert_eq!(source.state.max_str_len, 2048);
        assert_eq!(source.state.connection_limiter.max_connections(), 2);
        assert_eq!(source.state.execution_options.login_timeout_secs, Some(5));
        assert_eq!(source.state.execution_options.query_timeout_secs, Some(30));
        assert!(source.state.unknown_type_fallback_to_varchar);
        assert!(source.state.replace_invalid_utf16);
        assert!(source.state.replace_invalid_utf8);
        assert_eq!(source.profile_config(), &Db2ProfileConfig::default());
    }

    #[test]
    fn default_options_match_previous_defaults() {
        assert_eq!(
            Db2Options::default(),
            Db2Options {
                batch_size: DB2_DEFAULT_BATCH_SIZE,
                max_str_len: DB2_DEFAULT_MAX_STR_LEN,
                max_connections: None,
                login_timeout_secs: None,
                query_timeout_secs: None,
                unknown_type_fallback_to_varchar: false,
                replace_invalid_utf16: false,
                replace_invalid_utf8: false,
            }
        );
    }

    #[test]
    fn rejects_oversized_batch_and_buffer_options() {
        let conn = "Driver={IBM DB2 ODBC DRIVER};Database=test;";
        let too_many_rows = match Db2Source::with_options(
            conn,
            1,
            Db2Options {
                batch_size: odbc_core::MAX_BATCH_SIZE + 1,
                ..Db2Options::default()
            },
        ) {
            Ok(_) => panic!("expected oversized Db2 batch size to fail"),
            Err(err) => err.to_string(),
        };
        assert!(
            too_many_rows.contains("DB2_BATCH_SIZE"),
            "{}",
            too_many_rows
        );

        let too_much_buffer = match Db2Source::with_options(
            conn,
            1,
            Db2Options {
                max_str_len: odbc_core::MAX_STR_LEN + 1,
                ..Db2Options::default()
            },
        ) {
            Ok(_) => panic!("expected oversized Db2 max string length to fail"),
            Err(err) => err.to_string(),
        };
        assert!(
            too_much_buffer.contains("DB2_MAX_STR_LEN"),
            "{}",
            too_much_buffer
        );
    }

    #[test]
    fn replace_invalid_encoding_url_options_are_connector_only() {
        let conn = "db2://db2inst1:password@127.0.0.1:50000/testdb?driver=IBM%20DB2%20ODBC%20DRIVER&replace_invalid_utf16=true&replace_invalid_utf8=true&max_connections=3&login_timeout_secs=5&query_timeout_secs=30&db2_profile=sailfish&replication_key_columns=IBMREPKEY1,IBMREPKEY2";
        assert_eq!(
            db2_conn_string(conn).unwrap(),
            "Driver={IBM DB2 ODBC DRIVER};Hostname=127.0.0.1;Port=50000;Protocol=TCPIP;UID=db2inst1;PWD=password;Database=testdb;"
        );

        let source = Db2Source::with_options(conn, 1, Db2Options::default()).unwrap();
        assert!(source.state.replace_invalid_utf16);
        assert!(source.state.replace_invalid_utf8);
        assert_eq!(source.state.connection_limiter.max_connections(), 3);
        assert_eq!(source.state.execution_options.login_timeout_secs, Some(5));
        assert_eq!(source.state.execution_options.query_timeout_secs, Some(30));
        assert_eq!(source.profile_config().profile, Db2Profile::Sailfish);
        assert_eq!(
            source.profile_config().replication_key_columns,
            vec!["IBMREPKEY1".to_string(), "IBMREPKEY2".to_string()]
        );
    }
}
