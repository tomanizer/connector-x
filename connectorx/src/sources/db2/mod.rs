//! Source implementation for IBM Db2 through ODBC.

mod errors;
mod typesystem;

pub use self::errors::Db2SourceError;
pub use self::typesystem::Db2TypeSystem;

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
            is_valid_odbc_key, odbc_conn_value, param_bool_param, param_u32_param,
            param_usize_param, param_value, url_query_pairs, LOGIN_TIMEOUT_SECS_PARAM,
            MAX_CONNECTIONS_PARAM, QUERY_TIMEOUT_SECS_PARAM, REPLACE_INVALID_UTF16_PARAM,
        },
        odbc_core::{self, OdbcCoreError, OdbcExecutionOptions, OdbcTypePolicy},
        Source, SourcePartition,
    },
    sql::{count_query, CXQuery},
};
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
        }
    }
}

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
        }
    }
}

pub struct Db2Source {
    conn: String,
    origin_query: Option<String>,
    queries: Vec<CXQuery<String>>,
    names: Vec<String>,
    schema: Vec<Db2TypeSystem>,
    column_buffer_max_lens: Vec<usize>,
    batch_size: usize,
    max_str_len: usize,
    connection_limiter: Arc<odbc_core::OdbcConnectionLimiter>,
    execution_options: OdbcExecutionOptions,
    unknown_type_fallback_to_varchar: bool,
    replace_invalid_utf16: bool,
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
        let replace_invalid_utf16 = params
            .map(|params| param_bool_param(params, REPLACE_INVALID_UTF16_PARAM))
            .transpose()?
            .flatten()
            .unwrap_or(options.replace_invalid_utf16);
        let max_connections = params
            .map(|params| param_usize_param(params, MAX_CONNECTIONS_PARAM))
            .transpose()?
            .flatten()
            .or(options.max_connections);
        let connection_limiter = odbc_core::connection_limiter(max_connections, nconn)?;
        let execution_options = db2_execution_options_from_params(params, options)?;
        Self {
            conn: db2_conn_string(conn)?,
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

    #[throws(Db2SourceError)]
    fn execute_query(
        conn: &str,
        query: &str,
        execution_options: OdbcExecutionOptions,
    ) -> odbc_core::OdbcCursor {
        odbc_core::execute_query::<Db2SourceError>(
            Db2TypeSystem::source_name(),
            conn,
            query,
            execution_options,
        )?
    }
}

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
        self.queries = queries.iter().map(|q| q.map(Q::to_string)).collect();
    }

    fn set_origin_query(&mut self, query: Option<String>) {
        self.origin_query = query;
    }

    #[throws(Db2SourceError)]
    fn fetch_metadata(&mut self) {
        assert!(!self.queries.is_empty());

        let first_query = self.queries[0].to_string();
        let unknown_type_fallback_to_varchar = self.unknown_type_fallback_to_varchar;
        let (names, schema, column_buffer_max_lens) =
            odbc_core::fetch_metadata::<Db2TypeSystem, Db2SourceError, _>(
                Db2TypeSystem::source_name(),
                &self.conn,
                &first_query,
                self.max_str_len,
                &self.connection_limiter,
                self.execution_options,
                |data_type, nullability, column_name| {
                    Db2TypeSystem::from_odbc(
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

    #[throws(Db2SourceError)]
    fn result_rows(&mut self) -> Option<usize> {
        match &self.origin_query {
            Some(q) => Some(odbc_core::fetch_count::<Db2SourceError, _>(
                Db2TypeSystem::source_name(),
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

    #[throws(Db2SourceError)]
    fn partition(self) -> Vec<Self::Partition> {
        self.queries
            .iter()
            .map(|query| {
                Db2SourcePartition::new(
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

pub struct Db2SourcePartition {
    conn: String,
    query: CXQuery<String>,
    names: Arc<[String]>,
    schema: Arc<[Db2TypeSystem]>,
    column_buffer_max_lens: Vec<usize>,
    nrows: usize,
    ncols: usize,
    batch_size: usize,
    connection_limiter: Arc<odbc_core::OdbcConnectionLimiter>,
    execution_options: OdbcExecutionOptions,
    replace_invalid_utf16: bool,
}

impl Db2SourcePartition {
    pub(crate) fn new(
        conn: String,
        query: &CXQuery<String>,
        names: &[String],
        schema: &[Db2TypeSystem],
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

impl SourcePartition for Db2SourcePartition {
    type TypeSystem = Db2TypeSystem;
    type Parser<'a> = Db2SourceParser;
    type Error = Db2SourceError;

    #[throws(Db2SourceError)]
    fn result_rows(&mut self) {
        let cquery = count_query(&self.query, &GenericDialect {})?;
        self.nrows = odbc_core::fetch_count_query::<Db2SourceError>(
            Db2TypeSystem::source_name(),
            &self.conn,
            cquery.as_str(),
            &self.connection_limiter,
            self.execution_options,
        )?;
    }

    #[throws(Db2SourceError)]
    fn parser(&mut self) -> Self::Parser<'_> {
        let connection_permit = self.connection_limiter.acquire();
        let cursor =
            Db2Source::execute_query(&self.conn, self.query.as_str(), self.execution_options)?;
        let buffer = ColumnarAnyBuffer::try_from_descs(
            self.batch_size,
            self.schema
                .iter()
                .zip(&self.column_buffer_max_lens)
                .map(|(ty, max_len)| ty.buffer_desc(*max_len)),
        )?;
        let cursor = cursor.bind_buffer(buffer)?;
        Db2SourceParser::new(
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
    db2_execution_options_from_params(params.as_deref(), options)?
}

#[throws(Db2SourceError)]
fn db2_execution_options_from_params(
    params: Option<&[(String, String)]>,
    options: Db2Options,
) -> OdbcExecutionOptions {
    OdbcExecutionOptions::new(
        params
            .map(|params| param_u32_param(params, LOGIN_TIMEOUT_SECS_PARAM))
            .transpose()?
            .flatten()
            .or(options.login_timeout_secs),
        params
            .map(|params| param_usize_param(params, QUERY_TIMEOUT_SECS_PARAM))
            .transpose()?
            .flatten()
            .or(options.query_timeout_secs),
    )?
}

#[cfg(feature = "dst_arrow")]
pub(crate) fn db2_get_arrow(
    conn: &Url,
    origin_query: Option<String>,
    queries: &[CXQuery<String>],
) -> OutResult<ArrowDestination> {
    let options = Db2Options::from_env();
    validate_db2_options(&options)?;
    let params = url_query_pairs(conn)?;
    let conn_str = db2_conn_string(&conn[..])?;
    let unknown_type_fallback_to_varchar = options.unknown_type_fallback_to_varchar;
    let replace_invalid_utf16 = param_bool_param(&params, REPLACE_INVALID_UTF16_PARAM)?
        .unwrap_or(options.replace_invalid_utf16);
    let max_connections =
        param_usize_param(&params, MAX_CONNECTIONS_PARAM)?.or(options.max_connections);
    let connection_limiter = odbc_core::connection_limiter(max_connections, queries.len())?;
    let execution_options = db2_execution_options_from_params(Some(&params), options)?;
    Ok(odbc_core::odbc_get_arrow_impl::<
        Db2TypeSystem,
        Db2SourceError,
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
    let replace_invalid_utf16 = param_bool_param(&params, REPLACE_INVALID_UTF16_PARAM)?
        .unwrap_or(options.replace_invalid_utf16);
    let max_connections =
        param_usize_param(&params, MAX_CONNECTIONS_PARAM)?.or(options.max_connections);
    let connection_limiter = odbc_core::connection_limiter(max_connections, queries.len())?;
    let execution_options = db2_execution_options_from_params(Some(&params), options)?;
    let iterator = odbc_core::odbc_record_batch_iter_impl::<Db2TypeSystem, Db2SourceError>(
        &conn_str,
        origin_query,
        queries,
        options.max_str_len,
        batch_size,
        connection_limiter,
        execution_options,
        replace_invalid_utf16,
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
        odbc_conn_value(&driver),
        odbc_conn_value(&host),
        port,
        odbc_conn_value(&protocol),
        odbc_conn_value(&username),
        odbc_conn_value(&password)
    );
    if !database.is_empty() {
        ret.push_str(&format!("Database={};", odbc_conn_value(&database)));
    }
    for (key, value) in &params {
        if !is_connector_option_key(key)
            && !key.eq_ignore_ascii_case("driver")
            && !key.eq_ignore_ascii_case("protocol")
        {
            if !is_valid_odbc_key(key) {
                throw!(anyhow!("invalid ODBC connection-string key: {key:?}"));
            }
            ret.push_str(&format!("{}={};", key, odbc_conn_value(value)));
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
            "Driver={IBM DB2 ODBC DRIVER};Database=test;",
            1,
            Db2Options {
                batch_size: 7,
                max_str_len: 2048,
                max_connections: Some(2),
                login_timeout_secs: Some(5),
                query_timeout_secs: Some(30),
                unknown_type_fallback_to_varchar: true,
                replace_invalid_utf16: true,
            },
        )
        .unwrap();

        assert_eq!(source.batch_size, 7);
        assert_eq!(source.max_str_len, 2048);
        assert_eq!(source.connection_limiter.max_connections(), 2);
        assert_eq!(source.execution_options.login_timeout_secs, Some(5));
        assert_eq!(source.execution_options.query_timeout_secs, Some(30));
        assert!(source.unknown_type_fallback_to_varchar);
        assert!(source.replace_invalid_utf16);
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
    fn replace_invalid_utf16_url_option_is_connector_only() {
        let conn = "db2://db2inst1:password@127.0.0.1:50000/testdb?driver=IBM%20DB2%20ODBC%20DRIVER&replace_invalid_utf16=true&max_connections=3&login_timeout_secs=5&query_timeout_secs=30";
        assert_eq!(
            db2_conn_string(conn).unwrap(),
            "Driver={IBM DB2 ODBC DRIVER};Hostname={127.0.0.1};Port=50000;Protocol={TCPIP};UID={db2inst1};PWD={password};Database={testdb};"
        );

        let source = Db2Source::with_options(conn, 1, Db2Options::default()).unwrap();
        assert!(source.replace_invalid_utf16);
        assert_eq!(source.connection_limiter.max_connections(), 3);
        assert_eq!(source.execution_options.login_timeout_secs, Some(5));
        assert_eq!(source.execution_options.query_timeout_secs, Some(30));
    }
}
