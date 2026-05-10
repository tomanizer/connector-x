//! Source implementation for generic ODBC.

mod errors;
mod typesystem;

pub use self::errors::OdbcSourceError;
pub use self::typesystem::OdbcTypeSystem;

use crate::{
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{
        odbc_common::{is_raw_odbc_conn_string, is_valid_odbc_key, push_odbc_pair},
        odbc_core::{self, OdbcCoreError, OdbcTypePolicy},
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
}

impl OdbcOptions {
    pub fn from_env() -> Self {
        Self {
            batch_size: odbc_core::env_usize("ODBC_BATCH_SIZE").unwrap_or(ODBC_DEFAULT_BATCH_SIZE),
            max_str_len: odbc_core::env_usize(OdbcTypeSystem::max_str_len_env())
                .unwrap_or(ODBC_DEFAULT_MAX_STR_LEN),
        }
    }
}

impl Default for OdbcOptions {
    fn default() -> Self {
        Self {
            batch_size: ODBC_DEFAULT_BATCH_SIZE,
            max_str_len: ODBC_DEFAULT_MAX_STR_LEN,
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
}

impl OdbcSource {
    #[throws(OdbcSourceError)]
    pub fn new(conn: &str, nconn: usize) -> Self {
        Self::with_options(conn, nconn, OdbcOptions::from_env())?
    }

    #[throws(OdbcSourceError)]
    pub fn with_options(conn: &str, _nconn: usize, options: OdbcOptions) -> Self {
        Self {
            conn: odbc_conn_string(conn)?,
            origin_query: None,
            queries: vec![],
            names: vec![],
            schema: vec![],
            column_buffer_max_lens: vec![],
            batch_size: options.batch_size,
            max_str_len: options.max_str_len,
        }
    }

    #[throws(OdbcSourceError)]
    fn execute_query(conn: &str, query: &str) -> odbc_core::OdbcCursor {
        odbc_core::execute_query::<OdbcSourceError>(conn, query)?
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
        let (names, schema, column_buffer_max_lens) =
            odbc_core::fetch_metadata::<OdbcTypeSystem, OdbcSourceError, _>(
                &self.conn,
                &first_query,
                self.max_str_len,
                OdbcTypeSystem::from_odbc,
            )?;
        self.names = names;
        self.schema = schema;
        self.column_buffer_max_lens = column_buffer_max_lens;
    }

    #[throws(OdbcSourceError)]
    fn result_rows(&mut self) -> Option<usize> {
        match &self.origin_query {
            Some(q) => Some(odbc_core::fetch_count::<OdbcSourceError, _>(
                &self.conn,
                q,
                &GenericDialect {},
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
}

impl OdbcSourcePartition {
    pub fn new(
        conn: String,
        query: &CXQuery<String>,
        names: &[String],
        schema: &[OdbcTypeSystem],
        column_buffer_max_lens: &[usize],
        batch_size: usize,
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
        self.nrows = odbc_core::fetch_count_query::<OdbcSourceError>(&self.conn, cquery.as_str())?;
    }

    #[throws(OdbcSourceError)]
    fn parser(&mut self) -> Self::Parser<'_> {
        let cursor = OdbcSource::execute_query(&self.conn, self.query.as_str())?;
        let buffer = ColumnarAnyBuffer::try_from_descs(
            self.batch_size,
            self.schema
                .iter()
                .zip(&self.column_buffer_max_lens)
                .map(|(ty, max_len)| ty.buffer_desc(*max_len)),
        )?;
        let cursor = cursor.bind_buffer(buffer)?;
        OdbcSourceParser::new(cursor, Arc::clone(&self.names), Arc::clone(&self.schema))
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
    Ok(odbc_core::odbc_get_arrow_impl::<
        OdbcTypeSystem,
        OdbcSourceError,
    >(
        &conn_str,
        origin_query,
        queries,
        options.max_str_len,
        options.batch_size,
        OdbcTypeSystem::from_odbc,
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
            | OdbcTypeSystem::Numeric(nullable)
            | OdbcTypeSystem::Decimal(nullable)
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
            OdbcTypeSystem::Numeric(_)
            | OdbcTypeSystem::Decimal(_)
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

pub(crate) fn fetch_i64_pair(conn: &str, query: &str) -> Result<(i64, i64), OdbcSourceError> {
    odbc_core::fetch_i64_pair::<OdbcSourceError>(conn, query)
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
        if !matches!(
            key.to_ascii_lowercase().as_str(),
            "driver" | "dsn" | "server_key" | "cxprotocol"
        ) {
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
            },
        )
        .unwrap();
        let large = OdbcSource::with_options(
            conn,
            1,
            OdbcOptions {
                batch_size: 32,
                max_str_len: 4096,
            },
        )
        .unwrap();

        assert_eq!(small.batch_size, 2);
        assert_eq!(small.max_str_len, 8);
        assert_eq!(large.batch_size, 32);
        assert_eq!(large.max_str_len, 4096);
    }

    #[test]
    fn default_options_match_previous_defaults() {
        assert_eq!(
            OdbcOptions::default(),
            OdbcOptions {
                batch_size: ODBC_DEFAULT_BATCH_SIZE,
                max_str_len: ODBC_DEFAULT_MAX_STR_LEN,
            }
        );
    }
}
