//! Source implementation for IBM Db2 through ODBC.

mod errors;
mod typesystem;

pub use self::errors::Db2SourceError;
pub use self::typesystem::Db2TypeSystem;

use self::typesystem::DB2_UNKNOWN_TYPE_FALLBACK_ENV;
use crate::{
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{
        odbc_common::{
            connection_bool_param, is_connector_option_key, is_raw_odbc_conn_string,
            is_valid_odbc_key, odbc_conn_value, url_bool_param, REPLACE_INVALID_UTF16_PARAM,
        },
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

const DB2_DEFAULT_BATCH_SIZE: usize = 1024;
const DB2_DEFAULT_MAX_STR_LEN: usize = 1024;

pub type Db2SourceParser = odbc_core::OdbcParser<Db2TypeSystem, Db2SourceError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Db2Options {
    pub batch_size: usize,
    pub max_str_len: usize,
    pub unknown_type_fallback_to_varchar: bool,
    pub replace_invalid_utf16: bool,
}

impl Db2Options {
    pub fn from_env() -> Self {
        Self {
            batch_size: odbc_core::env_usize("DB2_BATCH_SIZE").unwrap_or(DB2_DEFAULT_BATCH_SIZE),
            max_str_len: odbc_core::env_usize(Db2TypeSystem::max_str_len_env())
                .unwrap_or(DB2_DEFAULT_MAX_STR_LEN),
            unknown_type_fallback_to_varchar: odbc_core::env_bool(DB2_UNKNOWN_TYPE_FALLBACK_ENV)
                .unwrap_or(false),
            replace_invalid_utf16: false,
        }
    }
}

impl Default for Db2Options {
    fn default() -> Self {
        Self {
            batch_size: DB2_DEFAULT_BATCH_SIZE,
            max_str_len: DB2_DEFAULT_MAX_STR_LEN,
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
    unknown_type_fallback_to_varchar: bool,
    replace_invalid_utf16: bool,
}

impl Db2Source {
    #[throws(Db2SourceError)]
    pub fn new(conn: &str, nconn: usize) -> Self {
        Self::with_options(conn, nconn, Db2Options::from_env())?
    }

    #[throws(Db2SourceError)]
    pub fn with_options(conn: &str, _nconn: usize, options: Db2Options) -> Self {
        let replace_invalid_utf16 = connection_bool_param(conn, REPLACE_INVALID_UTF16_PARAM)?
            .unwrap_or(options.replace_invalid_utf16);
        Self {
            conn: db2_conn_string(conn)?,
            origin_query: None,
            queries: vec![],
            names: vec![],
            schema: vec![],
            column_buffer_max_lens: vec![],
            batch_size: options.batch_size,
            max_str_len: options.max_str_len,
            unknown_type_fallback_to_varchar: options.unknown_type_fallback_to_varchar,
            replace_invalid_utf16,
        }
    }

    #[throws(Db2SourceError)]
    fn execute_query(conn: &str, query: &str) -> odbc_core::OdbcCursor {
        odbc_core::execute_query::<Db2SourceError>(conn, query)?
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
                &self.conn,
                &first_query,
                self.max_str_len,
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
    replace_invalid_utf16: bool,
}

impl Db2SourcePartition {
    pub fn new(
        conn: String,
        query: &CXQuery<String>,
        names: &[String],
        schema: &[Db2TypeSystem],
        column_buffer_max_lens: &[usize],
        batch_size: usize,
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
        self.nrows = odbc_core::fetch_count_query::<Db2SourceError>(&self.conn, cquery.as_str())?;
    }

    #[throws(Db2SourceError)]
    fn parser(&mut self) -> Self::Parser<'_> {
        let cursor = Db2Source::execute_query(&self.conn, self.query.as_str())?;
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

pub(crate) fn fetch_i64_pair(conn: &str, query: &str) -> Result<(i64, i64), Db2SourceError> {
    odbc_core::fetch_i64_pair::<Db2SourceError>(conn, query)
}

#[cfg(feature = "dst_arrow")]
pub(crate) fn db2_get_arrow(
    conn: &Url,
    origin_query: Option<String>,
    queries: &[CXQuery<String>],
) -> OutResult<ArrowDestination> {
    let options = Db2Options::from_env();
    let conn_str = db2_conn_string(&conn[..])?;
    let unknown_type_fallback_to_varchar = options.unknown_type_fallback_to_varchar;
    let replace_invalid_utf16 =
        url_bool_param(conn, REPLACE_INVALID_UTF16_PARAM)?.unwrap_or(options.replace_invalid_utf16);
    Ok(odbc_core::odbc_get_arrow_impl::<
        Db2TypeSystem,
        Db2SourceError,
    >(
        &conn_str,
        origin_query,
        queries,
        options.max_str_len,
        options.batch_size,
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

odbc_core::impl_odbc_arrow_policy!(Db2TypeSystem);

#[throws(Db2SourceError)]
pub fn db2_conn_string(conn: &str) -> String {
    if is_raw_odbc_conn_string(conn) {
        return conn.to_string();
    }

    let url = Url::parse(conn)?;
    let params = url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect::<std::collections::HashMap<_, _>>();

    let driver = params
        .get("driver")
        .cloned()
        .unwrap_or_else(|| "IBM DB2 ODBC DRIVER".to_string());
    let host = decode(url.host_str().unwrap_or("localhost"))?.into_owned();
    let port = url.port().unwrap_or(50000);
    let database = decode(url.path().trim_start_matches('/'))?.into_owned();
    let username = decode(url.username())?.into_owned();
    let password = decode(url.password().unwrap_or(""))?.into_owned();
    let protocol = params
        .get("protocol")
        .or_else(|| params.get("Protocol"))
        .cloned()
        .unwrap_or_else(|| "TCPIP".to_string());

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
    for (key, value) in params {
        if !is_connector_option_key(&key)
            && !matches!(key.as_str(), "driver" | "Driver" | "protocol" | "Protocol")
        {
            if !is_valid_odbc_key(&key) {
                throw!(anyhow!("invalid ODBC connection-string key: {key:?}"));
            }
            ret.push_str(&format!("{}={};", key, odbc_conn_value(&value)));
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
                unknown_type_fallback_to_varchar: true,
                replace_invalid_utf16: true,
            },
        )
        .unwrap();

        assert_eq!(source.batch_size, 7);
        assert_eq!(source.max_str_len, 2048);
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
                unknown_type_fallback_to_varchar: false,
                replace_invalid_utf16: false,
            }
        );
    }

    #[test]
    fn replace_invalid_utf16_url_option_is_connector_only() {
        let conn = "db2://db2inst1:password@127.0.0.1:50000/testdb?driver=IBM%20DB2%20ODBC%20DRIVER&replace_invalid_utf16=true";
        assert_eq!(
            db2_conn_string(conn).unwrap(),
            "Driver={IBM DB2 ODBC DRIVER};Hostname={127.0.0.1};Port=50000;Protocol={TCPIP};UID={db2inst1};PWD={password};Database={testdb};"
        );

        let source = Db2Source::with_options(conn, 1, Db2Options::default()).unwrap();
        assert!(source.replace_invalid_utf16);
    }
}
