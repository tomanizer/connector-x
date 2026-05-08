//! Source implementation for SAP Sybase ASE through ODBC.

mod errors;
mod typesystem;

pub use self::errors::SybaseSourceError;
pub use self::typesystem::SybaseTypeSystem;

use crate::{
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{
        odbc_common::{is_raw_odbc_conn_string, odbc_conn_value},
        odbc_core::{self, OdbcCoreError, OdbcTypePolicy},
        Produce, Source, SourcePartition,
    },
    sql::{count_query, CXQuery},
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use fehler::{throw, throws};
use odbc_api::{
    buffers::{BufferDesc, ColumnarAnyBuffer},
    Cursor,
};
use rust_decimal::Decimal;
use sqlparser::dialect::MsSqlDialect;
use url::Url;
use urlencoding::decode;

const SYBASE_DEFAULT_BATCH_SIZE: usize = 1024;
const SYBASE_DEFAULT_MAX_STR_LEN: usize = 1024;

pub type SybaseSourceParser = odbc_core::OdbcParser<SybaseTypeSystem, SybaseSourceError>;

pub struct SybaseSource {
    conn: String,
    origin_query: Option<String>,
    queries: Vec<CXQuery<String>>,
    names: Vec<String>,
    schema: Vec<SybaseTypeSystem>,
    batch_size: usize,
    max_str_len: usize,
}

impl SybaseSource {
    #[throws(SybaseSourceError)]
    pub fn new(conn: &str, _nconn: usize) -> Self {
        Self {
            conn: sybase_conn_string(conn)?,
            origin_query: None,
            queries: vec![],
            names: vec![],
            schema: vec![],
            batch_size: odbc_core::env_usize("SYBASE_BATCH_SIZE")
                .unwrap_or(SYBASE_DEFAULT_BATCH_SIZE),
            max_str_len: odbc_core::env_usize(SybaseTypeSystem::max_str_len_env())
                .unwrap_or(SYBASE_DEFAULT_MAX_STR_LEN),
        }
    }

    #[throws(SybaseSourceError)]
    fn execute_query(conn: &str, query: &str) -> odbc_core::OdbcCursor {
        odbc_core::execute_query::<SybaseSourceError>(conn, query)?
    }
}

impl Source for SybaseSource
where
    SybaseSourcePartition:
        SourcePartition<TypeSystem = SybaseTypeSystem, Error = SybaseSourceError>,
{
    const DATA_ORDERS: &'static [DataOrder] = &[DataOrder::RowMajor];
    type Partition = SybaseSourcePartition;
    type TypeSystem = SybaseTypeSystem;
    type Error = SybaseSourceError;

    #[throws(SybaseSourceError)]
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

    #[throws(SybaseSourceError)]
    fn fetch_metadata(&mut self) {
        assert!(!self.queries.is_empty());

        let first_query = self.queries[0].to_string();
        let (names, schema) = odbc_core::fetch_metadata::<SybaseTypeSystem, SybaseSourceError, _>(
            &self.conn,
            &first_query,
            SybaseTypeSystem::from_odbc,
        )?;
        self.names = names;
        self.schema = schema;
    }

    #[throws(SybaseSourceError)]
    fn result_rows(&mut self) -> Option<usize> {
        match &self.origin_query {
            Some(q) => Some(odbc_core::fetch_count::<SybaseSourceError, _>(
                &self.conn,
                q,
                &MsSqlDialect {},
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

    #[throws(SybaseSourceError)]
    fn partition(self) -> Vec<Self::Partition> {
        self.queries
            .iter()
            .map(|query| {
                SybaseSourcePartition::new(
                    self.conn.clone(),
                    query,
                    &self.schema,
                    self.batch_size,
                    self.max_str_len,
                )
            })
            .collect()
    }
}

pub struct SybaseSourcePartition {
    conn: String,
    query: CXQuery<String>,
    schema: Vec<SybaseTypeSystem>,
    nrows: usize,
    ncols: usize,
    batch_size: usize,
    max_str_len: usize,
}

impl SybaseSourcePartition {
    pub fn new(
        conn: String,
        query: &CXQuery<String>,
        schema: &[SybaseTypeSystem],
        batch_size: usize,
        max_str_len: usize,
    ) -> Self {
        Self {
            conn,
            query: query.clone(),
            schema: schema.to_vec(),
            nrows: 0,
            ncols: schema.len(),
            batch_size,
            max_str_len,
        }
    }
}

impl SourcePartition for SybaseSourcePartition {
    type TypeSystem = SybaseTypeSystem;
    type Parser<'a> = SybaseSourceParser;
    type Error = SybaseSourceError;

    #[throws(SybaseSourceError)]
    fn result_rows(&mut self) {
        let cquery = count_query(&self.query, &MsSqlDialect {})?;
        self.nrows =
            odbc_core::fetch_count_query::<SybaseSourceError>(&self.conn, cquery.as_str())?;
    }

    #[throws(SybaseSourceError)]
    fn parser(&mut self) -> Self::Parser<'_> {
        let cursor = SybaseSource::execute_query(&self.conn, self.query.as_str())?;
        let buffer = ColumnarAnyBuffer::try_from_descs(
            self.batch_size,
            self.schema
                .iter()
                .map(|ty| ty.buffer_desc(self.max_str_len)),
        )?;
        let cursor = cursor.bind_buffer(buffer)?;
        SybaseSourceParser::new(cursor, self.schema.len())
    }

    fn nrows(&self) -> usize {
        self.nrows
    }

    fn ncols(&self) -> usize {
        self.ncols
    }
}

impl OdbcCoreError for SybaseSourceError {
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

impl OdbcTypePolicy for SybaseTypeSystem {
    fn source_name() -> &'static str {
        "Sybase"
    }

    fn max_str_len_env() -> &'static str {
        "SYBASE_MAX_STR_LEN"
    }

    fn nullable(self) -> bool {
        match self {
            SybaseTypeSystem::TinyInt(nullable)
            | SybaseTypeSystem::SmallInt(nullable)
            | SybaseTypeSystem::Int(nullable)
            | SybaseTypeSystem::BigInt(nullable)
            | SybaseTypeSystem::Real(nullable)
            | SybaseTypeSystem::Double(nullable)
            | SybaseTypeSystem::Numeric(nullable)
            | SybaseTypeSystem::Decimal(nullable)
            | SybaseTypeSystem::Bit(nullable)
            | SybaseTypeSystem::Char(nullable)
            | SybaseTypeSystem::Varchar(nullable)
            | SybaseTypeSystem::Text(nullable)
            | SybaseTypeSystem::Binary(nullable)
            | SybaseTypeSystem::Date(nullable)
            | SybaseTypeSystem::Time(nullable)
            | SybaseTypeSystem::Timestamp(nullable) => nullable,
        }
    }

    fn buffer_desc(self, max_str_len: usize) -> BufferDesc {
        let nullable = self.nullable();
        match self {
            SybaseTypeSystem::TinyInt(_) => BufferDesc::U8 { nullable },
            SybaseTypeSystem::SmallInt(_) => BufferDesc::I16 { nullable },
            SybaseTypeSystem::Int(_) => BufferDesc::I32 { nullable },
            SybaseTypeSystem::BigInt(_) => BufferDesc::I64 { nullable },
            SybaseTypeSystem::Real(_) => BufferDesc::F32 { nullable },
            SybaseTypeSystem::Double(_) => BufferDesc::F64 { nullable },
            SybaseTypeSystem::Bit(_) => BufferDesc::Bit { nullable },
            SybaseTypeSystem::Numeric(_)
            | SybaseTypeSystem::Decimal(_)
            | SybaseTypeSystem::Char(_)
            | SybaseTypeSystem::Varchar(_)
            | SybaseTypeSystem::Text(_)
            | SybaseTypeSystem::Binary(_)
            | SybaseTypeSystem::Date(_)
            | SybaseTypeSystem::Time(_)
            | SybaseTypeSystem::Timestamp(_) => BufferDesc::Text { max_str_len },
        }
    }
}

odbc_core::impl_parse_from_bytes!(
    SybaseSourceParser,
    SybaseSourceError,
    Decimal,
    "Decimal",
    parse_decimal
);
odbc_core::impl_parse_from_bytes!(
    SybaseSourceParser,
    SybaseSourceError,
    NaiveDate,
    "NaiveDate",
    parse_date
);
odbc_core::impl_parse_from_bytes!(
    SybaseSourceParser,
    SybaseSourceError,
    NaiveTime,
    "NaiveTime",
    parse_time
);
odbc_core::impl_parse_from_bytes!(
    SybaseSourceParser,
    SybaseSourceError,
    NaiveDateTime,
    "NaiveDateTime",
    parse_timestamp
);
odbc_core::impl_parse_from_cell!(SybaseSourceParser, SybaseSourceError, u8, "u8", cell_u8);
odbc_core::impl_parse_from_cell!(SybaseSourceParser, SybaseSourceError, i16, "i16", cell_i16);
odbc_core::impl_parse_from_cell!(SybaseSourceParser, SybaseSourceError, i32, "i32", cell_i32);
odbc_core::impl_parse_from_cell!(SybaseSourceParser, SybaseSourceError, i64, "i64", cell_i64);
odbc_core::impl_parse_from_cell!(SybaseSourceParser, SybaseSourceError, f32, "f32", cell_f32);
odbc_core::impl_parse_from_cell!(SybaseSourceParser, SybaseSourceError, f64, "f64", cell_f64);
odbc_core::impl_bool_produce!(SybaseSourceParser, SybaseSourceError);
odbc_core::impl_string_produce!(SybaseSourceParser, SybaseSourceError);

impl<'r> Produce<'r, Vec<u8>> for SybaseSourceParser {
    type Error = SybaseSourceError;

    fn produce(&'r mut self) -> Result<Vec<u8>, Self::Error> {
        parse_hex_bytes(self.required_bytes::<Vec<u8>>("Vec<u8>")?)
    }
}

impl<'r> Produce<'r, Option<Vec<u8>>> for SybaseSourceParser {
    type Error = SybaseSourceError;

    fn produce(&'r mut self) -> Result<Option<Vec<u8>>, Self::Error> {
        match self.next_bytes::<Vec<u8>>()? {
            Some(bytes) => Ok(Some(parse_hex_bytes(bytes)?)),
            None => Ok(None),
        }
    }
}

fn parse_hex_bytes(bytes: &[u8]) -> Result<Vec<u8>, SybaseSourceError> {
    let bytes = odbc_core::trim_ascii(bytes);
    if bytes.len() % 2 != 0 {
        return Err(SybaseSourceError::ParseValue {
            value: odbc_core::bytes_to_string(bytes),
            ty: "hex bytes",
        });
    }

    bytes
        .chunks_exact(2)
        .map(|chunk| {
            let hi = hex_value(chunk[0]).ok_or_else(|| SybaseSourceError::ParseValue {
                value: odbc_core::bytes_to_string(bytes),
                ty: "hex bytes",
            })?;
            let lo = hex_value(chunk[1]).ok_or_else(|| SybaseSourceError::ParseValue {
                value: odbc_core::bytes_to_string(bytes),
                ty: "hex bytes",
            })?;
            Ok((hi << 4) | lo)
        })
        .collect()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(crate) fn fetch_i64_pair(conn: &str, query: &str) -> Result<(i64, i64), SybaseSourceError> {
    odbc_core::fetch_i64_pair::<SybaseSourceError>(conn, query)
}

#[throws(SybaseSourceError)]
pub fn sybase_conn_string(conn: &str) -> String {
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
        .unwrap_or_else(|| "FreeTDS".to_string());
    let host = decode(url.host_str().unwrap_or("localhost"))?.into_owned();
    let port = url.port().unwrap_or(5000);
    let database = decode(url.path().trim_start_matches('/'))?.into_owned();
    let username = decode(url.username())?.into_owned();
    let password = decode(url.password().unwrap_or(""))?.into_owned();
    let tds_version = params
        .get("tds_version")
        .or_else(|| params.get("TDS_Version"))
        .cloned()
        .unwrap_or_else(|| "5.0".to_string());

    let mut ret = format!(
        "Driver={};Server={};Port={};TDS_Version={};UID={};PWD={};",
        odbc_conn_value(&driver),
        odbc_conn_value(&host),
        port,
        odbc_conn_value(&tds_version),
        odbc_conn_value(&username),
        odbc_conn_value(&password)
    );
    if !database.is_empty() {
        ret.push_str(&format!("Database={};", odbc_conn_value(&database)));
    }
    ret
}
