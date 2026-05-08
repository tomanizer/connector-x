//! Source implementation for IBM Db2 through ODBC.

mod errors;
mod typesystem;

pub use self::errors::Db2SourceError;
pub use self::typesystem::Db2TypeSystem;

use crate::{
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{
        odbc_common::{is_raw_odbc_conn_string, is_valid_odbc_key, odbc_conn_value},
        odbc_core::{self, OdbcCoreError, OdbcTypePolicy},
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
use url::Url;
use urlencoding::decode;

const DB2_DEFAULT_BATCH_SIZE: usize = 1024;
const DB2_DEFAULT_MAX_STR_LEN: usize = 1024;

pub type Db2SourceParser = odbc_core::OdbcParser<Db2TypeSystem, Db2SourceError>;

pub struct Db2Source {
    conn: String,
    origin_query: Option<String>,
    queries: Vec<CXQuery<String>>,
    names: Vec<String>,
    schema: Vec<Db2TypeSystem>,
    batch_size: usize,
    max_str_len: usize,
}

impl Db2Source {
    #[throws(Db2SourceError)]
    pub fn new(conn: &str, _nconn: usize) -> Self {
        Self {
            conn: db2_conn_string(conn)?,
            origin_query: None,
            queries: vec![],
            names: vec![],
            schema: vec![],
            batch_size: odbc_core::env_usize("DB2_BATCH_SIZE").unwrap_or(DB2_DEFAULT_BATCH_SIZE),
            max_str_len: odbc_core::env_usize(Db2TypeSystem::max_str_len_env())
                .unwrap_or(DB2_DEFAULT_MAX_STR_LEN),
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
        let (names, schema) = odbc_core::fetch_metadata::<Db2TypeSystem, Db2SourceError, _>(
            &self.conn,
            &first_query,
            Db2TypeSystem::from_odbc,
        )?;
        self.names = names;
        self.schema = schema;
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
                    &self.schema,
                    self.batch_size,
                    self.max_str_len,
                )
            })
            .collect()
    }
}

pub struct Db2SourcePartition {
    conn: String,
    query: CXQuery<String>,
    schema: Vec<Db2TypeSystem>,
    nrows: usize,
    ncols: usize,
    batch_size: usize,
    max_str_len: usize,
}

impl Db2SourcePartition {
    pub fn new(
        conn: String,
        query: &CXQuery<String>,
        schema: &[Db2TypeSystem],
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
                .map(|ty| ty.buffer_desc(self.max_str_len)),
        )?;
        let cursor = cursor.bind_buffer(buffer)?;
        Db2SourceParser::new(cursor, self.schema.len())
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
            | Db2TypeSystem::Numeric(nullable)
            | Db2TypeSystem::Decimal(nullable)
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
            Db2TypeSystem::Numeric(_)
            | Db2TypeSystem::Decimal(_)
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
        if !matches!(
            key.as_str(),
            "driver" | "Driver" | "protocol" | "Protocol" | "cxprotocol"
        ) {
            if !is_valid_odbc_key(&key) {
                throw!(anyhow!("invalid ODBC connection-string key: {key:?}"));
            }
            ret.push_str(&format!("{}={};", key, odbc_conn_value(&value)));
        }
    }
    ret
}
