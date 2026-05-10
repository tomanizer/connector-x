//! Source implementation for SAP Sybase ASE through ODBC.

mod errors;
mod typesystem;

pub use self::errors::SybaseSourceError;
pub use self::typesystem::SybaseTypeSystem;

use self::typesystem::SYBASE_UNKNOWN_TYPE_FALLBACK_ENV;
use crate::{
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{
        odbc_common::{
            connection_bool_param, is_raw_odbc_conn_string, odbc_conn_value, url_bool_param,
            REPLACE_INVALID_UTF16_PARAM,
        },
        odbc_core::{self, OdbcCoreError, OdbcTypePolicy},
        Produce, Source, SourcePartition,
    },
    sql::{count_query, CXQuery},
};
#[cfg(feature = "dst_arrow")]
use crate::{destinations::arrow::ArrowDestination, errors::OutResult};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use fehler::{throw, throws};
use odbc_api::{
    buffers::{BufferDesc, ColumnarAnyBuffer},
    Cursor,
};
use rust_decimal::Decimal;
use sqlparser::dialect::MsSqlDialect;
use std::sync::Arc;
use url::Url;
use urlencoding::decode;

const SYBASE_DEFAULT_BATCH_SIZE: usize = 1024;
const SYBASE_DEFAULT_MAX_STR_LEN: usize = 1024;

pub type SybaseSourceParser = odbc_core::OdbcParser<SybaseTypeSystem, SybaseSourceError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SybaseOptions {
    pub batch_size: usize,
    pub max_str_len: usize,
    pub unknown_type_fallback_to_varchar: bool,
    pub replace_invalid_utf16: bool,
}

impl SybaseOptions {
    pub fn from_env() -> Self {
        Self {
            batch_size: odbc_core::env_usize("SYBASE_BATCH_SIZE")
                .unwrap_or(SYBASE_DEFAULT_BATCH_SIZE),
            max_str_len: odbc_core::env_usize(SybaseTypeSystem::max_str_len_env())
                .unwrap_or(SYBASE_DEFAULT_MAX_STR_LEN),
            unknown_type_fallback_to_varchar: odbc_core::env_bool(SYBASE_UNKNOWN_TYPE_FALLBACK_ENV)
                .unwrap_or(false),
            replace_invalid_utf16: false,
        }
    }
}

impl Default for SybaseOptions {
    fn default() -> Self {
        Self {
            batch_size: SYBASE_DEFAULT_BATCH_SIZE,
            max_str_len: SYBASE_DEFAULT_MAX_STR_LEN,
            unknown_type_fallback_to_varchar: false,
            replace_invalid_utf16: false,
        }
    }
}

pub struct SybaseSource {
    conn: String,
    origin_query: Option<String>,
    queries: Vec<CXQuery<String>>,
    names: Vec<String>,
    schema: Vec<SybaseTypeSystem>,
    column_buffer_max_lens: Vec<usize>,
    batch_size: usize,
    max_str_len: usize,
    unknown_type_fallback_to_varchar: bool,
    replace_invalid_utf16: bool,
}

impl SybaseSource {
    #[throws(SybaseSourceError)]
    pub fn new(conn: &str, nconn: usize) -> Self {
        Self::with_options(conn, nconn, SybaseOptions::from_env())?
    }

    #[throws(SybaseSourceError)]
    pub fn with_options(conn: &str, _nconn: usize, options: SybaseOptions) -> Self {
        let replace_invalid_utf16 = connection_bool_param(conn, REPLACE_INVALID_UTF16_PARAM)?
            .unwrap_or(options.replace_invalid_utf16);
        Self {
            conn: sybase_conn_string(conn)?,
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
        let unknown_type_fallback_to_varchar = self.unknown_type_fallback_to_varchar;
        let (names, schema, column_buffer_max_lens) =
            odbc_core::fetch_metadata::<SybaseTypeSystem, SybaseSourceError, _>(
                &self.conn,
                &first_query,
                self.max_str_len,
                |data_type, nullability, column_name| {
                    SybaseTypeSystem::from_odbc(
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

pub struct SybaseSourcePartition {
    conn: String,
    query: CXQuery<String>,
    names: Arc<[String]>,
    schema: Arc<[SybaseTypeSystem]>,
    column_buffer_max_lens: Vec<usize>,
    nrows: usize,
    ncols: usize,
    batch_size: usize,
    replace_invalid_utf16: bool,
}

impl SybaseSourcePartition {
    pub fn new(
        conn: String,
        query: &CXQuery<String>,
        names: &[String],
        schema: &[SybaseTypeSystem],
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
                .zip(&self.column_buffer_max_lens)
                .map(|(ty, max_len)| ty.buffer_desc(*max_len)),
        )?;
        let cursor = cursor.bind_buffer(buffer)?;
        SybaseSourceParser::new(
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
            | SybaseTypeSystem::Numeric(nullable, ..)
            | SybaseTypeSystem::Decimal(nullable, ..)
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
            SybaseTypeSystem::Numeric(..)
            | SybaseTypeSystem::Decimal(..)
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
    parse_hex_bytes_generic::<SybaseSourceError>(bytes)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(feature = "dst_arrow")]
use {
    crate::{
        destinations::arrow::ArrowTypeSystem,
        sources::odbc_core::{
            build_bool_array, build_date32_array, build_decimal_array, build_float32_array,
            build_float64_array, build_int64_array, build_string_array, build_time64_micro_array,
            build_timestamp_micro_array, require_nullable, OdbcArrowPolicy,
        },
    },
    arrow::array::{ArrayRef, LargeBinaryBuilder},
    arrow::datatypes::{DataType as ArrowDataType, TimeUnit},
    odbc_api::buffers::AnySlice,
};

pub(crate) fn fetch_i64_pair(conn: &str, query: &str) -> Result<(i64, i64), SybaseSourceError> {
    odbc_core::fetch_i64_pair::<SybaseSourceError>(conn, query)
}

#[cfg(feature = "dst_arrow")]
pub(crate) fn sybase_get_arrow(
    conn: &Url,
    origin_query: Option<String>,
    queries: &[CXQuery<String>],
) -> OutResult<ArrowDestination> {
    let options = SybaseOptions::from_env();
    let conn_str = sybase_conn_string(&conn[..])?;
    let unknown_type_fallback_to_varchar = options.unknown_type_fallback_to_varchar;
    let replace_invalid_utf16 =
        url_bool_param(conn, REPLACE_INVALID_UTF16_PARAM)?.unwrap_or(options.replace_invalid_utf16);
    Ok(odbc_core::odbc_get_arrow_impl::<
        SybaseTypeSystem,
        SybaseSourceError,
    >(
        &conn_str,
        origin_query,
        queries,
        options.max_str_len,
        options.batch_size,
        replace_invalid_utf16,
        move |data_type, nullability, column_name| {
            SybaseTypeSystem::from_odbc(
                data_type,
                nullability,
                column_name,
                unknown_type_fallback_to_varchar,
            )
            .map_err(Into::into)
        },
    )?)
}

/// Manual `OdbcArrowPolicy` for `SybaseTypeSystem`.
///
/// All variants are identical to the generic `impl_odbc_arrow_policy!` except
/// `Binary`, which must hex-decode text buffers because FreeTDS surfaces binary
/// values as ASCII hex strings (e.g. `"ABCD"` → `[0xAB, 0xCD]`).
#[cfg(feature = "dst_arrow")]
impl OdbcArrowPolicy for SybaseTypeSystem {
    fn arrow_type(self) -> ArrowTypeSystem {
        let nullable = OdbcTypePolicy::nullable(self);
        match self {
            SybaseTypeSystem::TinyInt(..)
            | SybaseTypeSystem::SmallInt(..)
            | SybaseTypeSystem::Int(..)
            | SybaseTypeSystem::BigInt(..) => ArrowTypeSystem::Int64(nullable),
            SybaseTypeSystem::Real(..) => ArrowTypeSystem::Float32(nullable),
            SybaseTypeSystem::Double(..) => ArrowTypeSystem::Float64(nullable),
            SybaseTypeSystem::Numeric(..) | SybaseTypeSystem::Decimal(..) => {
                ArrowTypeSystem::Decimal(nullable)
            }
            SybaseTypeSystem::Bit(..) => ArrowTypeSystem::Boolean(nullable),
            SybaseTypeSystem::Char(..)
            | SybaseTypeSystem::Varchar(..)
            | SybaseTypeSystem::Text(..) => ArrowTypeSystem::LargeUtf8(nullable),
            SybaseTypeSystem::Binary(..) => ArrowTypeSystem::LargeBinary(nullable),
            SybaseTypeSystem::Date(..) => ArrowTypeSystem::Date32(nullable),
            SybaseTypeSystem::Time(..) => ArrowTypeSystem::Time64Micro(nullable),
            SybaseTypeSystem::Timestamp(..) => ArrowTypeSystem::Date64Micro(nullable),
        }
    }

    fn arrow_data_type(self) -> ArrowDataType {
        match self {
            SybaseTypeSystem::TinyInt(..)
            | SybaseTypeSystem::SmallInt(..)
            | SybaseTypeSystem::Int(..)
            | SybaseTypeSystem::BigInt(..) => ArrowDataType::Int64,
            SybaseTypeSystem::Real(..) => ArrowDataType::Float32,
            SybaseTypeSystem::Double(..) => ArrowDataType::Float64,
            SybaseTypeSystem::Numeric(_, precision, scale)
            | SybaseTypeSystem::Decimal(_, precision, scale) => {
                ArrowDataType::Decimal128(precision, scale)
            }
            SybaseTypeSystem::Bit(..) => ArrowDataType::Boolean,
            SybaseTypeSystem::Char(..)
            | SybaseTypeSystem::Varchar(..)
            | SybaseTypeSystem::Text(..) => ArrowDataType::Utf8,
            SybaseTypeSystem::Binary(..) => ArrowDataType::LargeBinary,
            SybaseTypeSystem::Date(..) => ArrowDataType::Date32,
            SybaseTypeSystem::Time(..) => ArrowDataType::Time64(TimeUnit::Microsecond),
            SybaseTypeSystem::Timestamp(..) => {
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None)
            }
        }
    }

    fn build_arrow_array<E: odbc_core::OdbcCoreError>(
        self,
        column: AnySlice<'_>,
        nrows: usize,
        col_index: usize,
        column_name: Option<&str>,
        replace_invalid_utf16: bool,
    ) -> Result<ArrayRef, E> {
        let nullable = OdbcTypePolicy::nullable(self);
        match self {
            SybaseTypeSystem::TinyInt(..)
            | SybaseTypeSystem::SmallInt(..)
            | SybaseTypeSystem::Int(..)
            | SybaseTypeSystem::BigInt(..) => build_int64_array(column, nrows, nullable),
            SybaseTypeSystem::Real(..) => build_float32_array(column, nrows, nullable),
            SybaseTypeSystem::Double(..) => build_float64_array(column, nrows, nullable),
            SybaseTypeSystem::Numeric(_, precision, scale)
            | SybaseTypeSystem::Decimal(_, precision, scale) => build_decimal_array(
                column,
                nrows,
                nullable,
                precision,
                scale,
                <Self as OdbcTypePolicy>::source_name(),
                col_index,
                column_name,
                replace_invalid_utf16,
            ),
            SybaseTypeSystem::Bit(..) => build_bool_array(column, nrows, nullable),
            SybaseTypeSystem::Char(..)
            | SybaseTypeSystem::Varchar(..)
            | SybaseTypeSystem::Text(..) => build_string_array(
                column,
                nrows,
                nullable,
                <Self as OdbcTypePolicy>::source_name(),
                col_index,
                column_name,
                replace_invalid_utf16,
            ),
            SybaseTypeSystem::Binary(..) => build_sybase_binary_array::<E>(column, nrows, nullable),
            SybaseTypeSystem::Date(..) => build_date32_array(column, nrows, nullable),
            SybaseTypeSystem::Time(..) => build_time64_micro_array(column, nrows, nullable),
            SybaseTypeSystem::Timestamp(..) => build_timestamp_micro_array(column, nrows, nullable),
        }
    }
}

/// Build an Arrow `LargeBinaryArray` from a Sybase binary column.
///
/// FreeTDS typically surfaces `binary`/`varbinary` values through text buffers
/// as ASCII hex strings (e.g. `"ABCD"` for the bytes `[0xAB, 0xCD]`).  True
/// ODBC binary buffers (`AnySlice::Binary`) are passed through as-is.
#[cfg(feature = "dst_arrow")]
fn build_sybase_binary_array<E: odbc_core::OdbcCoreError>(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, E> {
    let mut builder = LargeBinaryBuilder::with_capacity(nrows, nrows * 8);
    match column {
        AnySlice::Binary(view) => {
            // True binary buffer – append raw bytes directly.
            for row_index in 0..nrows {
                match view.get(row_index) {
                    Some(bytes) => builder.append_value(bytes),
                    None => {
                        require_nullable::<E>(nullable, "Vec<u8>")?;
                        builder.append_null();
                    }
                }
            }
        }
        AnySlice::Text(view) => {
            // Text buffer with hex-encoded binary data (FreeTDS behaviour).
            for row_index in 0..nrows {
                match view.get(row_index) {
                    Some(hex_text) => {
                        let decoded = parse_hex_bytes_generic::<E>(hex_text)?;
                        builder.append_value(&decoded);
                    }
                    None => {
                        require_nullable::<E>(nullable, "Vec<u8>")?;
                        builder.append_null();
                    }
                }
            }
        }
        other => {
            // Fallback: try to obtain raw bytes from typed cells.
            for row_index in 0..nrows {
                use odbc_core::odbc_cell_from_column;
                match odbc_cell_from_column(other, row_index) {
                    Some(cell) => {
                        let bytes = cell.try_bytes().ok_or_else(|| {
                            crate::errors::ConnectorXError::cannot_produce::<Vec<u8>>(Some(
                                "Sybase typed value cannot be converted to bytes".to_string(),
                            ))
                        })?;
                        builder.append_value(bytes);
                    }
                    None => {
                        require_nullable::<E>(nullable, "Vec<u8>")?;
                        builder.append_null();
                    }
                }
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Decode an ASCII hex byte string (e.g. `b"ABCD"`) into raw bytes (`[0xAB, 0xCD]`).
fn parse_hex_bytes_generic<E: odbc_core::OdbcCoreError>(bytes: &[u8]) -> Result<Vec<u8>, E> {
    let bytes = odbc_core::trim_ascii(bytes);
    if bytes.len() % 2 != 0 {
        return Err(E::parse_value(
            odbc_core::bytes_to_string(bytes),
            "hex bytes",
        ));
    }
    bytes
        .chunks_exact(2)
        .map(|chunk| {
            let hi = hex_nibble(chunk[0])
                .ok_or_else(|| E::parse_value(odbc_core::bytes_to_string(bytes), "hex bytes"))?;
            let lo = hex_nibble(chunk[1])
                .ok_or_else(|| E::parse_value(odbc_core::bytes_to_string(bytes), "hex bytes"))?;
            Ok((hi << 4) | lo)
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_options_sets_instance_limits() {
        let source = SybaseSource::with_options(
            "Driver={FreeTDS};Server=localhost;",
            1,
            SybaseOptions {
                batch_size: 9,
                max_str_len: 8192,
                unknown_type_fallback_to_varchar: true,
                replace_invalid_utf16: true,
            },
        )
        .unwrap();

        assert_eq!(source.batch_size, 9);
        assert_eq!(source.max_str_len, 8192);
        assert!(source.unknown_type_fallback_to_varchar);
        assert!(source.replace_invalid_utf16);
    }

    #[test]
    fn default_options_match_previous_defaults() {
        assert_eq!(
            SybaseOptions::default(),
            SybaseOptions {
                batch_size: SYBASE_DEFAULT_BATCH_SIZE,
                max_str_len: SYBASE_DEFAULT_MAX_STR_LEN,
                unknown_type_fallback_to_varchar: false,
                replace_invalid_utf16: false,
            }
        );
    }

    #[test]
    fn replace_invalid_utf16_url_option_is_connector_only() {
        let conn =
            "sybase://sa:sybase@127.0.0.1:5000/tempdb?driver=FreeTDS&replace_invalid_utf16=true";
        assert_eq!(
            sybase_conn_string(conn).unwrap(),
            "Driver={FreeTDS};Server={127.0.0.1};Port=5000;TDS_Version={5.0};UID={sa};PWD={sybase};Database={tempdb};"
        );

        let source = SybaseSource::with_options(conn, 1, SybaseOptions::default()).unwrap();
        assert!(source.replace_invalid_utf16);
    }

    #[test]
    fn parse_hex_bytes_decodes_ascii_hex() {
        // "ABCD" should decode to [0xAB, 0xCD]
        let result: Vec<u8> = parse_hex_bytes(b"ABCD").unwrap();
        assert_eq!(result, vec![0xAB, 0xCD]);

        // lowercase also works
        let result: Vec<u8> = parse_hex_bytes(b"abcd").unwrap();
        assert_eq!(result, vec![0xAB, 0xCD]);

        // with whitespace padding
        let result: Vec<u8> = parse_hex_bytes(b"  0102  ").unwrap();
        assert_eq!(result, vec![0x01, 0x02]);

        // empty → empty
        let result: Vec<u8> = parse_hex_bytes(b"").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn parse_hex_bytes_rejects_odd_length() {
        assert!(parse_hex_bytes(b"ABC").is_err());
    }

    #[test]
    fn parse_hex_bytes_rejects_non_hex_chars() {
        assert!(parse_hex_bytes(b"GG").is_err());
    }
}
