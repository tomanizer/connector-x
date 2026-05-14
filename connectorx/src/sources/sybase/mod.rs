//! Source implementation for SAP Sybase ASE through ODBC.

mod errors;
mod typesystem;

pub use self::errors::SybaseSourceError;
pub use self::typesystem::SybaseTypeSystem;
pub use crate::sources::odbc_core::OdbcLobStrategy;

use self::typesystem::SYBASE_UNKNOWN_TYPE_FALLBACK_ENV;
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
            is_valid_odbc_key, odbc_conn_value, param_value, url_query_pairs,
        },
        odbc_core::{self, OdbcCoreError, OdbcExecutionOptions, OdbcTypePolicy},
        Produce, Source, SourcePartition,
    },
    sql::CXQuery,
};
use anyhow::anyhow;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use fehler::{throw, throws};
use odbc_api::buffers::BufferDesc;
use rust_decimal::Decimal;
#[cfg(feature = "dst_arrow")]
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
    pub max_connections: Option<usize>,
    pub login_timeout_secs: Option<u32>,
    pub query_timeout_secs: Option<usize>,
    pub lob_strategy: OdbcLobStrategy,
    pub unknown_type_fallback_to_varchar: bool,
    pub replace_invalid_utf16: bool,
    pub replace_invalid_utf8: bool,
}

impl SybaseOptions {
    pub fn from_env() -> Self {
        Self {
            batch_size: odbc_core::env_usize("SYBASE_BATCH_SIZE")
                .unwrap_or(SYBASE_DEFAULT_BATCH_SIZE),
            max_str_len: odbc_core::env_usize(SybaseTypeSystem::max_str_len_env())
                .unwrap_or(SYBASE_DEFAULT_MAX_STR_LEN),
            max_connections: odbc_core::env_usize("SYBASE_MAX_CONNECTIONS"),
            login_timeout_secs: odbc_core::env_u32("SYBASE_LOGIN_TIMEOUT_SECS"),
            query_timeout_secs: odbc_core::env_usize("SYBASE_QUERY_TIMEOUT_SECS"),
            lob_strategy: odbc_core::env_lob_strategy("SYBASE_LOB_STRATEGY").unwrap_or_default(),
            unknown_type_fallback_to_varchar: odbc_core::env_bool(SYBASE_UNKNOWN_TYPE_FALLBACK_ENV)
                .unwrap_or(false),
            replace_invalid_utf16: false,
            replace_invalid_utf8: false,
        }
    }
}

odbc_core::impl_odbc_runtime_options!(SybaseOptions);

fn validate_sybase_options(options: &SybaseOptions) -> Result<(), anyhow::Error> {
    odbc_core::validate_batch_and_buffer_limits(
        SybaseTypeSystem::source_name(),
        "SYBASE_BATCH_SIZE",
        options.batch_size,
        SybaseTypeSystem::max_str_len_env(),
        options.max_str_len,
    )
}

impl Default for SybaseOptions {
    fn default() -> Self {
        Self {
            batch_size: SYBASE_DEFAULT_BATCH_SIZE,
            max_str_len: SYBASE_DEFAULT_MAX_STR_LEN,
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

pub struct SybaseSource {
    state: odbc_core::OdbcSourceState<SybaseTypeSystem, SybaseSourceError>,
}

impl SybaseSource {
    #[throws(SybaseSourceError)]
    pub fn new(conn: &str, nconn: usize) -> Self {
        Self::with_options(conn, nconn, SybaseOptions::from_env())?
    }

    #[throws(SybaseSourceError)]
    pub fn with_options(conn: &str, nconn: usize, options: SybaseOptions) -> Self {
        validate_sybase_options(&options)?;
        let params = connection_query_pairs(conn)?;
        let params = params.as_deref();
        let runtime_options = odbc_core::resolve_runtime_options(params, &options, nconn)?;
        Self {
            state: odbc_core::OdbcSourceState::new(
                sybase_conn_string(conn)?,
                options.batch_size,
                options.max_str_len,
                options.unknown_type_fallback_to_varchar,
                runtime_options,
            ),
        }
    }
}

odbc_core::impl_odbc_source_partition_wrapper!(
    SybaseSourcePartition,
    SybaseTypeSystem,
    SybaseSourceError
);

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
        self.state.set_queries(queries);
    }

    fn set_origin_query(&mut self, query: Option<String>) {
        self.state.set_origin_query(query);
    }

    #[throws(SybaseSourceError)]
    fn fetch_metadata(&mut self) {
        let unknown_type_fallback_to_varchar = self.state.unknown_type_fallback_to_varchar;
        self.state
            .fetch_metadata(|data_type, nullability, column_name| {
                SybaseTypeSystem::from_odbc(
                    data_type,
                    nullability,
                    column_name,
                    unknown_type_fallback_to_varchar,
                )
                .map_err(Into::into)
            })?;
    }

    #[throws(SybaseSourceError)]
    fn result_rows(&mut self) -> Option<usize> {
        self.state.result_rows(odbc_core::OdbcSqlDialect::MsSql)?
    }

    fn names(&self) -> Vec<String> {
        self.state.names()
    }

    fn schema(&self) -> Vec<Self::TypeSystem> {
        self.state.schema()
    }

    #[throws(SybaseSourceError)]
    fn partition(self) -> Vec<Self::Partition> {
        self.state
            .partition(odbc_core::OdbcSqlDialect::MsSql)
            .into_iter()
            .map(SybaseSourcePartition::new)
            .collect()
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
            build_binary_array_from_owned, build_bool_array, build_bool_array_from_owned,
            build_date32_array, build_date32_array_from_owned, build_decimal_array,
            build_decimal_array_from_owned, build_float32_array, build_float32_array_from_owned,
            build_float64_array, build_float64_array_from_owned, build_int64_array,
            build_int64_array_from_owned, build_string_array, build_string_array_from_owned,
            build_time64_micro_array, build_time64_micro_array_from_owned,
            build_timestamp_micro_array, build_timestamp_micro_array_from_owned, require_nullable,
            OdbcArrowPolicy, OdbcColumn,
        },
    },
    arrow::array::{ArrayRef, LargeBinaryBuilder},
    arrow::datatypes::{DataType as ArrowDataType, TimeUnit},
    odbc_api::buffers::AnySlice,
};

pub(crate) fn fetch_i64_pair(
    conn: &str,
    query: &str,
    column_name: &str,
    execution_options: OdbcExecutionOptions,
) -> Result<(i64, i64), SybaseSourceError> {
    odbc_core::fetch_i64_pair::<SybaseSourceError>(
        conn,
        query,
        SybaseTypeSystem::source_name(),
        column_name,
        execution_options,
    )
}

#[throws(SybaseSourceError)]
pub(crate) fn sybase_execution_options(conn: &str, options: SybaseOptions) -> OdbcExecutionOptions {
    let params = connection_query_pairs(conn)?;
    odbc_core::execution_options_from_params(params.as_deref(), &options)?
}

#[cfg(feature = "dst_arrow")]
pub(crate) fn sybase_get_arrow(
    conn: &Url,
    origin_query: Option<String>,
    queries: &[CXQuery<String>],
    pre_execution_queries: Option<&[String]>,
) -> OutResult<ArrowDestination> {
    let options = SybaseOptions::from_env();
    validate_sybase_options(&options)?;
    let params = url_query_pairs(conn)?;
    let conn_str = sybase_conn_string(&conn[..])?;
    let unknown_type_fallback_to_varchar = options.unknown_type_fallback_to_varchar;
    let runtime_options =
        odbc_core::resolve_runtime_options(Some(&params), &options, queries.len())?;
    let lob_strategy = odbc_core::lob_strategy_from_params(Some(&params), options.lob_strategy)?;
    Ok(odbc_core::odbc_get_arrow_impl::<
        SybaseTypeSystem,
        SybaseSourceError,
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

#[cfg(feature = "dst_arrow")]
pub(crate) fn sybase_record_batch_iter(
    conn: &Url,
    origin_query: Option<String>,
    queries: &[CXQuery<String>],
    batch_size: usize,
    pre_execution_queries: Option<&[String]>,
) -> OutResult<Box<dyn RecordBatchIterator>> {
    let options = SybaseOptions::from_env();
    odbc_core::validate_batch_and_buffer_limits(
        SybaseTypeSystem::source_name(),
        "batch_size",
        batch_size,
        SybaseTypeSystem::max_str_len_env(),
        options.max_str_len,
    )?;
    let params = url_query_pairs(conn)?;
    let conn_str = sybase_conn_string(&conn[..])?;
    let unknown_type_fallback_to_varchar = options.unknown_type_fallback_to_varchar;
    let runtime_options =
        odbc_core::resolve_runtime_options(Some(&params), &options, queries.len())?;
    let lob_strategy = odbc_core::lob_strategy_from_params(Some(&params), options.lob_strategy)?;
    let iterator = odbc_core::odbc_record_batch_iter_impl::<SybaseTypeSystem, SybaseSourceError>(
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
            SybaseTypeSystem::from_odbc(
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
            SybaseTypeSystem::Numeric(_, precision, scale)
            | SybaseTypeSystem::Decimal(_, precision, scale) => {
                ArrowTypeSystem::Decimal128(nullable, precision, scale)
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
            | SybaseTypeSystem::Text(..) => ArrowDataType::LargeUtf8,
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
        replace_invalid_utf8: bool,
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
                replace_invalid_utf8,
            ),
            SybaseTypeSystem::Binary(..) => build_sybase_binary_array::<E>(column, nrows, nullable),
            SybaseTypeSystem::Date(..) => build_date32_array(column, nrows, nullable),
            SybaseTypeSystem::Time(..) => build_time64_micro_array(column, nrows, nullable),
            SybaseTypeSystem::Timestamp(..) => build_timestamp_micro_array(column, nrows, nullable),
        }
    }

    fn build_arrow_array_from_owned<E: odbc_core::OdbcCoreError>(
        self,
        column: &OdbcColumn,
        nrows: usize,
        col_index: usize,
        column_name: Option<&str>,
        replace_invalid_utf8: bool,
    ) -> Result<ArrayRef, E> {
        let nullable = OdbcTypePolicy::nullable(self);
        match self {
            SybaseTypeSystem::TinyInt(..)
            | SybaseTypeSystem::SmallInt(..)
            | SybaseTypeSystem::Int(..)
            | SybaseTypeSystem::BigInt(..) => build_int64_array_from_owned(column, nrows, nullable),
            SybaseTypeSystem::Real(..) => build_float32_array_from_owned(column, nrows, nullable),
            SybaseTypeSystem::Double(..) => build_float64_array_from_owned(column, nrows, nullable),
            SybaseTypeSystem::Numeric(_, precision, scale)
            | SybaseTypeSystem::Decimal(_, precision, scale) => {
                build_decimal_array_from_owned(column, nrows, nullable, precision, scale)
            }
            SybaseTypeSystem::Bit(..) => build_bool_array_from_owned(column, nrows, nullable),
            SybaseTypeSystem::Char(..)
            | SybaseTypeSystem::Varchar(..)
            | SybaseTypeSystem::Text(..) => build_string_array_from_owned(
                column,
                nrows,
                nullable,
                <Self as OdbcTypePolicy>::source_name(),
                col_index,
                column_name,
                replace_invalid_utf8,
            ),
            SybaseTypeSystem::Binary(..) => build_binary_array_from_owned(column, nrows, nullable),
            SybaseTypeSystem::Date(..) => build_date32_array_from_owned(column, nrows, nullable),
            SybaseTypeSystem::Time(..) => {
                build_time64_micro_array_from_owned(column, nrows, nullable)
            }
            SybaseTypeSystem::Timestamp(..) => {
                build_timestamp_micro_array_from_owned(column, nrows, nullable)
            }
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
    let params = url_query_pairs(&url)?;

    let driver = param_value(&params, "driver").unwrap_or("FreeTDS");
    let host = decode(url.host_str().unwrap_or("localhost"))?.into_owned();
    let port = url.port().unwrap_or(5000);
    let database = decode(url.path().trim_start_matches('/'))?.into_owned();
    let username = decode(url.username())?.into_owned();
    let password = decode(url.password().unwrap_or(""))?.into_owned();
    let tds_version = param_value(&params, "tds_version").unwrap_or("5.0");

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
    for (key, value) in &params {
        if !is_connector_option_key(key)
            && !key.eq_ignore_ascii_case("driver")
            && !key.eq_ignore_ascii_case("tds_version")
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
        let source = SybaseSource::with_options(
            "Driver={FreeTDS};Server=localhost;",
            1,
            SybaseOptions {
                batch_size: 9,
                max_str_len: 8192,
                max_connections: Some(2),
                login_timeout_secs: Some(5),
                query_timeout_secs: Some(30),
                lob_strategy: OdbcLobStrategy::Piecewise,
                unknown_type_fallback_to_varchar: true,
                replace_invalid_utf16: true,
                replace_invalid_utf8: true,
            },
        )
        .unwrap();

        assert_eq!(source.state.batch_size, 9);
        assert_eq!(source.state.max_str_len, 8192);
        assert_eq!(source.state.connection_limiter.max_connections(), 2);
        assert_eq!(source.state.execution_options.login_timeout_secs, Some(5));
        assert_eq!(source.state.execution_options.query_timeout_secs, Some(30));
        assert!(source.state.unknown_type_fallback_to_varchar);
        assert!(source.state.replace_invalid_utf16);
        assert!(source.state.replace_invalid_utf8);
    }

    #[test]
    fn default_options_match_previous_defaults() {
        assert_eq!(
            SybaseOptions::default(),
            SybaseOptions {
                batch_size: SYBASE_DEFAULT_BATCH_SIZE,
                max_str_len: SYBASE_DEFAULT_MAX_STR_LEN,
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
        let conn = "Driver={FreeTDS};Server=localhost;";
        let too_many_rows = match SybaseSource::with_options(
            conn,
            1,
            SybaseOptions {
                batch_size: odbc_core::MAX_BATCH_SIZE + 1,
                ..SybaseOptions::default()
            },
        ) {
            Ok(_) => panic!("expected oversized Sybase batch size to fail"),
            Err(err) => err.to_string(),
        };
        assert!(
            too_many_rows.contains("SYBASE_BATCH_SIZE"),
            "{}",
            too_many_rows
        );

        let too_much_buffer = match SybaseSource::with_options(
            conn,
            1,
            SybaseOptions {
                max_str_len: odbc_core::MAX_STR_LEN + 1,
                ..SybaseOptions::default()
            },
        ) {
            Ok(_) => panic!("expected oversized Sybase max string length to fail"),
            Err(err) => err.to_string(),
        };
        assert!(
            too_much_buffer.contains("SYBASE_MAX_STR_LEN"),
            "{}",
            too_much_buffer
        );
    }

    #[test]
    fn replace_invalid_encoding_url_options_are_connector_only() {
        let conn = "sybase://sa:sybase@127.0.0.1:5000/tempdb?driver=FreeTDS&replace_invalid_utf16=true&replace_invalid_utf8=true&max_connections=3&login_timeout_secs=5&query_timeout_secs=30";
        assert_eq!(
            sybase_conn_string(conn).unwrap(),
            "Driver={FreeTDS};Server={127.0.0.1};Port=5000;TDS_Version={5.0};UID={sa};PWD={sybase};Database={tempdb};"
        );

        let source = SybaseSource::with_options(conn, 1, SybaseOptions::default()).unwrap();
        assert!(source.state.replace_invalid_utf16);
        assert!(source.state.replace_invalid_utf8);
        assert_eq!(source.state.connection_limiter.max_connections(), 3);
        assert_eq!(source.state.execution_options.login_timeout_secs, Some(5));
        assert_eq!(source.state.execution_options.query_timeout_secs, Some(30));
    }

    #[test]
    fn sybase_url_passes_through_driver_odbc_options() {
        let conn = "sybase://sa:sybase@127.0.0.1:5000/tempdb?driver=FreeTDS&tds_version=5.0&charset=UTF-8&Encrypt=yes&APP=ConnectorX%3Bworker";
        assert_eq!(
            sybase_conn_string(conn).unwrap(),
            "Driver={FreeTDS};Server={127.0.0.1};Port=5000;TDS_Version={5.0};UID={sa};PWD={sybase};Database={tempdb};charset={UTF-8};Encrypt={yes};APP={ConnectorX;worker};"
        );
    }

    #[test]
    fn sybase_url_rejects_invalid_odbc_option_keys() {
        for invalid_key in ["bad%3Bkey", "bad%3Dkey"] {
            let err = sybase_conn_string(&format!(
                "sybase://sa:sybase@127.0.0.1:5000/tempdb?driver=FreeTDS&{invalid_key}=value"
            ))
            .unwrap_err()
            .to_string();

            assert!(err.contains("invalid ODBC connection-string key"));
            assert!(err.contains("bad"));
            assert!(err.contains("key"));
        }
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

    #[cfg(feature = "dst_arrow")]
    #[test]
    fn build_sybase_binary_array_passes_raw_binary_buffers_through() {
        use arrow::array::{Array, LargeBinaryArray};
        use odbc_api::buffers::{AnyBuffer, BufferDesc, ColumnBuffer};

        let mut buffer = AnyBuffer::from_desc(2, BufferDesc::Binary { max_bytes: 4 });
        if let AnyBuffer::Binary(column) = &mut buffer {
            column.set_value(0, Some(&[0x00, 0x7f, 0x80, 0xff]));
            column.set_value(1, None);
        } else {
            panic!("expected binary buffer");
        }

        let array = build_sybase_binary_array::<SybaseSourceError>(buffer.view(2), 2, true)
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .clone();
        assert_eq!(array.value(0), &[0x00_u8, 0x7f, 0x80, 0xff]);
        assert!(array.is_null(1));
    }

    #[cfg(feature = "dst_arrow")]
    #[test]
    fn build_sybase_binary_array_decodes_hex_text_buffers() {
        use arrow::array::{Array, LargeBinaryArray};
        use odbc_api::buffers::{AnyBuffer, BufferDesc, ColumnBuffer};

        let mut buffer = AnyBuffer::from_desc(2, BufferDesc::Text { max_str_len: 8 });
        if let AnyBuffer::Text(column) = &mut buffer {
            column.set_value(0, Some(b"007F80FF"));
            column.set_value(1, None);
        } else {
            panic!("expected text buffer");
        }

        let array = build_sybase_binary_array::<SybaseSourceError>(buffer.view(2), 2, true)
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .clone();
        assert_eq!(array.value(0), &[0x00_u8, 0x7f, 0x80, 0xff]);
        assert!(array.is_null(1));
    }
}
