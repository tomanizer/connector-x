//! Source implementation for generic ODBC.

mod errors;
mod typesystem;

pub use self::errors::OdbcSourceError;
pub use self::typesystem::OdbcTypeSystem;

#[cfg(feature = "dst_arrow")]
use crate::sources::odbc_core::{
    bit_to_bool, cell_bool, cell_date, cell_f32, cell_f64, cell_i64, cell_time, cell_timestamp,
    ensure_column_not_truncated, odbc_cell_from_column, odbc_date_to_naive, odbc_time_to_naive,
    odbc_timestamp_to_naive, parse_decimal, OdbcValue,
};
#[cfg(feature = "dst_arrow")]
use crate::{
    constants::DEFAULT_ARROW_DECIMAL_SCALE,
    destinations::{
        arrow::{ArrowDestination, ArrowTypeSystem},
        Destination,
    },
    errors::OutResult,
    utils::decimal_to_i128,
};
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
#[cfg(feature = "dst_arrow")]
use {
    arrow::{
        array::{
            ArrayRef, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
            Float64Builder, Int64Builder, LargeBinaryBuilder, StringBuilder,
            Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
        },
        datatypes::Schema,
        record_batch::RecordBatch,
    },
    odbc_api::{buffers::AnySlice, sys::NULL_DATA},
    rayon::prelude::*,
};

const ODBC_DEFAULT_BATCH_SIZE: usize = 1024;
const ODBC_DEFAULT_MAX_STR_LEN: usize = 1024;

pub type OdbcSourceParser = odbc_core::OdbcParser<OdbcTypeSystem, OdbcSourceError>;

#[derive(Debug, Clone, Copy)]
pub struct OdbcOptions {
    pub batch_size: usize,
    pub max_str_len: usize,
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
        let options = OdbcOptions {
            batch_size: odbc_core::env_usize("ODBC_BATCH_SIZE").unwrap_or(ODBC_DEFAULT_BATCH_SIZE),
            max_str_len: odbc_core::env_usize(OdbcTypeSystem::max_str_len_env())
                .unwrap_or(ODBC_DEFAULT_MAX_STR_LEN),
        };
        Self::with_options(conn, nconn, options)?
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
    let mut source = OdbcSource::new(&conn[..], queries.len())?;
    source.set_data_order(DataOrder::RowMajor)?;
    source.set_queries(queries);
    source.set_origin_query(origin_query);
    source.fetch_metadata()?;

    let source_schema = source.schema();
    let arrow_schema = source_schema
        .iter()
        .map(|&ty| odbc_arrow_type(ty))
        .collect::<Vec<_>>();
    let names = source.names();

    let mut destination = ArrowDestination::new();
    destination.allocate(0, &names, &arrow_schema, DataOrder::RowMajor)?;
    let record_schema = destination.arrow_schema();

    source
        .partition()?
        .into_par_iter()
        .try_for_each(|partition| {
            odbc_partition_record_batches(partition, Arc::clone(&record_schema), &destination)
        })?;

    Ok(destination)
}

#[cfg(feature = "dst_arrow")]
fn odbc_arrow_type(ty: OdbcTypeSystem) -> ArrowTypeSystem {
    let nullable = ty.nullable();
    match ty {
        OdbcTypeSystem::TinyInt(_)
        | OdbcTypeSystem::SmallInt(_)
        | OdbcTypeSystem::Int(_)
        | OdbcTypeSystem::BigInt(_) => ArrowTypeSystem::Int64(nullable),
        OdbcTypeSystem::Real(_) => ArrowTypeSystem::Float32(nullable),
        OdbcTypeSystem::Double(_) => ArrowTypeSystem::Float64(nullable),
        OdbcTypeSystem::Numeric(_) | OdbcTypeSystem::Decimal(_) => {
            ArrowTypeSystem::Decimal(nullable)
        }
        OdbcTypeSystem::Bit(_) => ArrowTypeSystem::Boolean(nullable),
        OdbcTypeSystem::Char(_) | OdbcTypeSystem::Varchar(_) | OdbcTypeSystem::Text(_) => {
            ArrowTypeSystem::LargeUtf8(nullable)
        }
        OdbcTypeSystem::Binary(_) => ArrowTypeSystem::LargeBinary(nullable),
        OdbcTypeSystem::Date(_) => ArrowTypeSystem::Date32(nullable),
        OdbcTypeSystem::Time(_) => ArrowTypeSystem::Time64Micro(nullable),
        OdbcTypeSystem::Timestamp(_) => ArrowTypeSystem::Date64Micro(nullable),
    }
}

#[cfg(feature = "dst_arrow")]
fn odbc_partition_record_batches(
    partition: OdbcSourcePartition,
    arrow_schema: Arc<Schema>,
    destination: &ArrowDestination,
) -> Result<(), OdbcSourceError> {
    let cursor = OdbcSource::execute_query(&partition.conn, partition.query.as_str())?;
    let buffer = ColumnarAnyBuffer::try_from_descs(
        partition.batch_size,
        partition
            .schema
            .iter()
            .zip(&partition.column_buffer_max_lens)
            .map(|(ty, max_len)| ty.buffer_desc(*max_len)),
    )?;
    let mut cursor = cursor.bind_buffer(buffer)?;

    while let Some(batch) = cursor.fetch()? {
        let mut columns = Vec::with_capacity(batch.num_cols());
        for col_index in 0..batch.num_cols() {
            let column = batch.column(col_index);
            ensure_column_not_truncated::<OdbcSourceError>(
                &column,
                OdbcTypeSystem::source_name(),
                OdbcTypeSystem::max_str_len_env(),
                col_index,
                partition.names.get(col_index).map(String::as_str),
                partition.schema.get(col_index).copied(),
            )?;
            columns.push(odbc_arrow_array(
                partition.schema[col_index],
                column,
                batch.num_rows(),
            )?);
        }
        let batch = RecordBatch::try_new(Arc::clone(&arrow_schema), columns)
            .map_err(anyhow::Error::from)?;
        destination
            .push_record_batch(batch)
            .map_err(anyhow::Error::from)?;
    }

    Ok(())
}

#[cfg(feature = "dst_arrow")]
fn odbc_arrow_array(
    ty: OdbcTypeSystem,
    column: AnySlice<'_>,
    nrows: usize,
) -> Result<ArrayRef, OdbcSourceError> {
    match ty {
        OdbcTypeSystem::TinyInt(_)
        | OdbcTypeSystem::SmallInt(_)
        | OdbcTypeSystem::Int(_)
        | OdbcTypeSystem::BigInt(_) => odbc_int64_array(column, nrows, ty.nullable()),
        OdbcTypeSystem::Real(_) => odbc_float32_array(column, nrows, ty.nullable()),
        OdbcTypeSystem::Double(_) => odbc_float64_array(column, nrows, ty.nullable()),
        OdbcTypeSystem::Numeric(_) | OdbcTypeSystem::Decimal(_) => {
            odbc_decimal_array(column, nrows, ty.nullable())
        }
        OdbcTypeSystem::Bit(_) => odbc_bool_array(column, nrows, ty.nullable()),
        OdbcTypeSystem::Char(_) | OdbcTypeSystem::Varchar(_) | OdbcTypeSystem::Text(_) => {
            odbc_string_array(column, nrows, ty.nullable())
        }
        OdbcTypeSystem::Binary(_) => odbc_binary_array(column, nrows, ty.nullable()),
        OdbcTypeSystem::Date(_) => odbc_date32_array(column, nrows, ty.nullable()),
        OdbcTypeSystem::Time(_) => odbc_time64_micro_array(column, nrows, ty.nullable()),
        OdbcTypeSystem::Timestamp(_) => odbc_timestamp_micro_array(column, nrows, ty.nullable()),
    }
}

#[cfg(feature = "dst_arrow")]
fn require_nullable(nullable: bool, ty: &'static str) -> Result<(), OdbcSourceError> {
    if nullable {
        Ok(())
    } else {
        Err(ConnectorXError::cannot_produce::<Vec<u8>>(Some(format!(
            "Odbc NULL for non-null {ty}"
        )))
        .into())
    }
}

#[cfg(feature = "dst_arrow")]
fn validity_from_indicators(
    indicators: &[isize],
    nullable: bool,
    ty: &'static str,
) -> Result<Vec<bool>, OdbcSourceError> {
    indicators
        .iter()
        .map(|&indicator| {
            if indicator == NULL_DATA {
                require_nullable(nullable, ty)?;
                Ok(false)
            } else {
                Ok(true)
            }
        })
        .collect()
}

#[cfg(feature = "dst_arrow")]
macro_rules! append_direct_cell {
    ($column:expr, $row:expr, $builder:expr, $nullable:expr, $ty:literal, $parse:expr) => {
        match odbc_cell_from_column($column, $row) {
            Some(cell) => $builder.append_value(($parse)(cell)?),
            None => {
                require_nullable($nullable, $ty)?;
                $builder.append_null();
            }
        }
    };
}

#[cfg(feature = "dst_arrow")]
fn odbc_int64_array(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, OdbcSourceError> {
    let mut builder = Int64Builder::with_capacity(nrows);
    match column {
        AnySlice::I8(values) => {
            for &value in &values[..nrows] {
                builder.append_value(i64::from(value));
            }
        }
        AnySlice::I16(values) => {
            for &value in &values[..nrows] {
                builder.append_value(i64::from(value));
            }
        }
        AnySlice::I32(values) => {
            for &value in &values[..nrows] {
                builder.append_value(i64::from(value));
            }
        }
        AnySlice::I64(values) => builder.append_values(&values[..nrows], &vec![true; nrows]),
        AnySlice::U8(values) => {
            for &value in &values[..nrows] {
                builder.append_value(i64::from(value));
            }
        }
        AnySlice::NullableI8(values) => {
            for value in values.take(nrows) {
                match value {
                    Some(&value) => builder.append_value(i64::from(value)),
                    None => {
                        require_nullable(nullable, "i64")?;
                        builder.append_null();
                    }
                }
            }
        }
        AnySlice::NullableI16(values) => {
            for value in values.take(nrows) {
                match value {
                    Some(&value) => builder.append_value(i64::from(value)),
                    None => {
                        require_nullable(nullable, "i64")?;
                        builder.append_null();
                    }
                }
            }
        }
        AnySlice::NullableI32(values) => {
            for value in values.take(nrows) {
                match value {
                    Some(&value) => builder.append_value(i64::from(value)),
                    None => {
                        require_nullable(nullable, "i64")?;
                        builder.append_null();
                    }
                }
            }
        }
        AnySlice::NullableI64(values) => {
            let (values, indicators) = values.raw_values();
            let validity = validity_from_indicators(&indicators[..nrows], nullable, "i64")?;
            builder.append_values(&values[..nrows], &validity);
        }
        AnySlice::NullableU8(values) => {
            for value in values.take(nrows) {
                match value {
                    Some(&value) => builder.append_value(i64::from(value)),
                    None => {
                        require_nullable(nullable, "i64")?;
                        builder.append_null();
                    }
                }
            }
        }
        other => {
            for row_index in 0..nrows {
                append_direct_cell!(
                    other,
                    row_index,
                    builder,
                    nullable,
                    "i64",
                    cell_i64::<OdbcSourceError>
                );
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "dst_arrow")]
fn odbc_float32_array(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, OdbcSourceError> {
    let mut builder = Float32Builder::with_capacity(nrows);
    match column {
        AnySlice::F32(values) => builder.append_values(&values[..nrows], &vec![true; nrows]),
        AnySlice::NullableF32(values) => {
            let (values, indicators) = values.raw_values();
            let validity = validity_from_indicators(&indicators[..nrows], nullable, "f32")?;
            builder.append_values(&values[..nrows], &validity);
        }
        other => {
            for row_index in 0..nrows {
                append_direct_cell!(
                    other,
                    row_index,
                    builder,
                    nullable,
                    "f32",
                    cell_f32::<OdbcSourceError>
                );
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "dst_arrow")]
fn odbc_float64_array(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, OdbcSourceError> {
    let mut builder = Float64Builder::with_capacity(nrows);
    match column {
        AnySlice::F32(values) => {
            for &value in &values[..nrows] {
                builder.append_value(f64::from(value));
            }
        }
        AnySlice::F64(values) => builder.append_values(&values[..nrows], &vec![true; nrows]),
        AnySlice::NullableF32(values) => {
            for value in values.take(nrows) {
                match value {
                    Some(&value) => builder.append_value(f64::from(value)),
                    None => {
                        require_nullable(nullable, "f64")?;
                        builder.append_null();
                    }
                }
            }
        }
        AnySlice::NullableF64(values) => {
            let (values, indicators) = values.raw_values();
            let validity = validity_from_indicators(&indicators[..nrows], nullable, "f64")?;
            builder.append_values(&values[..nrows], &validity);
        }
        other => {
            for row_index in 0..nrows {
                append_direct_cell!(
                    other,
                    row_index,
                    builder,
                    nullable,
                    "f64",
                    cell_f64::<OdbcSourceError>
                );
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "dst_arrow")]
fn odbc_bool_array(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, OdbcSourceError> {
    let mut builder = BooleanBuilder::with_capacity(nrows);
    match column {
        AnySlice::Bit(values) => {
            for &value in &values[..nrows] {
                builder.append_value(bit_to_bool(value));
            }
        }
        AnySlice::NullableBit(values) => {
            for value in values.take(nrows) {
                match value {
                    Some(&value) => builder.append_value(bit_to_bool(value)),
                    None => {
                        require_nullable(nullable, "bool")?;
                        builder.append_null();
                    }
                }
            }
        }
        other => {
            for row_index in 0..nrows {
                append_direct_cell!(
                    other,
                    row_index,
                    builder,
                    nullable,
                    "bool",
                    cell_bool::<OdbcSourceError>
                );
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "dst_arrow")]
fn odbc_decimal_array(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, OdbcSourceError> {
    let mut builder = Decimal128Builder::with_capacity(nrows)
        .with_data_type(crate::constants::DEFAULT_ARROW_DECIMAL);
    match column {
        AnySlice::Text(view) => {
            for row_index in 0..nrows {
                match view.get(row_index) {
                    Some(bytes) => append_decimal_value(&mut builder, bytes)?,
                    None => {
                        require_nullable(nullable, "Decimal")?;
                        builder.append_null();
                    }
                }
            }
        }
        AnySlice::WText(view) => {
            for row_index in 0..nrows {
                match view.get(row_index) {
                    Some(chars) => {
                        let value = String::from_utf16_lossy(chars);
                        append_decimal_value(&mut builder, value.as_bytes())?;
                    }
                    None => {
                        require_nullable(nullable, "Decimal")?;
                        builder.append_null();
                    }
                }
            }
        }
        other => {
            for row_index in 0..nrows {
                match odbc_cell_from_column(other, row_index) {
                    Some(cell) => {
                        let decimal = match &cell {
                            OdbcValue::Bytes(bytes) => {
                                parse_decimal::<OdbcSourceError>(bytes.as_ref())?
                            }
                            _ => {
                                parse_decimal::<OdbcSourceError>(cell.to_utf8_string().as_bytes())?
                            }
                        };
                        append_decimal(&mut builder, decimal)?;
                    }
                    None => {
                        require_nullable(nullable, "Decimal")?;
                        builder.append_null();
                    }
                }
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "dst_arrow")]
fn append_decimal_value(
    builder: &mut Decimal128Builder,
    bytes: &[u8],
) -> Result<(), OdbcSourceError> {
    append_decimal(builder, parse_decimal::<OdbcSourceError>(bytes)?)
}

#[cfg(feature = "dst_arrow")]
fn append_decimal(
    builder: &mut Decimal128Builder,
    decimal: Decimal,
) -> Result<(), OdbcSourceError> {
    builder.append_value(decimal_to_i128(
        decimal,
        DEFAULT_ARROW_DECIMAL_SCALE as u32,
    )?);
    Ok(())
}

#[cfg(feature = "dst_arrow")]
fn odbc_string_array(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, OdbcSourceError> {
    let mut builder = StringBuilder::with_capacity(nrows, nrows * 8);
    match column {
        AnySlice::Text(view) => {
            for row_index in 0..nrows {
                match view.get(row_index) {
                    Some(bytes) => {
                        let value = String::from_utf8_lossy(bytes);
                        builder.append_value(value.as_ref());
                    }
                    None => {
                        require_nullable(nullable, "String")?;
                        builder.append_null();
                    }
                }
            }
        }
        AnySlice::WText(view) => {
            for row_index in 0..nrows {
                match view.get(row_index) {
                    Some(chars) => builder.append_value(String::from_utf16_lossy(chars)),
                    None => {
                        require_nullable(nullable, "String")?;
                        builder.append_null();
                    }
                }
            }
        }
        other => {
            for row_index in 0..nrows {
                match odbc_cell_from_column(other, row_index) {
                    Some(cell) => builder.append_value(cell.to_utf8_string()),
                    None => {
                        require_nullable(nullable, "String")?;
                        builder.append_null();
                    }
                }
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "dst_arrow")]
fn odbc_binary_array(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, OdbcSourceError> {
    let mut builder = LargeBinaryBuilder::with_capacity(nrows, nrows * 8);
    match column {
        AnySlice::Binary(view) => {
            for row_index in 0..nrows {
                match view.get(row_index) {
                    Some(bytes) => builder.append_value(bytes),
                    None => {
                        require_nullable(nullable, "Vec<u8>")?;
                        builder.append_null();
                    }
                }
            }
        }
        AnySlice::Text(view) => {
            for row_index in 0..nrows {
                match view.get(row_index) {
                    Some(bytes) => builder.append_value(bytes),
                    None => {
                        require_nullable(nullable, "Vec<u8>")?;
                        builder.append_null();
                    }
                }
            }
        }
        other => {
            for row_index in 0..nrows {
                match odbc_cell_from_column(other, row_index) {
                    Some(cell) => builder.append_value(cell.try_bytes().ok_or_else(|| {
                        ConnectorXError::cannot_produce::<Vec<u8>>(Some(
                            "Odbc typed value for byte-only Vec<u8>".to_string(),
                        ))
                    })?),
                    None => {
                        require_nullable(nullable, "Vec<u8>")?;
                        builder.append_null();
                    }
                }
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "dst_arrow")]
fn odbc_date32_array(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, OdbcSourceError> {
    let mut builder = Date32Builder::with_capacity(nrows);
    match column {
        AnySlice::Date(values) => {
            for &value in &values[..nrows] {
                builder.append_value(naive_date_to_arrow(odbc_date_to_naive::<OdbcSourceError>(
                    value,
                )?)?);
            }
        }
        AnySlice::NullableDate(values) => {
            for value in values.take(nrows) {
                match value {
                    Some(&value) => {
                        builder.append_value(naive_date_to_arrow(odbc_date_to_naive::<
                            OdbcSourceError,
                        >(
                            value
                        )?)?);
                    }
                    None => {
                        require_nullable(nullable, "NaiveDate")?;
                        builder.append_null();
                    }
                }
            }
        }
        other => {
            for row_index in 0..nrows {
                match odbc_cell_from_column(other, row_index) {
                    Some(cell) => {
                        builder
                            .append_value(naive_date_to_arrow(cell_date::<OdbcSourceError>(cell)?)?)
                    }
                    None => {
                        require_nullable(nullable, "NaiveDate")?;
                        builder.append_null();
                    }
                }
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "dst_arrow")]
fn odbc_time64_micro_array(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, OdbcSourceError> {
    let mut builder = Time64MicrosecondBuilder::with_capacity(nrows);
    match column {
        AnySlice::Time(values) => {
            for &value in &values[..nrows] {
                builder.append_value(naive_time_to_arrow_micro(odbc_time_to_naive::<
                    OdbcSourceError,
                >(value)?));
            }
        }
        AnySlice::NullableTime(values) => {
            for value in values.take(nrows) {
                match value {
                    Some(&value) => {
                        builder.append_value(naive_time_to_arrow_micro(odbc_time_to_naive::<
                            OdbcSourceError,
                        >(
                            value
                        )?));
                    }
                    None => {
                        require_nullable(nullable, "NaiveTime")?;
                        builder.append_null();
                    }
                }
            }
        }
        other => {
            for row_index in 0..nrows {
                match odbc_cell_from_column(other, row_index) {
                    Some(cell) => {
                        builder.append_value(naive_time_to_arrow_micro(
                            cell_time::<OdbcSourceError>(cell)?,
                        ))
                    }
                    None => {
                        require_nullable(nullable, "NaiveTime")?;
                        builder.append_null();
                    }
                }
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "dst_arrow")]
fn odbc_timestamp_micro_array(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, OdbcSourceError> {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(nrows);
    match column {
        AnySlice::Timestamp(values) => {
            for &value in &values[..nrows] {
                builder.append_value(
                    odbc_timestamp_to_naive::<OdbcSourceError>(value)?
                        .and_utc()
                        .timestamp_micros(),
                );
            }
        }
        AnySlice::NullableTimestamp(values) => {
            for value in values.take(nrows) {
                match value {
                    Some(&value) => builder.append_value(
                        odbc_timestamp_to_naive::<OdbcSourceError>(value)?
                            .and_utc()
                            .timestamp_micros(),
                    ),
                    None => {
                        require_nullable(nullable, "NaiveDateTime")?;
                        builder.append_null();
                    }
                }
            }
        }
        other => {
            for row_index in 0..nrows {
                match odbc_cell_from_column(other, row_index) {
                    Some(cell) => {
                        builder.append_value(
                            cell_timestamp::<OdbcSourceError>(cell)?
                                .and_utc()
                                .timestamp_micros(),
                        );
                    }
                    None => {
                        require_nullable(nullable, "NaiveDateTime")?;
                        builder.append_null();
                    }
                }
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "dst_arrow")]
fn naive_date_to_arrow(value: NaiveDate) -> Result<i32, OdbcSourceError> {
    value
        .and_hms_opt(0, 0, 0)
        .map(|dt| (dt.and_utc().timestamp() / crate::constants::SECONDS_IN_DAY) as i32)
        .ok_or_else(|| OdbcSourceError::ParseValue {
            value: format!("{value:?}"),
            ty: "NaiveDate",
        })
}

#[cfg(feature = "dst_arrow")]
fn naive_time_to_arrow_micro(value: NaiveTime) -> i64 {
    use chrono::Timelike;

    value.num_seconds_from_midnight() as i64 * 1_000_000 + (value.nanosecond() as i64) / 1000
}

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
