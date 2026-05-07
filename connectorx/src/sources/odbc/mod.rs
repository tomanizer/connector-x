//! Source implementation for generic ODBC.

mod errors;
mod typesystem;

pub use self::errors::OdbcSourceError;
pub use self::typesystem::OdbcTypeSystem;

use std::convert::TryFrom;

use crate::{
    constants::DB_BUFFER_SIZE,
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{
        odbc_common::{is_raw_odbc_conn_string, is_valid_odbc_key, push_odbc_pair},
        PartitionParser, Produce, Source, SourcePartition,
    },
    sql::{count_query, CXQuery},
};
use anyhow::anyhow;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use fehler::{throw, throws};
use odbc_api::handles::StatementConnection;
use odbc_api::sys::{Date, Time, Timestamp};
use odbc_api::{
    buffers::{AnySlice, BufferDesc, ColumnarAnyBuffer, TextRowSet},
    environment, Bit, BlockCursor, Connection, ConnectionOptions, Cursor, CursorImpl,
    ResultSetMetadata,
};
use rust_decimal::Decimal;
use sqlparser::dialect::GenericDialect;
use url::Url;
use urlencoding::decode;

type OdbcCursor = CursorImpl<StatementConnection<Connection<'static>>>;
type OdbcBlockCursor = BlockCursor<OdbcCursor, ColumnarAnyBuffer>;

const ODBC_DEFAULT_BATCH_SIZE: usize = 1024;
const ODBC_DEFAULT_MAX_STR_LEN: usize = 1024;

pub struct OdbcSource {
    conn: String,
    origin_query: Option<String>,
    queries: Vec<CXQuery<String>>,
    names: Vec<String>,
    schema: Vec<OdbcTypeSystem>,
    batch_size: usize,
    max_str_len: usize,
}

impl OdbcSource {
    #[throws(OdbcSourceError)]
    pub fn new(conn: &str, _nconn: usize) -> Self {
        Self {
            conn: odbc_conn_string(conn)?,
            origin_query: None,
            queries: vec![],
            names: vec![],
            schema: vec![],
            batch_size: env_usize("ODBC_BATCH_SIZE").unwrap_or(ODBC_DEFAULT_BATCH_SIZE),
            max_str_len: env_usize("ODBC_MAX_STR_LEN").unwrap_or(ODBC_DEFAULT_MAX_STR_LEN),
        }
    }

    #[throws(OdbcSourceError)]
    fn execute_query(conn: &str, query: &str) -> OdbcCursor {
        let env = environment()?;
        let connection = env.connect_with_connection_string(conn, ConnectionOptions::default())?;
        connection
            .into_cursor(query, (), None)
            .map_err(|e| e.error)?
            .ok_or_else(|| OdbcSourceError::NoResultSet(query.to_string()))?
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
        let mut cursor = Self::execute_query(&self.conn, &first_query)?;
        let ncols = cursor.num_result_cols()?;
        if ncols < 0 {
            throw!(anyhow!("ODBC returned negative column count: {}", ncols));
        }

        let mut names = Vec::with_capacity(ncols as usize);
        let mut schema = Vec::with_capacity(ncols as usize);
        for col in 1..=ncols as u16 {
            names.push(cursor.col_name(col)?);
            let ty = cursor.col_data_type(col)?;
            let nullability = cursor.col_nullability(col)?;
            schema.push(OdbcTypeSystem::from_odbc(ty, nullability));
        }

        self.names = names;
        self.schema = schema;
    }

    #[throws(OdbcSourceError)]
    fn result_rows(&mut self) -> Option<usize> {
        match &self.origin_query {
            Some(q) => Some(fetch_count(&self.conn, q)?),
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
                    &self.schema,
                    self.batch_size,
                    self.max_str_len,
                )
            })
            .collect()
    }
}

pub struct OdbcSourcePartition {
    conn: String,
    query: CXQuery<String>,
    schema: Vec<OdbcTypeSystem>,
    nrows: usize,
    ncols: usize,
    batch_size: usize,
    max_str_len: usize,
}

impl OdbcSourcePartition {
    pub fn new(
        conn: String,
        query: &CXQuery<String>,
        schema: &[OdbcTypeSystem],
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

impl SourcePartition for OdbcSourcePartition {
    type TypeSystem = OdbcTypeSystem;
    type Parser<'a> = OdbcSourceParser;
    type Error = OdbcSourceError;

    #[throws(OdbcSourceError)]
    fn result_rows(&mut self) {
        let cquery = count_query(&self.query, &GenericDialect {})?;
        self.nrows = fetch_count_query(&self.conn, cquery.as_str())?;
    }

    #[throws(OdbcSourceError)]
    fn parser(&mut self) -> Self::Parser<'_> {
        let cursor = OdbcSource::execute_query(&self.conn, self.query.as_str())?;
        let buffer = ColumnarAnyBuffer::try_from_descs(
            self.batch_size,
            self.schema
                .iter()
                .map(|ty| odbc_buffer_desc(*ty, self.max_str_len)),
        )?;
        let cursor = cursor.bind_buffer(buffer)?;
        OdbcSourceParser::new(cursor, self.schema.len())
    }

    fn nrows(&self) -> usize {
        self.nrows
    }

    fn ncols(&self) -> usize {
        self.ncols
    }
}

pub struct OdbcSourceParser {
    cursor: OdbcBlockCursor,
    rowbuf: Vec<Option<OdbcCell>>,
    ncols: usize,
    current_cell: usize,
    is_finished: bool,
}

impl OdbcSourceParser {
    fn new(cursor: OdbcBlockCursor, ncols: usize) -> Self {
        Self {
            cursor,
            rowbuf: Vec::with_capacity(DB_BUFFER_SIZE),
            ncols,
            current_cell: 0,
            is_finished: false,
        }
    }

    #[throws(OdbcSourceError)]
    fn next_cell(&mut self) -> Option<&OdbcCell> {
        let cell_index = self.current_cell;
        self.current_cell += 1;
        self.rowbuf[cell_index].as_ref()
    }

    #[throws(OdbcSourceError)]
    fn next_bytes(&mut self) -> Option<&[u8]> {
        match self.next_cell()? {
            Some(cell) => Some(cell.try_bytes("bytes").ok_or_else(|| {
                ConnectorXError::cannot_produce::<Vec<u8>>(Some(
                    "Odbc typed value for byte-only parser".to_string(),
                ))
            })?),
            None => None,
        }
    }

    #[throws(OdbcSourceError)]
    fn required_cell(&mut self, ty: &'static str) -> &OdbcCell {
        let value = self.next_cell()?;
        value.ok_or_else(|| {
            ConnectorXError::cannot_produce::<Vec<u8>>(Some(format!("Odbc NULL for non-null {ty}")))
        })?
    }

    #[throws(OdbcSourceError)]
    fn required_bytes(&mut self, ty: &'static str) -> &[u8] {
        let value = self.required_cell(ty)?;
        value.try_bytes(ty).ok_or_else(|| {
            ConnectorXError::cannot_produce::<Vec<u8>>(Some(format!(
                "Odbc typed value for byte-only {ty}"
            )))
        })?
    }
}

impl PartitionParser<'_> for OdbcSourceParser {
    type TypeSystem = OdbcTypeSystem;
    type Error = OdbcSourceError;

    #[throws(OdbcSourceError)]
    fn fetch_next(&mut self) -> (usize, bool) {
        if self.ncols == 0 {
            self.is_finished = true;
            return (0, true);
        }
        assert!(matches!(self.current_cell.checked_rem(self.ncols), Some(0)));
        let remaining_cells = self.rowbuf.len() - self.current_cell;
        if remaining_cells > 0 {
            return (remaining_cells / self.ncols, self.is_finished);
        } else if self.is_finished {
            return (0, self.is_finished);
        }

        self.rowbuf.clear();
        if let Some(batch) = self.cursor.fetch()? {
            self.rowbuf.reserve(batch.num_rows() * self.ncols);
            for row_index in 0..batch.num_rows() {
                for col_index in 0..batch.num_cols() {
                    self.rowbuf
                        .push(odbc_cell_from_column(batch.column(col_index), row_index));
                }
            }
        } else {
            self.is_finished = true;
        }

        self.current_cell = 0;
        (self.rowbuf.len() / self.ncols, self.is_finished)
    }
}

#[derive(Clone, Debug)]
enum OdbcCell {
    Bytes(Vec<u8>),
    U8(u8),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Bool(bool),
    Date(Date),
    Time(Time),
    Timestamp(Timestamp),
}

impl OdbcCell {
    fn try_bytes(&self, _ty: &'static str) -> Option<&[u8]> {
        match self {
            OdbcCell::Bytes(bytes) => Some(bytes),
            _ => None,
        }
    }

    fn to_utf8_string(&self) -> String {
        match self {
            OdbcCell::Bytes(bytes) => bytes_to_string(bytes),
            OdbcCell::U8(value) => value.to_string(),
            OdbcCell::I8(value) => value.to_string(),
            OdbcCell::I16(value) => value.to_string(),
            OdbcCell::I32(value) => value.to_string(),
            OdbcCell::I64(value) => value.to_string(),
            OdbcCell::F32(value) => value.to_string(),
            OdbcCell::F64(value) => value.to_string(),
            OdbcCell::Bool(value) => value.to_string(),
            OdbcCell::Date(value) => {
                format!("{:04}-{:02}-{:02}", value.year, value.month, value.day)
            }
            OdbcCell::Time(value) => {
                format!("{:02}:{:02}:{:02}", value.hour, value.minute, value.second)
            }
            OdbcCell::Timestamp(value) => format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:09}",
                value.year,
                value.month,
                value.day,
                value.hour,
                value.minute,
                value.second,
                value.fraction
            ),
        }
    }
}

fn odbc_buffer_desc(ty: OdbcTypeSystem, max_str_len: usize) -> BufferDesc {
    let nullable = odbc_nullable(ty);
    match ty {
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
        OdbcTypeSystem::Date(_) | OdbcTypeSystem::Time(_) | OdbcTypeSystem::Timestamp(_) => {
            odbc_temporal_buffer_desc(ty, nullable)
        }
        OdbcTypeSystem::Binary(_) => BufferDesc::Binary {
            max_bytes: max_str_len,
        },
    }
}

fn odbc_temporal_buffer_desc(ty: OdbcTypeSystem, nullable: bool) -> BufferDesc {
    match ty {
        OdbcTypeSystem::Date(_) => BufferDesc::Date { nullable },
        OdbcTypeSystem::Time(_) => BufferDesc::Time { nullable },
        OdbcTypeSystem::Timestamp(_) => BufferDesc::Timestamp { nullable },
        _ => unreachable!("non-temporal Odbc type passed to temporal buffer desc"),
    }
}

fn odbc_nullable(ty: OdbcTypeSystem) -> bool {
    match ty {
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

fn odbc_cell_from_column(column: AnySlice<'_>, row_index: usize) -> Option<OdbcCell> {
    match column {
        AnySlice::Text(view) => view
            .get(row_index)
            .map(|bytes| OdbcCell::Bytes(bytes.to_vec())),
        AnySlice::WText(view) => view
            .get(row_index)
            .map(|chars| OdbcCell::Bytes(String::from_utf16_lossy(chars).into_bytes())),
        AnySlice::Binary(view) => view
            .get(row_index)
            .map(|bytes| OdbcCell::Bytes(bytes.to_vec())),
        AnySlice::F64(values) => Some(OdbcCell::F64(values[row_index])),
        AnySlice::F32(values) => Some(OdbcCell::F32(values[row_index])),
        AnySlice::I8(values) => Some(OdbcCell::I8(values[row_index])),
        AnySlice::I16(values) => Some(OdbcCell::I16(values[row_index])),
        AnySlice::I32(values) => Some(OdbcCell::I32(values[row_index])),
        AnySlice::I64(values) => Some(OdbcCell::I64(values[row_index])),
        AnySlice::U8(values) => Some(OdbcCell::U8(values[row_index])),
        AnySlice::Bit(values) => Some(OdbcCell::Bool(bit_to_bool(values[row_index]))),
        AnySlice::Date(values) => Some(OdbcCell::Date(values[row_index])),
        AnySlice::Time(values) => Some(OdbcCell::Time(values[row_index])),
        AnySlice::Timestamp(values) => Some(OdbcCell::Timestamp(values[row_index])),
        AnySlice::NullableF64(values) => values.get(row_index).copied().map(OdbcCell::F64),
        AnySlice::NullableF32(values) => values.get(row_index).copied().map(OdbcCell::F32),
        AnySlice::NullableI8(values) => values.get(row_index).copied().map(OdbcCell::I8),
        AnySlice::NullableI16(values) => values.get(row_index).copied().map(OdbcCell::I16),
        AnySlice::NullableI32(values) => values.get(row_index).copied().map(OdbcCell::I32),
        AnySlice::NullableI64(values) => values.get(row_index).copied().map(OdbcCell::I64),
        AnySlice::NullableU8(values) => values.get(row_index).copied().map(OdbcCell::U8),
        AnySlice::NullableBit(values) => values
            .get(row_index)
            .copied()
            .map(bit_to_bool)
            .map(OdbcCell::Bool),
        AnySlice::NullableDate(values) => values.get(row_index).copied().map(OdbcCell::Date),
        AnySlice::NullableTime(values) => values.get(row_index).copied().map(OdbcCell::Time),
        AnySlice::NullableTimestamp(values) => {
            values.get(row_index).copied().map(OdbcCell::Timestamp)
        }
        AnySlice::Numeric(_) | AnySlice::NullableNumeric(_) => None,
    }
}

fn bit_to_bool(value: Bit) -> bool {
    value.0 != 0
}

macro_rules! impl_parse_from_bytes {
    ($t:ty, $name:literal, $parse:expr) => {
        impl<'r> Produce<'r, $t> for OdbcSourceParser {
            type Error = OdbcSourceError;

            #[throws(OdbcSourceError)]
            fn produce(&'r mut self) -> $t {
                let bytes = self.required_bytes($name)?;
                ($parse)(bytes)?
            }
        }

        impl<'r> Produce<'r, Option<$t>> for OdbcSourceParser {
            type Error = OdbcSourceError;

            #[throws(OdbcSourceError)]
            fn produce(&'r mut self) -> Option<$t> {
                match self.next_bytes()? {
                    Some(bytes) => Some(($parse)(bytes)?),
                    None => None,
                }
            }
        }
    };
}

impl_parse_from_bytes!(Decimal, "Decimal", parse_decimal);

macro_rules! impl_parse_from_cell {
    ($t:ty, $name:literal, $parse:expr) => {
        impl<'r> Produce<'r, $t> for OdbcSourceParser {
            type Error = OdbcSourceError;

            #[throws(OdbcSourceError)]
            fn produce(&'r mut self) -> $t {
                let cell = self.required_cell($name)?;
                ($parse)(cell)?
            }
        }

        impl<'r> Produce<'r, Option<$t>> for OdbcSourceParser {
            type Error = OdbcSourceError;

            #[throws(OdbcSourceError)]
            fn produce(&'r mut self) -> Option<$t> {
                match self.next_cell()? {
                    Some(cell) => Some(($parse)(cell)?),
                    None => None,
                }
            }
        }
    };
}

impl_parse_from_cell!(u8, "u8", cell_u8);
impl_parse_from_cell!(i16, "i16", cell_i16);
impl_parse_from_cell!(i32, "i32", cell_i32);
impl_parse_from_cell!(i64, "i64", cell_i64);
impl_parse_from_cell!(f32, "f32", cell_f32);
impl_parse_from_cell!(f64, "f64", cell_f64);
impl_parse_from_cell!(NaiveDate, "NaiveDate", cell_date);
impl_parse_from_cell!(NaiveTime, "NaiveTime", cell_time);
impl_parse_from_cell!(NaiveDateTime, "NaiveDateTime", cell_timestamp);

impl<'r> Produce<'r, bool> for OdbcSourceParser {
    type Error = OdbcSourceError;

    #[throws(OdbcSourceError)]
    fn produce(&'r mut self) -> bool {
        cell_bool(self.required_cell("bool")?)?
    }
}

impl<'r> Produce<'r, Option<bool>> for OdbcSourceParser {
    type Error = OdbcSourceError;

    #[throws(OdbcSourceError)]
    fn produce(&'r mut self) -> Option<bool> {
        match self.next_cell()? {
            Some(cell) => Some(cell_bool(cell)?),
            None => None,
        }
    }
}

impl<'r> Produce<'r, String> for OdbcSourceParser {
    type Error = OdbcSourceError;

    #[throws(OdbcSourceError)]
    fn produce(&'r mut self) -> String {
        self.required_cell("String")?.to_utf8_string()
    }
}

impl<'r> Produce<'r, Option<String>> for OdbcSourceParser {
    type Error = OdbcSourceError;

    #[throws(OdbcSourceError)]
    fn produce(&'r mut self) -> Option<String> {
        self.next_cell()?.map(OdbcCell::to_utf8_string)
    }
}

impl<'r> Produce<'r, Vec<u8>> for OdbcSourceParser {
    type Error = OdbcSourceError;

    #[throws(OdbcSourceError)]
    fn produce(&'r mut self) -> Vec<u8> {
        self.required_bytes("Vec<u8>")?.to_vec()
    }
}

impl<'r> Produce<'r, Option<Vec<u8>>> for OdbcSourceParser {
    type Error = OdbcSourceError;

    #[throws(OdbcSourceError)]
    fn produce(&'r mut self) -> Option<Vec<u8>> {
        self.next_bytes()?.map(|bytes| bytes.to_vec())
    }
}

fn parse_bool(bytes: &[u8]) -> Result<bool, OdbcSourceError> {
    match trim_ascii(bytes) {
        b"1" => Ok(true),
        b"0" => Ok(false),
        value if eq_ascii_ignore_case(value, b"true") => Ok(true),
        value if eq_ascii_ignore_case(value, b"false") => Ok(false),
        _ => Err(OdbcSourceError::ParseValue {
            value: bytes_to_string(bytes),
            ty: "bool",
        }),
    }
}

fn cell_u8(cell: &OdbcCell) -> Result<u8, OdbcSourceError> {
    match cell {
        OdbcCell::U8(value) => Ok(*value),
        OdbcCell::I8(value) => u8::try_from(*value).map_err(|_| cell_parse_error(cell, "u8")),
        OdbcCell::I16(value) => u8::try_from(*value).map_err(|_| cell_parse_error(cell, "u8")),
        OdbcCell::I32(value) => u8::try_from(*value).map_err(|_| cell_parse_error(cell, "u8")),
        OdbcCell::I64(value) => u8::try_from(*value).map_err(|_| cell_parse_error(cell, "u8")),
        OdbcCell::Bytes(bytes) => parse_u8(bytes),
        _ => Err(cell_parse_error(cell, "u8")),
    }
}

fn cell_i16(cell: &OdbcCell) -> Result<i16, OdbcSourceError> {
    match cell {
        OdbcCell::I8(value) => Ok(i16::from(*value)),
        OdbcCell::U8(value) => Ok(i16::from(*value)),
        OdbcCell::I16(value) => Ok(*value),
        OdbcCell::I32(value) => i16::try_from(*value).map_err(|_| cell_parse_error(cell, "i16")),
        OdbcCell::I64(value) => i16::try_from(*value).map_err(|_| cell_parse_error(cell, "i16")),
        OdbcCell::Bytes(bytes) => parse_i16(bytes),
        _ => Err(cell_parse_error(cell, "i16")),
    }
}

fn cell_i32(cell: &OdbcCell) -> Result<i32, OdbcSourceError> {
    match cell {
        OdbcCell::I8(value) => Ok(i32::from(*value)),
        OdbcCell::U8(value) => Ok(i32::from(*value)),
        OdbcCell::I16(value) => Ok(i32::from(*value)),
        OdbcCell::I32(value) => Ok(*value),
        OdbcCell::I64(value) => i32::try_from(*value).map_err(|_| cell_parse_error(cell, "i32")),
        OdbcCell::Bytes(bytes) => parse_i32(bytes),
        _ => Err(cell_parse_error(cell, "i32")),
    }
}

fn cell_i64(cell: &OdbcCell) -> Result<i64, OdbcSourceError> {
    match cell {
        OdbcCell::I8(value) => Ok(i64::from(*value)),
        OdbcCell::U8(value) => Ok(i64::from(*value)),
        OdbcCell::I16(value) => Ok(i64::from(*value)),
        OdbcCell::I32(value) => Ok(i64::from(*value)),
        OdbcCell::I64(value) => Ok(*value),
        OdbcCell::Bytes(bytes) => parse_i64(bytes),
        _ => Err(cell_parse_error(cell, "i64")),
    }
}

fn cell_f32(cell: &OdbcCell) -> Result<f32, OdbcSourceError> {
    match cell {
        OdbcCell::F32(value) => Ok(*value),
        OdbcCell::Bytes(bytes) => parse_f32(bytes),
        _ => Err(cell_parse_error(cell, "f32")),
    }
}

fn cell_f64(cell: &OdbcCell) -> Result<f64, OdbcSourceError> {
    match cell {
        OdbcCell::F32(value) => Ok(f64::from(*value)),
        OdbcCell::F64(value) => Ok(*value),
        OdbcCell::Bytes(bytes) => parse_f64(bytes),
        _ => Err(cell_parse_error(cell, "f64")),
    }
}

fn cell_bool(cell: &OdbcCell) -> Result<bool, OdbcSourceError> {
    match cell {
        OdbcCell::Bool(value) => Ok(*value),
        OdbcCell::U8(value) => Ok(*value != 0),
        OdbcCell::I8(value) => Ok(*value != 0),
        OdbcCell::I16(value) => Ok(*value != 0),
        OdbcCell::I32(value) => Ok(*value != 0),
        OdbcCell::I64(value) => Ok(*value != 0),
        OdbcCell::Bytes(bytes) => parse_bool(bytes),
        _ => Err(cell_parse_error(cell, "bool")),
    }
}

fn cell_date(cell: &OdbcCell) -> Result<NaiveDate, OdbcSourceError> {
    match cell {
        OdbcCell::Date(value) => odbc_date_to_naive(*value),
        OdbcCell::Bytes(bytes) => parse_date(bytes),
        _ => Err(cell_parse_error(cell, "NaiveDate")),
    }
}

fn cell_time(cell: &OdbcCell) -> Result<NaiveTime, OdbcSourceError> {
    match cell {
        OdbcCell::Time(value) => odbc_time_to_naive(*value),
        OdbcCell::Bytes(bytes) => parse_time(bytes),
        _ => Err(cell_parse_error(cell, "NaiveTime")),
    }
}

fn cell_timestamp(cell: &OdbcCell) -> Result<NaiveDateTime, OdbcSourceError> {
    match cell {
        OdbcCell::Timestamp(value) => odbc_timestamp_to_naive(*value),
        OdbcCell::Bytes(bytes) => parse_timestamp(bytes),
        _ => Err(cell_parse_error(cell, "NaiveDateTime")),
    }
}

fn cell_parse_error(cell: &OdbcCell, ty: &'static str) -> OdbcSourceError {
    OdbcSourceError::ParseValue {
        value: cell.to_utf8_string(),
        ty,
    }
}

fn odbc_date_to_naive(value: Date) -> Result<NaiveDate, OdbcSourceError> {
    NaiveDate::from_ymd_opt(value.year.into(), value.month.into(), value.day.into()).ok_or_else(
        || OdbcSourceError::ParseValue {
            value: format!("{:04}-{:02}-{:02}", value.year, value.month, value.day),
            ty: "NaiveDate",
        },
    )
}

fn odbc_time_to_naive(value: Time) -> Result<NaiveTime, OdbcSourceError> {
    NaiveTime::from_hms_opt(value.hour.into(), value.minute.into(), value.second.into()).ok_or_else(
        || OdbcSourceError::ParseValue {
            value: format!("{:02}:{:02}:{:02}", value.hour, value.minute, value.second),
            ty: "NaiveTime",
        },
    )
}

fn odbc_timestamp_to_naive(value: Timestamp) -> Result<NaiveDateTime, OdbcSourceError> {
    let date = odbc_date_to_naive(Date {
        year: value.year,
        month: value.month,
        day: value.day,
    })?;
    date.and_hms_nano_opt(
        value.hour.into(),
        value.minute.into(),
        value.second.into(),
        value.fraction,
    )
    .ok_or_else(|| OdbcSourceError::ParseValue {
        value: format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:09}",
            value.year,
            value.month,
            value.day,
            value.hour,
            value.minute,
            value.second,
            value.fraction
        ),
        ty: "NaiveDateTime",
    })
}

fn parse_u8(bytes: &[u8]) -> Result<u8, OdbcSourceError> {
    let value = parse_i64_with_ty(bytes, "u8")?;
    u8::try_from(value).map_err(|_| OdbcSourceError::ParseValue {
        value: bytes_to_string(bytes),
        ty: "u8",
    })
}

fn parse_i16(bytes: &[u8]) -> Result<i16, OdbcSourceError> {
    let value = parse_i64_with_ty(bytes, "i16")?;
    i16::try_from(value).map_err(|_| OdbcSourceError::ParseValue {
        value: bytes_to_string(bytes),
        ty: "i16",
    })
}

fn parse_i32(bytes: &[u8]) -> Result<i32, OdbcSourceError> {
    let value = parse_i64_with_ty(bytes, "i32")?;
    i32::try_from(value).map_err(|_| OdbcSourceError::ParseValue {
        value: bytes_to_string(bytes),
        ty: "i32",
    })
}

fn parse_i64(bytes: &[u8]) -> Result<i64, OdbcSourceError> {
    parse_i64_with_ty(bytes, "i64")
}

fn parse_f32(bytes: &[u8]) -> Result<f32, OdbcSourceError> {
    Ok(bytes_to_str(trim_ascii(bytes), "f32")?.parse::<f32>()?)
}

fn parse_f64(bytes: &[u8]) -> Result<f64, OdbcSourceError> {
    Ok(bytes_to_str(trim_ascii(bytes), "f64")?.parse::<f64>()?)
}

fn parse_decimal(bytes: &[u8]) -> Result<Decimal, OdbcSourceError> {
    Ok(bytes_to_str(trim_ascii(bytes), "Decimal")?.parse::<Decimal>()?)
}

fn parse_date(bytes: &[u8]) -> Result<NaiveDate, OdbcSourceError> {
    Ok(NaiveDate::parse_from_str(
        bytes_to_str(trim_ascii(bytes), "NaiveDate")?,
        "%Y-%m-%d",
    )?)
}

fn parse_time(bytes: &[u8]) -> Result<NaiveTime, OdbcSourceError> {
    let s = bytes_to_str(trim_ascii(bytes), "NaiveTime")?;
    NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S"))
        .map_err(OdbcSourceError::from)
}

fn parse_timestamp(bytes: &[u8]) -> Result<NaiveDateTime, OdbcSourceError> {
    let s = bytes_to_str(trim_ascii(bytes), "NaiveDateTime")?;
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .map_err(OdbcSourceError::from)
}

#[throws(OdbcSourceError)]
fn fetch_count(conn: &str, query: &str) -> usize {
    let cxq = CXQuery::Naked(query.to_string());
    let cquery = count_query(&cxq, &GenericDialect {})?;
    fetch_count_query(conn, cquery.as_str())?
}

#[throws(OdbcSourceError)]
fn fetch_count_query(conn: &str, query: &str) -> usize {
    let mut cursor = OdbcSource::execute_query(conn, query)?;
    let buffer = TextRowSet::for_cursor(1, &mut cursor, Some(64))?;
    let mut cursor = cursor.bind_buffer(buffer)?;
    let batch = cursor.fetch()?.ok_or(OdbcSourceError::GetNRowsFailed)?;
    let value = batch.at(0, 0).ok_or(OdbcSourceError::GetNRowsFailed)?;
    let value = parse_i64_with_ty(value, "usize")?;
    usize::try_from(value).map_err(|_| OdbcSourceError::ParseValue {
        value: bytes_to_string(batch.at(0, 0).unwrap_or_default()),
        ty: "usize",
    })?
}

#[throws(OdbcSourceError)]
pub(crate) fn fetch_i64_pair(conn: &str, query: &str) -> (i64, i64) {
    let mut cursor = OdbcSource::execute_query(conn, query)?;
    let buffer = TextRowSet::for_cursor(1, &mut cursor, Some(128))?;
    let mut cursor = cursor.bind_buffer(buffer)?;
    let batch = cursor.fetch()?.ok_or(OdbcSourceError::GetNRowsFailed)?;
    let min = parse_partition_value(batch.at(0, 0).ok_or(OdbcSourceError::GetNRowsFailed)?)?;
    let max = parse_partition_value(batch.at(1, 0).ok_or(OdbcSourceError::GetNRowsFailed)?)?;
    (min, max)
}

fn parse_partition_value(value: &[u8]) -> Result<i64, OdbcSourceError> {
    let trimmed = trim_ascii(value);
    if trimmed.is_empty() {
        return Ok(0);
    }

    match parse_i64_with_ty(trimmed, "partition range") {
        Ok(value) => Ok(value),
        Err(_) => bytes_to_str(trimmed, "partition range")?
            .parse::<f64>()
            .map(|value| value as i64)
            .map_err(|_| OdbcSourceError::ParseValue {
                value: bytes_to_string(trimmed),
                ty: "partition range",
            }),
    }
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.parse().ok()
}

fn trim_ascii(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map(|idx| idx + 1)
        .unwrap_or(start);
    &bytes[start..end]
}

fn bytes_to_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn bytes_to_str<'a>(bytes: &'a [u8], ty: &'static str) -> Result<&'a str, OdbcSourceError> {
    std::str::from_utf8(bytes).map_err(|_| OdbcSourceError::ParseValue {
        value: bytes_to_string(bytes),
        ty,
    })
}

fn eq_ascii_ignore_case(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn parse_i64_with_ty(bytes: &[u8], ty: &'static str) -> Result<i64, OdbcSourceError> {
    let bytes = trim_ascii(bytes);
    if bytes.is_empty() {
        return Err(OdbcSourceError::ParseValue {
            value: String::new(),
            ty,
        });
    }

    let (negative, digits) = match bytes[0] {
        b'-' => (true, &bytes[1..]),
        b'+' => (false, &bytes[1..]),
        _ => (false, bytes),
    };
    if digits.is_empty() {
        return Err(OdbcSourceError::ParseValue {
            value: bytes_to_string(bytes),
            ty,
        });
    }

    let mut value = 0i64;
    for &byte in digits {
        if !byte.is_ascii_digit() {
            return Err(OdbcSourceError::ParseValue {
                value: bytes_to_string(bytes),
                ty,
            });
        }
        let digit = i64::from(byte - b'0');
        value = if negative {
            value
                .checked_mul(10)
                .and_then(|value| value.checked_sub(digit))
        } else {
            value
                .checked_mul(10)
                .and_then(|value| value.checked_add(digit))
        }
        .ok_or_else(|| OdbcSourceError::ParseValue {
            value: bytes_to_string(bytes),
            ty,
        })?;
    }

    Ok(value)
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
