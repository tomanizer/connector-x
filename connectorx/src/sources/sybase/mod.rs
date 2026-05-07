//! Source implementation for SAP Sybase ASE through ODBC.

mod errors;
mod typesystem;

pub use self::errors::SybaseSourceError;
pub use self::typesystem::SybaseTypeSystem;

use std::convert::TryFrom;

use crate::{
    constants::DB_BUFFER_SIZE,
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{
        odbc_common::{is_raw_odbc_conn_string, odbc_conn_value},
        PartitionParser, Produce, Source, SourcePartition,
    },
    sql::{count_query, CXQuery},
};
use anyhow::anyhow;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use fehler::{throw, throws};
use odbc_api::handles::StatementConnection;
use odbc_api::{
    buffers::{AnySlice, BufferDesc, ColumnarAnyBuffer, TextRowSet},
    environment, Bit, BlockCursor, Connection, ConnectionOptions, Cursor, CursorImpl,
    ResultSetMetadata,
};
use rust_decimal::Decimal;
use sqlparser::dialect::MsSqlDialect;
use url::Url;
use urlencoding::decode;

type SybaseCursor = CursorImpl<StatementConnection<Connection<'static>>>;
type SybaseBlockCursor = BlockCursor<SybaseCursor, ColumnarAnyBuffer>;

const SYBASE_DEFAULT_BATCH_SIZE: usize = 1024;
const SYBASE_DEFAULT_MAX_STR_LEN: usize = 1024;

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
            batch_size: env_usize("SYBASE_BATCH_SIZE").unwrap_or(SYBASE_DEFAULT_BATCH_SIZE),
            max_str_len: env_usize("SYBASE_MAX_STR_LEN").unwrap_or(SYBASE_DEFAULT_MAX_STR_LEN),
        }
    }

    #[throws(SybaseSourceError)]
    fn execute_query(conn: &str, query: &str) -> SybaseCursor {
        let env = environment()?;
        let connection = env.connect_with_connection_string(conn, ConnectionOptions::default())?;
        connection
            .into_cursor(query, (), None)
            .map_err(|e| e.error)?
            .ok_or_else(|| SybaseSourceError::NoResultSet(query.to_string()))?
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
            schema.push(SybaseTypeSystem::from_odbc(ty, nullability));
        }

        self.names = names;
        self.schema = schema;
    }

    #[throws(SybaseSourceError)]
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
        self.nrows = fetch_count_query(&self.conn, cquery.as_str())?;
    }

    #[throws(SybaseSourceError)]
    fn parser(&mut self) -> Self::Parser<'_> {
        let cursor = SybaseSource::execute_query(&self.conn, self.query.as_str())?;
        let buffer = ColumnarAnyBuffer::try_from_descs(
            self.batch_size,
            self.schema
                .iter()
                .map(|ty| sybase_buffer_desc(*ty, self.max_str_len)),
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

pub struct SybaseSourceParser {
    cursor: SybaseBlockCursor,
    rowbuf: Vec<Vec<Option<SybaseCell>>>,
    ncols: usize,
    current_col: usize,
    current_row: usize,
    is_finished: bool,
}

impl SybaseSourceParser {
    fn new(cursor: SybaseBlockCursor, ncols: usize) -> Self {
        Self {
            cursor,
            rowbuf: Vec::with_capacity(DB_BUFFER_SIZE),
            ncols,
            current_col: 0,
            current_row: 0,
            is_finished: false,
        }
    }

    #[throws(SybaseSourceError)]
    fn next_cell(&mut self) -> Option<&SybaseCell> {
        let ridx = self.current_row;
        let cidx = self.current_col;
        self.current_row += (self.current_col + 1) / self.ncols;
        self.current_col = (self.current_col + 1) % self.ncols;
        self.rowbuf[ridx][cidx].as_ref()
    }

    #[throws(SybaseSourceError)]
    fn next_bytes(&mut self) -> Option<&[u8]> {
        match self.next_cell()? {
            Some(cell) => Some(cell.try_bytes("bytes").ok_or_else(|| {
                ConnectorXError::cannot_produce::<Vec<u8>>(Some(
                    "Sybase typed value for byte-only parser".to_string(),
                ))
            })?),
            None => None,
        }
    }

    #[throws(SybaseSourceError)]
    fn required_cell<T>(&mut self, ty: &'static str) -> &SybaseCell {
        let value = self.next_cell()?;
        value.ok_or_else(|| {
            ConnectorXError::cannot_produce::<T>(Some(format!("Sybase NULL for non-null {ty}")))
        })?
    }

    #[throws(SybaseSourceError)]
    fn required_bytes(&mut self, ty: &'static str) -> &[u8] {
        let value = self.required_cell::<Vec<u8>>(ty)?;
        value.try_bytes(ty).ok_or_else(|| {
            ConnectorXError::cannot_produce::<Vec<u8>>(Some(format!(
                "Sybase typed value for byte-only {ty}"
            )))
        })?
    }
}

impl PartitionParser<'_> for SybaseSourceParser {
    type TypeSystem = SybaseTypeSystem;
    type Error = SybaseSourceError;

    #[throws(SybaseSourceError)]
    fn fetch_next(&mut self) -> (usize, bool) {
        if self.ncols == 0 {
            self.is_finished = true;
            return (0, true);
        }
        assert!(self.current_col == 0);
        let remaining_rows = self.rowbuf.len() - self.current_row;
        if remaining_rows > 0 {
            return (remaining_rows, self.is_finished);
        } else if self.is_finished {
            return (0, self.is_finished);
        }

        self.rowbuf.clear();
        if let Some(batch) = self.cursor.fetch()? {
            for row_index in 0..batch.num_rows() {
                let row = (0..batch.num_cols())
                    .map(|col_index| sybase_cell_from_column(batch.column(col_index), row_index))
                    .collect::<Vec<_>>();
                self.rowbuf.push(row);
            }
        } else {
            self.is_finished = true;
        }

        self.current_row = 0;
        self.current_col = 0;
        (self.rowbuf.len(), self.is_finished)
    }
}

#[derive(Clone, Debug)]
enum SybaseCell {
    Bytes(Vec<u8>),
    U8(u8),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Bool(bool),
}

impl SybaseCell {
    fn try_bytes(&self, _ty: &'static str) -> Option<&[u8]> {
        match self {
            SybaseCell::Bytes(bytes) => Some(bytes),
            _ => None,
        }
    }

    fn to_utf8_string(&self) -> String {
        match self {
            SybaseCell::Bytes(bytes) => bytes_to_string(bytes),
            SybaseCell::U8(value) => value.to_string(),
            SybaseCell::I8(value) => value.to_string(),
            SybaseCell::I16(value) => value.to_string(),
            SybaseCell::I32(value) => value.to_string(),
            SybaseCell::I64(value) => value.to_string(),
            SybaseCell::F32(value) => value.to_string(),
            SybaseCell::F64(value) => value.to_string(),
            SybaseCell::Bool(value) => value.to_string(),
        }
    }
}

fn sybase_buffer_desc(ty: SybaseTypeSystem, max_str_len: usize) -> BufferDesc {
    let nullable = sybase_nullable(ty);
    match ty {
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

fn sybase_nullable(ty: SybaseTypeSystem) -> bool {
    match ty {
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

fn sybase_cell_from_column(column: AnySlice<'_>, row_index: usize) -> Option<SybaseCell> {
    match column {
        AnySlice::Text(view) => view
            .get(row_index)
            .map(|bytes| SybaseCell::Bytes(bytes.to_vec())),
        AnySlice::WText(view) => view
            .get(row_index)
            .map(|chars| SybaseCell::Bytes(String::from_utf16_lossy(chars).into_bytes())),
        AnySlice::Binary(view) => view
            .get(row_index)
            .map(|bytes| SybaseCell::Bytes(bytes.to_vec())),
        AnySlice::F64(values) => Some(SybaseCell::F64(values[row_index])),
        AnySlice::F32(values) => Some(SybaseCell::F32(values[row_index])),
        AnySlice::I8(values) => Some(SybaseCell::I8(values[row_index])),
        AnySlice::I16(values) => Some(SybaseCell::I16(values[row_index])),
        AnySlice::I32(values) => Some(SybaseCell::I32(values[row_index])),
        AnySlice::I64(values) => Some(SybaseCell::I64(values[row_index])),
        AnySlice::U8(values) => Some(SybaseCell::U8(values[row_index])),
        AnySlice::Bit(values) => Some(SybaseCell::Bool(bit_to_bool(values[row_index]))),
        AnySlice::NullableF64(values) => values.get(row_index).copied().map(SybaseCell::F64),
        AnySlice::NullableF32(values) => values.get(row_index).copied().map(SybaseCell::F32),
        AnySlice::NullableI8(values) => values.get(row_index).copied().map(SybaseCell::I8),
        AnySlice::NullableI16(values) => values.get(row_index).copied().map(SybaseCell::I16),
        AnySlice::NullableI32(values) => values.get(row_index).copied().map(SybaseCell::I32),
        AnySlice::NullableI64(values) => values.get(row_index).copied().map(SybaseCell::I64),
        AnySlice::NullableU8(values) => values.get(row_index).copied().map(SybaseCell::U8),
        AnySlice::NullableBit(values) => values
            .get(row_index)
            .copied()
            .map(bit_to_bool)
            .map(SybaseCell::Bool),
        AnySlice::Date(_)
        | AnySlice::Time(_)
        | AnySlice::Timestamp(_)
        | AnySlice::Numeric(_)
        | AnySlice::NullableDate(_)
        | AnySlice::NullableTime(_)
        | AnySlice::NullableTimestamp(_)
        | AnySlice::NullableNumeric(_) => None,
    }
}

fn bit_to_bool(value: Bit) -> bool {
    value.0 != 0
}

macro_rules! impl_parse_from_bytes {
    ($t:ty, $name:literal, $parse:expr) => {
        impl<'r> Produce<'r, $t> for SybaseSourceParser {
            type Error = SybaseSourceError;

            #[throws(SybaseSourceError)]
            fn produce(&'r mut self) -> $t {
                let bytes = self.required_bytes($name)?;
                ($parse)(bytes)?
            }
        }

        impl<'r> Produce<'r, Option<$t>> for SybaseSourceParser {
            type Error = SybaseSourceError;

            #[throws(SybaseSourceError)]
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
impl_parse_from_bytes!(NaiveDate, "NaiveDate", parse_date);
impl_parse_from_bytes!(NaiveTime, "NaiveTime", parse_time);
impl_parse_from_bytes!(NaiveDateTime, "NaiveDateTime", parse_timestamp);

macro_rules! impl_parse_from_cell {
    ($t:ty, $name:literal, $parse:expr) => {
        impl<'r> Produce<'r, $t> for SybaseSourceParser {
            type Error = SybaseSourceError;

            #[throws(SybaseSourceError)]
            fn produce(&'r mut self) -> $t {
                let cell = self.required_cell::<$t>($name)?;
                ($parse)(cell)?
            }
        }

        impl<'r> Produce<'r, Option<$t>> for SybaseSourceParser {
            type Error = SybaseSourceError;

            #[throws(SybaseSourceError)]
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

impl<'r> Produce<'r, bool> for SybaseSourceParser {
    type Error = SybaseSourceError;

    #[throws(SybaseSourceError)]
    fn produce(&'r mut self) -> bool {
        cell_bool(self.required_cell::<bool>("bool")?)?
    }
}

impl<'r> Produce<'r, Option<bool>> for SybaseSourceParser {
    type Error = SybaseSourceError;

    #[throws(SybaseSourceError)]
    fn produce(&'r mut self) -> Option<bool> {
        match self.next_cell()? {
            Some(cell) => Some(cell_bool(cell)?),
            None => None,
        }
    }
}

impl<'r> Produce<'r, String> for SybaseSourceParser {
    type Error = SybaseSourceError;

    #[throws(SybaseSourceError)]
    fn produce(&'r mut self) -> String {
        self.required_cell::<String>("String")?.to_utf8_string()
    }
}

impl<'r> Produce<'r, Option<String>> for SybaseSourceParser {
    type Error = SybaseSourceError;

    #[throws(SybaseSourceError)]
    fn produce(&'r mut self) -> Option<String> {
        self.next_cell()?.map(SybaseCell::to_utf8_string)
    }
}

impl<'r> Produce<'r, Vec<u8>> for SybaseSourceParser {
    type Error = SybaseSourceError;

    #[throws(SybaseSourceError)]
    fn produce(&'r mut self) -> Vec<u8> {
        parse_hex_bytes(self.required_bytes("Vec<u8>")?)?
    }
}

impl<'r> Produce<'r, Option<Vec<u8>>> for SybaseSourceParser {
    type Error = SybaseSourceError;

    #[throws(SybaseSourceError)]
    fn produce(&'r mut self) -> Option<Vec<u8>> {
        match self.next_bytes()? {
            Some(bytes) => Some(parse_hex_bytes(bytes)?),
            None => None,
        }
    }
}

fn parse_bool(bytes: &[u8]) -> Result<bool, SybaseSourceError> {
    match trim_ascii(bytes) {
        b"1" => Ok(true),
        b"0" => Ok(false),
        value if eq_ascii_ignore_case(value, b"true") => Ok(true),
        value if eq_ascii_ignore_case(value, b"false") => Ok(false),
        _ => Err(SybaseSourceError::ParseValue {
            value: bytes_to_string(bytes),
            ty: "bool",
        }),
    }
}

fn cell_u8(cell: &SybaseCell) -> Result<u8, SybaseSourceError> {
    match cell {
        SybaseCell::U8(value) => Ok(*value),
        SybaseCell::I8(value) => u8::try_from(*value).map_err(|_| cell_parse_error(cell, "u8")),
        SybaseCell::I16(value) => u8::try_from(*value).map_err(|_| cell_parse_error(cell, "u8")),
        SybaseCell::I32(value) => u8::try_from(*value).map_err(|_| cell_parse_error(cell, "u8")),
        SybaseCell::I64(value) => u8::try_from(*value).map_err(|_| cell_parse_error(cell, "u8")),
        SybaseCell::Bytes(bytes) => parse_u8(bytes),
        _ => Err(cell_parse_error(cell, "u8")),
    }
}

fn cell_i16(cell: &SybaseCell) -> Result<i16, SybaseSourceError> {
    match cell {
        SybaseCell::I8(value) => Ok(i16::from(*value)),
        SybaseCell::U8(value) => Ok(i16::from(*value)),
        SybaseCell::I16(value) => Ok(*value),
        SybaseCell::I32(value) => i16::try_from(*value).map_err(|_| cell_parse_error(cell, "i16")),
        SybaseCell::I64(value) => i16::try_from(*value).map_err(|_| cell_parse_error(cell, "i16")),
        SybaseCell::Bytes(bytes) => parse_i16(bytes),
        _ => Err(cell_parse_error(cell, "i16")),
    }
}

fn cell_i32(cell: &SybaseCell) -> Result<i32, SybaseSourceError> {
    match cell {
        SybaseCell::I8(value) => Ok(i32::from(*value)),
        SybaseCell::U8(value) => Ok(i32::from(*value)),
        SybaseCell::I16(value) => Ok(i32::from(*value)),
        SybaseCell::I32(value) => Ok(*value),
        SybaseCell::I64(value) => i32::try_from(*value).map_err(|_| cell_parse_error(cell, "i32")),
        SybaseCell::Bytes(bytes) => parse_i32(bytes),
        _ => Err(cell_parse_error(cell, "i32")),
    }
}

fn cell_i64(cell: &SybaseCell) -> Result<i64, SybaseSourceError> {
    match cell {
        SybaseCell::I8(value) => Ok(i64::from(*value)),
        SybaseCell::U8(value) => Ok(i64::from(*value)),
        SybaseCell::I16(value) => Ok(i64::from(*value)),
        SybaseCell::I32(value) => Ok(i64::from(*value)),
        SybaseCell::I64(value) => Ok(*value),
        SybaseCell::Bytes(bytes) => parse_i64(bytes),
        _ => Err(cell_parse_error(cell, "i64")),
    }
}

fn cell_f32(cell: &SybaseCell) -> Result<f32, SybaseSourceError> {
    match cell {
        SybaseCell::F32(value) => Ok(*value),
        SybaseCell::Bytes(bytes) => parse_f32(bytes),
        _ => Err(cell_parse_error(cell, "f32")),
    }
}

fn cell_f64(cell: &SybaseCell) -> Result<f64, SybaseSourceError> {
    match cell {
        SybaseCell::F32(value) => Ok(f64::from(*value)),
        SybaseCell::F64(value) => Ok(*value),
        SybaseCell::Bytes(bytes) => parse_f64(bytes),
        _ => Err(cell_parse_error(cell, "f64")),
    }
}

fn cell_bool(cell: &SybaseCell) -> Result<bool, SybaseSourceError> {
    match cell {
        SybaseCell::Bool(value) => Ok(*value),
        SybaseCell::U8(value) => Ok(*value != 0),
        SybaseCell::I8(value) => Ok(*value != 0),
        SybaseCell::I16(value) => Ok(*value != 0),
        SybaseCell::I32(value) => Ok(*value != 0),
        SybaseCell::I64(value) => Ok(*value != 0),
        SybaseCell::Bytes(bytes) => parse_bool(bytes),
        _ => Err(cell_parse_error(cell, "bool")),
    }
}

fn cell_parse_error(cell: &SybaseCell, ty: &'static str) -> SybaseSourceError {
    SybaseSourceError::ParseValue {
        value: cell.to_utf8_string(),
        ty,
    }
}

fn parse_hex_bytes(bytes: &[u8]) -> Result<Vec<u8>, SybaseSourceError> {
    let bytes = trim_ascii(bytes);
    if bytes.len() % 2 != 0 {
        return Err(SybaseSourceError::ParseValue {
            value: bytes_to_string(bytes),
            ty: "hex bytes",
        });
    }

    bytes
        .chunks_exact(2)
        .map(|chunk| {
            let hi = hex_value(chunk[0]).ok_or_else(|| SybaseSourceError::ParseValue {
                value: bytes_to_string(bytes),
                ty: "hex bytes",
            })?;
            let lo = hex_value(chunk[1]).ok_or_else(|| SybaseSourceError::ParseValue {
                value: bytes_to_string(bytes),
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

fn parse_u8(bytes: &[u8]) -> Result<u8, SybaseSourceError> {
    let value = parse_i64_with_ty(bytes, "u8")?;
    u8::try_from(value).map_err(|_| SybaseSourceError::ParseValue {
        value: bytes_to_string(bytes),
        ty: "u8",
    })
}

fn parse_i16(bytes: &[u8]) -> Result<i16, SybaseSourceError> {
    let value = parse_i64_with_ty(bytes, "i16")?;
    i16::try_from(value).map_err(|_| SybaseSourceError::ParseValue {
        value: bytes_to_string(bytes),
        ty: "i16",
    })
}

fn parse_i32(bytes: &[u8]) -> Result<i32, SybaseSourceError> {
    let value = parse_i64_with_ty(bytes, "i32")?;
    i32::try_from(value).map_err(|_| SybaseSourceError::ParseValue {
        value: bytes_to_string(bytes),
        ty: "i32",
    })
}

fn parse_i64(bytes: &[u8]) -> Result<i64, SybaseSourceError> {
    parse_i64_with_ty(bytes, "i64")
}

fn parse_f32(bytes: &[u8]) -> Result<f32, SybaseSourceError> {
    Ok(bytes_to_str(trim_ascii(bytes), "f32")?.parse::<f32>()?)
}

fn parse_f64(bytes: &[u8]) -> Result<f64, SybaseSourceError> {
    Ok(bytes_to_str(trim_ascii(bytes), "f64")?.parse::<f64>()?)
}

fn parse_decimal(bytes: &[u8]) -> Result<Decimal, SybaseSourceError> {
    Ok(bytes_to_str(trim_ascii(bytes), "Decimal")?.parse::<Decimal>()?)
}

fn parse_date(bytes: &[u8]) -> Result<NaiveDate, SybaseSourceError> {
    Ok(NaiveDate::parse_from_str(
        bytes_to_str(trim_ascii(bytes), "NaiveDate")?,
        "%Y-%m-%d",
    )?)
}

fn parse_time(bytes: &[u8]) -> Result<NaiveTime, SybaseSourceError> {
    let s = bytes_to_str(trim_ascii(bytes), "NaiveTime")?;
    NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S"))
        .map_err(SybaseSourceError::from)
}

fn parse_timestamp(bytes: &[u8]) -> Result<NaiveDateTime, SybaseSourceError> {
    let s = bytes_to_str(trim_ascii(bytes), "NaiveDateTime")?;
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .map_err(SybaseSourceError::from)
}

#[throws(SybaseSourceError)]
fn fetch_count(conn: &str, query: &str) -> usize {
    let cxq = CXQuery::Naked(query.to_string());
    let cquery = count_query(&cxq, &MsSqlDialect {})?;
    fetch_count_query(conn, cquery.as_str())?
}

#[throws(SybaseSourceError)]
fn fetch_count_query(conn: &str, query: &str) -> usize {
    let mut cursor = SybaseSource::execute_query(conn, query)?;
    let buffer = TextRowSet::for_cursor(1, &mut cursor, Some(64))?;
    let mut cursor = cursor.bind_buffer(buffer)?;
    let batch = cursor.fetch()?.ok_or(SybaseSourceError::GetNRowsFailed)?;
    let value = batch.at(0, 0).ok_or(SybaseSourceError::GetNRowsFailed)?;
    let value = parse_i64_with_ty(value, "usize")?;
    usize::try_from(value).map_err(|_| SybaseSourceError::ParseValue {
        value: bytes_to_string(batch.at(0, 0).unwrap_or_default()),
        ty: "usize",
    })?
}

#[throws(SybaseSourceError)]
pub(crate) fn fetch_i64_pair(conn: &str, query: &str) -> (i64, i64) {
    let mut cursor = SybaseSource::execute_query(conn, query)?;
    let buffer = TextRowSet::for_cursor(1, &mut cursor, Some(128))?;
    let mut cursor = cursor.bind_buffer(buffer)?;
    let batch = cursor.fetch()?.ok_or(SybaseSourceError::GetNRowsFailed)?;
    let min = parse_partition_value(batch.at(0, 0).ok_or(SybaseSourceError::GetNRowsFailed)?)?;
    let max = parse_partition_value(batch.at(1, 0).ok_or(SybaseSourceError::GetNRowsFailed)?)?;
    (min, max)
}

fn parse_partition_value(value: &[u8]) -> Result<i64, SybaseSourceError> {
    let trimmed = trim_ascii(value);
    if trimmed.is_empty() {
        return Ok(0);
    }

    match parse_i64_with_ty(trimmed, "partition range") {
        Ok(value) => Ok(value),
        Err(_) => bytes_to_str(trimmed, "partition range")?
            .parse::<f64>()
            .map(|value| value as i64)
            .map_err(|_| SybaseSourceError::ParseValue {
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

fn bytes_to_str<'a>(bytes: &'a [u8], ty: &'static str) -> Result<&'a str, SybaseSourceError> {
    std::str::from_utf8(bytes).map_err(|_| SybaseSourceError::ParseValue {
        value: bytes_to_string(bytes),
        ty,
    })
}

fn eq_ascii_ignore_case(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn parse_i64_with_ty(bytes: &[u8], ty: &'static str) -> Result<i64, SybaseSourceError> {
    let bytes = trim_ascii(bytes);
    if bytes.is_empty() {
        return Err(SybaseSourceError::ParseValue {
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
        return Err(SybaseSourceError::ParseValue {
            value: bytes_to_string(bytes),
            ty,
        });
    }

    let mut value = 0i64;
    for &byte in digits {
        if !byte.is_ascii_digit() {
            return Err(SybaseSourceError::ParseValue {
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
        .ok_or_else(|| SybaseSourceError::ParseValue {
            value: bytes_to_string(bytes),
            ty,
        })?;
    }

    Ok(value)
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
