//! Source implementation for IBM Db2 through ODBC.

mod errors;
mod typesystem;

pub use self::errors::Db2SourceError;
pub use self::typesystem::Db2TypeSystem;

use std::convert::TryFrom;

use crate::{
    constants::DB_BUFFER_SIZE,
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{
        odbc_common::{is_raw_odbc_conn_string, is_valid_odbc_key, odbc_conn_value},
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

type Db2Cursor = CursorImpl<StatementConnection<Connection<'static>>>;
type Db2BlockCursor = BlockCursor<Db2Cursor, ColumnarAnyBuffer>;

const DB2_DEFAULT_BATCH_SIZE: usize = 1024;
const DB2_DEFAULT_MAX_STR_LEN: usize = 1024;

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
            batch_size: env_usize("DB2_BATCH_SIZE").unwrap_or(DB2_DEFAULT_BATCH_SIZE),
            max_str_len: env_usize("DB2_MAX_STR_LEN").unwrap_or(DB2_DEFAULT_MAX_STR_LEN),
        }
    }

    #[throws(Db2SourceError)]
    fn execute_query(conn: &str, query: &str) -> Db2Cursor {
        let env = environment()?;
        let connection = env.connect_with_connection_string(conn, ConnectionOptions::default())?;
        connection
            .into_cursor(query, (), None)
            .map_err(|e| e.error)?
            .ok_or_else(|| Db2SourceError::NoResultSet(query.to_string()))?
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
            schema.push(Db2TypeSystem::from_odbc(ty, nullability));
        }

        self.names = names;
        self.schema = schema;
    }

    #[throws(Db2SourceError)]
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
        self.nrows = fetch_count_query(&self.conn, cquery.as_str())?;
    }

    #[throws(Db2SourceError)]
    fn parser(&mut self) -> Self::Parser<'_> {
        let cursor = Db2Source::execute_query(&self.conn, self.query.as_str())?;
        let buffer = ColumnarAnyBuffer::try_from_descs(
            self.batch_size,
            self.schema
                .iter()
                .map(|ty| db2_buffer_desc(*ty, self.max_str_len)),
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

pub struct Db2SourceParser {
    cursor: Db2BlockCursor,
    rowbuf: Vec<Option<Db2Cell>>,
    ncols: usize,
    current_col: usize,
    current_row: usize,
    is_finished: bool,
}

impl Db2SourceParser {
    fn new(cursor: Db2BlockCursor, ncols: usize) -> Self {
        Self {
            cursor,
            rowbuf: Vec::with_capacity(DB_BUFFER_SIZE),
            ncols,
            current_col: 0,
            current_row: 0,
            is_finished: false,
        }
    }

    #[throws(Db2SourceError)]
    fn next_cell(&mut self) -> Option<&Db2Cell> {
        let ridx = self.current_row;
        let cidx = self.current_col;
        self.current_row += (self.current_col + 1) / self.ncols;
        self.current_col = (self.current_col + 1) % self.ncols;
        self.rowbuf[ridx * self.ncols + cidx].as_ref()
    }

    #[throws(Db2SourceError)]
    fn next_bytes(&mut self) -> Option<&[u8]> {
        match self.next_cell()? {
            Some(cell) => Some(cell.try_bytes("bytes").ok_or_else(|| {
                ConnectorXError::cannot_produce::<Vec<u8>>(Some(
                    "Db2 typed value for byte-only parser".to_string(),
                ))
            })?),
            None => None,
        }
    }

    #[throws(Db2SourceError)]
    fn required_cell(&mut self, ty: &'static str) -> &Db2Cell {
        let value = self.next_cell()?;
        value.ok_or_else(|| {
            ConnectorXError::cannot_produce::<Vec<u8>>(Some(format!("Db2 NULL for non-null {ty}")))
        })?
    }

    #[throws(Db2SourceError)]
    fn required_bytes(&mut self, ty: &'static str) -> &[u8] {
        let value = self.required_cell(ty)?;
        value.try_bytes(ty).ok_or_else(|| {
            ConnectorXError::cannot_produce::<Vec<u8>>(Some(format!(
                "Db2 typed value for byte-only {ty}"
            )))
        })?
    }
}

impl PartitionParser<'_> for Db2SourceParser {
    type TypeSystem = Db2TypeSystem;
    type Error = Db2SourceError;

    #[throws(Db2SourceError)]
    fn fetch_next(&mut self) -> (usize, bool) {
        if self.ncols == 0 {
            self.is_finished = true;
            return (0, true);
        }
        assert!(self.current_col == 0);
        let remaining_rows = self.rowbuf.len() / self.ncols - self.current_row;
        if remaining_rows > 0 {
            return (remaining_rows, self.is_finished);
        } else if self.is_finished {
            return (0, self.is_finished);
        }

        self.rowbuf.clear();
        if let Some(batch) = self.cursor.fetch()? {
            self.rowbuf.reserve(batch.num_rows() * self.ncols);
            for row_index in 0..batch.num_rows() {
                for col_index in 0..batch.num_cols() {
                    self.rowbuf
                        .push(db2_cell_from_column(batch.column(col_index), row_index));
                }
            }
        } else {
            self.is_finished = true;
        }

        self.current_row = 0;
        self.current_col = 0;
        (self.rowbuf.len() / self.ncols, self.is_finished)
    }
}

#[derive(Clone, Debug)]
enum Db2Cell {
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

impl Db2Cell {
    fn try_bytes(&self, _ty: &'static str) -> Option<&[u8]> {
        match self {
            Db2Cell::Bytes(bytes) => Some(bytes),
            _ => None,
        }
    }

    fn to_utf8_string(&self) -> String {
        match self {
            Db2Cell::Bytes(bytes) => bytes_to_string(bytes),
            Db2Cell::U8(value) => value.to_string(),
            Db2Cell::I8(value) => value.to_string(),
            Db2Cell::I16(value) => value.to_string(),
            Db2Cell::I32(value) => value.to_string(),
            Db2Cell::I64(value) => value.to_string(),
            Db2Cell::F32(value) => value.to_string(),
            Db2Cell::F64(value) => value.to_string(),
            Db2Cell::Bool(value) => value.to_string(),
            Db2Cell::Date(value) => {
                format!("{:04}-{:02}-{:02}", value.year, value.month, value.day)
            }
            Db2Cell::Time(value) => {
                format!("{:02}:{:02}:{:02}", value.hour, value.minute, value.second)
            }
            Db2Cell::Timestamp(value) => format!(
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

fn db2_buffer_desc(ty: Db2TypeSystem, max_str_len: usize) -> BufferDesc {
    let nullable = db2_nullable(ty);
    match ty {
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
        Db2TypeSystem::Date(_) | Db2TypeSystem::Time(_) | Db2TypeSystem::Timestamp(_) => {
            db2_temporal_buffer_desc(ty, nullable)
        }
        Db2TypeSystem::Binary(_) => BufferDesc::Binary {
            max_bytes: max_str_len,
        },
    }
}

fn db2_temporal_buffer_desc(ty: Db2TypeSystem, nullable: bool) -> BufferDesc {
    match ty {
        Db2TypeSystem::Date(_) => BufferDesc::Date { nullable },
        Db2TypeSystem::Time(_) => BufferDesc::Time { nullable },
        Db2TypeSystem::Timestamp(_) => BufferDesc::Timestamp { nullable },
        _ => unreachable!("non-temporal Db2 type passed to temporal buffer desc"),
    }
}

fn db2_nullable(ty: Db2TypeSystem) -> bool {
    match ty {
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

fn db2_cell_from_column(column: AnySlice<'_>, row_index: usize) -> Option<Db2Cell> {
    match column {
        AnySlice::Text(view) => view
            .get(row_index)
            .map(|bytes| Db2Cell::Bytes(bytes.to_vec())),
        AnySlice::WText(view) => view
            .get(row_index)
            .map(|chars| Db2Cell::Bytes(String::from_utf16_lossy(chars).into_bytes())),
        AnySlice::Binary(view) => view
            .get(row_index)
            .map(|bytes| Db2Cell::Bytes(bytes.to_vec())),
        AnySlice::F64(values) => Some(Db2Cell::F64(values[row_index])),
        AnySlice::F32(values) => Some(Db2Cell::F32(values[row_index])),
        AnySlice::I8(values) => Some(Db2Cell::I8(values[row_index])),
        AnySlice::I16(values) => Some(Db2Cell::I16(values[row_index])),
        AnySlice::I32(values) => Some(Db2Cell::I32(values[row_index])),
        AnySlice::I64(values) => Some(Db2Cell::I64(values[row_index])),
        AnySlice::U8(values) => Some(Db2Cell::U8(values[row_index])),
        AnySlice::Bit(values) => Some(Db2Cell::Bool(bit_to_bool(values[row_index]))),
        AnySlice::Date(values) => Some(Db2Cell::Date(values[row_index])),
        AnySlice::Time(values) => Some(Db2Cell::Time(values[row_index])),
        AnySlice::Timestamp(values) => Some(Db2Cell::Timestamp(values[row_index])),
        AnySlice::NullableF64(values) => values.get(row_index).copied().map(Db2Cell::F64),
        AnySlice::NullableF32(values) => values.get(row_index).copied().map(Db2Cell::F32),
        AnySlice::NullableI8(values) => values.get(row_index).copied().map(Db2Cell::I8),
        AnySlice::NullableI16(values) => values.get(row_index).copied().map(Db2Cell::I16),
        AnySlice::NullableI32(values) => values.get(row_index).copied().map(Db2Cell::I32),
        AnySlice::NullableI64(values) => values.get(row_index).copied().map(Db2Cell::I64),
        AnySlice::NullableU8(values) => values.get(row_index).copied().map(Db2Cell::U8),
        AnySlice::NullableBit(values) => values
            .get(row_index)
            .copied()
            .map(bit_to_bool)
            .map(Db2Cell::Bool),
        AnySlice::NullableDate(values) => values.get(row_index).copied().map(Db2Cell::Date),
        AnySlice::NullableTime(values) => values.get(row_index).copied().map(Db2Cell::Time),
        AnySlice::NullableTimestamp(values) => {
            values.get(row_index).copied().map(Db2Cell::Timestamp)
        }
        AnySlice::Numeric(_) | AnySlice::NullableNumeric(_) => None,
    }
}

fn bit_to_bool(value: Bit) -> bool {
    value.0 != 0
}

macro_rules! impl_parse_from_bytes {
    ($t:ty, $name:literal, $parse:expr) => {
        impl<'r> Produce<'r, $t> for Db2SourceParser {
            type Error = Db2SourceError;

            #[throws(Db2SourceError)]
            fn produce(&'r mut self) -> $t {
                let bytes = self.required_bytes($name)?;
                ($parse)(bytes)?
            }
        }

        impl<'r> Produce<'r, Option<$t>> for Db2SourceParser {
            type Error = Db2SourceError;

            #[throws(Db2SourceError)]
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
        impl<'r> Produce<'r, $t> for Db2SourceParser {
            type Error = Db2SourceError;

            #[throws(Db2SourceError)]
            fn produce(&'r mut self) -> $t {
                let cell = self.required_cell($name)?;
                ($parse)(cell)?
            }
        }

        impl<'r> Produce<'r, Option<$t>> for Db2SourceParser {
            type Error = Db2SourceError;

            #[throws(Db2SourceError)]
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

impl<'r> Produce<'r, bool> for Db2SourceParser {
    type Error = Db2SourceError;

    #[throws(Db2SourceError)]
    fn produce(&'r mut self) -> bool {
        cell_bool(self.required_cell("bool")?)?
    }
}

impl<'r> Produce<'r, Option<bool>> for Db2SourceParser {
    type Error = Db2SourceError;

    #[throws(Db2SourceError)]
    fn produce(&'r mut self) -> Option<bool> {
        match self.next_cell()? {
            Some(cell) => Some(cell_bool(cell)?),
            None => None,
        }
    }
}

impl<'r> Produce<'r, String> for Db2SourceParser {
    type Error = Db2SourceError;

    #[throws(Db2SourceError)]
    fn produce(&'r mut self) -> String {
        self.required_cell("String")?.to_utf8_string()
    }
}

impl<'r> Produce<'r, Option<String>> for Db2SourceParser {
    type Error = Db2SourceError;

    #[throws(Db2SourceError)]
    fn produce(&'r mut self) -> Option<String> {
        self.next_cell()?.map(Db2Cell::to_utf8_string)
    }
}

impl<'r> Produce<'r, Vec<u8>> for Db2SourceParser {
    type Error = Db2SourceError;

    #[throws(Db2SourceError)]
    fn produce(&'r mut self) -> Vec<u8> {
        self.required_bytes("Vec<u8>")?.to_vec()
    }
}

impl<'r> Produce<'r, Option<Vec<u8>>> for Db2SourceParser {
    type Error = Db2SourceError;

    #[throws(Db2SourceError)]
    fn produce(&'r mut self) -> Option<Vec<u8>> {
        self.next_bytes()?.map(|bytes| bytes.to_vec())
    }
}

fn parse_bool(bytes: &[u8]) -> Result<bool, Db2SourceError> {
    match trim_ascii(bytes) {
        b"1" => Ok(true),
        b"0" => Ok(false),
        value if eq_ascii_ignore_case(value, b"true") => Ok(true),
        value if eq_ascii_ignore_case(value, b"false") => Ok(false),
        _ => Err(Db2SourceError::ParseValue {
            value: bytes_to_string(bytes),
            ty: "bool",
        }),
    }
}

fn cell_u8(cell: &Db2Cell) -> Result<u8, Db2SourceError> {
    match cell {
        Db2Cell::U8(value) => Ok(*value),
        Db2Cell::I8(value) => u8::try_from(*value).map_err(|_| cell_parse_error(cell, "u8")),
        Db2Cell::I16(value) => u8::try_from(*value).map_err(|_| cell_parse_error(cell, "u8")),
        Db2Cell::I32(value) => u8::try_from(*value).map_err(|_| cell_parse_error(cell, "u8")),
        Db2Cell::I64(value) => u8::try_from(*value).map_err(|_| cell_parse_error(cell, "u8")),
        Db2Cell::Bytes(bytes) => parse_u8(bytes),
        _ => Err(cell_parse_error(cell, "u8")),
    }
}

fn cell_i16(cell: &Db2Cell) -> Result<i16, Db2SourceError> {
    match cell {
        Db2Cell::I8(value) => Ok(i16::from(*value)),
        Db2Cell::U8(value) => Ok(i16::from(*value)),
        Db2Cell::I16(value) => Ok(*value),
        Db2Cell::I32(value) => i16::try_from(*value).map_err(|_| cell_parse_error(cell, "i16")),
        Db2Cell::I64(value) => i16::try_from(*value).map_err(|_| cell_parse_error(cell, "i16")),
        Db2Cell::Bytes(bytes) => parse_i16(bytes),
        _ => Err(cell_parse_error(cell, "i16")),
    }
}

fn cell_i32(cell: &Db2Cell) -> Result<i32, Db2SourceError> {
    match cell {
        Db2Cell::I8(value) => Ok(i32::from(*value)),
        Db2Cell::U8(value) => Ok(i32::from(*value)),
        Db2Cell::I16(value) => Ok(i32::from(*value)),
        Db2Cell::I32(value) => Ok(*value),
        Db2Cell::I64(value) => i32::try_from(*value).map_err(|_| cell_parse_error(cell, "i32")),
        Db2Cell::Bytes(bytes) => parse_i32(bytes),
        _ => Err(cell_parse_error(cell, "i32")),
    }
}

fn cell_i64(cell: &Db2Cell) -> Result<i64, Db2SourceError> {
    match cell {
        Db2Cell::I8(value) => Ok(i64::from(*value)),
        Db2Cell::U8(value) => Ok(i64::from(*value)),
        Db2Cell::I16(value) => Ok(i64::from(*value)),
        Db2Cell::I32(value) => Ok(i64::from(*value)),
        Db2Cell::I64(value) => Ok(*value),
        Db2Cell::Bytes(bytes) => parse_i64(bytes),
        _ => Err(cell_parse_error(cell, "i64")),
    }
}

fn cell_f32(cell: &Db2Cell) -> Result<f32, Db2SourceError> {
    match cell {
        Db2Cell::F32(value) => Ok(*value),
        Db2Cell::Bytes(bytes) => parse_f32(bytes),
        _ => Err(cell_parse_error(cell, "f32")),
    }
}

fn cell_f64(cell: &Db2Cell) -> Result<f64, Db2SourceError> {
    match cell {
        Db2Cell::F32(value) => Ok(f64::from(*value)),
        Db2Cell::F64(value) => Ok(*value),
        Db2Cell::Bytes(bytes) => parse_f64(bytes),
        _ => Err(cell_parse_error(cell, "f64")),
    }
}

fn cell_bool(cell: &Db2Cell) -> Result<bool, Db2SourceError> {
    match cell {
        Db2Cell::Bool(value) => Ok(*value),
        Db2Cell::U8(value) => Ok(*value != 0),
        Db2Cell::I8(value) => Ok(*value != 0),
        Db2Cell::I16(value) => Ok(*value != 0),
        Db2Cell::I32(value) => Ok(*value != 0),
        Db2Cell::I64(value) => Ok(*value != 0),
        Db2Cell::Bytes(bytes) => parse_bool(bytes),
        _ => Err(cell_parse_error(cell, "bool")),
    }
}

fn cell_date(cell: &Db2Cell) -> Result<NaiveDate, Db2SourceError> {
    match cell {
        Db2Cell::Date(value) => odbc_date_to_naive(*value),
        Db2Cell::Bytes(bytes) => parse_date(bytes),
        _ => Err(cell_parse_error(cell, "NaiveDate")),
    }
}

fn cell_time(cell: &Db2Cell) -> Result<NaiveTime, Db2SourceError> {
    match cell {
        Db2Cell::Time(value) => odbc_time_to_naive(*value),
        Db2Cell::Bytes(bytes) => parse_time(bytes),
        _ => Err(cell_parse_error(cell, "NaiveTime")),
    }
}

fn cell_timestamp(cell: &Db2Cell) -> Result<NaiveDateTime, Db2SourceError> {
    match cell {
        Db2Cell::Timestamp(value) => odbc_timestamp_to_naive(*value),
        Db2Cell::Bytes(bytes) => parse_timestamp(bytes),
        _ => Err(cell_parse_error(cell, "NaiveDateTime")),
    }
}

fn cell_parse_error(cell: &Db2Cell, ty: &'static str) -> Db2SourceError {
    Db2SourceError::ParseValue {
        value: cell.to_utf8_string(),
        ty,
    }
}

fn odbc_date_to_naive(value: Date) -> Result<NaiveDate, Db2SourceError> {
    NaiveDate::from_ymd_opt(value.year.into(), value.month.into(), value.day.into()).ok_or_else(
        || Db2SourceError::ParseValue {
            value: format!("{:04}-{:02}-{:02}", value.year, value.month, value.day),
            ty: "NaiveDate",
        },
    )
}

fn odbc_time_to_naive(value: Time) -> Result<NaiveTime, Db2SourceError> {
    NaiveTime::from_hms_opt(value.hour.into(), value.minute.into(), value.second.into()).ok_or_else(
        || Db2SourceError::ParseValue {
            value: format!("{:02}:{:02}:{:02}", value.hour, value.minute, value.second),
            ty: "NaiveTime",
        },
    )
}

fn odbc_timestamp_to_naive(value: Timestamp) -> Result<NaiveDateTime, Db2SourceError> {
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
    .ok_or_else(|| Db2SourceError::ParseValue {
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

fn parse_u8(bytes: &[u8]) -> Result<u8, Db2SourceError> {
    let value = parse_i64_with_ty(bytes, "u8")?;
    u8::try_from(value).map_err(|_| Db2SourceError::ParseValue {
        value: bytes_to_string(bytes),
        ty: "u8",
    })
}

fn parse_i16(bytes: &[u8]) -> Result<i16, Db2SourceError> {
    let value = parse_i64_with_ty(bytes, "i16")?;
    i16::try_from(value).map_err(|_| Db2SourceError::ParseValue {
        value: bytes_to_string(bytes),
        ty: "i16",
    })
}

fn parse_i32(bytes: &[u8]) -> Result<i32, Db2SourceError> {
    let value = parse_i64_with_ty(bytes, "i32")?;
    i32::try_from(value).map_err(|_| Db2SourceError::ParseValue {
        value: bytes_to_string(bytes),
        ty: "i32",
    })
}

fn parse_i64(bytes: &[u8]) -> Result<i64, Db2SourceError> {
    parse_i64_with_ty(bytes, "i64")
}

fn parse_f32(bytes: &[u8]) -> Result<f32, Db2SourceError> {
    Ok(bytes_to_str(trim_ascii(bytes), "f32")?.parse::<f32>()?)
}

fn parse_f64(bytes: &[u8]) -> Result<f64, Db2SourceError> {
    Ok(bytes_to_str(trim_ascii(bytes), "f64")?.parse::<f64>()?)
}

fn parse_decimal(bytes: &[u8]) -> Result<Decimal, Db2SourceError> {
    Ok(bytes_to_str(trim_ascii(bytes), "Decimal")?.parse::<Decimal>()?)
}

fn parse_date(bytes: &[u8]) -> Result<NaiveDate, Db2SourceError> {
    Ok(NaiveDate::parse_from_str(
        bytes_to_str(trim_ascii(bytes), "NaiveDate")?,
        "%Y-%m-%d",
    )?)
}

fn parse_time(bytes: &[u8]) -> Result<NaiveTime, Db2SourceError> {
    let s = bytes_to_str(trim_ascii(bytes), "NaiveTime")?;
    NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S"))
        .map_err(Db2SourceError::from)
}

fn parse_timestamp(bytes: &[u8]) -> Result<NaiveDateTime, Db2SourceError> {
    let s = bytes_to_str(trim_ascii(bytes), "NaiveDateTime")?;
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .map_err(Db2SourceError::from)
}

#[throws(Db2SourceError)]
fn fetch_count(conn: &str, query: &str) -> usize {
    let cxq = CXQuery::Naked(query.to_string());
    let cquery = count_query(&cxq, &GenericDialect {})?;
    fetch_count_query(conn, cquery.as_str())?
}

#[throws(Db2SourceError)]
fn fetch_count_query(conn: &str, query: &str) -> usize {
    let mut cursor = Db2Source::execute_query(conn, query)?;
    let buffer = TextRowSet::for_cursor(1, &mut cursor, Some(64))?;
    let mut cursor = cursor.bind_buffer(buffer)?;
    let batch = cursor.fetch()?.ok_or(Db2SourceError::GetNRowsFailed)?;
    let value = batch.at(0, 0).ok_or(Db2SourceError::GetNRowsFailed)?;
    let value = parse_i64_with_ty(value, "usize")?;
    usize::try_from(value).map_err(|_| Db2SourceError::ParseValue {
        value: bytes_to_string(batch.at(0, 0).unwrap_or_default()),
        ty: "usize",
    })?
}

#[throws(Db2SourceError)]
pub(crate) fn fetch_i64_pair(conn: &str, query: &str) -> (i64, i64) {
    let mut cursor = Db2Source::execute_query(conn, query)?;
    let buffer = TextRowSet::for_cursor(1, &mut cursor, Some(128))?;
    let mut cursor = cursor.bind_buffer(buffer)?;
    let batch = cursor.fetch()?.ok_or(Db2SourceError::GetNRowsFailed)?;
    let min = parse_partition_value(batch.at(0, 0).ok_or(Db2SourceError::GetNRowsFailed)?)?;
    let max = parse_partition_value(batch.at(1, 0).ok_or(Db2SourceError::GetNRowsFailed)?)?;
    (min, max)
}

fn parse_partition_value(value: &[u8]) -> Result<i64, Db2SourceError> {
    let trimmed = trim_ascii(value);
    if trimmed.is_empty() {
        return Ok(0);
    }

    match parse_i64_with_ty(trimmed, "partition range") {
        Ok(value) => Ok(value),
        Err(_) => bytes_to_str(trimmed, "partition range")?
            .parse::<f64>()
            .map(|value| value as i64)
            .map_err(|_| Db2SourceError::ParseValue {
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

fn bytes_to_str<'a>(bytes: &'a [u8], ty: &'static str) -> Result<&'a str, Db2SourceError> {
    std::str::from_utf8(bytes).map_err(|_| Db2SourceError::ParseValue {
        value: bytes_to_string(bytes),
        ty,
    })
}

fn eq_ascii_ignore_case(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn parse_i64_with_ty(bytes: &[u8], ty: &'static str) -> Result<i64, Db2SourceError> {
    let bytes = trim_ascii(bytes);
    if bytes.is_empty() {
        return Err(Db2SourceError::ParseValue {
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
        return Err(Db2SourceError::ParseValue {
            value: bytes_to_string(bytes),
            ty,
        });
    }

    let mut value = 0i64;
    for &byte in digits {
        if !byte.is_ascii_digit() {
            return Err(Db2SourceError::ParseValue {
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
        .ok_or_else(|| Db2SourceError::ParseValue {
            value: bytes_to_string(bytes),
            ty,
        })?;
    }

    Ok(value)
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
