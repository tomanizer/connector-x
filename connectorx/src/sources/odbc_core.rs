use std::{convert::TryFrom, fmt::Debug, marker::PhantomData, num::ParseFloatError};

use anyhow::anyhow;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use odbc_api::handles::StatementConnection;
use odbc_api::sys::{Date, Time, Timestamp};
use odbc_api::{
    buffers::{AnySlice, BufferDesc, ColumnarAnyBuffer, TextRowSet},
    environment, Bit, BlockCursor, Connection, ConnectionOptions, Cursor, CursorImpl, DataType,
    Nullability, ResultSetMetadata,
};
use rust_decimal::{Decimal, Error as DecimalParseError};
use sqlparser::dialect::Dialect;

use crate::{
    constants::DB_BUFFER_SIZE,
    errors::ConnectorXError,
    sources::PartitionParser,
    sql::{count_query, CXQuery},
    typesystem::TypeSystem,
};

pub(crate) type OdbcCursor = CursorImpl<StatementConnection<Connection<'static>>>;
pub(crate) type OdbcBlockCursor = BlockCursor<OdbcCursor, ColumnarAnyBuffer>;

pub trait OdbcCoreError:
    From<ConnectorXError>
    + From<odbc_api::Error>
    + From<anyhow::Error>
    + From<ParseFloatError>
    + From<DecimalParseError>
    + From<chrono::ParseError>
    + Send
    + Debug
{
    fn get_nrows_failed() -> Self;
    fn no_result_set(query: String) -> Self;
    fn parse_value(value: String, ty: &'static str) -> Self;
}

pub(crate) trait OdbcTypePolicy: TypeSystem + Copy {
    fn nullable(self) -> bool;
    fn buffer_desc(self, max_str_len: usize) -> BufferDesc;
}

pub struct OdbcParser<TS, E> {
    cursor: OdbcBlockCursor,
    rowbuf: Vec<Option<OdbcCell>>,
    ncols: usize,
    current_cell: usize,
    is_finished: bool,
    source_name: &'static str,
    _marker: PhantomData<(TS, E)>,
}

impl<TS, E> OdbcParser<TS, E>
where
    E: OdbcCoreError,
{
    pub(crate) fn new(cursor: OdbcBlockCursor, ncols: usize, source_name: &'static str) -> Self {
        Self {
            cursor,
            rowbuf: Vec::with_capacity(DB_BUFFER_SIZE),
            ncols,
            current_cell: 0,
            is_finished: false,
            source_name,
            _marker: PhantomData,
        }
    }

    pub(crate) fn next_cell(&mut self) -> Result<Option<&OdbcCell>, E> {
        let cell_index = self.current_cell;
        self.current_cell += 1;
        Ok(self.rowbuf[cell_index].as_ref())
    }

    pub(crate) fn next_bytes<T>(&mut self) -> Result<Option<&[u8]>, E> {
        let source_name = self.source_name;
        match self.next_cell()? {
            Some(cell) => Ok(Some(cell.try_bytes().ok_or_else(|| {
                ConnectorXError::cannot_produce::<T>(Some(format!(
                    "{source_name} typed value for byte-only parser"
                )))
            })?)),
            None => Ok(None),
        }
    }

    pub(crate) fn required_cell<T>(&mut self, ty: &'static str) -> Result<&OdbcCell, E> {
        let source_name = self.source_name;
        let value = self.next_cell()?;
        Ok(value.ok_or_else(|| {
            ConnectorXError::cannot_produce::<T>(Some(format!(
                "{source_name} NULL for non-null {ty}"
            )))
        })?)
    }

    pub(crate) fn required_bytes<T>(&mut self, ty: &'static str) -> Result<&[u8], E> {
        let source_name = self.source_name;
        let value = self.required_cell::<T>(ty)?;
        Ok(value.try_bytes().ok_or_else(|| {
            ConnectorXError::cannot_produce::<T>(Some(format!(
                "{source_name} typed value for byte-only {ty}"
            )))
        })?)
    }
}

impl<'a, TS, E> PartitionParser<'a> for OdbcParser<TS, E>
where
    TS: TypeSystem,
    E: OdbcCoreError,
{
    type TypeSystem = TS;
    type Error = E;

    fn fetch_next(&mut self) -> Result<(usize, bool), Self::Error> {
        if self.ncols == 0 {
            self.is_finished = true;
            return Ok((0, true));
        }
        assert!(matches!(self.current_cell.checked_rem(self.ncols), Some(0)));
        let remaining_cells = self.rowbuf.len() - self.current_cell;
        if remaining_cells > 0 {
            return Ok((remaining_cells / self.ncols, self.is_finished));
        } else if self.is_finished {
            return Ok((0, self.is_finished));
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
        Ok((self.rowbuf.len() / self.ncols, self.is_finished))
    }
}

#[derive(Clone, Debug)]
pub(crate) enum OdbcCell {
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
    pub(crate) fn try_bytes(&self) -> Option<&[u8]> {
        match self {
            OdbcCell::Bytes(bytes) => Some(bytes),
            _ => None,
        }
    }

    pub(crate) fn to_utf8_string(&self) -> String {
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

pub(crate) fn execute_query<E>(conn: &str, query: &str) -> Result<OdbcCursor, E>
where
    E: OdbcCoreError,
{
    let env = environment()?;
    let connection = env.connect_with_connection_string(conn, ConnectionOptions::default())?;
    Ok(connection
        .into_cursor(query, (), None)
        .map_err(|e| e.error)?
        .ok_or_else(|| E::no_result_set(query.to_string()))?)
}

pub(crate) fn fetch_metadata<T, E, F>(
    conn: &str,
    query: &str,
    map_type: F,
) -> Result<(Vec<String>, Vec<T>), E>
where
    E: OdbcCoreError,
    F: Fn(DataType, Nullability) -> T,
{
    let mut cursor = execute_query::<E>(conn, query)?;
    let ncols = cursor.num_result_cols()?;
    if ncols < 0 {
        return Err(anyhow!("ODBC returned negative column count: {}", ncols).into());
    }

    let mut names = Vec::with_capacity(ncols as usize);
    let mut schema = Vec::with_capacity(ncols as usize);
    for col in 1..=ncols as u16 {
        names.push(cursor.col_name(col)?);
        let ty = cursor.col_data_type(col)?;
        let nullability = cursor.col_nullability(col)?;
        schema.push(map_type(ty, nullability));
    }

    Ok((names, schema))
}

pub(crate) fn fetch_count<E, D>(conn: &str, query: &str, dialect: &D) -> Result<usize, E>
where
    E: OdbcCoreError,
    D: Dialect,
{
    let cxq = CXQuery::Naked(query.to_string());
    let cquery = count_query(&cxq, dialect)?;
    fetch_count_query(conn, cquery.as_str())
}

pub(crate) fn fetch_count_query<E>(conn: &str, query: &str) -> Result<usize, E>
where
    E: OdbcCoreError,
{
    let mut cursor = execute_query::<E>(conn, query)?;
    let buffer = TextRowSet::for_cursor(1, &mut cursor, Some(64))?;
    let mut cursor = cursor.bind_buffer(buffer)?;
    let batch = cursor.fetch()?.ok_or_else(E::get_nrows_failed)?;
    let value = batch.at(0, 0).ok_or_else(E::get_nrows_failed)?;
    let value = parse_i64_with_ty::<E>(value, "usize")?;
    Ok(usize::try_from(value).map_err(|_| {
        E::parse_value(bytes_to_string(batch.at(0, 0).unwrap_or_default()), "usize")
    })?)
}

pub(crate) fn fetch_i64_pair<E>(conn: &str, query: &str) -> Result<(i64, i64), E>
where
    E: OdbcCoreError,
{
    let mut cursor = execute_query::<E>(conn, query)?;
    let buffer = TextRowSet::for_cursor(1, &mut cursor, Some(128))?;
    let mut cursor = cursor.bind_buffer(buffer)?;
    let batch = cursor.fetch()?.ok_or_else(E::get_nrows_failed)?;
    let min = parse_partition_value::<E>(batch.at(0, 0).ok_or_else(E::get_nrows_failed)?)?;
    let max = parse_partition_value::<E>(batch.at(1, 0).ok_or_else(E::get_nrows_failed)?)?;
    Ok((min, max))
}

pub(crate) fn odbc_cell_from_column(column: AnySlice<'_>, row_index: usize) -> Option<OdbcCell> {
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

pub(crate) fn bit_to_bool(value: Bit) -> bool {
    value.0 != 0
}

pub(crate) fn parse_bool<E>(bytes: &[u8]) -> Result<bool, E>
where
    E: OdbcCoreError,
{
    match trim_ascii(bytes) {
        b"1" => Ok(true),
        b"0" => Ok(false),
        value if eq_ascii_ignore_case(value, b"true") => Ok(true),
        value if eq_ascii_ignore_case(value, b"false") => Ok(false),
        _ => Err(E::parse_value(bytes_to_string(bytes), "bool")),
    }
}

pub(crate) fn cell_u8<E>(cell: &OdbcCell) -> Result<u8, E>
where
    E: OdbcCoreError,
{
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

pub(crate) fn cell_i16<E>(cell: &OdbcCell) -> Result<i16, E>
where
    E: OdbcCoreError,
{
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

pub(crate) fn cell_i32<E>(cell: &OdbcCell) -> Result<i32, E>
where
    E: OdbcCoreError,
{
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

pub(crate) fn cell_i64<E>(cell: &OdbcCell) -> Result<i64, E>
where
    E: OdbcCoreError,
{
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

pub(crate) fn cell_f32<E>(cell: &OdbcCell) -> Result<f32, E>
where
    E: OdbcCoreError,
{
    match cell {
        OdbcCell::F32(value) => Ok(*value),
        OdbcCell::Bytes(bytes) => parse_f32(bytes),
        _ => Err(cell_parse_error(cell, "f32")),
    }
}

pub(crate) fn cell_f64<E>(cell: &OdbcCell) -> Result<f64, E>
where
    E: OdbcCoreError,
{
    match cell {
        OdbcCell::F32(value) => Ok(f64::from(*value)),
        OdbcCell::F64(value) => Ok(*value),
        OdbcCell::Bytes(bytes) => parse_f64(bytes),
        _ => Err(cell_parse_error(cell, "f64")),
    }
}

pub(crate) fn cell_bool<E>(cell: &OdbcCell) -> Result<bool, E>
where
    E: OdbcCoreError,
{
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

#[allow(dead_code)]
pub(crate) fn cell_date<E>(cell: &OdbcCell) -> Result<NaiveDate, E>
where
    E: OdbcCoreError,
{
    match cell {
        OdbcCell::Date(value) => odbc_date_to_naive(*value),
        OdbcCell::Bytes(bytes) => parse_date(bytes),
        _ => Err(cell_parse_error(cell, "NaiveDate")),
    }
}

#[allow(dead_code)]
pub(crate) fn cell_time<E>(cell: &OdbcCell) -> Result<NaiveTime, E>
where
    E: OdbcCoreError,
{
    match cell {
        OdbcCell::Time(value) => odbc_time_to_naive(*value),
        OdbcCell::Bytes(bytes) => parse_time(bytes),
        _ => Err(cell_parse_error(cell, "NaiveTime")),
    }
}

#[allow(dead_code)]
pub(crate) fn cell_timestamp<E>(cell: &OdbcCell) -> Result<NaiveDateTime, E>
where
    E: OdbcCoreError,
{
    match cell {
        OdbcCell::Timestamp(value) => odbc_timestamp_to_naive(*value),
        OdbcCell::Bytes(bytes) => parse_timestamp(bytes),
        _ => Err(cell_parse_error(cell, "NaiveDateTime")),
    }
}

fn cell_parse_error<E>(cell: &OdbcCell, ty: &'static str) -> E
where
    E: OdbcCoreError,
{
    E::parse_value(cell.to_utf8_string(), ty)
}

#[allow(dead_code)]
pub(crate) fn odbc_date_to_naive<E>(value: Date) -> Result<NaiveDate, E>
where
    E: OdbcCoreError,
{
    NaiveDate::from_ymd_opt(value.year.into(), value.month.into(), value.day.into()).ok_or_else(
        || {
            E::parse_value(
                format!("{:04}-{:02}-{:02}", value.year, value.month, value.day),
                "NaiveDate",
            )
        },
    )
}

#[allow(dead_code)]
pub(crate) fn odbc_time_to_naive<E>(value: Time) -> Result<NaiveTime, E>
where
    E: OdbcCoreError,
{
    NaiveTime::from_hms_opt(value.hour.into(), value.minute.into(), value.second.into()).ok_or_else(
        || {
            E::parse_value(
                format!("{:02}:{:02}:{:02}", value.hour, value.minute, value.second),
                "NaiveTime",
            )
        },
    )
}

#[allow(dead_code)]
pub(crate) fn odbc_timestamp_to_naive<E>(value: Timestamp) -> Result<NaiveDateTime, E>
where
    E: OdbcCoreError,
{
    let date = odbc_date_to_naive::<E>(Date {
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
    .ok_or_else(|| {
        E::parse_value(
            format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:09}",
                value.year,
                value.month,
                value.day,
                value.hour,
                value.minute,
                value.second,
                value.fraction
            ),
            "NaiveDateTime",
        )
    })
}

pub(crate) fn parse_u8<E>(bytes: &[u8]) -> Result<u8, E>
where
    E: OdbcCoreError,
{
    let value = parse_i64_with_ty::<E>(bytes, "u8")?;
    u8::try_from(value).map_err(|_| E::parse_value(bytes_to_string(bytes), "u8"))
}

pub(crate) fn parse_i16<E>(bytes: &[u8]) -> Result<i16, E>
where
    E: OdbcCoreError,
{
    let value = parse_i64_with_ty::<E>(bytes, "i16")?;
    i16::try_from(value).map_err(|_| E::parse_value(bytes_to_string(bytes), "i16"))
}

pub(crate) fn parse_i32<E>(bytes: &[u8]) -> Result<i32, E>
where
    E: OdbcCoreError,
{
    let value = parse_i64_with_ty::<E>(bytes, "i32")?;
    i32::try_from(value).map_err(|_| E::parse_value(bytes_to_string(bytes), "i32"))
}

pub(crate) fn parse_i64<E>(bytes: &[u8]) -> Result<i64, E>
where
    E: OdbcCoreError,
{
    parse_i64_with_ty::<E>(bytes, "i64")
}

pub(crate) fn parse_f32<E>(bytes: &[u8]) -> Result<f32, E>
where
    E: OdbcCoreError,
{
    Ok(bytes_to_str::<E>(trim_ascii(bytes), "f32")?.parse::<f32>()?)
}

pub(crate) fn parse_f64<E>(bytes: &[u8]) -> Result<f64, E>
where
    E: OdbcCoreError,
{
    Ok(bytes_to_str::<E>(trim_ascii(bytes), "f64")?.parse::<f64>()?)
}

pub(crate) fn parse_decimal<E>(bytes: &[u8]) -> Result<Decimal, E>
where
    E: OdbcCoreError,
{
    Ok(bytes_to_str::<E>(trim_ascii(bytes), "Decimal")?.parse::<Decimal>()?)
}

pub(crate) fn parse_date<E>(bytes: &[u8]) -> Result<NaiveDate, E>
where
    E: OdbcCoreError,
{
    Ok(NaiveDate::parse_from_str(
        bytes_to_str::<E>(trim_ascii(bytes), "NaiveDate")?,
        "%Y-%m-%d",
    )?)
}

pub(crate) fn parse_time<E>(bytes: &[u8]) -> Result<NaiveTime, E>
where
    E: OdbcCoreError,
{
    let s = bytes_to_str::<E>(trim_ascii(bytes), "NaiveTime")?;
    NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S"))
        .map_err(E::from)
}

pub(crate) fn parse_timestamp<E>(bytes: &[u8]) -> Result<NaiveDateTime, E>
where
    E: OdbcCoreError,
{
    let s = bytes_to_str::<E>(trim_ascii(bytes), "NaiveDateTime")?;
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .map_err(E::from)
}

fn parse_partition_value<E>(value: &[u8]) -> Result<i64, E>
where
    E: OdbcCoreError,
{
    let trimmed = trim_ascii(value);
    if trimmed.is_empty() {
        return Ok(0);
    }

    match parse_i64_with_ty::<E>(trimmed, "partition range") {
        Ok(value) => Ok(value),
        Err(_) => bytes_to_str::<E>(trimmed, "partition range")?
            .parse::<f64>()
            .map(|value| value as i64)
            .map_err(|_| E::parse_value(bytes_to_string(trimmed), "partition range")),
    }
}

pub(crate) fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.parse().ok()
}

pub(crate) fn trim_ascii(bytes: &[u8]) -> &[u8] {
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

pub(crate) fn bytes_to_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

pub(crate) fn bytes_to_str<'a, E>(bytes: &'a [u8], ty: &'static str) -> Result<&'a str, E>
where
    E: OdbcCoreError,
{
    std::str::from_utf8(bytes).map_err(|_| E::parse_value(bytes_to_string(bytes), ty))
}

pub(crate) fn eq_ascii_ignore_case(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

pub(crate) fn parse_i64_with_ty<E>(bytes: &[u8], ty: &'static str) -> Result<i64, E>
where
    E: OdbcCoreError,
{
    let bytes = trim_ascii(bytes);
    if bytes.is_empty() {
        return Err(E::parse_value(String::new(), ty));
    }

    let (negative, digits) = match bytes[0] {
        b'-' => (true, &bytes[1..]),
        b'+' => (false, &bytes[1..]),
        _ => (false, bytes),
    };
    if digits.is_empty() {
        return Err(E::parse_value(bytes_to_string(bytes), ty));
    }

    let mut value = 0i64;
    for &byte in digits {
        if !byte.is_ascii_digit() {
            return Err(E::parse_value(bytes_to_string(bytes), ty));
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
        .ok_or_else(|| E::parse_value(bytes_to_string(bytes), ty))?;
    }

    Ok(value)
}

macro_rules! impl_parse_from_bytes {
    ($parser:ty, $error:ty, $t:ty, $name:literal, $parse:ident) => {
        impl<'r> $crate::sources::Produce<'r, $t> for $parser {
            type Error = $error;

            fn produce(&'r mut self) -> Result<$t, $error> {
                let bytes = self.required_bytes::<$t>($name)?;
                $crate::sources::odbc_core::$parse::<$error>(bytes)
            }
        }

        impl<'r> $crate::sources::Produce<'r, Option<$t>> for $parser {
            type Error = $error;

            fn produce(&'r mut self) -> Result<Option<$t>, $error> {
                match self.next_bytes::<$t>()? {
                    Some(bytes) => Ok(Some($crate::sources::odbc_core::$parse::<$error>(bytes)?)),
                    None => Ok(None),
                }
            }
        }
    };
}

macro_rules! impl_parse_from_cell {
    ($parser:ty, $error:ty, $t:ty, $name:literal, $parse:ident) => {
        impl<'r> $crate::sources::Produce<'r, $t> for $parser {
            type Error = $error;

            fn produce(&'r mut self) -> Result<$t, $error> {
                let cell = self.required_cell::<$t>($name)?;
                $crate::sources::odbc_core::$parse::<$error>(cell)
            }
        }

        impl<'r> $crate::sources::Produce<'r, Option<$t>> for $parser {
            type Error = $error;

            fn produce(&'r mut self) -> Result<Option<$t>, $error> {
                match self.next_cell()? {
                    Some(cell) => Ok(Some($crate::sources::odbc_core::$parse::<$error>(cell)?)),
                    None => Ok(None),
                }
            }
        }
    };
}

macro_rules! impl_bool_produce {
    ($parser:ty, $error:ty) => {
        impl<'r> $crate::sources::Produce<'r, bool> for $parser {
            type Error = $error;

            fn produce(&'r mut self) -> Result<bool, $error> {
                $crate::sources::odbc_core::cell_bool::<$error>(self.required_cell::<bool>("bool")?)
            }
        }

        impl<'r> $crate::sources::Produce<'r, Option<bool>> for $parser {
            type Error = $error;

            fn produce(&'r mut self) -> Result<Option<bool>, $error> {
                match self.next_cell()? {
                    Some(cell) => Ok(Some($crate::sources::odbc_core::cell_bool::<$error>(cell)?)),
                    None => Ok(None),
                }
            }
        }
    };
}

macro_rules! impl_string_produce {
    ($parser:ty, $error:ty) => {
        impl<'r> $crate::sources::Produce<'r, String> for $parser {
            type Error = $error;

            fn produce(&'r mut self) -> Result<String, $error> {
                Ok(self.required_cell::<String>("String")?.to_utf8_string())
            }
        }

        impl<'r> $crate::sources::Produce<'r, Option<String>> for $parser {
            type Error = $error;

            fn produce(&'r mut self) -> Result<Option<String>, $error> {
                Ok(self
                    .next_cell()?
                    .map($crate::sources::odbc_core::OdbcCell::to_utf8_string))
            }
        }
    };
}

#[allow(unused_macros)]
macro_rules! impl_bytes_clone_produce {
    ($parser:ty, $error:ty) => {
        impl<'r> $crate::sources::Produce<'r, Vec<u8>> for $parser {
            type Error = $error;

            fn produce(&'r mut self) -> Result<Vec<u8>, $error> {
                Ok(self.required_bytes::<Vec<u8>>("Vec<u8>")?.to_vec())
            }
        }

        impl<'r> $crate::sources::Produce<'r, Option<Vec<u8>>> for $parser {
            type Error = $error;

            fn produce(&'r mut self) -> Result<Option<Vec<u8>>, $error> {
                Ok(self.next_bytes::<Vec<u8>>()?.map(|bytes| bytes.to_vec()))
            }
        }
    };
}

pub(crate) use impl_bool_produce;
#[allow(unused_imports)]
pub(crate) use impl_bytes_clone_produce;
pub(crate) use impl_parse_from_bytes;
pub(crate) use impl_parse_from_cell;
pub(crate) use impl_string_produce;
