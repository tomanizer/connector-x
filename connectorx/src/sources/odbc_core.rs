//! Shared ODBC source machinery for generic ODBC, Db2, and Sybase.
//!
//! Backends provide connection-string handling, SQL dialects, error conversion, and
//! buffer/type policy. This module owns the common ODBC execution, metadata,
//! columnar batch parsing, primitive conversion, count, and partition helpers.

use std::{borrow::Cow, convert::TryFrom, fmt::Debug, marker::PhantomData, num::ParseFloatError};

use anyhow::anyhow;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use odbc_api::handles::StatementConnection;
use odbc_api::sys::{Date, Time, Timestamp};
use odbc_api::{
    buffers::{AnySlice, BufferDesc, ColumnarAnyBuffer, Indicator, TextRowSet},
    environment, Bit, BlockCursor, Connection, ConnectionOptions, Cursor, CursorImpl, DataType,
    Nullability, ResultSetMetadata,
};
use rust_decimal::{Decimal, Error as DecimalParseError};
use sqlparser::dialect::Dialect;

use crate::{
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

/// Backend-specific ODBC metadata and buffer policy for the shared parser.
pub trait OdbcTypePolicy: TypeSystem + Copy {
    fn source_name() -> &'static str;
    fn max_str_len_env() -> &'static str;
    fn nullable(self) -> bool;
    fn buffer_desc(self, max_str_len: usize) -> BufferDesc;

    /// Return the per-column variable buffer length. `default_max_len` is a
    /// fallback for drivers that do not report useful metadata, not a cap on
    /// precise driver-reported widths.
    fn buffer_max_len(self, data_type: DataType, default_max_len: usize) -> usize {
        match self.buffer_desc(default_max_len) {
            BufferDesc::Text { .. } => data_type
                .utf8_len()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(default_max_len),
            BufferDesc::WText { .. } => data_type
                .column_size()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(default_max_len),
            BufferDesc::Binary { .. } => data_type
                .column_size()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(default_max_len),
            _ => default_max_len,
        }
    }
}

pub struct OdbcParser<TS, E> {
    cursor: OdbcBlockCursor,
    batch: OdbcBatch,
    ncols: usize,
    current_cell: usize,
    is_finished: bool,
    _marker: PhantomData<(TS, E)>,
}

impl<TS, E> OdbcParser<TS, E>
where
    TS: OdbcTypePolicy,
    E: OdbcCoreError,
{
    pub(crate) fn new(cursor: OdbcBlockCursor, ncols: usize) -> Self {
        Self {
            cursor,
            batch: OdbcBatch::with_capacity(ncols),
            ncols,
            current_cell: 0,
            is_finished: false,
            _marker: PhantomData,
        }
    }

    fn next_position(&mut self) -> Option<(usize, usize)> {
        if self.ncols == 0 {
            return None;
        }
        let cell_index = self.current_cell;
        self.current_cell += 1;
        Some((cell_index / self.ncols, cell_index % self.ncols))
    }

    pub(crate) fn next_cell(&mut self) -> Option<OdbcValue<'_>> {
        let (row_index, col_index) = self.next_position()?;
        self.batch.cell(row_index, col_index)
    }

    pub(crate) fn next_bytes<T>(&mut self) -> Result<Option<&[u8]>, E> {
        let source_name = TS::source_name();
        let Some((row_index, col_index)) = self.next_position() else {
            return Ok(None);
        };
        match self.batch.bytes(row_index, col_index) {
            Some(bytes) => Ok(bytes),
            None => Err(ConnectorXError::cannot_produce::<T>(Some(format!(
                "{source_name} typed value for byte-only parser"
            )))
            .into()),
        }
    }

    pub(crate) fn required_cell<T>(&mut self, ty: &'static str) -> Result<OdbcValue<'_>, E> {
        let source_name = TS::source_name();
        let value = self.next_cell();
        Ok(value.ok_or_else(|| {
            ConnectorXError::cannot_produce::<T>(Some(format!(
                "{source_name} NULL for non-null {ty}"
            )))
        })?)
    }

    pub(crate) fn required_bytes<T>(&mut self, ty: &'static str) -> Result<&[u8], E> {
        let source_name = TS::source_name();
        let value = self.next_bytes::<T>()?;
        Ok(value.ok_or_else(|| {
            ConnectorXError::cannot_produce::<T>(Some(format!(
                "{source_name} NULL for non-null {ty}"
            )))
        })?)
    }
}

impl<'a, TS, E> PartitionParser<'a> for OdbcParser<TS, E>
where
    TS: OdbcTypePolicy,
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
        let remaining_cells = self.batch.nrows * self.ncols - self.current_cell;
        if remaining_cells > 0 {
            return Ok((remaining_cells / self.ncols, self.is_finished));
        } else if self.is_finished {
            return Ok((0, self.is_finished));
        }

        self.batch.nrows = 0;
        if let Some(batch) = self.cursor.fetch()? {
            let num_rows = batch.num_rows();
            let num_cols = batch.num_cols();
            self.batch.nrows = num_rows;
            for col_index in 0..num_cols {
                self.batch.replace_column::<E>(
                    col_index,
                    batch.column(col_index),
                    TS::source_name(),
                    TS::max_str_len_env(),
                )?;
            }
            self.batch.truncate_columns(num_cols);
        } else {
            self.batch.clear();
            self.is_finished = true;
        }

        self.current_cell = 0;
        Ok((self.batch.nrows, self.is_finished))
    }
}

#[derive(Default)]
struct OdbcBatch {
    columns: Vec<OdbcColumn>,
    nrows: usize,
}

impl OdbcBatch {
    fn with_capacity(ncols: usize) -> Self {
        Self {
            columns: Vec::with_capacity(ncols),
            nrows: 0,
        }
    }

    fn clear(&mut self) {
        self.columns.clear();
        self.nrows = 0;
    }

    fn replace_column<E>(
        &mut self,
        col_index: usize,
        column: AnySlice<'_>,
        source_name: &'static str,
        max_str_len_env: &'static str,
    ) -> Result<(), E>
    where
        E: OdbcCoreError,
    {
        ensure_column_not_truncated::<E>(&column, source_name, max_str_len_env, col_index)?;
        if col_index == self.columns.len() {
            self.columns
                .push(OdbcColumn::from_slice(column, self.nrows));
        } else {
            self.columns[col_index].replace_from_slice(column, self.nrows);
        }
        Ok(())
    }

    fn truncate_columns(&mut self, ncols: usize) {
        self.columns.truncate(ncols);
    }

    fn cell(&self, row_index: usize, col_index: usize) -> Option<OdbcValue<'_>> {
        self.columns[col_index].cell(row_index)
    }

    fn bytes(&self, row_index: usize, col_index: usize) -> Option<Option<&[u8]>> {
        self.columns[col_index].bytes(row_index)
    }
}

pub(crate) fn ensure_column_not_truncated<E>(
    column: &AnySlice<'_>,
    source_name: &'static str,
    max_str_len_env: &'static str,
    col_index: usize,
) -> Result<(), E>
where
    E: OdbcCoreError,
{
    let indicator = truncated_indicator(column);
    if let Some(indicator) = indicator {
        return Err(truncation_error(source_name, max_str_len_env, col_index, indicator).into());
    }
    Ok(())
}

fn truncated_indicator(column: &AnySlice<'_>) -> Option<Indicator> {
    match column {
        AnySlice::Text(view) => view.has_truncated_values(),
        AnySlice::WText(view) => view.has_truncated_values(),
        AnySlice::Binary(view) => view.has_truncated_values(),
        _ => None,
    }
}

fn truncation_error(
    source_name: &'static str,
    max_str_len_env: &'static str,
    col_index: usize,
    indicator: Indicator,
) -> anyhow::Error {
    let column_number = col_index + 1;
    match indicator {
        Indicator::Length(required_len) => anyhow!(
            "{source_name} column {column_number} was truncated by the ODBC fetch buffer \
             ({required_len} bytes required); increase {max_str_len_env} or cast/substr \
             the column in the query"
        ),
        Indicator::NoTotal => anyhow!(
            "{source_name} column {column_number} could not be fully fetched because the ODBC \
             driver did not report the value length (NoTotal); increasing {max_str_len_env} \
             may not help, so cast the column to a sized varchar(N) or varbinary(N) or substr \
             it in the query"
        ),
        other => anyhow!(
            "{source_name} column {column_number} was truncated by the ODBC fetch buffer \
             ({other:?}); increase {max_str_len_env} or cast/substr the column in the query"
        ),
    }
}

// A fetched ODBC batch cannot be borrowed across `fetch_next`, so the parser
// keeps a columnar snapshot instead of a row-major `Vec<Option<OdbcCell>>`.
// Primitive columns are copied compactly once per batch. Text, wide text, and
// binary values are owned only in their source column until the destination
// builder requests a `String` or `Vec<u8>`.
enum OdbcColumn {
    Bytes(Vec<Option<Vec<u8>>>),
    U8(Vec<u8>),
    I8(Vec<i8>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Bool(Vec<bool>),
    Date(Vec<Date>),
    Time(Vec<Time>),
    Timestamp(Vec<Timestamp>),
    NullableU8(Vec<Option<u8>>),
    NullableI8(Vec<Option<i8>>),
    NullableI16(Vec<Option<i16>>),
    NullableI32(Vec<Option<i32>>),
    NullableI64(Vec<Option<i64>>),
    NullableF32(Vec<Option<f32>>),
    NullableF64(Vec<Option<f64>>),
    NullableBool(Vec<Option<bool>>),
    NullableDate(Vec<Option<Date>>),
    NullableTime(Vec<Option<Time>>),
    NullableTimestamp(Vec<Option<Timestamp>>),
    Unsupported,
}

macro_rules! ensure_column {
    ($method:ident, $variant:ident, $value:ty) => {
        fn $method(&mut self) -> &mut Vec<$value> {
            if !matches!(self, Self::$variant(_)) {
                *self = Self::$variant(Vec::new());
            }
            match self {
                Self::$variant(values) => values,
                _ => unreachable!(),
            }
        }
    };
}

impl OdbcColumn {
    fn from_slice(column: AnySlice<'_>, nrows: usize) -> Self {
        let mut value = Self::Unsupported;
        value.replace_from_slice(column, nrows);
        value
    }

    fn replace_from_slice(&mut self, column: AnySlice<'_>, nrows: usize) {
        match column {
            AnySlice::Text(view) => {
                let values = self.ensure_bytes();
                fill_byte_column(values, nrows, |row_index| view.get(row_index));
            }
            AnySlice::WText(view) => {
                let values = self.ensure_bytes();
                values.resize_with(nrows, || None);
                for row_index in 0..nrows {
                    match view.get(row_index) {
                        Some(chars) => {
                            let slot = values[row_index].get_or_insert_with(Vec::new);
                            slot.clear();
                            for item in std::char::decode_utf16(chars.iter().copied()) {
                                let ch = item.unwrap_or(std::char::REPLACEMENT_CHARACTER);
                                let mut bytes = [0; 4];
                                slot.extend_from_slice(ch.encode_utf8(&mut bytes).as_bytes());
                            }
                        }
                        None => values[row_index] = None,
                    }
                }
            }
            AnySlice::Binary(view) => {
                let values = self.ensure_bytes();
                fill_byte_column(values, nrows, |row_index| view.get(row_index));
            }
            AnySlice::F64(values) => copy_slice(self.ensure_f64(), values, nrows),
            AnySlice::F32(values) => copy_slice(self.ensure_f32(), values, nrows),
            AnySlice::I8(values) => copy_slice(self.ensure_i8(), values, nrows),
            AnySlice::I16(values) => copy_slice(self.ensure_i16(), values, nrows),
            AnySlice::I32(values) => copy_slice(self.ensure_i32(), values, nrows),
            AnySlice::I64(values) => copy_slice(self.ensure_i64(), values, nrows),
            AnySlice::U8(values) => copy_slice(self.ensure_u8(), values, nrows),
            AnySlice::Bit(values) => fill_from_iter(
                self.ensure_bool(),
                values[..nrows].iter().copied().map(bit_to_bool),
            ),
            AnySlice::Date(values) => copy_slice(self.ensure_date(), values, nrows),
            AnySlice::Time(values) => copy_slice(self.ensure_time(), values, nrows),
            AnySlice::Timestamp(values) => copy_slice(self.ensure_timestamp(), values, nrows),
            AnySlice::NullableF64(values) => {
                fill_from_iter(
                    self.ensure_nullable_f64(),
                    (0..nrows).map(|row| values.get(row).copied()),
                );
            }
            AnySlice::NullableF32(values) => {
                fill_from_iter(
                    self.ensure_nullable_f32(),
                    (0..nrows).map(|row| values.get(row).copied()),
                );
            }
            AnySlice::NullableI8(values) => {
                fill_from_iter(
                    self.ensure_nullable_i8(),
                    (0..nrows).map(|row| values.get(row).copied()),
                );
            }
            AnySlice::NullableI16(values) => {
                fill_from_iter(
                    self.ensure_nullable_i16(),
                    (0..nrows).map(|row| values.get(row).copied()),
                );
            }
            AnySlice::NullableI32(values) => {
                fill_from_iter(
                    self.ensure_nullable_i32(),
                    (0..nrows).map(|row| values.get(row).copied()),
                );
            }
            AnySlice::NullableI64(values) => {
                fill_from_iter(
                    self.ensure_nullable_i64(),
                    (0..nrows).map(|row| values.get(row).copied()),
                );
            }
            AnySlice::NullableU8(values) => {
                fill_from_iter(
                    self.ensure_nullable_u8(),
                    (0..nrows).map(|row| values.get(row).copied()),
                );
            }
            AnySlice::NullableBit(values) => {
                fill_from_iter(
                    self.ensure_nullable_bool(),
                    (0..nrows).map(|row| values.get(row).copied().map(bit_to_bool)),
                );
            }
            AnySlice::NullableDate(values) => {
                fill_from_iter(
                    self.ensure_nullable_date(),
                    (0..nrows).map(|row| values.get(row).copied()),
                );
            }
            AnySlice::NullableTime(values) => {
                fill_from_iter(
                    self.ensure_nullable_time(),
                    (0..nrows).map(|row| values.get(row).copied()),
                );
            }
            AnySlice::NullableTimestamp(values) => {
                fill_from_iter(
                    self.ensure_nullable_timestamp(),
                    (0..nrows).map(|row| values.get(row).copied()),
                );
            }
            AnySlice::Numeric(_) | AnySlice::NullableNumeric(_) => *self = Self::Unsupported,
        }
    }

    ensure_column!(ensure_bytes, Bytes, Option<Vec<u8>>);
    ensure_column!(ensure_u8, U8, u8);
    ensure_column!(ensure_i8, I8, i8);
    ensure_column!(ensure_i16, I16, i16);
    ensure_column!(ensure_i32, I32, i32);
    ensure_column!(ensure_i64, I64, i64);
    ensure_column!(ensure_f32, F32, f32);
    ensure_column!(ensure_f64, F64, f64);
    ensure_column!(ensure_bool, Bool, bool);
    ensure_column!(ensure_date, Date, Date);
    ensure_column!(ensure_time, Time, Time);
    ensure_column!(ensure_timestamp, Timestamp, Timestamp);
    ensure_column!(ensure_nullable_u8, NullableU8, Option<u8>);
    ensure_column!(ensure_nullable_i8, NullableI8, Option<i8>);
    ensure_column!(ensure_nullable_i16, NullableI16, Option<i16>);
    ensure_column!(ensure_nullable_i32, NullableI32, Option<i32>);
    ensure_column!(ensure_nullable_i64, NullableI64, Option<i64>);
    ensure_column!(ensure_nullable_f32, NullableF32, Option<f32>);
    ensure_column!(ensure_nullable_f64, NullableF64, Option<f64>);
    ensure_column!(ensure_nullable_bool, NullableBool, Option<bool>);
    ensure_column!(ensure_nullable_date, NullableDate, Option<Date>);
    ensure_column!(ensure_nullable_time, NullableTime, Option<Time>);
    ensure_column!(
        ensure_nullable_timestamp,
        NullableTimestamp,
        Option<Timestamp>
    );

    fn cell(&self, row_index: usize) -> Option<OdbcValue<'_>> {
        match self {
            Self::Bytes(values) => values
                .get(row_index)
                .and_then(Option::as_deref)
                .map(|bytes| OdbcValue::Bytes(Cow::Borrowed(bytes))),
            Self::U8(values) => values.get(row_index).copied().map(OdbcValue::U8),
            Self::I8(values) => values.get(row_index).copied().map(OdbcValue::I8),
            Self::I16(values) => values.get(row_index).copied().map(OdbcValue::I16),
            Self::I32(values) => values.get(row_index).copied().map(OdbcValue::I32),
            Self::I64(values) => values.get(row_index).copied().map(OdbcValue::I64),
            Self::F32(values) => values.get(row_index).copied().map(OdbcValue::F32),
            Self::F64(values) => values.get(row_index).copied().map(OdbcValue::F64),
            Self::Bool(values) => values.get(row_index).copied().map(OdbcValue::Bool),
            Self::Date(values) => values.get(row_index).copied().map(OdbcValue::Date),
            Self::Time(values) => values.get(row_index).copied().map(OdbcValue::Time),
            Self::Timestamp(values) => values.get(row_index).copied().map(OdbcValue::Timestamp),
            Self::NullableU8(values) => values.get(row_index).copied().flatten().map(OdbcValue::U8),
            Self::NullableI8(values) => values.get(row_index).copied().flatten().map(OdbcValue::I8),
            Self::NullableI16(values) => {
                values.get(row_index).copied().flatten().map(OdbcValue::I16)
            }
            Self::NullableI32(values) => {
                values.get(row_index).copied().flatten().map(OdbcValue::I32)
            }
            Self::NullableI64(values) => {
                values.get(row_index).copied().flatten().map(OdbcValue::I64)
            }
            Self::NullableF32(values) => {
                values.get(row_index).copied().flatten().map(OdbcValue::F32)
            }
            Self::NullableF64(values) => {
                values.get(row_index).copied().flatten().map(OdbcValue::F64)
            }
            Self::NullableBool(values) => values
                .get(row_index)
                .copied()
                .flatten()
                .map(OdbcValue::Bool),
            Self::NullableDate(values) => values
                .get(row_index)
                .copied()
                .flatten()
                .map(OdbcValue::Date),
            Self::NullableTime(values) => values
                .get(row_index)
                .copied()
                .flatten()
                .map(OdbcValue::Time),
            Self::NullableTimestamp(values) => values
                .get(row_index)
                .copied()
                .flatten()
                .map(OdbcValue::Timestamp),
            Self::Unsupported => None,
        }
    }

    fn bytes(&self, row_index: usize) -> Option<Option<&[u8]>> {
        match self {
            Self::Bytes(values) => Some(values.get(row_index).and_then(Option::as_deref)),
            Self::Unsupported => Some(None),
            _ => None,
        }
    }
}

fn copy_slice<T: Copy>(values: &mut Vec<T>, source: &[T], nrows: usize) {
    values.clear();
    values.extend_from_slice(&source[..nrows]);
}

fn fill_from_iter<T>(values: &mut Vec<T>, source: impl IntoIterator<Item = T>) {
    values.clear();
    values.extend(source);
}

fn fill_byte_column<'a>(
    values: &mut Vec<Option<Vec<u8>>>,
    nrows: usize,
    mut get: impl FnMut(usize) -> Option<&'a [u8]>,
) {
    values.resize_with(nrows, || None);
    for row_index in 0..nrows {
        match get(row_index) {
            Some(bytes) => {
                let slot = values[row_index].get_or_insert_with(Vec::new);
                slot.clear();
                slot.extend_from_slice(bytes);
            }
            None => values[row_index] = None,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum OdbcValue<'a> {
    // The parser's columnar batch path always yields `Borrowed` bytes. The
    // Arrow fallback may produce `Owned` bytes when converting WText rows.
    Bytes(Cow<'a, [u8]>),
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

impl OdbcValue<'_> {
    #[allow(dead_code)]
    pub(crate) fn try_bytes(&self) -> Option<&[u8]> {
        match self {
            OdbcValue::Bytes(bytes) => Some(bytes.as_ref()),
            _ => None,
        }
    }

    pub(crate) fn to_utf8_string(&self) -> String {
        match self {
            OdbcValue::Bytes(bytes) => bytes_to_string(bytes.as_ref()),
            OdbcValue::U8(value) => value.to_string(),
            OdbcValue::I8(value) => value.to_string(),
            OdbcValue::I16(value) => value.to_string(),
            OdbcValue::I32(value) => value.to_string(),
            OdbcValue::I64(value) => value.to_string(),
            OdbcValue::F32(value) => value.to_string(),
            OdbcValue::F64(value) => value.to_string(),
            OdbcValue::Bool(value) => value.to_string(),
            OdbcValue::Date(value) => {
                format!("{:04}-{:02}-{:02}", value.year, value.month, value.day)
            }
            OdbcValue::Time(value) => {
                format!("{:02}:{:02}:{:02}", value.hour, value.minute, value.second)
            }
            OdbcValue::Timestamp(value) => format!(
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
    default_max_len: usize,
    map_type: F,
) -> Result<(Vec<String>, Vec<T>, Vec<usize>), E>
where
    E: OdbcCoreError,
    T: OdbcTypePolicy,
    F: Fn(DataType, Nullability) -> T,
{
    let mut cursor = execute_query::<E>(conn, query)?;
    let ncols = cursor.num_result_cols()?;
    if ncols < 0 {
        return Err(anyhow!("ODBC returned negative column count: {}", ncols).into());
    }

    let ncols = u16::try_from(ncols).map_err(|_| {
        anyhow!(
            "ODBC returned too many columns for u16 metadata index: {}",
            ncols
        )
    })?;
    let mut names = Vec::with_capacity(ncols as usize);
    let mut schema = Vec::with_capacity(ncols as usize);
    let mut buffer_max_lens = Vec::with_capacity(ncols as usize);
    for col in 1..=ncols {
        names.push(cursor.col_name(col)?);
        let data_type = cursor.col_data_type(col)?;
        let nullability = cursor.col_nullability(col)?;
        let ty = map_type(data_type, nullability);
        buffer_max_lens.push(ty.buffer_max_len(data_type, default_max_len));
        schema.push(ty);
    }

    Ok((names, schema, buffer_max_lens))
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
    let raw_value = batch.at(0, 0).ok_or_else(E::get_nrows_failed)?;
    let parsed_value = parse_i64_with_ty::<E>(raw_value, "usize")?;
    Ok(usize::try_from(parsed_value)
        .map_err(|_| E::parse_value(bytes_to_string(raw_value), "usize"))?)
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

#[allow(dead_code)]
pub(crate) fn odbc_cell_from_column(
    column: AnySlice<'_>,
    row_index: usize,
) -> Option<OdbcValue<'_>> {
    match column {
        AnySlice::Text(view) => view
            .get(row_index)
            .map(|bytes| OdbcValue::Bytes(Cow::Borrowed(bytes))),
        AnySlice::WText(view) => view.get(row_index).map(|chars| {
            OdbcValue::Bytes(Cow::Owned(String::from_utf16_lossy(chars).into_bytes()))
        }),
        AnySlice::Binary(view) => view
            .get(row_index)
            .map(|bytes| OdbcValue::Bytes(Cow::Borrowed(bytes))),
        AnySlice::F64(values) => Some(OdbcValue::F64(values[row_index])),
        AnySlice::F32(values) => Some(OdbcValue::F32(values[row_index])),
        AnySlice::I8(values) => Some(OdbcValue::I8(values[row_index])),
        AnySlice::I16(values) => Some(OdbcValue::I16(values[row_index])),
        AnySlice::I32(values) => Some(OdbcValue::I32(values[row_index])),
        AnySlice::I64(values) => Some(OdbcValue::I64(values[row_index])),
        AnySlice::U8(values) => Some(OdbcValue::U8(values[row_index])),
        AnySlice::Bit(values) => Some(OdbcValue::Bool(bit_to_bool(values[row_index]))),
        AnySlice::Date(values) => Some(OdbcValue::Date(values[row_index])),
        AnySlice::Time(values) => Some(OdbcValue::Time(values[row_index])),
        AnySlice::Timestamp(values) => Some(OdbcValue::Timestamp(values[row_index])),
        AnySlice::NullableF64(values) => values.get(row_index).copied().map(OdbcValue::F64),
        AnySlice::NullableF32(values) => values.get(row_index).copied().map(OdbcValue::F32),
        AnySlice::NullableI8(values) => values.get(row_index).copied().map(OdbcValue::I8),
        AnySlice::NullableI16(values) => values.get(row_index).copied().map(OdbcValue::I16),
        AnySlice::NullableI32(values) => values.get(row_index).copied().map(OdbcValue::I32),
        AnySlice::NullableI64(values) => values.get(row_index).copied().map(OdbcValue::I64),
        AnySlice::NullableU8(values) => values.get(row_index).copied().map(OdbcValue::U8),
        AnySlice::NullableBit(values) => values
            .get(row_index)
            .copied()
            .map(bit_to_bool)
            .map(OdbcValue::Bool),
        AnySlice::NullableDate(values) => values.get(row_index).copied().map(OdbcValue::Date),
        AnySlice::NullableTime(values) => values.get(row_index).copied().map(OdbcValue::Time),
        AnySlice::NullableTimestamp(values) => {
            values.get(row_index).copied().map(OdbcValue::Timestamp)
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

pub(crate) fn cell_u8<E>(cell: OdbcValue<'_>) -> Result<u8, E>
where
    E: OdbcCoreError,
{
    match &cell {
        OdbcValue::U8(value) => Ok(*value),
        OdbcValue::I8(value) => u8::try_from(*value).map_err(|_| cell_parse_error(&cell, "u8")),
        OdbcValue::I16(value) => u8::try_from(*value).map_err(|_| cell_parse_error(&cell, "u8")),
        OdbcValue::I32(value) => u8::try_from(*value).map_err(|_| cell_parse_error(&cell, "u8")),
        OdbcValue::I64(value) => u8::try_from(*value).map_err(|_| cell_parse_error(&cell, "u8")),
        OdbcValue::Bytes(bytes) => parse_u8(bytes.as_ref()),
        _ => Err(cell_parse_error(&cell, "u8")),
    }
}

pub(crate) fn cell_i16<E>(cell: OdbcValue<'_>) -> Result<i16, E>
where
    E: OdbcCoreError,
{
    match &cell {
        OdbcValue::I8(value) => Ok(i16::from(*value)),
        OdbcValue::U8(value) => Ok(i16::from(*value)),
        OdbcValue::I16(value) => Ok(*value),
        OdbcValue::I32(value) => i16::try_from(*value).map_err(|_| cell_parse_error(&cell, "i16")),
        OdbcValue::I64(value) => i16::try_from(*value).map_err(|_| cell_parse_error(&cell, "i16")),
        OdbcValue::Bytes(bytes) => parse_i16(bytes.as_ref()),
        _ => Err(cell_parse_error(&cell, "i16")),
    }
}

pub(crate) fn cell_i32<E>(cell: OdbcValue<'_>) -> Result<i32, E>
where
    E: OdbcCoreError,
{
    match &cell {
        OdbcValue::I8(value) => Ok(i32::from(*value)),
        OdbcValue::U8(value) => Ok(i32::from(*value)),
        OdbcValue::I16(value) => Ok(i32::from(*value)),
        OdbcValue::I32(value) => Ok(*value),
        OdbcValue::I64(value) => i32::try_from(*value).map_err(|_| cell_parse_error(&cell, "i32")),
        OdbcValue::Bytes(bytes) => parse_i32(bytes.as_ref()),
        _ => Err(cell_parse_error(&cell, "i32")),
    }
}

pub(crate) fn cell_i64<E>(cell: OdbcValue<'_>) -> Result<i64, E>
where
    E: OdbcCoreError,
{
    match &cell {
        OdbcValue::I8(value) => Ok(i64::from(*value)),
        OdbcValue::U8(value) => Ok(i64::from(*value)),
        OdbcValue::I16(value) => Ok(i64::from(*value)),
        OdbcValue::I32(value) => Ok(i64::from(*value)),
        OdbcValue::I64(value) => Ok(*value),
        OdbcValue::Bytes(bytes) => parse_i64(bytes.as_ref()),
        _ => Err(cell_parse_error(&cell, "i64")),
    }
}

pub(crate) fn cell_f32<E>(cell: OdbcValue<'_>) -> Result<f32, E>
where
    E: OdbcCoreError,
{
    match &cell {
        OdbcValue::F32(value) => Ok(*value),
        OdbcValue::Bytes(bytes) => parse_f32(bytes.as_ref()),
        _ => Err(cell_parse_error(&cell, "f32")),
    }
}

pub(crate) fn cell_f64<E>(cell: OdbcValue<'_>) -> Result<f64, E>
where
    E: OdbcCoreError,
{
    match &cell {
        OdbcValue::F32(value) => Ok(f64::from(*value)),
        OdbcValue::F64(value) => Ok(*value),
        OdbcValue::Bytes(bytes) => parse_f64(bytes.as_ref()),
        _ => Err(cell_parse_error(&cell, "f64")),
    }
}

pub(crate) fn cell_bool<E>(cell: OdbcValue<'_>) -> Result<bool, E>
where
    E: OdbcCoreError,
{
    match &cell {
        OdbcValue::Bool(value) => Ok(*value),
        OdbcValue::U8(value) => Ok(*value != 0),
        OdbcValue::I8(value) => Ok(*value != 0),
        OdbcValue::I16(value) => Ok(*value != 0),
        OdbcValue::I32(value) => Ok(*value != 0),
        OdbcValue::I64(value) => Ok(*value != 0),
        OdbcValue::Bytes(bytes) => parse_bool(bytes.as_ref()),
        _ => Err(cell_parse_error(&cell, "bool")),
    }
}

#[allow(dead_code)]
pub(crate) fn cell_date<E>(cell: OdbcValue<'_>) -> Result<NaiveDate, E>
where
    E: OdbcCoreError,
{
    match &cell {
        OdbcValue::Date(value) => odbc_date_to_naive(*value),
        OdbcValue::Bytes(bytes) => parse_date(bytes.as_ref()),
        _ => Err(cell_parse_error(&cell, "NaiveDate")),
    }
}

#[allow(dead_code)]
pub(crate) fn cell_time<E>(cell: OdbcValue<'_>) -> Result<NaiveTime, E>
where
    E: OdbcCoreError,
{
    match &cell {
        OdbcValue::Time(value) => odbc_time_to_naive(*value),
        OdbcValue::Bytes(bytes) => parse_time(bytes.as_ref()),
        _ => Err(cell_parse_error(&cell, "NaiveTime")),
    }
}

#[allow(dead_code)]
pub(crate) fn cell_timestamp<E>(cell: OdbcValue<'_>) -> Result<NaiveDateTime, E>
where
    E: OdbcCoreError,
{
    match &cell {
        OdbcValue::Timestamp(value) => odbc_timestamp_to_naive(*value),
        OdbcValue::Bytes(bytes) => parse_timestamp(bytes.as_ref()),
        _ => Err(cell_parse_error(&cell, "NaiveDateTime")),
    }
}

fn cell_parse_error<E>(cell: &OdbcValue<'_>, ty: &'static str) -> E
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Copy, Clone)]
    enum TestType {
        Text,
        Binary,
        WText,
        I32,
    }

    impl TypeSystem for TestType {}

    impl OdbcTypePolicy for TestType {
        fn source_name() -> &'static str {
            "Test"
        }

        fn max_str_len_env() -> &'static str {
            "TEST_MAX_STR_LEN"
        }

        fn nullable(self) -> bool {
            false
        }

        fn buffer_desc(self, max_str_len: usize) -> BufferDesc {
            match self {
                Self::Text => BufferDesc::Text { max_str_len },
                Self::Binary => BufferDesc::Binary {
                    max_bytes: max_str_len,
                },
                Self::WText => BufferDesc::WText { max_str_len },
                Self::I32 => BufferDesc::I32 { nullable: false },
            }
        }
    }

    #[derive(Debug)]
    enum TestError {
        ParseValue { value: String, ty: &'static str },
        Other(String),
    }

    impl OdbcCoreError for TestError {
        fn get_nrows_failed() -> Self {
            Self::Other("get nrows failed".to_string())
        }

        fn no_result_set(query: String) -> Self {
            Self::Other(format!("no result set: {query}"))
        }

        fn parse_value(value: String, ty: &'static str) -> Self {
            Self::ParseValue { value, ty }
        }
    }

    impl From<ConnectorXError> for TestError {
        fn from(value: ConnectorXError) -> Self {
            Self::Other(value.to_string())
        }
    }

    impl From<odbc_api::Error> for TestError {
        fn from(value: odbc_api::Error) -> Self {
            Self::Other(value.to_string())
        }
    }

    impl From<anyhow::Error> for TestError {
        fn from(value: anyhow::Error) -> Self {
            Self::Other(value.to_string())
        }
    }

    impl From<ParseFloatError> for TestError {
        fn from(value: ParseFloatError) -> Self {
            Self::Other(value.to_string())
        }
    }

    impl From<DecimalParseError> for TestError {
        fn from(value: DecimalParseError) -> Self {
            Self::Other(value.to_string())
        }
    }

    impl From<chrono::ParseError> for TestError {
        fn from(value: chrono::ParseError) -> Self {
            Self::Other(value.to_string())
        }
    }

    fn parse_value_error<T>(result: Result<T, TestError>) -> (String, &'static str) {
        match result {
            Err(TestError::ParseValue { value, ty }) => (value, ty),
            Err(TestError::Other(value)) => panic!("unexpected error: {}", value),
            Ok(_) => panic!("expected parse error"),
        }
    }

    #[test]
    fn trims_ascii_whitespace() {
        assert_eq!(trim_ascii(b" \t\r\n42 \n"), b"42");
        assert_eq!(trim_ascii(b" \t "), b"");
    }

    #[test]
    fn compares_ascii_case_insensitively() {
        assert!(eq_ascii_ignore_case(b"TRUE", b"true"));
        assert!(!eq_ascii_ignore_case(b"truth", b"true"));
    }

    #[test]
    fn parses_bool_variants() {
        assert!(parse_bool::<TestError>(b" true ").unwrap());
        assert!(parse_bool::<TestError>(b"1").unwrap());
        assert!(!parse_bool::<TestError>(b"FALSE").unwrap());
        assert!(!parse_bool::<TestError>(b"0").unwrap());

        let (value, ty) = parse_value_error(parse_bool::<TestError>(b"maybe"));
        assert_eq!(value, "maybe");
        assert_eq!(ty, "bool");
    }

    #[test]
    fn parses_i64_with_overflow_detection() {
        assert_eq!(
            parse_i64_with_ty::<TestError>(b" -42 ", "i64").unwrap(),
            -42
        );
        assert_eq!(parse_i64_with_ty::<TestError>(b"+42", "i64").unwrap(), 42);

        let (_, ty) = parse_value_error(parse_i64_with_ty::<TestError>(
            b"9223372036854775808",
            "i64",
        ));
        assert_eq!(ty, "i64");
    }

    #[test]
    fn parses_numeric_and_temporal_values() {
        assert_eq!(parse_u8::<TestError>(b"255").unwrap(), 255);
        assert_eq!(parse_i16::<TestError>(b"-12").unwrap(), -12);
        assert_eq!(parse_i32::<TestError>(b"1234").unwrap(), 1234);
        assert_eq!(parse_i64::<TestError>(b"-1234").unwrap(), -1234);
        assert_eq!(parse_f32::<TestError>(b"1.5").unwrap(), 1.5);
        assert_eq!(parse_f64::<TestError>(b"1.25").unwrap(), 1.25);
        assert_eq!(
            parse_decimal::<TestError>(b"123.45").unwrap(),
            "123.45".parse::<Decimal>().unwrap()
        );
        assert_eq!(
            parse_date::<TestError>(b"2026-05-07").unwrap(),
            NaiveDate::from_ymd_opt(2026, 5, 7).unwrap()
        );
        assert_eq!(
            parse_time::<TestError>(b"12:34:56.123456").unwrap(),
            NaiveTime::from_hms_micro_opt(12, 34, 56, 123456).unwrap()
        );
        assert_eq!(
            parse_timestamp::<TestError>(b"2026-05-07 12:34:56.123456").unwrap(),
            NaiveDate::from_ymd_opt(2026, 5, 7)
                .unwrap()
                .and_hms_micro_opt(12, 34, 56, 123456)
                .unwrap()
        );
    }

    #[test]
    fn parses_partition_values() {
        assert_eq!(parse_partition_value::<TestError>(b"").unwrap(), 0);
        assert_eq!(parse_partition_value::<TestError>(b"  -42  ").unwrap(), -42);
        assert_eq!(parse_partition_value::<TestError>(b"42").unwrap(), 42);
        assert_eq!(parse_partition_value::<TestError>(b"42.9").unwrap(), 42);
        assert_eq!(parse_partition_value::<TestError>(b"-42.9").unwrap(), -42);
        assert_eq!(
            parse_partition_value::<TestError>(b"123.0001").unwrap(),
            123
        );
        assert!(parse_partition_value::<TestError>(b"not-a-number").is_err());
    }

    #[test]
    fn derives_buffer_lengths_from_column_metadata_when_available() {
        assert_eq!(
            TestType::Text.buffer_max_len(
                DataType::Varchar {
                    length: std::num::NonZeroUsize::new(12),
                },
                1024,
            ),
            48
        );
        assert_eq!(
            TestType::Binary.buffer_max_len(
                DataType::Varbinary {
                    length: std::num::NonZeroUsize::new(12),
                },
                1024,
            ),
            12
        );
        assert_eq!(
            TestType::Text.buffer_max_len(
                DataType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                1024,
            ),
            12
        );
        assert_eq!(
            TestType::WText.buffer_max_len(
                DataType::WVarchar {
                    length: std::num::NonZeroUsize::new(12),
                },
                1024,
            ),
            12
        );
    }

    #[test]
    fn falls_back_to_default_buffer_length_without_metadata() {
        assert_eq!(
            TestType::Text.buffer_max_len(DataType::LongVarchar { length: None }, 1024),
            1024
        );
        assert_eq!(
            TestType::Binary.buffer_max_len(DataType::LongVarbinary { length: None }, 1024),
            1024
        );
        assert_eq!(
            TestType::I32.buffer_max_len(
                DataType::Varchar {
                    length: std::num::NonZeroUsize::new(12),
                },
                1024,
            ),
            1024
        );
    }

    #[test]
    fn truncation_error_reports_required_length_for_real_truncation() {
        let message =
            truncation_error("Odbc", "ODBC_MAX_STR_LEN", 6, Indicator::Length(2048)).to_string();

        assert!(message.contains("column 7"), "{}", message);
        assert!(message.contains("2048 bytes required"), "{}", message);
        assert!(message.contains("increase ODBC_MAX_STR_LEN"), "{}", message);
    }

    #[test]
    fn truncation_error_explains_no_total_requires_sized_cast() {
        let message =
            truncation_error("Sybase", "SYBASE_MAX_STR_LEN", 2, Indicator::NoTotal).to_string();

        assert!(message.contains("column 3"), "{}", message);
        assert!(message.contains("NoTotal"), "{}", message);
        assert!(message.contains("may not help"), "{}", message);
        assert!(message.contains("varchar(N)"), "{}", message);
        assert!(message.contains("varbinary(N)"), "{}", message);
    }

    #[test]
    fn formats_cells_as_strings() {
        assert_eq!(
            OdbcValue::Bytes(Cow::Borrowed(b"hello")).to_utf8_string(),
            "hello"
        );
        assert_eq!(OdbcValue::U8(7).to_utf8_string(), "7");
        assert_eq!(
            OdbcValue::Date(Date {
                year: 2026,
                month: 5,
                day: 7,
            })
            .to_utf8_string(),
            "2026-05-07"
        );
        assert_eq!(
            OdbcValue::Time(Time {
                hour: 12,
                minute: 34,
                second: 56,
            })
            .to_utf8_string(),
            "12:34:56"
        );
        assert_eq!(
            OdbcValue::Timestamp(Timestamp {
                year: 2026,
                month: 5,
                day: 7,
                hour: 12,
                minute: 34,
                second: 56,
                fraction: 123456789,
            })
            .to_utf8_string(),
            "2026-05-07 12:34:56.123456789"
        );
    }

    #[test]
    fn columnar_batch_cells_borrow_stored_bytes() {
        let bytes = OdbcColumn::Bytes(vec![Some(b"abc".to_vec()), None]);
        assert_eq!(bytes.bytes(0).unwrap().unwrap(), b"abc");
        assert!(bytes.bytes(1).unwrap().is_none());

        match bytes.cell(0).unwrap() {
            OdbcValue::Bytes(value) => assert!(matches!(value, Cow::Borrowed(b"abc"))),
            value => panic!("unexpected value: {:?}", value),
        }

        let ints = OdbcColumn::NullableI32(vec![Some(42), None]);
        assert_eq!(cell_i32::<TestError>(ints.cell(0).unwrap()).unwrap(), 42);
        assert!(ints.cell(1).is_none());
        assert!(ints.bytes(0).is_none());
    }

    #[test]
    fn byte_column_reuses_existing_slot_capacity() {
        let mut values = Vec::new();
        fill_byte_column(&mut values, 1, |_| Some(b"abcdef"));
        let first_ptr = values[0].as_ref().unwrap().as_ptr();
        let first_capacity = values[0].as_ref().unwrap().capacity();

        fill_byte_column(&mut values, 1, |_| Some(b"abc"));
        let slot = values[0].as_ref().unwrap();
        assert_eq!(slot, b"abc");
        assert_eq!(slot.as_ptr(), first_ptr);
        assert_eq!(slot.capacity(), first_capacity);
    }
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
                match self.next_cell() {
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
                match self.next_cell() {
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
                Ok(self.next_cell().map(|cell| cell.to_utf8_string()))
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
