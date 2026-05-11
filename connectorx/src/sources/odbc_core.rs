//! Shared ODBC source machinery for generic ODBC, Db2, and Sybase.
//!
//! Backends provide connection-string handling, SQL dialects, error conversion, and
//! buffer/type policy. This module owns the common ODBC execution, metadata,
//! columnar batch parsing, primitive conversion, count, and partition helpers.

use std::{
    borrow::Cow,
    convert::TryFrom,
    fmt::Debug,
    marker::PhantomData,
    num::{NonZeroUsize, ParseFloatError},
    sync::{Arc, Condvar, Mutex, MutexGuard},
};

use anyhow::anyhow;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use odbc_api::handles::StatementConnection;
use odbc_api::sys::{Date, Time, Timestamp};
use odbc_api::{
    buffers::{AnySlice, BufferDesc, ColumnarAnyBuffer, Indicator, TextRowSet},
    environment, Bit, BlockCursor, Connection, ConnectionOptions, Cursor, CursorImpl, DataType,
    Environment, Nullability, ResultSetMetadata,
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

#[derive(Debug)]
pub(crate) struct OdbcConnectionLimiter {
    max_connections: usize,
    active_connections: Mutex<usize>,
    available: Condvar,
}

#[derive(Debug)]
pub(crate) struct OdbcConnectionPermit {
    limiter: Arc<OdbcConnectionLimiter>,
}

impl OdbcConnectionLimiter {
    pub(crate) fn new(max_connections: usize) -> Arc<Self> {
        assert!(max_connections > 0);
        Arc::new(Self {
            max_connections,
            active_connections: Mutex::new(0),
            available: Condvar::new(),
        })
    }

    #[cfg(test)]
    pub(crate) fn max_connections(&self) -> usize {
        self.max_connections
    }

    pub(crate) fn acquire(self: &Arc<Self>) -> OdbcConnectionPermit {
        let mut active = self.lock_active_connections();
        while *active >= self.max_connections {
            active = self
                .available
                .wait(active)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        *active += 1;
        OdbcConnectionPermit {
            limiter: Arc::clone(self),
        }
    }

    fn lock_active_connections(&self) -> MutexGuard<'_, usize> {
        self.active_connections
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Drop for OdbcConnectionPermit {
    fn drop(&mut self) {
        let mut active = self.limiter.lock_active_connections();
        *active = active.saturating_sub(1);
        self.limiter.available.notify_one();
    }
}

pub(crate) fn connection_limiter(
    max_connections: Option<usize>,
    default_max_connections: usize,
) -> Result<Arc<OdbcConnectionLimiter>, anyhow::Error> {
    let max_connections = max_connections.unwrap_or(default_max_connections.max(1));
    if max_connections == 0 {
        return Err(anyhow!("max_connections must be at least 1"));
    }
    Ok(OdbcConnectionLimiter::new(max_connections))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct OdbcExecutionOptions {
    pub(crate) login_timeout_secs: Option<u32>,
    pub(crate) query_timeout_secs: Option<usize>,
}

impl OdbcExecutionOptions {
    pub(crate) fn new(
        login_timeout_secs: Option<u32>,
        query_timeout_secs: Option<usize>,
    ) -> Result<Self, anyhow::Error> {
        if matches!(login_timeout_secs, Some(0)) {
            return Err(anyhow!("login_timeout_secs must be at least 1"));
        }
        if matches!(query_timeout_secs, Some(0)) {
            return Err(anyhow!("query_timeout_secs must be at least 1"));
        }
        Ok(Self {
            login_timeout_secs,
            query_timeout_secs,
        })
    }

    pub(crate) fn connection_options(self) -> ConnectionOptions {
        ConnectionOptions {
            login_timeout_sec: self.login_timeout_secs,
            ..ConnectionOptions::default()
        }
    }
}

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
    fn connection_timeout(source_name: &'static str, timeout_secs: u32, cause: String) -> Self;
    fn query_timeout(
        source_name: &'static str,
        query: String,
        timeout_secs: usize,
        cause: String,
    ) -> Self;
    fn invalid_partition_bound(
        source_name: &'static str,
        column_name: &str,
        bound_name: &'static str,
        value: String,
        reason: &'static str,
    ) -> Self;
    fn invalid_utf16(
        source_name: &'static str,
        column_name: Option<&str>,
        row_index: usize,
        byte_offset: usize,
        surrogate: u16,
    ) -> Self;
}

/// Backend-specific ODBC metadata and buffer policy for the shared parser.
pub trait OdbcTypePolicy: TypeSystem + Copy + Debug {
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
    names: Arc<[String]>,
    schema: Arc<[TS]>,
    ncols: usize,
    current_cell: usize,
    is_finished: bool,
    replace_invalid_utf16: bool,
    _connection_permit: OdbcConnectionPermit,
    _marker: PhantomData<(TS, E)>,
}

impl<TS, E> OdbcParser<TS, E>
where
    TS: OdbcTypePolicy,
    E: OdbcCoreError,
{
    pub(crate) fn new(
        cursor: OdbcBlockCursor,
        names: Arc<[String]>,
        schema: Arc<[TS]>,
        replace_invalid_utf16: bool,
        connection_permit: OdbcConnectionPermit,
    ) -> Self {
        let ncols = schema.len();
        debug_assert_eq!(names.len(), ncols);
        Self {
            cursor,
            batch: OdbcBatch::with_capacity(ncols),
            names,
            schema,
            ncols,
            current_cell: 0,
            is_finished: false,
            replace_invalid_utf16,
            _connection_permit: connection_permit,
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
                    self.names.get(col_index).map(String::as_str),
                    self.schema.get(col_index).copied(),
                    TS::source_name(),
                    TS::max_str_len_env(),
                    self.replace_invalid_utf16,
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
        column_name: Option<&str>,
        column_type: Option<impl Debug + Copy>,
        source_name: &'static str,
        max_str_len_env: &'static str,
        replace_invalid_utf16: bool,
    ) -> Result<(), E>
    where
        E: OdbcCoreError,
    {
        ensure_column_not_truncated::<E>(
            &column,
            source_name,
            max_str_len_env,
            col_index,
            column_name,
            column_type,
        )?;
        if col_index == self.columns.len() {
            let mut value = OdbcColumn::Unsupported;
            value.replace_from_slice::<E>(
                column,
                self.nrows,
                source_name,
                col_index,
                column_name,
                replace_invalid_utf16,
            )?;
            self.columns.push(value);
        } else {
            self.columns[col_index].replace_from_slice::<E>(
                column,
                self.nrows,
                source_name,
                col_index,
                column_name,
                replace_invalid_utf16,
            )?;
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
    column_name: Option<&str>,
    column_type: Option<impl Debug + Copy>,
) -> Result<(), E>
where
    E: OdbcCoreError,
{
    let indicator = truncated_indicator(column);
    if let Some(indicator) = indicator {
        return Err(truncation_error(
            source_name,
            max_str_len_env,
            col_index,
            column_name,
            column_type,
            indicator,
        )
        .into());
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
    column_name: Option<&str>,
    column_type: Option<impl Debug + Copy>,
    indicator: Indicator,
) -> anyhow::Error {
    let column = column_description(col_index, column_name, column_type);
    match indicator {
        Indicator::Length(required_len) => anyhow!(
            "{source_name} {column} was truncated by the ODBC fetch buffer \
             ({required_len} bytes required); increase {max_str_len_env} or cast/substr \
             the column in the query"
        ),
        Indicator::NoTotal => anyhow!(
            "{source_name} {column} could not be fully fetched because the ODBC \
             driver did not report the value length (NoTotal); increasing {max_str_len_env} \
             may not help, so cast the column to a sized varchar(N) or varbinary(N) or substr \
             it in the query"
        ),
        other => anyhow!(
            "{source_name} {column} was truncated by the ODBC fetch buffer \
             ({other:?}); increase {max_str_len_env} or cast/substr the column in the query"
        ),
    }
}

fn column_description(
    col_index: usize,
    column_name: Option<&str>,
    column_type: Option<impl Debug + Copy>,
) -> String {
    let column_number = col_index + 1;
    match (column_name, column_type) {
        (Some(name), Some(ty)) => format!("column \"{name}\" (#{column_number}, {ty:?})"),
        (Some(name), None) => format!("column \"{name}\" (#{column_number})"),
        (None, Some(ty)) => format!("column {column_number} ({ty:?})"),
        (None, None) => format!("column {column_number}"),
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
    fn replace_from_slice<E>(
        &mut self,
        column: AnySlice<'_>,
        nrows: usize,
        source_name: &'static str,
        col_index: usize,
        column_name: Option<&str>,
        replace_invalid_utf16: bool,
    ) -> Result<(), E>
    where
        E: OdbcCoreError,
    {
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
                            decode_utf16_to_utf8::<E>(
                                chars,
                                slot,
                                source_name,
                                col_index,
                                column_name,
                                row_index,
                                replace_invalid_utf16,
                            )?;
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
        Ok(())
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

pub(crate) fn decode_utf16_to_string<E>(
    chars: &[u16],
    source_name: &'static str,
    col_index: usize,
    column_name: Option<&str>,
    row_index: usize,
    replace_invalid_utf16: bool,
) -> Result<String, E>
where
    E: OdbcCoreError,
{
    let mut bytes = Vec::with_capacity(chars.len() * 2);
    decode_utf16_to_utf8::<E>(
        chars,
        &mut bytes,
        source_name,
        col_index,
        column_name,
        row_index,
        replace_invalid_utf16,
    )?;
    String::from_utf8(bytes).map_err(|err| E::parse_value(err.to_string(), "UTF-16 text"))
}

pub(crate) fn decode_utf16_to_utf8<E>(
    chars: &[u16],
    output: &mut Vec<u8>,
    source_name: &'static str,
    col_index: usize,
    column_name: Option<&str>,
    row_index: usize,
    replace_invalid_utf16: bool,
) -> Result<(), E>
where
    E: OdbcCoreError,
{
    output.clear();
    let mut code_unit_index = 0;
    let mut warned = false;
    while code_unit_index < chars.len() {
        let unit = chars[code_unit_index];
        let decoded = if (0xD800..=0xDBFF).contains(&unit) {
            match chars.get(code_unit_index + 1).copied() {
                Some(next) if (0xDC00..=0xDFFF).contains(&next) => {
                    code_unit_index += 2;
                    let scalar =
                        0x10000 + ((((unit - 0xD800) as u32) << 10) | ((next - 0xDC00) as u32));
                    char::from_u32(scalar)
                }
                _ => {
                    handle_invalid_utf16::<E>(
                        source_name,
                        col_index,
                        column_name,
                        row_index,
                        code_unit_index,
                        unit,
                        replace_invalid_utf16,
                        &mut warned,
                    )?;
                    code_unit_index += 1;
                    Some(std::char::REPLACEMENT_CHARACTER)
                }
            }
        } else if (0xDC00..=0xDFFF).contains(&unit) {
            handle_invalid_utf16::<E>(
                source_name,
                col_index,
                column_name,
                row_index,
                code_unit_index,
                unit,
                replace_invalid_utf16,
                &mut warned,
            )?;
            code_unit_index += 1;
            Some(std::char::REPLACEMENT_CHARACTER)
        } else {
            code_unit_index += 1;
            char::from_u32(u32::from(unit))
        };

        if let Some(ch) = decoded {
            let mut bytes = [0; 4];
            output.extend_from_slice(ch.encode_utf8(&mut bytes).as_bytes());
        }
    }
    Ok(())
}

fn handle_invalid_utf16<E>(
    source_name: &'static str,
    col_index: usize,
    column_name: Option<&str>,
    row_index: usize,
    code_unit_index: usize,
    surrogate: u16,
    replace_invalid_utf16: bool,
    warned: &mut bool,
) -> Result<(), E>
where
    E: OdbcCoreError,
{
    let byte_offset = code_unit_index * 2;
    if replace_invalid_utf16 {
        if !*warned {
            let column = column_description(col_index, column_name, None::<()>);
            log::warn!(
                "{source_name} {column} row_index={row_index} contains invalid UTF-16 at \
                 byte_offset={byte_offset}; replacing invalid sequence with U+FFFD because \
                 replace_invalid_utf16=true"
            );
            *warned = true;
        }
        Ok(())
    } else {
        Err(E::invalid_utf16(
            source_name,
            column_name,
            row_index,
            byte_offset,
            surrogate,
        ))
    }
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

pub(crate) fn execute_query<E>(
    source_name: &'static str,
    conn: &str,
    query: &str,
    execution_options: OdbcExecutionOptions,
) -> Result<OdbcCursor, E>
where
    E: OdbcCoreError,
{
    let env = shared_environment::<E>()?;
    let connection =
        match env.connect_with_connection_string(conn, execution_options.connection_options()) {
            Ok(connection) => connection,
            Err(error) => {
                if let Some(timeout_secs) = execution_options.login_timeout_secs {
                    if is_odbc_timeout_error(&error) {
                        return Err(E::connection_timeout(
                            source_name,
                            timeout_secs,
                            odbc_error_message(&error),
                        ));
                    }
                }
                return Err(error.into());
            }
        };

    match connection.into_cursor(query, (), execution_options.query_timeout_secs) {
        Ok(Some(cursor)) => Ok(cursor),
        Ok(None) => Err(E::no_result_set(query.to_string())),
        Err(error_with_connection) => {
            let error = error_with_connection.error;
            if let Some(timeout_secs) = execution_options.query_timeout_secs {
                if is_odbc_timeout_error(&error) {
                    return Err(E::query_timeout(
                        source_name,
                        query.to_string(),
                        timeout_secs,
                        odbc_error_message(&error),
                    ));
                }
            }
            Err(error.into())
        }
    }
}

fn odbc_error_message(error: &odbc_api::Error) -> String {
    let display = error.to_string();
    let debug = format!("{error:?}");
    if debug == display {
        display
    } else {
        format!("{display}; {debug}")
    }
}

fn is_odbc_timeout_error(error: &odbc_api::Error) -> bool {
    is_odbc_timeout_message(&odbc_error_message(error))
}

fn is_odbc_timeout_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("hyt00")
        || message.contains("hyt01")
        || message.contains("timeout")
        || message.contains("timed out")
}

pub(crate) fn shared_environment<E>() -> Result<&'static Environment, E>
where
    E: OdbcCoreError,
{
    Ok(environment()?)
}

pub(crate) fn fetch_metadata<T, E, F>(
    source_name: &'static str,
    conn: &str,
    query: &str,
    default_max_len: usize,
    connection_limiter: &Arc<OdbcConnectionLimiter>,
    execution_options: OdbcExecutionOptions,
    map_type: F,
) -> Result<(Vec<String>, Vec<T>, Vec<usize>), E>
where
    E: OdbcCoreError,
    T: OdbcTypePolicy,
    F: Fn(DataType, Nullability, &str) -> Result<T, E>,
{
    let _connection_permit = connection_limiter.acquire();
    let mut cursor = execute_query::<E>(source_name, conn, query, execution_options)?;
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
        let column_name = cursor.col_name(col)?;
        let data_type = cursor.col_data_type(col)?;
        let nullability = cursor.col_nullability(col)?;
        let ty = map_type(data_type, nullability, &column_name)?;
        buffer_max_lens.push(ty.buffer_max_len(data_type, default_max_len));
        schema.push(ty);
        names.push(column_name);
    }

    Ok((names, schema, buffer_max_lens))
}

pub(crate) fn fetch_count<E, D>(
    source_name: &'static str,
    conn: &str,
    query: &str,
    dialect: &D,
    connection_limiter: &Arc<OdbcConnectionLimiter>,
    execution_options: OdbcExecutionOptions,
) -> Result<usize, E>
where
    E: OdbcCoreError,
    D: Dialect,
{
    let cxq = CXQuery::Naked(query.to_string());
    let cquery = count_query(&cxq, dialect)?;
    fetch_count_query(
        source_name,
        conn,
        cquery.as_str(),
        connection_limiter,
        execution_options,
    )
}

pub(crate) fn fetch_count_query<E>(
    source_name: &'static str,
    conn: &str,
    query: &str,
    connection_limiter: &Arc<OdbcConnectionLimiter>,
    execution_options: OdbcExecutionOptions,
) -> Result<usize, E>
where
    E: OdbcCoreError,
{
    let _connection_permit = connection_limiter.acquire();
    let mut cursor = execute_query::<E>(source_name, conn, query, execution_options)?;
    let buffer = TextRowSet::for_cursor(1, &mut cursor, Some(64))?;
    let mut cursor = cursor.bind_buffer(buffer)?;
    let batch = cursor.fetch()?.ok_or_else(E::get_nrows_failed)?;
    let raw_value = batch.at(0, 0).ok_or_else(E::get_nrows_failed)?;
    let parsed_value = parse_i64_with_ty::<E>(raw_value, "usize")?;
    Ok(usize::try_from(parsed_value)
        .map_err(|_| E::parse_value(bytes_to_string(raw_value), "usize"))?)
}

pub(crate) fn fetch_i64_pair<E>(
    conn: &str,
    query: &str,
    source_name: &'static str,
    column_name: &str,
    execution_options: OdbcExecutionOptions,
) -> Result<(i64, i64), E>
where
    E: OdbcCoreError,
{
    let connection_limiter = OdbcConnectionLimiter::new(1);
    let _connection_permit = connection_limiter.acquire();
    let mut cursor = execute_query::<E>(source_name, conn, query, execution_options)?;
    let buffer = TextRowSet::for_cursor(1, &mut cursor, Some(128))?;
    let mut cursor = cursor.bind_buffer(buffer)?;
    let batch = cursor.fetch()?.ok_or_else(E::get_nrows_failed)?;
    let min = parse_partition_bound::<E>(batch.at(0, 0), source_name, column_name, "min")?;
    let max = parse_partition_bound::<E>(batch.at(1, 0), source_name, column_name, "max")?;
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

fn parse_partition_bound<E>(
    value: Option<&[u8]>,
    source_name: &'static str,
    column_name: &str,
    bound_name: &'static str,
) -> Result<i64, E>
where
    E: OdbcCoreError,
{
    match value {
        Some(value) => parse_partition_value::<E>(value, source_name, column_name, bound_name),
        None => Err(E::invalid_partition_bound(
            source_name,
            column_name,
            bound_name,
            "NULL".to_string(),
            "partition range query returned NULL",
        )),
    }
}

fn parse_partition_value<E>(
    value: &[u8],
    source_name: &'static str,
    column_name: &str,
    bound_name: &'static str,
) -> Result<i64, E>
where
    E: OdbcCoreError,
{
    let trimmed = trim_ascii(value);
    if trimmed.is_empty() {
        return Err(E::invalid_partition_bound(
            source_name,
            column_name,
            bound_name,
            bytes_to_string(trimmed),
            "partition range query returned an empty value",
        ));
    }

    if let Ok(value) = parse_i64_with_ty::<E>(trimmed, "partition range") {
        return Ok(value);
    }

    let value = bytes_to_string(trimmed);
    let has_decimal_marker = trimmed
        .iter()
        .copied()
        .any(|byte| matches!(byte, b'.' | b'e' | b'E'));
    let uses_numeric_notation = trimmed
        .iter()
        .copied()
        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'+' | b'-' | b'.' | b'e' | b'E'));
    let reason = if has_decimal_marker && uses_numeric_notation {
        "partition range must be an i64 integer; decimal or fractional bounds are not supported"
    } else {
        "partition range value is not a valid i64 integer"
    };
    Err(E::invalid_partition_bound(
        source_name,
        column_name,
        bound_name,
        value,
        reason,
    ))
}

pub(crate) fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.parse().ok()
}

pub(crate) fn env_u32(name: &str) -> Option<u32> {
    std::env::var(name).ok()?.parse().ok()
}

pub(crate) fn env_bool(name: &str) -> Option<bool> {
    match std::env::var(name).ok()?.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

pub(crate) fn unknown_odbc_type_error(
    source_name: &'static str,
    fallback_env: &'static str,
    column_name: &str,
    data_type: DataType,
    nullability: Nullability,
) -> anyhow::Error {
    anyhow!(
        "Unsupported ODBC type for source={source_name} column_name={column_name} \
         odbc_type_code={} column_size={:?} decimal_digits={} nullability={:?}. \
         Set {fallback_env}=true to fallback unknown/vendor-specific ODBC types to VARCHAR.",
        data_type.data_type().0,
        data_type.column_size().map(NonZeroUsize::get),
        data_type.decimal_digits(),
        nullability
    )
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
    use std::{sync::mpsc, thread, time::Duration};

    #[derive(Copy, Clone, Debug)]
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

    #[allow(dead_code)]
    #[derive(Debug)]
    enum TestError {
        ParseValue {
            value: String,
            ty: &'static str,
        },
        ConnectionTimeout {
            source_name: &'static str,
            timeout_secs: u32,
            cause: String,
        },
        QueryTimeout {
            source_name: &'static str,
            query: String,
            timeout_secs: usize,
            cause: String,
        },
        InvalidPartitionBound {
            source_name: &'static str,
            column_name: String,
            bound_name: &'static str,
            value: String,
            reason: &'static str,
        },
        InvalidUtf16 {
            source_name: &'static str,
            column_name: String,
            row_index: usize,
            byte_offset: usize,
            surrogate: u16,
        },
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
            Err(TestError::ConnectionTimeout { .. }) => {
                panic!("unexpected connection timeout error")
            }
            Err(TestError::QueryTimeout { .. }) => panic!("unexpected query timeout error"),
            Err(TestError::InvalidPartitionBound { .. }) => {
                panic!("unexpected partition bound error")
            }
            Err(TestError::InvalidUtf16 { .. }) => panic!("unexpected UTF-16 error"),
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
    fn connection_limiter_blocks_until_a_permit_is_released() {
        let limiter = OdbcConnectionLimiter::new(1);
        let permit = limiter.acquire();
        let limiter_for_thread = Arc::clone(&limiter);
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            let _permit = limiter_for_thread.acquire();
            tx.send(()).unwrap();
        });

        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
        drop(permit);
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn connection_limiter_uses_query_count_default() {
        assert_eq!(connection_limiter(None, 0).unwrap().max_connections(), 1);
        assert_eq!(connection_limiter(None, 3).unwrap().max_connections(), 3);
        assert_eq!(connection_limiter(Some(2), 8).unwrap().max_connections(), 2);
    }

    #[test]
    fn execution_options_map_login_timeout_to_connection_options() {
        let options = OdbcExecutionOptions::new(Some(7), Some(11)).unwrap();

        assert_eq!(options.connection_options().login_timeout_sec, Some(7));
    }

    #[test]
    fn execution_options_reject_zero_timeouts() {
        assert!(OdbcExecutionOptions::new(Some(0), None).is_err());
        assert!(OdbcExecutionOptions::new(None, Some(0)).is_err());
    }

    #[test]
    fn timeout_classifier_matches_odbc_timeout_states_and_text() {
        assert!(is_odbc_timeout_message("HYT00: timeout expired"));
        assert!(is_odbc_timeout_message("HYT01 connection timeout"));
        assert!(is_odbc_timeout_message("Login timed out"));
        assert!(!is_odbc_timeout_message("28000 invalid authorization"));
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
    fn parses_integer_partition_values() {
        assert_eq!(
            parse_partition_value::<TestError>(b"  -42  ", "Odbc", "id", "min").unwrap(),
            -42
        );
        assert_eq!(
            parse_partition_value::<TestError>(b"42", "Odbc", "id", "max").unwrap(),
            42
        );
    }

    #[test]
    fn rejects_empty_partition_values() {
        match parse_partition_value::<TestError>(b"", "Odbc", "id", "min") {
            Err(TestError::InvalidPartitionBound {
                source_name,
                column_name,
                bound_name,
                value,
                reason,
            }) => {
                assert_eq!(source_name, "Odbc");
                assert_eq!(column_name, "id");
                assert_eq!(bound_name, "min");
                assert_eq!(value, "");
                assert_eq!(reason, "partition range query returned an empty value");
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }

    #[test]
    fn rejects_null_partition_bounds() {
        match parse_partition_bound::<TestError>(None, "Db2", "amount", "max") {
            Err(TestError::InvalidPartitionBound {
                source_name,
                column_name,
                bound_name,
                value,
                reason,
            }) => {
                assert_eq!(source_name, "Db2");
                assert_eq!(column_name, "amount");
                assert_eq!(bound_name, "max");
                assert_eq!(value, "NULL");
                assert_eq!(reason, "partition range query returned NULL");
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }

    #[test]
    fn rejects_decimal_partition_values_without_truncation() {
        for value in [b"42.9".as_slice(), b"-42.9", b"123.0001", b"123.0"] {
            match parse_partition_value::<TestError>(value, "Sybase", "price", "min") {
                Err(TestError::InvalidPartitionBound {
                    source_name,
                    column_name,
                    bound_name,
                    value,
                    reason,
                }) => {
                    assert_eq!(source_name, "Sybase");
                    assert_eq!(column_name, "price");
                    assert_eq!(bound_name, "min");
                    assert!(value.contains('.') || value.contains('e') || value.contains('E'));
                    assert_eq!(
                        reason,
                        "partition range must be an i64 integer; decimal or fractional bounds are not supported"
                    );
                }
                other => panic!("unexpected result: {:?}", other),
            }
        }
    }

    #[test]
    fn rejects_non_numeric_partition_values() {
        match parse_partition_value::<TestError>(b"not-a-number", "Odbc", "id", "max") {
            Err(TestError::InvalidPartitionBound {
                source_name,
                column_name,
                bound_name,
                value,
                reason,
            }) => {
                assert_eq!(source_name, "Odbc");
                assert_eq!(column_name, "id");
                assert_eq!(bound_name, "max");
                assert_eq!(value, "not-a-number");
                assert_eq!(reason, "partition range value is not a valid i64 integer");
            }
            other => panic!("unexpected result: {:?}", other),
        }
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
        let message = truncation_error(
            "Odbc",
            "ODBC_MAX_STR_LEN",
            6,
            Some("long_text"),
            Some(TestType::Text),
            Indicator::Length(2048),
        )
        .to_string();

        assert!(
            message.contains("column \"long_text\" (#7, Text)"),
            "{}",
            message
        );
        assert!(message.contains("2048 bytes required"), "{}", message);
        assert!(message.contains("increase ODBC_MAX_STR_LEN"), "{}", message);
    }

    #[test]
    fn truncation_error_explains_no_total_requires_sized_cast() {
        let message = truncation_error(
            "Sybase",
            "SYBASE_MAX_STR_LEN",
            2,
            Some("payload"),
            Some(TestType::Binary),
            Indicator::NoTotal,
        )
        .to_string();

        assert!(
            message.contains("column \"payload\" (#3, Binary)"),
            "{}",
            message
        );
        assert!(message.contains("NoTotal"), "{}", message);
        assert!(message.contains("may not help"), "{}", message);
        assert!(message.contains("varchar(N)"), "{}", message);
        assert!(message.contains("varbinary(N)"), "{}", message);
    }

    #[test]
    fn truncation_error_falls_back_to_index_when_metadata_is_missing() {
        let message = truncation_error(
            "Odbc",
            "ODBC_MAX_STR_LEN",
            1,
            None,
            None::<TestType>,
            Indicator::Length(256),
        )
        .to_string();

        assert!(message.contains("column 2"), "{}", message);
        assert!(!message.contains("#2"), "{}", message);
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

    #[test]
    fn invalid_utf16_errors_by_default_with_column_context() {
        let mut bytes = Vec::new();
        let error = decode_utf16_to_utf8::<TestError>(
            &[b'o' as u16, 0xD800, b'k' as u16],
            &mut bytes,
            "Odbc",
            2,
            Some("wide_text"),
            7,
            false,
        )
        .unwrap_err();

        match error {
            TestError::InvalidUtf16 {
                source_name,
                column_name,
                row_index,
                byte_offset,
                surrogate,
            } => {
                assert_eq!(source_name, "Odbc");
                assert_eq!(column_name, "wide_text");
                assert_eq!(row_index, 7);
                assert_eq!(byte_offset, 2);
                assert_eq!(surrogate, 0xD800);
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn invalid_utf16_can_be_replaced_by_explicit_opt_in() {
        let mut bytes = Vec::new();
        decode_utf16_to_utf8::<TestError>(
            &[b'o' as u16, 0xD800, b'k' as u16],
            &mut bytes,
            "Odbc",
            2,
            Some("wide_text"),
            7,
            true,
        )
        .unwrap();

        assert_eq!(String::from_utf8(bytes).unwrap(), "o\u{FFFD}k");
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

// ==========================================================================
// Arrow fast-path infrastructure (feature = "dst_arrow")
// ==========================================================================

#[cfg(feature = "dst_arrow")]
use crate::{
    arrow_batch_iter::RecordBatchIterator,
    constants::SECONDS_IN_DAY,
    destinations::arrow::{ArrowDestination, ArrowTypeSystem},
    utils::decimal_to_i128,
};
#[cfg(feature = "dst_arrow")]
use arrow::{
    array::{
        ArrayRef, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder, Float64Builder,
        Int64Builder, LargeBinaryBuilder, StringBuilder, Time64MicrosecondBuilder,
        TimestampMicrosecondBuilder,
    },
    datatypes::{DataType as ArrowDataType, Field, Schema},
    record_batch::RecordBatch,
};
#[cfg(feature = "dst_arrow")]
use odbc_api::sys::NULL_DATA;
#[cfg(feature = "dst_arrow")]
use rayon::prelude::*;
#[cfg(feature = "dst_arrow")]
use std::{
    sync::mpsc::{channel, Receiver, Sender},
    thread,
};

/// Arrow-specific extension of [`OdbcTypePolicy`].
///
/// Maps each type-system variant to an Arrow schema type and builds an
/// [`ArrayRef`] from an ODBC columnar buffer slice.  Invoke
/// [`impl_odbc_arrow_policy!`] to generate this implementation for any
/// standard ODBC-like type system that carries the canonical variants
/// (TinyInt / SmallInt / Int / BigInt / Real / Double / Numeric / Decimal /
/// Bit / Char / Varchar / Text / Binary / Date / Time / Timestamp).
#[cfg(feature = "dst_arrow")]
pub(crate) trait OdbcArrowPolicy: OdbcTypePolicy {
    fn arrow_type(self) -> ArrowTypeSystem;
    fn arrow_data_type(self) -> ArrowDataType;
    fn build_arrow_array<E: OdbcCoreError>(
        self,
        column: AnySlice<'_>,
        nrows: usize,
        col_index: usize,
        column_name: Option<&str>,
        replace_invalid_utf16: bool,
    ) -> Result<ArrayRef, E>;
}

// --- helper functions used by the generic array builders ------------------

#[cfg(feature = "dst_arrow")]
pub(crate) fn require_nullable<E: OdbcCoreError>(
    nullable: bool,
    ty: &'static str,
) -> Result<(), E> {
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
pub(crate) fn validity_from_indicators<E: OdbcCoreError>(
    indicators: &[isize],
    nullable: bool,
    ty: &'static str,
) -> Result<Vec<bool>, E> {
    indicators
        .iter()
        .map(|&indicator| {
            if indicator == NULL_DATA {
                require_nullable::<E>(nullable, ty)?;
                Ok(false)
            } else {
                Ok(true)
            }
        })
        .collect()
}

#[cfg(feature = "dst_arrow")]
pub(crate) fn naive_date_to_arrow_i32<E: OdbcCoreError>(value: NaiveDate) -> Result<i32, E> {
    value
        .and_hms_opt(0, 0, 0)
        .map(|dt| (dt.and_utc().timestamp() / SECONDS_IN_DAY) as i32)
        .ok_or_else(|| {
            ConnectorXError::cannot_produce::<NaiveDate>(Some(format!(
                "cannot convert NaiveDate {value:?} to Arrow Date32"
            )))
            .into()
        })
}

#[cfg(feature = "dst_arrow")]
pub(crate) fn naive_time_to_micro(value: NaiveTime) -> i64 {
    use chrono::Timelike;
    value.num_seconds_from_midnight() as i64 * 1_000_000 + (value.nanosecond() as i64) / 1000
}

#[cfg(feature = "dst_arrow")]
fn append_decimal<E: OdbcCoreError>(
    builder: &mut Decimal128Builder,
    decimal: Decimal,
    scale: i8,
) -> Result<(), E> {
    if scale < 0 {
        return Err(ConnectorXError::cannot_produce::<Decimal>(Some(format!(
            "negative decimal scale {scale}"
        )))
        .into());
    }
    builder.append_value(decimal_to_i128(decimal, scale as u32)?);
    Ok(())
}

#[cfg(feature = "dst_arrow")]
fn append_decimal_value<E: OdbcCoreError>(
    builder: &mut Decimal128Builder,
    bytes: &[u8],
    scale: i8,
) -> Result<(), E> {
    append_decimal::<E>(builder, parse_decimal::<E>(bytes)?, scale)
}

// local macro used by the array builder functions below
#[cfg(feature = "dst_arrow")]
macro_rules! append_direct_cell_arrow {
    ($E:ty, $column:expr, $row:expr, $builder:expr, $nullable:expr, $ty:literal, $parse:expr) => {
        match odbc_cell_from_column($column, $row) {
            Some(cell) => $builder.append_value(($parse)(cell)?),
            None => {
                require_nullable::<$E>($nullable, $ty)?;
                $builder.append_null();
            }
        }
    };
}

// --- generic array builder functions --------------------------------------

#[cfg(feature = "dst_arrow")]
pub(crate) fn build_int64_array<E: OdbcCoreError>(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, E> {
    let mut builder = Int64Builder::with_capacity(nrows);
    match column {
        AnySlice::I8(values) => {
            for &v in &values[..nrows] {
                builder.append_value(i64::from(v));
            }
        }
        AnySlice::I16(values) => {
            for &v in &values[..nrows] {
                builder.append_value(i64::from(v));
            }
        }
        AnySlice::I32(values) => {
            for &v in &values[..nrows] {
                builder.append_value(i64::from(v));
            }
        }
        AnySlice::I64(values) => builder.append_values(&values[..nrows], &vec![true; nrows]),
        AnySlice::U8(values) => {
            for &v in &values[..nrows] {
                builder.append_value(i64::from(v));
            }
        }
        AnySlice::NullableI8(values) => {
            for value in values.take(nrows) {
                match value {
                    Some(&v) => builder.append_value(i64::from(v)),
                    None => {
                        require_nullable::<E>(nullable, "i64")?;
                        builder.append_null();
                    }
                }
            }
        }
        AnySlice::NullableI16(values) => {
            for value in values.take(nrows) {
                match value {
                    Some(&v) => builder.append_value(i64::from(v)),
                    None => {
                        require_nullable::<E>(nullable, "i64")?;
                        builder.append_null();
                    }
                }
            }
        }
        AnySlice::NullableI32(values) => {
            for value in values.take(nrows) {
                match value {
                    Some(&v) => builder.append_value(i64::from(v)),
                    None => {
                        require_nullable::<E>(nullable, "i64")?;
                        builder.append_null();
                    }
                }
            }
        }
        AnySlice::NullableI64(values) => {
            let (vals, indicators) = values.raw_values();
            let validity = validity_from_indicators::<E>(&indicators[..nrows], nullable, "i64")?;
            builder.append_values(&vals[..nrows], &validity);
        }
        AnySlice::NullableU8(values) => {
            for value in values.take(nrows) {
                match value {
                    Some(&v) => builder.append_value(i64::from(v)),
                    None => {
                        require_nullable::<E>(nullable, "i64")?;
                        builder.append_null();
                    }
                }
            }
        }
        other => {
            for row_index in 0..nrows {
                append_direct_cell_arrow!(
                    E,
                    other,
                    row_index,
                    builder,
                    nullable,
                    "i64",
                    cell_i64::<E>
                );
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "dst_arrow")]
pub(crate) fn build_float32_array<E: OdbcCoreError>(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, E> {
    let mut builder = Float32Builder::with_capacity(nrows);
    match column {
        AnySlice::F32(values) => builder.append_values(&values[..nrows], &vec![true; nrows]),
        AnySlice::NullableF32(values) => {
            let (vals, indicators) = values.raw_values();
            let validity = validity_from_indicators::<E>(&indicators[..nrows], nullable, "f32")?;
            builder.append_values(&vals[..nrows], &validity);
        }
        other => {
            for row_index in 0..nrows {
                append_direct_cell_arrow!(
                    E,
                    other,
                    row_index,
                    builder,
                    nullable,
                    "f32",
                    cell_f32::<E>
                );
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "dst_arrow")]
pub(crate) fn build_float64_array<E: OdbcCoreError>(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, E> {
    let mut builder = Float64Builder::with_capacity(nrows);
    match column {
        AnySlice::F32(values) => {
            for &v in &values[..nrows] {
                builder.append_value(f64::from(v));
            }
        }
        AnySlice::F64(values) => builder.append_values(&values[..nrows], &vec![true; nrows]),
        AnySlice::NullableF32(values) => {
            for value in values.take(nrows) {
                match value {
                    Some(&v) => builder.append_value(f64::from(v)),
                    None => {
                        require_nullable::<E>(nullable, "f64")?;
                        builder.append_null();
                    }
                }
            }
        }
        AnySlice::NullableF64(values) => {
            let (vals, indicators) = values.raw_values();
            let validity = validity_from_indicators::<E>(&indicators[..nrows], nullable, "f64")?;
            builder.append_values(&vals[..nrows], &validity);
        }
        other => {
            for row_index in 0..nrows {
                append_direct_cell_arrow!(
                    E,
                    other,
                    row_index,
                    builder,
                    nullable,
                    "f64",
                    cell_f64::<E>
                );
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "dst_arrow")]
pub(crate) fn build_decimal_array<E: OdbcCoreError>(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
    precision: u8,
    scale: i8,
    source_name: &'static str,
    col_index: usize,
    column_name: Option<&str>,
    replace_invalid_utf16: bool,
) -> Result<ArrayRef, E> {
    let mut builder = Decimal128Builder::with_capacity(nrows)
        .with_data_type(ArrowDataType::Decimal128(precision, scale));
    match column {
        AnySlice::Text(view) => {
            for row_index in 0..nrows {
                match view.get(row_index) {
                    Some(bytes) => append_decimal_value::<E>(&mut builder, bytes, scale)?,
                    None => {
                        require_nullable::<E>(nullable, "Decimal")?;
                        builder.append_null();
                    }
                }
            }
        }
        AnySlice::WText(view) => {
            for row_index in 0..nrows {
                match view.get(row_index) {
                    Some(chars) => {
                        let s = decode_utf16_to_string::<E>(
                            chars,
                            source_name,
                            col_index,
                            column_name,
                            row_index,
                            replace_invalid_utf16,
                        )?;
                        append_decimal_value::<E>(&mut builder, s.as_bytes(), scale)?;
                    }
                    None => {
                        require_nullable::<E>(nullable, "Decimal")?;
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
                            OdbcValue::Bytes(bytes) => parse_decimal::<E>(bytes.as_ref())?,
                            _ => parse_decimal::<E>(cell.to_utf8_string().as_bytes())?,
                        };
                        append_decimal::<E>(&mut builder, decimal, scale)?;
                    }
                    None => {
                        require_nullable::<E>(nullable, "Decimal")?;
                        builder.append_null();
                    }
                }
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "dst_arrow")]
pub(crate) fn build_bool_array<E: OdbcCoreError>(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, E> {
    let mut builder = BooleanBuilder::with_capacity(nrows);
    match column {
        AnySlice::Bit(values) => {
            for &v in &values[..nrows] {
                builder.append_value(bit_to_bool(v));
            }
        }
        AnySlice::NullableBit(values) => {
            for value in values.take(nrows) {
                match value {
                    Some(&v) => builder.append_value(bit_to_bool(v)),
                    None => {
                        require_nullable::<E>(nullable, "bool")?;
                        builder.append_null();
                    }
                }
            }
        }
        other => {
            for row_index in 0..nrows {
                append_direct_cell_arrow!(
                    E,
                    other,
                    row_index,
                    builder,
                    nullable,
                    "bool",
                    cell_bool::<E>
                );
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "dst_arrow")]
pub(crate) fn build_string_array<E: OdbcCoreError>(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
    source_name: &'static str,
    col_index: usize,
    column_name: Option<&str>,
    replace_invalid_utf16: bool,
) -> Result<ArrayRef, E> {
    let mut builder = StringBuilder::with_capacity(nrows, nrows * 8);
    match column {
        AnySlice::Text(view) => {
            for row_index in 0..nrows {
                match view.get(row_index) {
                    Some(bytes) => builder.append_value(String::from_utf8_lossy(bytes).as_ref()),
                    None => {
                        require_nullable::<E>(nullable, "String")?;
                        builder.append_null();
                    }
                }
            }
        }
        AnySlice::WText(view) => {
            for row_index in 0..nrows {
                match view.get(row_index) {
                    Some(chars) => builder.append_value(decode_utf16_to_string::<E>(
                        chars,
                        source_name,
                        col_index,
                        column_name,
                        row_index,
                        replace_invalid_utf16,
                    )?),
                    None => {
                        require_nullable::<E>(nullable, "String")?;
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
                        require_nullable::<E>(nullable, "String")?;
                        builder.append_null();
                    }
                }
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "dst_arrow")]
#[allow(dead_code)]
pub(crate) fn build_binary_array<E: OdbcCoreError>(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, E> {
    let mut builder = LargeBinaryBuilder::with_capacity(nrows, nrows * 8);
    match column {
        AnySlice::Binary(view) => {
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
        other => {
            for row_index in 0..nrows {
                match odbc_cell_from_column(other, row_index) {
                    Some(cell) => builder.append_value(cell.try_bytes().ok_or_else(|| {
                        ConnectorXError::cannot_produce::<Vec<u8>>(Some(
                            "Odbc typed value for byte-only Vec<u8>".to_string(),
                        ))
                    })?),
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

#[cfg(feature = "dst_arrow")]
pub(crate) fn build_date32_array<E: OdbcCoreError>(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, E> {
    let mut builder = Date32Builder::with_capacity(nrows);
    match column {
        AnySlice::Date(values) => {
            for &v in &values[..nrows] {
                builder.append_value(naive_date_to_arrow_i32::<E>(odbc_date_to_naive::<E>(v)?)?);
            }
        }
        AnySlice::NullableDate(values) => {
            for value in values.take(nrows) {
                match value {
                    Some(&v) => builder
                        .append_value(naive_date_to_arrow_i32::<E>(odbc_date_to_naive::<E>(v)?)?),
                    None => {
                        require_nullable::<E>(nullable, "NaiveDate")?;
                        builder.append_null();
                    }
                }
            }
        }
        other => {
            for row_index in 0..nrows {
                match odbc_cell_from_column(other, row_index) {
                    Some(cell) => {
                        builder.append_value(naive_date_to_arrow_i32::<E>(cell_date::<E>(cell)?)?)
                    }
                    None => {
                        require_nullable::<E>(nullable, "NaiveDate")?;
                        builder.append_null();
                    }
                }
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "dst_arrow")]
pub(crate) fn build_time64_micro_array<E: OdbcCoreError>(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, E> {
    let mut builder = Time64MicrosecondBuilder::with_capacity(nrows);
    match column {
        AnySlice::Time(values) => {
            for &v in &values[..nrows] {
                builder.append_value(naive_time_to_micro(odbc_time_to_naive::<E>(v)?));
            }
        }
        AnySlice::NullableTime(values) => {
            for value in values.take(nrows) {
                match value {
                    Some(&v) => {
                        builder.append_value(naive_time_to_micro(odbc_time_to_naive::<E>(v)?))
                    }
                    None => {
                        require_nullable::<E>(nullable, "NaiveTime")?;
                        builder.append_null();
                    }
                }
            }
        }
        other => {
            for row_index in 0..nrows {
                match odbc_cell_from_column(other, row_index) {
                    Some(cell) => builder.append_value(naive_time_to_micro(cell_time::<E>(cell)?)),
                    None => {
                        require_nullable::<E>(nullable, "NaiveTime")?;
                        builder.append_null();
                    }
                }
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(feature = "dst_arrow")]
pub(crate) fn build_timestamp_micro_array<E: OdbcCoreError>(
    column: AnySlice<'_>,
    nrows: usize,
    nullable: bool,
) -> Result<ArrayRef, E> {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(nrows);
    match column {
        AnySlice::Timestamp(values) => {
            for &v in &values[..nrows] {
                builder.append_value(
                    odbc_timestamp_to_naive::<E>(v)?
                        .and_utc()
                        .timestamp_micros(),
                );
            }
        }
        AnySlice::NullableTimestamp(values) => {
            for value in values.take(nrows) {
                match value {
                    Some(&v) => builder.append_value(
                        odbc_timestamp_to_naive::<E>(v)?
                            .and_utc()
                            .timestamp_micros(),
                    ),
                    None => {
                        require_nullable::<E>(nullable, "NaiveDateTime")?;
                        builder.append_null();
                    }
                }
            }
        }
        other => {
            for row_index in 0..nrows {
                match odbc_cell_from_column(other, row_index) {
                    Some(cell) => builder
                        .append_value(cell_timestamp::<E>(cell)?.and_utc().timestamp_micros()),
                    None => {
                        require_nullable::<E>(nullable, "NaiveDateTime")?;
                        builder.append_null();
                    }
                }
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

// --- impl_odbc_arrow_policy! macro ----------------------------------------

/// Generate an [`OdbcArrowPolicy`] implementation for a standard ODBC-like
/// type system.
///
/// The type system must expose the canonical variants:
/// `TinyInt`, `SmallInt`, `Int`, `BigInt`, `Real`, `Double`, `Numeric`,
/// `Decimal`, `Bit`, `Char`, `Varchar`, `Text`, `Binary`, `Date`, `Time`,
/// `Timestamp` — each carrying a single `bool` nullable flag, except
/// `Numeric` and `Decimal`, which carry `(nullable, precision, scale)`.
#[allow(unused_macros)]
macro_rules! impl_odbc_arrow_policy {
    ($TS:ty) => {
        #[cfg(feature = "dst_arrow")]
        impl $crate::sources::odbc_core::OdbcArrowPolicy for $TS {
            fn arrow_type(self) -> $crate::destinations::arrow::ArrowTypeSystem {
                use $crate::destinations::arrow::ArrowTypeSystem;
                let nullable = $crate::sources::odbc_core::OdbcTypePolicy::nullable(self);
                match self {
                    Self::TinyInt(..) | Self::SmallInt(..) | Self::Int(..) | Self::BigInt(..) => {
                        ArrowTypeSystem::Int64(nullable)
                    }
                    Self::Real(..) => ArrowTypeSystem::Float32(nullable),
                    Self::Double(..) => ArrowTypeSystem::Float64(nullable),
                    Self::Numeric(_, precision, scale) | Self::Decimal(_, precision, scale) => {
                        ArrowTypeSystem::Decimal128(nullable, precision, scale)
                    }
                    Self::Bit(..) => ArrowTypeSystem::Boolean(nullable),
                    Self::Char(..) | Self::Varchar(..) | Self::Text(..) => {
                        ArrowTypeSystem::LargeUtf8(nullable)
                    }
                    Self::Binary(..) => ArrowTypeSystem::LargeBinary(nullable),
                    Self::Date(..) => ArrowTypeSystem::Date32(nullable),
                    Self::Time(..) => ArrowTypeSystem::Time64Micro(nullable),
                    Self::Timestamp(..) => ArrowTypeSystem::Date64Micro(nullable),
                }
            }

            fn arrow_data_type(self) -> ::arrow::datatypes::DataType {
                use ::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
                match self {
                    Self::TinyInt(..) | Self::SmallInt(..) | Self::Int(..) | Self::BigInt(..) => {
                        ArrowDataType::Int64
                    }
                    Self::Real(..) => ArrowDataType::Float32,
                    Self::Double(..) => ArrowDataType::Float64,
                    Self::Numeric(_, precision, scale) | Self::Decimal(_, precision, scale) => {
                        ArrowDataType::Decimal128(precision, scale)
                    }
                    Self::Bit(..) => ArrowDataType::Boolean,
                    Self::Char(..) | Self::Varchar(..) | Self::Text(..) => ArrowDataType::Utf8,
                    Self::Binary(..) => ArrowDataType::LargeBinary,
                    Self::Date(..) => ArrowDataType::Date32,
                    Self::Time(..) => ArrowDataType::Time64(TimeUnit::Microsecond),
                    Self::Timestamp(..) => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                }
            }

            fn build_arrow_array<E: $crate::sources::odbc_core::OdbcCoreError>(
                self,
                column: ::odbc_api::buffers::AnySlice<'_>,
                nrows: usize,
                col_index: usize,
                column_name: Option<&str>,
                replace_invalid_utf16: bool,
            ) -> Result<::std::sync::Arc<dyn ::arrow::array::Array>, E> {
                use $crate::sources::odbc_core::{
                    build_binary_array, build_bool_array, build_date32_array, build_decimal_array,
                    build_float32_array, build_float64_array, build_int64_array,
                    build_string_array, build_time64_micro_array, build_timestamp_micro_array,
                };
                let nullable = $crate::sources::odbc_core::OdbcTypePolicy::nullable(self);
                let source_name =
                    <Self as $crate::sources::odbc_core::OdbcTypePolicy>::source_name();
                match self {
                    Self::TinyInt(..) | Self::SmallInt(..) | Self::Int(..) | Self::BigInt(..) => {
                        build_int64_array(column, nrows, nullable)
                    }
                    Self::Real(..) => build_float32_array(column, nrows, nullable),
                    Self::Double(..) => build_float64_array(column, nrows, nullable),
                    Self::Numeric(_, precision, scale) | Self::Decimal(_, precision, scale) => {
                        build_decimal_array(
                            column,
                            nrows,
                            nullable,
                            precision,
                            scale,
                            source_name,
                            col_index,
                            column_name,
                            replace_invalid_utf16,
                        )
                    }
                    Self::Bit(..) => build_bool_array(column, nrows, nullable),
                    Self::Char(..) | Self::Varchar(..) | Self::Text(..) => build_string_array(
                        column,
                        nrows,
                        nullable,
                        source_name,
                        col_index,
                        column_name,
                        replace_invalid_utf16,
                    ),
                    Self::Binary(..) => build_binary_array(column, nrows, nullable),
                    Self::Date(..) => build_date32_array(column, nrows, nullable),
                    Self::Time(..) => build_time64_micro_array(column, nrows, nullable),
                    Self::Timestamp(..) => build_timestamp_micro_array(column, nrows, nullable),
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use impl_odbc_arrow_policy;

// --- generic Arrow extraction implementation ------------------------------

/// Fetch all `queries` from `conn` and return an [`ArrowDestination`]
/// containing the resulting record batches.
///
/// This is the shared implementation used by `odbc_get_arrow`,
/// `db2_get_arrow`, and `sybase_get_arrow`.  Callers supply the
/// already-resolved ODBC connection string, per-source options, and a
/// `map_type` closure that converts ODBC column metadata to the caller's
/// type system.
#[cfg(feature = "dst_arrow")]
pub(crate) fn odbc_get_arrow_impl<TS, E>(
    conn: &str,
    _origin_query: Option<String>,
    queries: &[CXQuery<String>],
    max_str_len: usize,
    batch_size: usize,
    connection_limiter: Arc<OdbcConnectionLimiter>,
    execution_options: OdbcExecutionOptions,
    replace_invalid_utf16: bool,
    map_type: impl Fn(DataType, Nullability, &str) -> Result<TS, E> + Send + Sync,
) -> Result<ArrowDestination, E>
where
    TS: OdbcTypePolicy + OdbcArrowPolicy + Send + Sync + 'static,
    E: OdbcCoreError + Send + 'static,
{
    let first_query = queries.first().ok_or_else(|| {
        ConnectorXError::Other(anyhow!("ODBC Arrow stream requires at least one query"))
    })?;
    let (names, schema, column_buffer_max_lens) = fetch_metadata::<TS, E, _>(
        TS::source_name(),
        conn,
        &first_query.to_string(),
        max_str_len,
        &connection_limiter,
        execution_options,
        map_type,
    )?;

    let (arrow_types, record_schema) = odbc_arrow_schema(&names, &schema);

    let mut destination = ArrowDestination::new();
    destination.allocate_with_schema(names.clone(), arrow_types, Arc::clone(&record_schema));

    let names = Arc::from(names.into_boxed_slice());
    let schema = Arc::from(schema.into_boxed_slice());
    let column_buffer_max_lens = Arc::from(column_buffer_max_lens.into_boxed_slice());

    queries.par_iter().try_for_each(|query| {
        arrow_fetch_partition::<TS, E>(
            conn,
            query.as_str(),
            &names,
            &schema,
            &column_buffer_max_lens,
            batch_size,
            Arc::clone(&connection_limiter),
            execution_options,
            replace_invalid_utf16,
            Arc::clone(&record_schema),
            &destination,
        )
    })?;

    Ok(destination)
}

#[cfg(feature = "dst_arrow")]
fn odbc_arrow_schema<TS>(names: &[String], schema: &[TS]) -> (Vec<ArrowTypeSystem>, Arc<Schema>)
where
    TS: OdbcTypePolicy + OdbcArrowPolicy,
{
    let arrow_types = schema.iter().map(|&ty| ty.arrow_type()).collect();
    let fields = names
        .iter()
        .zip(schema)
        .map(|(name, &ty)| Field::new(name, ty.arrow_data_type(), ty.nullable()))
        .collect::<Vec<_>>();
    (arrow_types, Arc::new(Schema::new(fields)))
}

#[cfg(feature = "dst_arrow")]
fn build_record_batch<TS, E>(
    batch: &ColumnarAnyBuffer,
    names: &[String],
    schema: &[TS],
    replace_invalid_utf16: bool,
    arrow_schema: Arc<Schema>,
) -> Result<RecordBatch, E>
where
    TS: OdbcTypePolicy + OdbcArrowPolicy + Send + Sync,
    E: OdbcCoreError + Send,
{
    let mut columns = Vec::with_capacity(batch.num_cols());
    for col_index in 0..batch.num_cols() {
        let column = batch.column(col_index);
        ensure_column_not_truncated::<E>(
            &column,
            TS::source_name(),
            TS::max_str_len_env(),
            col_index,
            names.get(col_index).map(String::as_str),
            schema.get(col_index).copied(),
        )?;
        columns.push(schema[col_index].build_arrow_array::<E>(
            column,
            batch.num_rows(),
            col_index,
            names.get(col_index).map(String::as_str),
            replace_invalid_utf16,
        )?);
    }
    Ok(RecordBatch::try_new(arrow_schema, columns).map_err(anyhow::Error::from)?)
}

#[cfg(feature = "dst_arrow")]
fn arrow_fetch_partition<TS, E>(
    conn: &str,
    query: &str,
    names: &[String],
    schema: &[TS],
    column_buffer_max_lens: &[usize],
    batch_size: usize,
    connection_limiter: Arc<OdbcConnectionLimiter>,
    execution_options: OdbcExecutionOptions,
    replace_invalid_utf16: bool,
    arrow_schema: Arc<Schema>,
    destination: &ArrowDestination,
) -> Result<(), E>
where
    TS: OdbcTypePolicy + OdbcArrowPolicy + Send + Sync,
    E: OdbcCoreError + Send,
{
    let _connection_permit = connection_limiter.acquire();
    let cursor = execute_query::<E>(TS::source_name(), conn, query, execution_options)?;
    let buffer = ColumnarAnyBuffer::try_from_descs(
        batch_size,
        schema
            .iter()
            .zip(column_buffer_max_lens)
            .map(|(ty, &max_len)| ty.buffer_desc(max_len)),
    )?;
    let mut cursor = cursor.bind_buffer(buffer)?;

    while let Some(batch) = cursor.fetch()? {
        let record_batch = build_record_batch::<TS, E>(
            batch,
            names,
            schema,
            replace_invalid_utf16,
            Arc::clone(&arrow_schema),
        )?;
        destination
            .push_record_batch(record_batch)
            .map_err(anyhow::Error::from)?;
    }
    Ok(())
}

#[cfg(feature = "dst_arrow")]
struct OdbcRecordBatchStreamPlan<TS, E> {
    conn: String,
    queries: Vec<String>,
    names: Arc<[String]>,
    schema: Arc<[TS]>,
    column_buffer_max_lens: Arc<[usize]>,
    batch_size: usize,
    connection_limiter: Arc<OdbcConnectionLimiter>,
    execution_options: OdbcExecutionOptions,
    replace_invalid_utf16: bool,
    arrow_schema: Arc<Schema>,
    _marker: PhantomData<fn() -> E>,
}

#[cfg(feature = "dst_arrow")]
pub(crate) struct OdbcRecordBatchIterator<TS, E> {
    names: Vec<String>,
    arrow_schema: Arc<Schema>,
    receiver: Receiver<Result<RecordBatch, ConnectorXError>>,
    sender: Option<Sender<Result<RecordBatch, ConnectorXError>>>,
    plan: Option<OdbcRecordBatchStreamPlan<TS, E>>,
}

#[cfg(feature = "dst_arrow")]
impl<TS, E> OdbcRecordBatchIterator<TS, E>
where
    TS: OdbcTypePolicy + OdbcArrowPolicy + Send + Sync + 'static,
    E: OdbcCoreError + Send + 'static,
{
    fn new(
        names: Vec<String>,
        arrow_schema: Arc<Schema>,
        plan: OdbcRecordBatchStreamPlan<TS, E>,
    ) -> Self {
        let (sender, receiver) = channel();
        Self {
            names,
            arrow_schema,
            receiver,
            sender: Some(sender),
            plan: Some(plan),
        }
    }
}

#[cfg(feature = "dst_arrow")]
impl<TS, E> RecordBatchIterator for OdbcRecordBatchIterator<TS, E>
where
    TS: OdbcTypePolicy + OdbcArrowPolicy + Send + Sync + 'static,
    E: OdbcCoreError + Send + 'static,
{
    fn get_schema(&self) -> (RecordBatch, &[String]) {
        (
            RecordBatch::new_empty(Arc::clone(&self.arrow_schema)),
            &self.names,
        )
    }

    fn prepare(&mut self) {
        let Some(plan) = self.plan.take() else {
            return;
        };
        let Some(sender) = self.sender.take() else {
            return;
        };

        thread::spawn(move || {
            let result = plan.queries.par_iter().try_for_each(|query| {
                arrow_stream_fetch_partition::<TS, E>(
                    &plan.conn,
                    query,
                    &plan.names,
                    &plan.schema,
                    &plan.column_buffer_max_lens,
                    plan.batch_size,
                    Arc::clone(&plan.connection_limiter),
                    plan.execution_options,
                    plan.replace_invalid_utf16,
                    Arc::clone(&plan.arrow_schema),
                    sender.clone(),
                )
            });
            if let Err(error) = result {
                let _ = sender.send(Err(ConnectorXError::Other(anyhow!(
                    "ODBC Arrow stream worker failed: {error:?}"
                ))));
            }
        });
    }

    fn next_batch(&mut self) -> Option<RecordBatch> {
        match self.next_batch_result() {
            Ok(record_batch) => record_batch,
            Err(error) => {
                log::error!("ODBC Arrow stream worker failed: {}", error);
                None
            }
        }
    }

    fn next_batch_result(&mut self) -> crate::errors::Result<Option<RecordBatch>> {
        if self.plan.is_some() {
            self.prepare();
        }
        match self.receiver.recv() {
            Ok(Ok(record_batch)) => Ok(Some(record_batch)),
            Ok(Err(error)) => Err(error),
            Err(_) => Ok(None),
        }
    }
}

#[cfg(feature = "dst_arrow")]
pub(crate) fn odbc_record_batch_iter_impl<TS, E>(
    conn: &str,
    _origin_query: Option<String>,
    queries: &[CXQuery<String>],
    max_str_len: usize,
    batch_size: usize,
    connection_limiter: Arc<OdbcConnectionLimiter>,
    execution_options: OdbcExecutionOptions,
    replace_invalid_utf16: bool,
    map_type: impl Fn(DataType, Nullability, &str) -> Result<TS, E> + Send + Sync,
) -> Result<OdbcRecordBatchIterator<TS, E>, E>
where
    TS: OdbcTypePolicy + OdbcArrowPolicy + Send + Sync + 'static,
    E: OdbcCoreError + Send + 'static,
{
    let (names, schema, column_buffer_max_lens) = fetch_metadata::<TS, E, _>(
        TS::source_name(),
        conn,
        &queries[0].to_string(),
        max_str_len,
        &connection_limiter,
        execution_options,
        map_type,
    )?;
    let (_, record_schema) = odbc_arrow_schema(&names, &schema);

    let plan = OdbcRecordBatchStreamPlan {
        conn: conn.to_string(),
        queries: queries
            .iter()
            .map(|query| query.as_str().to_string())
            .collect(),
        names: Arc::from(names.clone().into_boxed_slice()),
        schema: Arc::from(schema.into_boxed_slice()),
        column_buffer_max_lens: Arc::from(column_buffer_max_lens.into_boxed_slice()),
        batch_size,
        connection_limiter,
        execution_options,
        replace_invalid_utf16,
        arrow_schema: Arc::clone(&record_schema),
        _marker: PhantomData,
    };

    Ok(OdbcRecordBatchIterator::new(names, record_schema, plan))
}

#[cfg(feature = "dst_arrow")]
fn arrow_stream_fetch_partition<TS, E>(
    conn: &str,
    query: &str,
    names: &[String],
    schema: &[TS],
    column_buffer_max_lens: &[usize],
    batch_size: usize,
    connection_limiter: Arc<OdbcConnectionLimiter>,
    execution_options: OdbcExecutionOptions,
    replace_invalid_utf16: bool,
    arrow_schema: Arc<Schema>,
    sender: Sender<Result<RecordBatch, ConnectorXError>>,
) -> Result<(), E>
where
    TS: OdbcTypePolicy + OdbcArrowPolicy + Send + Sync,
    E: OdbcCoreError + Send,
{
    let _connection_permit = connection_limiter.acquire();
    let cursor = execute_query::<E>(TS::source_name(), conn, query, execution_options)?;
    let buffer = ColumnarAnyBuffer::try_from_descs(
        batch_size,
        schema
            .iter()
            .zip(column_buffer_max_lens)
            .map(|(ty, &max_len)| ty.buffer_desc(max_len)),
    )?;
    let mut cursor = cursor.bind_buffer(buffer)?;

    while let Some(batch) = cursor.fetch()? {
        let record_batch = build_record_batch::<TS, E>(
            batch,
            names,
            schema,
            replace_invalid_utf16,
            Arc::clone(&arrow_schema),
        )?;
        if sender.send(Ok(record_batch)).is_err() {
            return Ok(());
        }
    }
    Ok(())
}
