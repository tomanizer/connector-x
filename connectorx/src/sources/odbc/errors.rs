use std::{num::ParseFloatError, num::ParseIntError, string::FromUtf8Error};

use chrono::ParseError as ChronoParseError;
use rust_decimal::Error as DecimalParseError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum OdbcSourceError {
    #[error("Cannot get # of rows in the partition")]
    GetNRowsFailed,

    #[error("ODBC query returned no result set: {0}")]
    NoResultSet(String),

    #[error("Cannot parse ODBC value {value:?} as {ty}")]
    ParseValue { value: String, ty: &'static str },

    #[error(
        "Invalid UTF-16 sequence for source={source_name} column_name={column_name} row_index={row_index} byte_offset={byte_offset} surrogate={surrogate:#06X}. Set replace_invalid_utf16=true to replace invalid UTF-16 with U+FFFD."
    )]
    InvalidUtf16 {
        source_name: &'static str,
        column_name: String,
        row_index: usize,
        byte_offset: usize,
        surrogate: u16,
    },

    #[error(transparent)]
    ConnectorXError(#[from] crate::errors::ConnectorXError),

    #[error(transparent)]
    OdbcError(#[from] odbc_api::Error),

    #[error(transparent)]
    UrlError(#[from] url::ParseError),

    #[error(transparent)]
    UrlDecodeError(#[from] FromUtf8Error),

    #[error(transparent)]
    ParseIntError(#[from] ParseIntError),

    #[error(transparent)]
    ParseFloatError(#[from] ParseFloatError),

    #[error(transparent)]
    ParseDecimalError(#[from] DecimalParseError),

    #[error(transparent)]
    ParseChronoError(#[from] ChronoParseError),

    #[error(transparent)]
    Utf8Error(#[from] std::str::Utf8Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
