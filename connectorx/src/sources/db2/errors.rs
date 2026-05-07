use std::{num::ParseFloatError, num::ParseIntError, string::FromUtf8Error};

use chrono::ParseError as ChronoParseError;
use rust_decimal::Error as DecimalParseError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Db2SourceError {
    #[error("Cannot get # of rows in the partition")]
    GetNRowsFailed,

    #[error("Db2 query returned no result set: {0}")]
    NoResultSet(String),

    #[error("Cannot parse Db2 value {value:?} as {ty}")]
    ParseValue { value: String, ty: &'static str },

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
