//! Transport from Sybase Source to Arrow Destination.

use crate::destinations::arrow::{
    typesystem::{NaiveDateTimeWrapperMicro, NaiveTimeWrapperMicro},
    ArrowDestination, ArrowDestinationError, ArrowTypeSystem,
};
use crate::sources::sybase::{SybaseSource, SybaseSourceError, SybaseTypeSystem};
use crate::typesystem::TypeConversion;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use thiserror::Error;

pub struct SybaseArrowTransport;

#[derive(Error, Debug)]
pub enum SybaseArrowTransportError {
    #[error(transparent)]
    Source(#[from] SybaseSourceError),

    #[error(transparent)]
    Destination(#[from] ArrowDestinationError),

    #[error(transparent)]
    ConnectorX(#[from] crate::errors::ConnectorXError),
}

impl_transport!(
    name = SybaseArrowTransport,
    error = SybaseArrowTransportError,
    systems = SybaseTypeSystem => ArrowTypeSystem,
    route = SybaseSource => ArrowDestination,
    mappings = {
        { TinyInt[u8]                  => Int64[i64]            | conversion auto }
        { SmallInt[i16]                => Int64[i64]            | conversion auto }
        { Int[i32]                     => Int64[i64]            | conversion auto }
        { BigInt[i64]                  => Int64[i64]            | conversion auto }
        { Real[f32]                    => Float32[f32]          | conversion auto }
        { Double[f64]                  => Float64[f64]          | conversion auto }
        { Numeric[Decimal]             => Decimal128[Decimal]   | conversion auto | preserve decimal }
        { Decimal[Decimal]             => Decimal128[Decimal]   | conversion none | preserve decimal }
        { Bit[bool]                    => Boolean[bool]         | conversion auto }
        { Char[String]                 => LargeUtf8[String]     | conversion auto }
        { Varchar[String]              => LargeUtf8[String]     | conversion none }
        { Text[String]                 => LargeUtf8[String]     | conversion none }
        { Binary[Vec<u8>]              => LargeBinary[Vec<u8>]  | conversion none }
        { Date[NaiveDate]              => Date32[NaiveDate]     | conversion auto }
        { Time[NaiveTime]              => Time64Micro[NaiveTimeWrapperMicro]       | conversion option }
        { Timestamp[NaiveDateTime]     => Date64Micro[NaiveDateTimeWrapperMicro]   | conversion option }
    }
);

impl TypeConversion<NaiveTime, NaiveTimeWrapperMicro> for SybaseArrowTransport {
    fn convert(val: NaiveTime) -> NaiveTimeWrapperMicro {
        NaiveTimeWrapperMicro(val)
    }
}

impl TypeConversion<NaiveDateTime, NaiveDateTimeWrapperMicro> for SybaseArrowTransport {
    fn convert(val: NaiveDateTime) -> NaiveDateTimeWrapperMicro {
        NaiveDateTimeWrapperMicro(val)
    }
}

impl TypeConversion<Vec<u8>, Vec<u8>> for SybaseArrowTransport {
    fn convert(val: Vec<u8>) -> Vec<u8> {
        val
    }
}

impl TypeConversion<Option<Vec<u8>>, Option<Vec<u8>>> for SybaseArrowTransport {
    fn convert(val: Option<Vec<u8>>) -> Option<Vec<u8>> {
        val
    }
}
