//! Transport from Db2 Source to ArrowStream Destination.

use crate::destinations::arrowstream::{
    typesystem::{NaiveDateTimeWrapperMicro, NaiveTimeWrapperMicro},
    ArrowDestination, ArrowDestinationError, ArrowTypeSystem,
};
use crate::sources::db2::{Db2Source, Db2SourceError, Db2TypeSystem};
use crate::typesystem::TypeConversion;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use thiserror::Error;

pub struct Db2ArrowTransport;

#[derive(Error, Debug)]
pub enum Db2ArrowTransportError {
    #[error(transparent)]
    Source(#[from] Db2SourceError),

    #[error(transparent)]
    Destination(#[from] ArrowDestinationError),

    #[error(transparent)]
    ConnectorX(#[from] crate::errors::ConnectorXError),
}

impl_transport!(
    name = Db2ArrowTransport,
    error = Db2ArrowTransportError,
    systems = Db2TypeSystem => ArrowTypeSystem,
    route = Db2Source => ArrowDestination,
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

impl TypeConversion<NaiveTime, NaiveTimeWrapperMicro> for Db2ArrowTransport {
    fn convert(val: NaiveTime) -> NaiveTimeWrapperMicro {
        NaiveTimeWrapperMicro(val)
    }
}

impl TypeConversion<NaiveDateTime, NaiveDateTimeWrapperMicro> for Db2ArrowTransport {
    fn convert(val: NaiveDateTime) -> NaiveDateTimeWrapperMicro {
        NaiveDateTimeWrapperMicro(val)
    }
}

impl TypeConversion<Vec<u8>, Vec<u8>> for Db2ArrowTransport {
    fn convert(val: Vec<u8>) -> Vec<u8> {
        val
    }
}

impl TypeConversion<Option<Vec<u8>>, Option<Vec<u8>>> for Db2ArrowTransport {
    fn convert(val: Option<Vec<u8>>) -> Option<Vec<u8>> {
        val
    }
}
