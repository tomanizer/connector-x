use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use odbc_api::{DataType, Nullability};
use rust_decimal::Decimal;

#[derive(Copy, Clone, Debug)]
pub enum SybaseTypeSystem {
    TinyInt(bool),
    SmallInt(bool),
    Int(bool),
    BigInt(bool),
    Real(bool),
    Double(bool),
    Numeric(bool),
    Decimal(bool),
    Bit(bool),
    Char(bool),
    Varchar(bool),
    Text(bool),
    Binary(bool),
    Date(bool),
    Time(bool),
    Timestamp(bool),
}

impl_typesystem! {
    system = SybaseTypeSystem,
    mappings = {
        { TinyInt => u8 }
        { SmallInt => i16 }
        { Int => i32 }
        { BigInt => i64 }
        { Real => f32 }
        { Double => f64 }
        { Numeric | Decimal => Decimal }
        { Bit => bool }
        { Char | Varchar | Text => String }
        { Binary => Vec<u8> }
        { Date => NaiveDate }
        { Time => NaiveTime }
        { Timestamp => NaiveDateTime }
    }
}

impl SybaseTypeSystem {
    pub fn from_odbc(ty: DataType, nullability: Nullability) -> Self {
        let nullable = nullability.could_be_nullable();
        use SybaseTypeSystem::*;

        match ty {
            DataType::TinyInt => TinyInt(nullable),
            DataType::SmallInt => SmallInt(nullable),
            DataType::Integer => Int(nullable),
            DataType::BigInt => BigInt(nullable),
            DataType::Real => Real(nullable),
            DataType::Float { precision } if precision <= 24 => Real(nullable),
            DataType::Float { .. } | DataType::Double => Double(nullable),
            DataType::Numeric { .. } => Numeric(nullable),
            DataType::Decimal { .. } => Decimal(nullable),
            DataType::Bit => Bit(nullable),
            DataType::Char { .. } | DataType::WChar { .. } => Char(nullable),
            DataType::Varchar { .. } | DataType::WVarchar { .. } => Varchar(nullable),
            DataType::LongVarchar { .. } | DataType::WLongVarchar { .. } => Text(nullable),
            DataType::Binary { .. }
            | DataType::Varbinary { .. }
            | DataType::LongVarbinary { .. } => Binary(nullable),
            DataType::Date => Date(nullable),
            DataType::Time { .. } => Time(nullable),
            DataType::Timestamp { .. } => Timestamp(nullable),
            // FreeTDS reports ASE time and bigtime as the SQL Server TIME2 extension.
            DataType::Other { data_type, .. } if data_type.0 == -154 => Time(nullable),
            DataType::Unknown | DataType::Other { .. } => Varchar(nullable),
        }
    }
}
