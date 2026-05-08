use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use odbc_api::{DataType, Nullability};
use rust_decimal::Decimal;

#[derive(Copy, Clone, Debug)]
pub enum Db2TypeSystem {
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
    system = Db2TypeSystem,
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

impl Db2TypeSystem {
    pub fn from_odbc(ty: DataType, nullability: Nullability) -> Self {
        let nullable = nullability.could_be_nullable();
        use Db2TypeSystem::*;

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
            DataType::Unknown | DataType::Other { .. } => Varchar(nullable),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use odbc_api::sys::SqlDataType;

    #[test]
    fn maps_db2_odbc_types_and_nullability() {
        assert!(matches!(
            Db2TypeSystem::from_odbc(DataType::Integer, Nullability::NoNulls),
            Db2TypeSystem::Int(false)
        ));
        assert!(matches!(
            Db2TypeSystem::from_odbc(
                DataType::Numeric {
                    precision: 31,
                    scale: 6
                },
                Nullability::Nullable
            ),
            Db2TypeSystem::Numeric(true)
        ));
        assert!(matches!(
            Db2TypeSystem::from_odbc(DataType::Double, Nullability::Unknown),
            Db2TypeSystem::Double(true)
        ));
        assert!(matches!(
            Db2TypeSystem::from_odbc(DataType::Varbinary { length: None }, Nullability::NoNulls),
            Db2TypeSystem::Binary(false)
        ));
        assert!(matches!(
            Db2TypeSystem::from_odbc(DataType::Time { precision: 6 }, Nullability::Nullable),
            Db2TypeSystem::Time(true)
        ));
    }

    #[test]
    fn falls_back_unknown_and_vendor_types_to_text() {
        assert!(matches!(
            Db2TypeSystem::from_odbc(DataType::Unknown, Nullability::Unknown),
            Db2TypeSystem::Varchar(true)
        ));
        assert!(matches!(
            Db2TypeSystem::from_odbc(
                DataType::Other {
                    data_type: SqlDataType(-370),
                    column_size: None,
                    decimal_digits: 0,
                },
                Nullability::NoNulls
            ),
            Db2TypeSystem::Varchar(false)
        ));
    }
}
