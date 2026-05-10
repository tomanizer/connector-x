use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use log::warn;
use odbc_api::{DataType, Nullability};
use rust_decimal::Decimal;

use crate::constants::{DEFAULT_ARROW_DECIMAL_PRECISION, DEFAULT_ARROW_DECIMAL_SCALE};

#[derive(Copy, Clone, Debug)]
pub enum SybaseTypeSystem {
    TinyInt(bool),
    SmallInt(bool),
    Int(bool),
    BigInt(bool),
    Real(bool),
    Double(bool),
    /// `(nullable, precision, scale)` – precision and scale from the ODBC data-source.
    /// Falls back to `DEFAULT_ARROW_DECIMAL_PRECISION` / `DEFAULT_ARROW_DECIMAL_SCALE`
    /// when the driver reports values outside the valid Arrow Decimal128 range.
    Numeric(bool, u8, i8),
    /// Same as `Numeric`.
    Decimal(bool, u8, i8),
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
            DataType::Numeric { precision, scale } => {
                Numeric(nullable, decimal_precision(precision), decimal_scale(scale))
            }
            DataType::Decimal { precision, scale } => {
                Decimal(nullable, decimal_precision(precision), decimal_scale(scale))
            }
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

/// Clamp an ODBC precision (`usize`) to a valid Arrow Decimal128 precision (`u8`).
/// Falls back to `DEFAULT_ARROW_DECIMAL_PRECISION` for out-of-range values, and emits a
/// `warn!` log so callers can diagnose unexpected driver metadata.
pub(crate) fn decimal_precision(precision: usize) -> u8 {
    if (1..=DEFAULT_ARROW_DECIMAL_PRECISION as usize).contains(&precision) {
        precision as u8
    } else {
        warn!(
            "ODBC decimal precision {precision} is outside Arrow Decimal128 range 1..={}; falling back to DEFAULT_ARROW_DECIMAL_PRECISION ({})",
            DEFAULT_ARROW_DECIMAL_PRECISION,
            DEFAULT_ARROW_DECIMAL_PRECISION
        );
        DEFAULT_ARROW_DECIMAL_PRECISION
    }
}

/// Clamp an ODBC scale (`i16`) to a valid non-negative Arrow Decimal128 scale.
/// Falls back to `DEFAULT_ARROW_DECIMAL_SCALE` for out-of-range values, and emits a
/// `warn!` log so callers can diagnose unexpected driver metadata.
pub(crate) fn decimal_scale(scale: i16) -> i8 {
    if (0..=i8::MAX as i16).contains(&scale) {
        scale as i8
    } else {
        warn!(
            "ODBC decimal scale {scale} is outside non-negative i8 range; falling back to DEFAULT_ARROW_DECIMAL_SCALE ({})",
            DEFAULT_ARROW_DECIMAL_SCALE
        );
        DEFAULT_ARROW_DECIMAL_SCALE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use odbc_api::sys::SqlDataType;

    #[test]
    fn maps_sybase_odbc_types_and_nullability() {
        assert!(matches!(
            SybaseTypeSystem::from_odbc(DataType::SmallInt, Nullability::NoNulls),
            SybaseTypeSystem::SmallInt(false)
        ));
        assert!(matches!(
            SybaseTypeSystem::from_odbc(
                DataType::Decimal {
                    precision: 18,
                    scale: 4
                },
                Nullability::Nullable
            ),
            SybaseTypeSystem::Decimal(true, 18, 4)
        ));
        assert!(matches!(
            SybaseTypeSystem::from_odbc(
                DataType::Numeric {
                    precision: 18,
                    scale: 4
                },
                Nullability::NoNulls
            ),
            SybaseTypeSystem::Numeric(false, 18, 4)
        ));
        assert!(matches!(
            SybaseTypeSystem::from_odbc(DataType::Real, Nullability::Unknown),
            SybaseTypeSystem::Real(true)
        ));
        assert!(matches!(
            SybaseTypeSystem::from_odbc(DataType::Binary { length: None }, Nullability::NoNulls),
            SybaseTypeSystem::Binary(false)
        ));
        assert!(matches!(
            SybaseTypeSystem::from_odbc(
                DataType::Timestamp { precision: 3 },
                Nullability::Nullable
            ),
            SybaseTypeSystem::Timestamp(true)
        ));
    }

    #[test]
    fn maps_freetds_time2_extension_and_text_fallbacks() {
        assert!(matches!(
            SybaseTypeSystem::from_odbc(
                DataType::Other {
                    data_type: SqlDataType(-154),
                    column_size: None,
                    decimal_digits: 0,
                },
                Nullability::NoNulls
            ),
            SybaseTypeSystem::Time(false)
        ));
        assert!(matches!(
            SybaseTypeSystem::from_odbc(DataType::Unknown, Nullability::Unknown),
            SybaseTypeSystem::Varchar(true)
        ));
        assert!(matches!(
            SybaseTypeSystem::from_odbc(
                DataType::Other {
                    data_type: SqlDataType(-9999),
                    column_size: None,
                    decimal_digits: 0,
                },
                Nullability::Nullable
            ),
            SybaseTypeSystem::Varchar(true)
        ));
    }
}
