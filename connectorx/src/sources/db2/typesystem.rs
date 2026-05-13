use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use log::warn;
use odbc_api::{DataType, Nullability};
use rust_decimal::Decimal;

use crate::{
    constants::{DEFAULT_ARROW_DECIMAL_PRECISION, DEFAULT_ARROW_DECIMAL_SCALE},
    sources::odbc_core::unknown_odbc_type_error,
};

pub(crate) const DB2_UNKNOWN_TYPE_FALLBACK_ENV: &str = "DB2_TYPE_FALLBACK_TO_VARCHAR";
pub(crate) const DB2_SQL_GRAPHIC_LUW: i16 = -95;
pub(crate) const DB2_SQL_VARGRAPHIC_LUW: i16 = -96;
pub(crate) const DB2_SQL_LONGVARGRAPHIC_LUW: i16 = -97;
pub(crate) const DB2_SQL_BLOB_LUW: i16 = -98;
pub(crate) const DB2_SQL_CLOB_LUW: i16 = -99;
pub(crate) const DB2_SQL_DBCLOB_LUW: i16 = -350;
pub(crate) const DB2_SQL_DECFLOAT: i16 = -360;
pub(crate) const DB2_SQL_XML: i16 = -370;

// Db2 for i documents positive graphic type codes, while IBM's LUW clidriver
// exposes the same families as negative extension codes.
pub(crate) const DB2_SQL_GRAPHIC_I: i16 = 95;
pub(crate) const DB2_SQL_VARGRAPHIC_I: i16 = 96;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Db2TypeSystem {
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
    pub fn from_odbc(
        ty: DataType,
        nullability: Nullability,
        column_name: &str,
        unknown_type_fallback_to_varchar: bool,
    ) -> Result<Self> {
        let nullable = nullability.could_be_nullable();
        use Db2TypeSystem::*;

        Ok(match ty {
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
            DataType::Other { data_type, .. } => match db2_vendor_type(data_type.0, nullable) {
                Some(known_type) => known_type,
                None if unknown_type_fallback_to_varchar => Varchar(nullable),
                None => {
                    return Err(unknown_odbc_type_error(
                        "Db2",
                        DB2_UNKNOWN_TYPE_FALLBACK_ENV,
                        column_name,
                        ty,
                        nullability,
                    ));
                }
            },
            DataType::Unknown if unknown_type_fallback_to_varchar => Varchar(nullable),
            DataType::Unknown => {
                return Err(unknown_odbc_type_error(
                    "Db2",
                    DB2_UNKNOWN_TYPE_FALLBACK_ENV,
                    column_name,
                    ty,
                    nullability,
                ));
            }
        })
    }
}

fn db2_vendor_type(type_code: i16, nullable: bool) -> Option<Db2TypeSystem> {
    use Db2TypeSystem::*;
    match type_code {
        DB2_SQL_DECFLOAT => Some(Varchar(nullable)),
        DB2_SQL_XML => Some(Binary(nullable)),
        DB2_SQL_GRAPHIC_LUW | DB2_SQL_GRAPHIC_I => Some(Char(nullable)),
        DB2_SQL_VARGRAPHIC_LUW | DB2_SQL_VARGRAPHIC_I => Some(Varchar(nullable)),
        DB2_SQL_LONGVARGRAPHIC_LUW => Some(Text(nullable)),
        DB2_SQL_CLOB_LUW | DB2_SQL_DBCLOB_LUW => Some(Text(nullable)),
        DB2_SQL_BLOB_LUW => Some(Binary(nullable)),
        _ => None,
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
    use std::num::NonZeroUsize;

    use odbc_api::sys::SqlDataType;

    #[test]
    fn maps_db2_odbc_types_and_nullability() {
        assert!(matches!(
            Db2TypeSystem::from_odbc(DataType::Integer, Nullability::NoNulls, "id", false).unwrap(),
            Db2TypeSystem::Int(false)
        ));
        assert!(matches!(
            Db2TypeSystem::from_odbc(
                DataType::Numeric {
                    precision: 31,
                    scale: 6
                },
                Nullability::Nullable,
                "amount",
                false
            )
            .unwrap(),
            Db2TypeSystem::Numeric(true, 31, 6)
        ));
        assert!(matches!(
            Db2TypeSystem::from_odbc(
                DataType::Decimal {
                    precision: 18,
                    scale: 4
                },
                Nullability::NoNulls,
                "balance",
                false
            )
            .unwrap(),
            Db2TypeSystem::Decimal(false, 18, 4)
        ));
        assert!(matches!(
            Db2TypeSystem::from_odbc(DataType::Double, Nullability::Unknown, "ratio", false)
                .unwrap(),
            Db2TypeSystem::Double(true)
        ));
        assert!(matches!(
            Db2TypeSystem::from_odbc(
                DataType::Varbinary { length: None },
                Nullability::NoNulls,
                "payload",
                false
            )
            .unwrap(),
            Db2TypeSystem::Binary(false)
        ));
        assert!(matches!(
            Db2TypeSystem::from_odbc(
                DataType::Time { precision: 6 },
                Nullability::Nullable,
                "time_col",
                false
            )
            .unwrap(),
            Db2TypeSystem::Time(true)
        ));
    }

    #[test]
    fn maps_known_db2_vendor_types_by_default() {
        for (code, expected) in [
            (DB2_SQL_DECFLOAT, Db2TypeSystem::Varchar(false)),
            (DB2_SQL_XML, Db2TypeSystem::Binary(false)),
            (DB2_SQL_GRAPHIC_LUW, Db2TypeSystem::Char(false)),
            (DB2_SQL_VARGRAPHIC_LUW, Db2TypeSystem::Varchar(false)),
            (DB2_SQL_LONGVARGRAPHIC_LUW, Db2TypeSystem::Text(false)),
            (DB2_SQL_BLOB_LUW, Db2TypeSystem::Binary(false)),
            (DB2_SQL_CLOB_LUW, Db2TypeSystem::Text(false)),
            (DB2_SQL_DBCLOB_LUW, Db2TypeSystem::Text(false)),
            (DB2_SQL_GRAPHIC_I, Db2TypeSystem::Char(false)),
            (DB2_SQL_VARGRAPHIC_I, Db2TypeSystem::Varchar(false)),
        ] {
            let mapped = Db2TypeSystem::from_odbc(
                DataType::Other {
                    data_type: SqlDataType(code),
                    column_size: None,
                    decimal_digits: 0,
                },
                Nullability::NoNulls,
                "vendor_col",
                false,
            )
            .unwrap();
            assert_eq!(mapped, expected);
        }
    }

    #[test]
    fn rejects_unknown_vendor_types_by_default() {
        let error = Db2TypeSystem::from_odbc(
            DataType::Other {
                data_type: SqlDataType(-999),
                column_size: None,
                decimal_digits: 0,
            },
            Nullability::NoNulls,
            "vendor_col",
            false,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("source=Db2"));
        assert!(error.contains("column_name=vendor_col"));
        assert!(error.contains("odbc_type_code=-999"));
        assert!(error.contains(DB2_UNKNOWN_TYPE_FALLBACK_ENV));
    }

    #[test]
    fn maps_db2_lob_binary_and_wide_text_metadata() {
        assert!(matches!(
            Db2TypeSystem::from_odbc(
                DataType::LongVarchar {
                    length: NonZeroUsize::new(2048)
                },
                Nullability::Nullable,
                "clob_v",
                false
            )
            .unwrap(),
            Db2TypeSystem::Text(true)
        ));
        assert!(matches!(
            Db2TypeSystem::from_odbc(
                DataType::WLongVarchar {
                    length: NonZeroUsize::new(2048)
                },
                Nullability::Nullable,
                "dbclob_v",
                false
            )
            .unwrap(),
            Db2TypeSystem::Text(true)
        ));
        assert!(matches!(
            Db2TypeSystem::from_odbc(
                DataType::LongVarbinary {
                    length: NonZeroUsize::new(2048)
                },
                Nullability::Nullable,
                "blob_v",
                false
            )
            .unwrap(),
            Db2TypeSystem::Binary(true)
        ));
        assert!(matches!(
            Db2TypeSystem::from_odbc(
                DataType::WChar {
                    length: NonZeroUsize::new(16)
                },
                Nullability::NoNulls,
                "graphic_v",
                false
            )
            .unwrap(),
            Db2TypeSystem::Char(false)
        ));
        assert!(matches!(
            Db2TypeSystem::from_odbc(
                DataType::WVarchar {
                    length: NonZeroUsize::new(64)
                },
                Nullability::Nullable,
                "vargraphic_v",
                false
            )
            .unwrap(),
            Db2TypeSystem::Varchar(true)
        ));
    }

    #[test]
    fn allows_unknown_and_vendor_types_with_permissive_fallback() {
        assert!(matches!(
            Db2TypeSystem::from_odbc(DataType::Unknown, Nullability::Unknown, "unknown_col", true)
                .unwrap(),
            Db2TypeSystem::Varchar(true)
        ));
        assert!(matches!(
            Db2TypeSystem::from_odbc(
                DataType::Other {
                    data_type: SqlDataType(-999),
                    column_size: None,
                    decimal_digits: 0,
                },
                Nullability::NoNulls,
                "vendor_col",
                true
            )
            .unwrap(),
            Db2TypeSystem::Varchar(false)
        ));
    }
}
