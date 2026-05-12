#![cfg(all(
    feature = "dst_arrow",
    feature = "src_odbc",
    any(feature = "src_db2", feature = "src_sybase")
))]

use arrow::{
    array::{
        Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array,
        Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
        LargeStringArray, StringArray, Time32MillisecondArray, Time32SecondArray,
        Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
        UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::DataType,
    record_batch::RecordBatch,
};
use connectorx::{
    get_arrow::get_arrow,
    partition::{partition, PartitionQuery},
    prelude::*,
    sql::CXQuery,
};

#[allow(dead_code)]
mod test_db;

#[cfg(feature = "src_db2")]
fn use_db2_testcontainer() -> bool {
    std::env::var("CONNECTORX_DB2_TESTCONTAINER").is_ok()
}

#[cfg(feature = "src_db2")]
fn db2_route_pair() -> Option<(String, String)> {
    if use_db2_testcontainer() {
        let dedicated = test_db::db2_odbc_url();
        let generic = generic_odbc_url(&test_db::db2_odbc_conn());
        return Some((dedicated, generic));
    }

    let dedicated = std::env::var("DB2_URL").ok()?;
    let generic = std::env::var("DB2_GENERIC_ODBC_URL").ok().or_else(|| {
        std::env::var("DB2_ODBC_CONN")
            .ok()
            .map(|conn| generic_odbc_url(&conn))
    })?;
    Some((dedicated, generic))
}

#[cfg(feature = "src_sybase")]
fn use_sybase_testcontainer() -> bool {
    std::env::var("CONNECTORX_SYBASE_TESTCONTAINER").is_ok()
}

#[cfg(feature = "src_sybase")]
fn sybase_route_pair() -> Option<(String, String)> {
    if use_sybase_testcontainer() {
        let dedicated = test_db::sybase_odbc_url();
        let generic = generic_odbc_url(&test_db::sybase_odbc_conn());
        return Some((dedicated, generic));
    }

    let dedicated = std::env::var("SYBASE_URL").ok()?;
    let generic = std::env::var("SYBASE_GENERIC_ODBC_URL").ok().or_else(|| {
        std::env::var("SYBASE_ODBC_CONN")
            .ok()
            .map(|conn| generic_odbc_url(&conn))
    })?;
    Some((dedicated, generic))
}

fn generic_odbc_url(raw_conn: &str) -> String {
    format!("odbc:///?odbc_connect={}", urlencoding::encode(raw_conn))
}

#[cfg(feature = "src_db2")]
fn db2_basic_query() -> CXQuery<String> {
    CXQuery::naked(
        "select id, flag, name from ( \
             select cast(1 as integer) as id, cast(1 as smallint) as flag, cast('alpha' as varchar(16)) as name from sysibm.sysdummy1 \
             union all \
             select cast(2 as integer) as id, cast(0 as smallint) as flag, cast('beta' as varchar(16)) as name from sysibm.sysdummy1 \
         ) q",
    )
}

#[cfg(feature = "src_sybase")]
fn sybase_basic_query() -> CXQuery<String> {
    CXQuery::naked(
        "select convert(int, 1) as id, convert(bit, 1) as flag, convert(varchar(16), 'alpha') as name \
         union all \
         select convert(int, 2) as id, convert(bit, 0) as flag, convert(varchar(16), 'beta') as name",
    )
}

#[cfg(feature = "src_db2")]
#[test]
fn test_db2_dedicated_and_generic_odbc_routes_match_basic_arrow() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some((dedicated, generic)) = db2_route_pair() else {
        eprintln!(
            "CONNECTORX_SKIP: skipping Db2 route comparison test: DB2_URL and DB2_GENERIC_ODBC_URL or DB2_ODBC_CONN are not set"
        );
        return;
    };

    let query = db2_basic_query();
    let dedicated_batches = read_arrow(&dedicated, &[query.clone()], None);
    let generic_batches = read_arrow(&generic, &[query], None);

    assert_arrow_equivalent(&dedicated_batches, &generic_batches);
}

#[cfg(feature = "src_db2")]
#[test]
fn test_db2_dedicated_and_generic_odbc_partition_routes_match() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some((dedicated, generic)) = db2_route_pair() else {
        eprintln!(
            "CONNECTORX_SKIP: skipping Db2 route partition comparison test: DB2_URL and DB2_GENERIC_ODBC_URL or DB2_ODBC_CONN are not set"
        );
        return;
    };

    let query = db2_basic_query();
    let dedicated_batches = read_partitioned_arrow(&dedicated, &query, "id");
    let generic_batches = read_partitioned_arrow(&generic, &query, "id");

    assert_arrow_equivalent(&dedicated_batches, &generic_batches);
}

#[cfg(feature = "src_sybase")]
#[test]
fn test_sybase_dedicated_and_generic_odbc_routes_match_basic_arrow() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some((dedicated, generic)) = sybase_route_pair() else {
        eprintln!(
            "CONNECTORX_SKIP: skipping Sybase route comparison test: SYBASE_URL and SYBASE_GENERIC_ODBC_URL or SYBASE_ODBC_CONN are not set"
        );
        return;
    };

    let query = sybase_basic_query();
    let dedicated_batches = read_arrow(&dedicated, &[query.clone()], None);
    let generic_batches = read_arrow(&generic, &[query], None);

    assert_arrow_equivalent(&dedicated_batches, &generic_batches);
}

#[cfg(feature = "src_sybase")]
#[test]
fn test_sybase_dedicated_and_generic_odbc_partition_routes_match() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some((dedicated, generic)) = sybase_route_pair() else {
        eprintln!(
            "CONNECTORX_SKIP: skipping Sybase route partition comparison test: SYBASE_URL and SYBASE_GENERIC_ODBC_URL or SYBASE_ODBC_CONN are not set"
        );
        return;
    };

    let query = sybase_basic_query();
    let dedicated_batches = read_partitioned_arrow(&dedicated, &query, "id");
    let generic_batches = read_partitioned_arrow(&generic, &query, "id");

    assert_arrow_equivalent(&dedicated_batches, &generic_batches);
}

fn read_arrow(
    conn: &str,
    queries: &[CXQuery<String>],
    origin_query: Option<String>,
) -> Vec<RecordBatch> {
    let source_conn = parse_source(conn, None).unwrap();
    get_arrow(&source_conn, origin_query, queries, None)
        .unwrap()
        .arrow()
        .unwrap()
}

fn read_partitioned_arrow(
    conn: &str,
    query: &CXQuery<String>,
    partition_on: &str,
) -> Vec<RecordBatch> {
    let source_conn = parse_source(conn, None).unwrap();
    let part = PartitionQuery::new(query.as_str(), partition_on, None, None, 2);
    let queries = partition(&part, &source_conn).unwrap();
    assert_eq!(queries.len(), 2);
    get_arrow(&source_conn, Some(query.to_string()), &queries, None)
        .unwrap()
        .arrow()
        .unwrap()
}

fn assert_arrow_equivalent(left: &[RecordBatch], right: &[RecordBatch]) {
    assert_eq!(
        total_rows(left),
        total_rows(right),
        "route row counts should match"
    );

    let left_schema = left
        .first()
        .expect("left route returned no batches")
        .schema();
    let right_schema = right
        .first()
        .expect("right route returned no batches")
        .schema();
    assert_eq!(
        left_schema.fields(),
        right_schema.fields(),
        "route Arrow schemas should match"
    );

    assert_eq!(
        null_counts(left),
        null_counts(right),
        "route null counts should match"
    );
    assert_eq!(canonical_rows(left), canonical_rows(right));
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn null_counts(batches: &[RecordBatch]) -> Vec<usize> {
    let columns = batches
        .first()
        .expect("route returned no batches")
        .num_columns();
    let mut counts = vec![0; columns];
    for batch in batches {
        assert_eq!(batch.num_columns(), columns);
        for (index, count) in counts.iter_mut().enumerate() {
            *count += batch.column(index).null_count();
        }
    }
    counts
}

fn canonical_rows(batches: &[RecordBatch]) -> Vec<Vec<u8>> {
    let mut rows = Vec::new();
    for batch in batches {
        let mut batch_rows = vec![Vec::new(); batch.num_rows()];
        for col in 0..batch.num_columns() {
            append_column_keys(
                batch.schema().field(col).data_type(),
                batch.column(col).as_ref(),
                &mut batch_rows,
            );
        }
        rows.extend(batch_rows);
    }
    rows.sort();
    rows
}

fn append_column_keys(data_type: &DataType, array: &dyn Array, rows: &mut [Vec<u8>]) {
    match data_type {
        DataType::Boolean => append_primitive(rows, array, |array, row| {
            array.downcast_ref::<BooleanArray>().unwrap().value(row) as u8
        }),
        DataType::Int8 => append_primitive(rows, array, |array, row| {
            array.downcast_ref::<Int8Array>().unwrap().value(row)
        }),
        DataType::Int16 => append_primitive(rows, array, |array, row| {
            array.downcast_ref::<Int16Array>().unwrap().value(row)
        }),
        DataType::Int32 => append_primitive(rows, array, |array, row| {
            array.downcast_ref::<Int32Array>().unwrap().value(row)
        }),
        DataType::Int64 => append_primitive(rows, array, |array, row| {
            array.downcast_ref::<Int64Array>().unwrap().value(row)
        }),
        DataType::UInt8 => append_primitive(rows, array, |array, row| {
            array.downcast_ref::<UInt8Array>().unwrap().value(row)
        }),
        DataType::UInt16 => append_primitive(rows, array, |array, row| {
            array.downcast_ref::<UInt16Array>().unwrap().value(row)
        }),
        DataType::UInt32 => append_primitive(rows, array, |array, row| {
            array.downcast_ref::<UInt32Array>().unwrap().value(row)
        }),
        DataType::UInt64 => append_primitive(rows, array, |array, row| {
            array.downcast_ref::<UInt64Array>().unwrap().value(row)
        }),
        DataType::Float32 => append_primitive(rows, array, |array, row| {
            array
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row)
                .to_bits()
        }),
        DataType::Float64 => append_primitive(rows, array, |array, row| {
            array
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row)
                .to_bits()
        }),
        DataType::Decimal128(_, _) => append_primitive(rows, array, |array, row| {
            array.downcast_ref::<Decimal128Array>().unwrap().value(row)
        }),
        DataType::Date32 => append_primitive(rows, array, |array, row| {
            array.downcast_ref::<Date32Array>().unwrap().value(row)
        }),
        DataType::Date64 => append_primitive(rows, array, |array, row| {
            array.downcast_ref::<Date64Array>().unwrap().value(row)
        }),
        DataType::Time32(unit) => match unit {
            arrow::datatypes::TimeUnit::Second => append_primitive(rows, array, |array, row| {
                array
                    .downcast_ref::<Time32SecondArray>()
                    .unwrap()
                    .value(row)
            }),
            arrow::datatypes::TimeUnit::Millisecond => {
                append_primitive(rows, array, |array, row| {
                    array
                        .downcast_ref::<Time32MillisecondArray>()
                        .unwrap()
                        .value(row)
                })
            }
            _ => unreachable!("Time32 only supports second and millisecond units"),
        },
        DataType::Time64(unit) => match unit {
            arrow::datatypes::TimeUnit::Microsecond => {
                append_primitive(rows, array, |array, row| {
                    array
                        .downcast_ref::<Time64MicrosecondArray>()
                        .unwrap()
                        .value(row)
                })
            }
            arrow::datatypes::TimeUnit::Nanosecond => {
                append_primitive(rows, array, |array, row| {
                    array
                        .downcast_ref::<Time64NanosecondArray>()
                        .unwrap()
                        .value(row)
                })
            }
            _ => unreachable!("Time64 only supports microsecond and nanosecond units"),
        },
        DataType::Timestamp(unit, _) => match unit {
            arrow::datatypes::TimeUnit::Second => append_primitive(rows, array, |array, row| {
                array
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap()
                    .value(row)
            }),
            arrow::datatypes::TimeUnit::Millisecond => {
                append_primitive(rows, array, |array, row| {
                    array
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .value(row)
                })
            }
            arrow::datatypes::TimeUnit::Microsecond => {
                append_primitive(rows, array, |array, row| {
                    array
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .value(row)
                })
            }
            arrow::datatypes::TimeUnit::Nanosecond => {
                append_primitive(rows, array, |array, row| {
                    array
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .value(row)
                })
            }
        },
        DataType::Utf8 => append_bytes(rows, array, |array, row| {
            array
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .as_bytes()
        }),
        DataType::LargeUtf8 => append_bytes(rows, array, |array, row| {
            array
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row)
                .as_bytes()
        }),
        DataType::Binary => append_bytes(rows, array, |array, row| {
            array.downcast_ref::<BinaryArray>().unwrap().value(row)
        }),
        DataType::LargeBinary => append_bytes(rows, array, |array, row| {
            array.downcast_ref::<LargeBinaryArray>().unwrap().value(row)
        }),
        other => panic!("unsupported route comparison Arrow type: {:?}", other),
    }
}

fn append_primitive<T: Copy>(
    rows: &mut [Vec<u8>],
    array: &dyn Array,
    value_at: impl Fn(&dyn Array, usize) -> T,
) where
    T: IntoKeyBytes,
{
    for (row, key) in rows.iter_mut().enumerate() {
        append_validity(key, array, row);
        if !array.is_null(row) {
            value_at(array, row).append_key_bytes(key);
        }
    }
}

fn append_bytes<'a>(
    rows: &mut [Vec<u8>],
    array: &'a dyn Array,
    value_at: impl Fn(&'a dyn Array, usize) -> &'a [u8],
) {
    for (row, key) in rows.iter_mut().enumerate() {
        append_validity(key, array, row);
        if !array.is_null(row) {
            let value = value_at(array, row);
            key.extend_from_slice(&(value.len() as u64).to_be_bytes());
            key.extend_from_slice(value);
        }
    }
}

fn append_validity(key: &mut Vec<u8>, array: &dyn Array, row: usize) {
    key.push(u8::from(!array.is_null(row)));
}

trait ArrayDowncast {
    fn downcast_ref<T: 'static>(&self) -> Option<&T>;
}

impl ArrayDowncast for dyn Array + '_ {
    fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.as_any().downcast_ref::<T>()
    }
}

trait IntoKeyBytes {
    fn append_key_bytes(self, key: &mut Vec<u8>);
}

macro_rules! impl_key_bytes {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl IntoKeyBytes for $ty {
                fn append_key_bytes(self, key: &mut Vec<u8>) {
                    key.extend_from_slice(&self.to_be_bytes());
                }
            }
        )+
    };
}

impl_key_bytes!(i8, i16, i32, i64, i128, u8, u16, u32, u64);
