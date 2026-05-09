#![cfg(all(feature = "src_sybase", feature = "dst_arrow"))]

use arrow::{
    array::Array,
    array::{
        BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int64Array,
        LargeBinaryArray, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    },
    record_batch::RecordBatch,
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use connectorx::{
    destinations::arrow::ArrowDestination,
    get_arrow::get_arrow,
    partition::{partition, PartitionQuery},
    prelude::*,
    sources::sybase::{sybase_conn_string, SybaseSource},
    sql::CXQuery,
    transports::SybaseArrowTransport,
};

mod test_db;

fn use_sybase_testcontainer() -> bool {
    std::env::var("CONNECTORX_SYBASE_TESTCONTAINER").is_ok()
}

fn sybase_odbc_conn() -> Option<String> {
    if use_sybase_testcontainer() {
        return Some(test_db::sybase_odbc_conn());
    }
    std::env::var("SYBASE_ODBC_CONN").ok()
}

fn sybase_url() -> Option<String> {
    if use_sybase_testcontainer() {
        return Some(test_db::sybase_odbc_url());
    }
    std::env::var("SYBASE_URL").ok()
}

#[test]
fn test_sybase_url_to_odbc_conn_string_escapes_values() {
    let conn = sybase_conn_string(
        "sybase://user%3Bname:pa%3Dss%7Dword@example.com:5000/db%3Bname?driver=Free%7DTDS&tds_version=5.0%3Bfoo",
    )
    .unwrap();

    assert_eq!(
        conn,
        "Driver={Free}}TDS};Server={example.com};Port=5000;TDS_Version={5.0;foo};UID={user;name};PWD={pa=ss}}word};Database={db;name};"
    );
}

#[test]
fn test_sybase_url_to_odbc_conn_string_keeps_raw_odbc_string() {
    let conn = "Driver=/opt/libtdsodbc.so;Server=127.0.0.1;Port=5000;UID=sa;PWD=sybase;";
    assert_eq!(sybase_conn_string(conn).unwrap(), conn);
}

#[test]
fn test_sybase_url_to_odbc_conn_string_braces_encoded_driver_path() {
    let conn = sybase_conn_string(
        "sybase://sa:sybase@127.0.0.1:5000/tempdb?driver=%2Fopt%2Fhomebrew%2Flib%2Flibtdsodbc.so",
    )
    .unwrap();

    assert_eq!(
        conn,
        "Driver={/opt/homebrew/lib/libtdsodbc.so};Server={127.0.0.1};Port=5000;TDS_Version={5.0};UID={sa};PWD={sybase};Database={tempdb};"
    );
}

fn basic_type_query() -> CXQuery<String> {
    CXQuery::naked(
        "select convert(int, 1) as id, convert(bit, 1) as flag, convert(varchar(16), 'alpha') as name \
         union all \
         select convert(int, 2) as id, convert(bit, 0) as flag, convert(varchar(16), 'beta') as name",
    )
}

#[test]
fn test_sybase_arrow_basic_types() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = sybase_odbc_conn() else {
        eprintln!("CONNECTORX_SKIP: skipping Sybase integration test: SYBASE_ODBC_CONN is not set");
        return;
    };

    let queries = [basic_type_query()];

    let source = SybaseSource::new(&conn, 1).unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher =
        Dispatcher::<_, _, SybaseArrowTransport>::new(source, &mut destination, &queries, None);
    dispatcher.run().unwrap();

    verify_arrow_results(destination.arrow().unwrap());
}

#[test]
fn test_sybase_arrow_decimal_timestamp() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = sybase_odbc_conn() else {
        eprintln!("CONNECTORX_SKIP: skipping Sybase integration test: SYBASE_ODBC_CONN is not set");
        return;
    };

    let queries = [CXQuery::naked(
        "select convert(numeric(18,4), 123.4567) as amount, \
         convert(datetime, '2024-01-02 03:04:05.123') as created_at",
    )];

    let source = SybaseSource::new(&conn, 1).unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher =
        Dispatcher::<_, _, SybaseArrowTransport>::new(source, &mut destination, &queries, None);
    dispatcher.run().unwrap();

    let mut result = destination.arrow().unwrap();
    assert_eq!(result.len(), 1);
    let rb = result.pop().unwrap();
    assert_eq!(rb.num_rows(), 1);
    assert_eq!(rb.num_columns(), 2);

    let amount = rb
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amount.value(0), 1_234_567_000_000);

    let created_at = rb
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let expected = NaiveDateTime::parse_from_str("2024-01-02 03:04:05.123", "%Y-%m-%d %H:%M:%S%.f")
        .unwrap()
        .and_utc()
        .timestamp_micros();
    assert_eq!(created_at.value(0), expected);
}

#[test]
fn test_sybase_arrow_binary_time_and_nullable_values() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = sybase_odbc_conn() else {
        eprintln!("CONNECTORX_SKIP: skipping Sybase integration test: SYBASE_ODBC_CONN is not set");
        return;
    };

    let queries = [CXQuery::naked(
        "select convert(varbinary(4), 0x0304beef) as bytes_v, \
         convert(bigtime, '03:04:05.123456') as time_v, \
         convert(int, null) as nullable_int",
    )];

    let source = SybaseSource::new(&conn, 1).unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher =
        Dispatcher::<_, _, SybaseArrowTransport>::new(source, &mut destination, &queries, None);
    dispatcher.run().unwrap();

    let mut result = destination.arrow().unwrap();
    assert_eq!(result.len(), 1);
    let rb = result.pop().unwrap();
    assert_eq!(rb.num_rows(), 1);
    assert_eq!(rb.num_columns(), 3);

    assert!(rb
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap()
        .eq(&LargeBinaryArray::from(vec![Some(
            &[0x03_u8, 0x04, 0xbe, 0xef][..]
        )])));

    let time_v = rb
        .column(1)
        .as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap();
    let expected_time = NaiveTime::parse_from_str("03:04:05.123456", "%H:%M:%S%.f")
        .unwrap()
        .num_seconds_from_midnight() as i64
        * 1_000_000
        + 123_456;
    assert_eq!(time_v.value(0), expected_time);

    let nullable_int = rb.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(nullable_int.is_null(0));
}

#[test]
fn test_sybase_arrow_primitive_type_matrix() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = sybase_odbc_conn() else {
        eprintln!("CONNECTORX_SKIP: skipping Sybase integration test: SYBASE_ODBC_CONN is not set");
        return;
    };

    let queries = [CXQuery::naked(
        "select convert(tinyint, 255) as tiny_v, \
         convert(smallint, -123) as small_v, \
         convert(int, 123456) as int_v, \
         convert(bigint, 1234567890123) as big_v, \
         convert(real, 1.5) as real_v, \
         convert(float, 2.25) as double_v, \
         convert(bit, 1) as bit_v",
    )];

    let source = SybaseSource::new(&conn, 1).unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher =
        Dispatcher::<_, _, SybaseArrowTransport>::new(source, &mut destination, &queries, None);
    dispatcher.run().unwrap();

    let mut result = destination.arrow().unwrap();
    assert_eq!(result.len(), 1);
    let rb = result.pop().unwrap();
    assert_eq!(rb.num_rows(), 1);
    assert_eq!(rb.num_columns(), 7);

    assert_eq!(
        rb.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        255
    );
    assert_eq!(
        rb.column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        -123
    );
    assert_eq!(
        rb.column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        123456
    );
    assert_eq!(
        rb.column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1_234_567_890_123
    );
    assert_eq!(
        rb.column(4)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(0),
        1.5
    );
    assert_eq!(
        rb.column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        2.25
    );
    assert!(rb
        .column(6)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(0));
}

#[test]
fn test_sybase_arrow_date_money_and_text_variants() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = sybase_odbc_conn() else {
        eprintln!("CONNECTORX_SKIP: skipping Sybase integration test: SYBASE_ODBC_CONN is not set");
        return;
    };

    let queries = [CXQuery::naked(
        "select convert(date, '2024-02-03') as date_v, \
         convert(smalldatetime, '2024-02-03 04:05') as small_dt_v, \
         convert(money, 123.45) as money_v, \
         convert(smallmoney, -12.34) as smallmoney_v, \
         convert(char(5), 'xy') as char_v, \
         convert(text, 'long text value') as text_v",
    )];

    let source = SybaseSource::new(&conn, 1).unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher =
        Dispatcher::<_, _, SybaseArrowTransport>::new(source, &mut destination, &queries, None);
    dispatcher.run().unwrap();

    let mut result = destination.arrow().unwrap();
    assert_eq!(result.len(), 1);
    let rb = result.pop().unwrap();
    assert_eq!(rb.num_rows(), 1);
    assert_eq!(rb.num_columns(), 6);

    let date_v = rb.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
    let expected_date = NaiveDate::from_ymd_opt(2024, 2, 3)
        .unwrap()
        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
    assert_eq!(date_v.value(0), expected_date);

    let small_dt_v = rb
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let expected_dt = NaiveDateTime::parse_from_str("2024-02-03 04:05:00", "%Y-%m-%d %H:%M:%S")
        .unwrap()
        .and_utc()
        .timestamp_micros();
    assert_eq!(small_dt_v.value(0), expected_dt);

    let money_v = rb
        .column(2)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(money_v.value(0), 1_234_500_000_000);

    let smallmoney_v = rb
        .column(3)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(smallmoney_v.value(0), -123_400_000_000);

    let char_v = rb.column(4).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(char_v.value(0).trim_end(), "xy");

    let text_v = rb.column(5).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(text_v.value(0), "long text value");
}

#[test]
fn test_sybase_testcontainer_time2_and_null_bit() {
    let _ = env_logger::builder().is_test(true).try_init();

    if !use_sybase_testcontainer() {
        eprintln!(
            "CONNECTORX_SKIP: skipping Sybase TIME2/null bit test: CONNECTORX_SYBASE_TESTCONTAINER is not set"
        );
        return;
    }

    let conn = test_db::sybase_odbc_conn();
    let queries = [CXQuery::naked(
        "select convert(bigtime, '03:04:05.123456') as time_v, \
         convert(bit, null) as nullable_bit",
    )];

    let source = SybaseSource::new(&conn, 1).unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher =
        Dispatcher::<_, _, SybaseArrowTransport>::new(source, &mut destination, &queries, None);
    dispatcher.run().unwrap();

    let mut result = destination.arrow().unwrap();
    assert_eq!(result.len(), 1);
    let rb = result.pop().unwrap();
    assert_eq!(rb.num_rows(), 1);
    assert_eq!(rb.num_columns(), 2);

    let time_v = rb
        .column(0)
        .as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap();
    let expected_time = NaiveTime::parse_from_str("03:04:05.123456", "%H:%M:%S%.f")
        .unwrap()
        .num_seconds_from_midnight() as i64
        * 1_000_000
        + 123_456;
    assert_eq!(time_v.value(0), expected_time);

    let nullable_bit = rb
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(nullable_bit.is_null(0));
}

#[test]
fn test_sybase_get_arrow_route() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = sybase_url() else {
        eprintln!("CONNECTORX_SKIP: skipping Sybase get_arrow test: SYBASE_URL is not set");
        return;
    };

    let source_conn = parse_source(&conn, None).unwrap();
    let queries = [basic_type_query()];
    let destination = get_arrow(&source_conn, None, &queries, None).unwrap();

    verify_arrow_results(destination.arrow().unwrap());
}

#[test]
fn test_sybase_partition_query() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = sybase_url() else {
        eprintln!("CONNECTORX_SKIP: skipping Sybase partition test: SYBASE_URL is not set");
        return;
    };

    let source_conn = parse_source(&conn, None).unwrap();
    let query = CXQuery::naked(
        "select convert(int, 1) as id, convert(bit, 1) as flag, convert(varchar(16), 'alpha') as name",
    );
    let part = PartitionQuery::new(query.as_str(), "id", None, None, 1);
    let queries = partition(&part, &source_conn).unwrap();
    assert_eq!(queries.len(), 1);

    let destination = get_arrow(&source_conn, Some(query.to_string()), &queries, None).unwrap();
    let rows = destination
        .arrow()
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    assert_eq!(rows, 1);
}

fn verify_arrow_results(mut result: Vec<RecordBatch>) {
    assert_eq!(result.len(), 1);
    let rb = result.pop().unwrap();
    assert_eq!(rb.num_rows(), 2);
    assert_eq!(rb.num_columns(), 3);

    assert!(rb
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .eq(&Int64Array::from(vec![1, 2])));

    assert!(rb
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .eq(&BooleanArray::from(vec![true, false])));

    assert!(rb
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .eq(&StringArray::from(vec!["alpha", "beta"])));
}
