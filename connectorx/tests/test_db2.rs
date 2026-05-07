#![cfg(all(feature = "src_db2", feature = "dst_arrow"))]

use arrow::{
    array::Array,
    array::{
        Date32Array, Decimal128Array, Float32Array, Float64Array, Int64Array, LargeBinaryArray,
        StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    },
    record_batch::RecordBatch,
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use connectorx::{
    destinations::arrow::ArrowDestination,
    get_arrow::get_arrow,
    partition::{partition, PartitionQuery},
    prelude::*,
    sources::db2::{db2_conn_string, Db2Source},
    sql::CXQuery,
    transports::Db2ArrowTransport,
};

fn db2_odbc_conn() -> Option<String> {
    std::env::var("DB2_ODBC_CONN").ok()
}

fn db2_url() -> Option<String> {
    std::env::var("DB2_URL").ok()
}

#[test]
fn test_db2_url_to_odbc_conn_string_escapes_values() {
    let conn = db2_conn_string(
        "db2://user%3Bname:pa%3Dss%7Dword@example.com:50000/db%3Bname?driver=IBM%7DDB2&protocol=TCPIP%3Bfoo",
    )
    .unwrap();

    assert_eq!(
        conn,
        "Driver={IBM}}DB2};Hostname={example.com};Port=50000;Protocol={TCPIP;foo};UID={user;name};PWD={pa=ss}}word};Database={db;name};"
    );
}

#[test]
fn test_db2_url_to_odbc_conn_string_keeps_raw_odbc_string() {
    let conn =
        "Driver={IBM DB2 ODBC DRIVER};Hostname=127.0.0.1;Port=50000;UID=db2inst1;PWD=password;";
    assert_eq!(db2_conn_string(conn).unwrap(), conn);
}

#[test]
fn test_db2_url_to_odbc_conn_string_rejects_invalid_keys() {
    assert!(db2_conn_string(
        "db2://user:pass@example.com:50000/db?driver=IBM%20DB2&Bad%3BKey=value"
    )
    .is_err());
}

fn basic_type_query() -> CXQuery<String> {
    CXQuery::naked(
        "select id, flag, name from ( \
             select cast(1 as integer) as id, cast(1 as smallint) as flag, cast('alpha' as varchar(16)) as name from sysibm.sysdummy1 \
             union all \
             select cast(2 as integer) as id, cast(0 as smallint) as flag, cast('beta' as varchar(16)) as name from sysibm.sysdummy1 \
         ) q order by id",
    )
}

#[test]
fn test_db2_arrow_basic_types() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = db2_odbc_conn() else {
        eprintln!("skipping Db2 integration test: DB2_ODBC_CONN is not set");
        return;
    };

    let queries = [basic_type_query()];

    let source = Db2Source::new(&conn, 1).unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher =
        Dispatcher::<_, _, Db2ArrowTransport>::new(source, &mut destination, &queries, None);
    dispatcher.run().unwrap();

    verify_arrow_results(destination.arrow().unwrap());
}

#[test]
fn test_db2_arrow_decimal_timestamp() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = db2_odbc_conn() else {
        eprintln!("skipping Db2 integration test: DB2_ODBC_CONN is not set");
        return;
    };

    let queries = [CXQuery::naked(
        "select cast(123.4567 as decimal(18,4)) as amount, \
         cast('2024-01-02 03:04:05.123' as timestamp) as created_at \
         from sysibm.sysdummy1",
    )];

    let source = Db2Source::new(&conn, 1).unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher =
        Dispatcher::<_, _, Db2ArrowTransport>::new(source, &mut destination, &queries, None);
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
fn test_db2_arrow_binary_time_and_nullable_values() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = db2_odbc_conn() else {
        eprintln!("skipping Db2 integration test: DB2_ODBC_CONN is not set");
        return;
    };

    let queries = [CXQuery::naked(
        "select cast(X'0304BEEF' as varbinary(4)) as bytes_v, \
         cast('03:04:05' as time) as time_v, \
         cast(null as integer) as nullable_int \
         from sysibm.sysdummy1",
    )];

    let source = Db2Source::new(&conn, 1).unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher =
        Dispatcher::<_, _, Db2ArrowTransport>::new(source, &mut destination, &queries, None);
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
    let expected_time = NaiveTime::parse_from_str("03:04:05", "%H:%M:%S")
        .unwrap()
        .num_seconds_from_midnight() as i64
        * 1_000_000;
    assert_eq!(time_v.value(0), expected_time);

    let nullable_int = rb.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(nullable_int.is_null(0));
}

#[test]
fn test_db2_arrow_primitive_type_matrix() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = db2_odbc_conn() else {
        eprintln!("skipping Db2 integration test: DB2_ODBC_CONN is not set");
        return;
    };

    let queries = [CXQuery::naked(
        "select cast(-123 as smallint) as small_v, \
         cast(123456 as integer) as int_v, \
         cast(1234567890123 as bigint) as big_v, \
         cast(1.5 as real) as real_v, \
         cast(2.25 as double) as double_v \
         from sysibm.sysdummy1",
    )];

    let source = Db2Source::new(&conn, 1).unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher =
        Dispatcher::<_, _, Db2ArrowTransport>::new(source, &mut destination, &queries, None);
    dispatcher.run().unwrap();

    let mut result = destination.arrow().unwrap();
    assert_eq!(result.len(), 1);
    let rb = result.pop().unwrap();
    assert_eq!(rb.num_rows(), 1);
    assert_eq!(rb.num_columns(), 5);

    assert_eq!(
        rb.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        -123
    );
    assert_eq!(
        rb.column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        123456
    );
    assert_eq!(
        rb.column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1_234_567_890_123
    );
    assert_eq!(
        rb.column(3)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(0),
        1.5
    );
    assert_eq!(
        rb.column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        2.25
    );
}

#[test]
fn test_db2_arrow_date_decimal_and_text_variants() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = db2_odbc_conn() else {
        eprintln!("skipping Db2 integration test: DB2_ODBC_CONN is not set");
        return;
    };

    let queries = [CXQuery::naked(
        "select cast('2024-02-03' as date) as date_v, \
         cast('2024-02-03 04:05:00' as timestamp) as timestamp_v, \
         cast(123.45 as decimal(18,2)) as decimal_v, \
         cast(-12.34 as decimal(9,2)) as small_decimal_v, \
         cast('xy' as char(5)) as char_v, \
         cast('long text value' as clob(64)) as text_v \
         from sysibm.sysdummy1",
    )];

    let source = Db2Source::new(&conn, 1).unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher =
        Dispatcher::<_, _, Db2ArrowTransport>::new(source, &mut destination, &queries, None);
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

    let timestamp_v = rb
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let expected_dt = NaiveDateTime::parse_from_str("2024-02-03 04:05:00", "%Y-%m-%d %H:%M:%S")
        .unwrap()
        .and_utc()
        .timestamp_micros();
    assert_eq!(timestamp_v.value(0), expected_dt);

    let decimal_v = rb
        .column(2)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(decimal_v.value(0), 1_234_500_000_000);

    let small_decimal_v = rb
        .column(3)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(small_decimal_v.value(0), -123_400_000_000);

    let char_v = rb.column(4).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(char_v.value(0).trim_end(), "xy");

    let text_v = rb.column(5).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(text_v.value(0), "long text value");
}

#[test]
fn test_db2_get_arrow_route() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = db2_url() else {
        eprintln!("skipping Db2 get_arrow test: DB2_URL is not set");
        return;
    };

    let source_conn = parse_source(&conn, None).unwrap();
    let queries = [basic_type_query()];
    let destination = get_arrow(&source_conn, None, &queries, None).unwrap();

    verify_arrow_results(destination.arrow().unwrap());
}

#[test]
fn test_db2_partition_query() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = db2_url() else {
        eprintln!("skipping Db2 partition test: DB2_URL is not set");
        return;
    };

    let source_conn = parse_source(&conn, None).unwrap();
    let query = CXQuery::naked(
        "select cast(1 as integer) as id, cast(1 as smallint) as flag, cast('alpha' as varchar(16)) as name from sysibm.sysdummy1",
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
        .downcast_ref::<Int64Array>()
        .unwrap()
        .eq(&Int64Array::from(vec![1, 0])));

    assert!(rb
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .eq(&StringArray::from(vec!["alpha", "beta"])));
}
