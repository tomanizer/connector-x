#![cfg(all(feature = "src_db2", feature = "dst_arrow"))]

use arrow::{
    array::Array,
    array::{
        Date32Array, Decimal128Array, Float32Array, Float64Array, Int64Array, LargeBinaryArray,
        LargeStringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    },
    record_batch::RecordBatch,
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use connectorx::{
    destinations::arrow::ArrowDestination,
    get_arrow::get_arrow,
    partition::{partition, PartitionQuery},
    prelude::*,
    sources::db2::{db2_conn_string, Db2Options, Db2Source},
    sql::{count_query, get_partition_range_query, single_col_partition_query, CXQuery},
    transports::Db2ArrowTransport,
};
use sqlparser::dialect::GenericDialect;

mod test_db;

fn use_db2_testcontainer() -> bool {
    std::env::var("CONNECTORX_DB2_TESTCONTAINER").is_ok()
}

fn db2_odbc_conn() -> Option<String> {
    if use_db2_testcontainer() {
        return Some(test_db::db2_odbc_conn());
    }
    std::env::var("DB2_ODBC_CONN").ok()
}

fn db2_url() -> Option<String> {
    if use_db2_testcontainer() {
        return Some(test_db::db2_odbc_url());
    }
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
        "Driver={IBM}}DB2};Hostname=example.com;Port=50000;Protocol={TCPIP;foo};UID={user;name};PWD={pa=ss}}word};Database={db;name};"
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

#[test]
fn test_db2_url_to_odbc_conn_string_rejects_duplicate_params() {
    let err =
        db2_conn_string("db2://user:pass@example.com:50000/db?driver=IBM%20DB2&Driver=BadDriver")
            .unwrap_err()
            .to_string();
    assert!(err.contains("duplicate ODBC URL query parameter"));
    assert!(err.contains("driver"));

    let err = db2_conn_string("db2://user:pass@example.com:50000/db?protocol=TCPIP&Protocol=IPC")
        .unwrap_err()
        .to_string();
    assert!(err.contains("protocol"));
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
        eprintln!("CONNECTORX_SKIP: skipping Db2 integration test: DB2_ODBC_CONN is not set");
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
        eprintln!("CONNECTORX_SKIP: skipping Db2 integration test: DB2_ODBC_CONN is not set");
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
    assert_eq!(
        rb.schema().field(0).data_type(),
        &arrow::datatypes::DataType::Decimal128(18, 4)
    );
    assert_eq!(amount.value(0), 1_234_567);

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
        eprintln!("CONNECTORX_SKIP: skipping Db2 integration test: DB2_ODBC_CONN is not set");
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
        eprintln!("CONNECTORX_SKIP: skipping Db2 integration test: DB2_ODBC_CONN is not set");
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
        eprintln!("CONNECTORX_SKIP: skipping Db2 integration test: DB2_ODBC_CONN is not set");
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
    assert_eq!(
        rb.schema().field(2).data_type(),
        &arrow::datatypes::DataType::Decimal128(18, 2)
    );
    assert_eq!(decimal_v.value(0), 12_345);

    let small_decimal_v = rb
        .column(3)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(
        rb.schema().field(3).data_type(),
        &arrow::datatypes::DataType::Decimal128(9, 2)
    );
    assert_eq!(small_decimal_v.value(0), -1_234);

    let char_v = rb
        .column(4)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert_eq!(char_v.value(0).trim_end(), "xy");

    let text_v = rb
        .column(5)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert_eq!(text_v.value(0), "long text value");
}

#[test]
fn test_db2_testcontainer_vendor_type_fallback_opt_in() {
    let _ = env_logger::builder().is_test(true).try_init();

    if !use_db2_testcontainer() {
        eprintln!(
            "CONNECTORX_SKIP: skipping Db2 vendor type test: CONNECTORX_DB2_TESTCONTAINER is not set"
        );
        return;
    }

    let conn = test_db::db2_odbc_conn();
    let queries = [CXQuery::naked(
        "select decfloat_v, xml_varchar, graphic_varchar from ( \
             select decfloat(123.5) as decfloat_v, \
                    xmlserialize(xmlparse(document '<root>alpha</root>') as varchar(64)) as xml_varchar, \
                    cast(cast('wide' as vargraphic(32)) as varchar(32)) as graphic_varchar \
             from sysibm.sysdummy1 \
         ) q",
    )];

    let source = Db2Source::with_options(
        &conn,
        1,
        Db2Options {
            unknown_type_fallback_to_varchar: true,
            ..Db2Options::default()
        },
    )
    .unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher =
        Dispatcher::<_, _, Db2ArrowTransport>::new(source, &mut destination, &queries, None);
    dispatcher.run().unwrap();

    let mut result = destination.arrow().unwrap();
    assert_eq!(result.len(), 1);
    let rb = result.pop().unwrap();
    assert_eq!(rb.num_rows(), 1);
    assert_eq!(rb.num_columns(), 3);

    let decfloat_v = rb
        .column(0)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert!(decfloat_v.value(0).contains("123.5"));
    let xml_v = rb
        .column(1)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert!(xml_v.value(0).contains("<root>alpha</root>"));
    let graphic_v = rb
        .column(2)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert_eq!(graphic_v.value(0).trim_end(), "wide");
}

#[test]
fn test_db2_testcontainer_type_edge_supported_fast_path() {
    let _ = env_logger::builder().is_test(true).try_init();

    if !use_db2_testcontainer() {
        eprintln!(
            "CONNECTORX_SKIP: skipping Db2 type edge test: CONNECTORX_DB2_TESTCONTAINER is not set"
        );
        return;
    }

    let conn = test_db::db2_odbc_url();
    let source_conn = parse_source(&conn, None).unwrap();
    let queries = [CXQuery::naked(
        "select id, \
                cast(decfloat16_v as varchar(64)) as decfloat16_text, \
                cast(decfloat34_v as varchar(128)) as decfloat34_text, \
                xmlserialize(xml_v as varchar(256)) as xml_text, \
                clob_v, \
                blob_v, \
                graphic_v, \
                vargraphic_v \
         from cx_db2_type_edge \
         order by id",
    )];

    let destination = get_arrow(&source_conn, None, &queries, None).unwrap();
    let mut batches = destination.arrow().unwrap();
    assert_eq!(batches.len(), 1);
    let rb = batches.pop().unwrap();
    assert_eq!(rb.num_rows(), 2);
    assert_eq!(rb.num_columns(), 8);

    let schema = rb.schema();
    assert_eq!(
        schema.field(0).data_type(),
        &arrow::datatypes::DataType::Int64
    );
    for index in [1, 2, 3, 4, 6, 7] {
        assert_eq!(
            schema.field(index).data_type(),
            &arrow::datatypes::DataType::LargeUtf8
        );
        assert_eq!(rb.column(index).null_count(), 1);
    }
    assert_eq!(
        schema.field(5).data_type(),
        &arrow::datatypes::DataType::LargeBinary
    );
    assert_eq!(rb.column(5).null_count(), 1);

    let decfloat16 = rb
        .column(1)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert!(decfloat16.value(0).contains("123.5"));
    assert!(decfloat16.is_null(1));

    let decfloat34 = rb
        .column(2)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert!(decfloat34.value(0).contains("9876543210.123456"));
    assert!(decfloat34.is_null(1));

    let xml = rb
        .column(3)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert!(xml.value(0).contains("<name>alpha</name>"));
    assert!(xml.is_null(1));

    let clob = rb
        .column(4)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert_eq!(clob.value(0).len(), "clob-value-".len() * 64);
    assert!(clob.value(0).starts_with("clob-value-clob-value-"));
    assert!(clob.is_null(1));

    let blob = rb
        .column(5)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(blob.value(0), &[0x00, 0x01, 0x02, 0xff]);
    assert!(blob.is_null(1));

    let graphic = rb
        .column(6)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert_eq!(graphic.value(0).trim_end(), "wide-alpha");
    assert!(graphic.is_null(1));

    let vargraphic = rb
        .column(7)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert_eq!(vargraphic.value(0), "varwide-alpha");
    assert!(vargraphic.is_null(1));
}

#[test]
fn test_db2_testcontainer_known_vendor_types_are_supported_by_default() {
    let _ = env_logger::builder().is_test(true).try_init();

    if !use_db2_testcontainer() {
        eprintln!(
            "CONNECTORX_SKIP: skipping Db2 known vendor type test: CONNECTORX_DB2_TESTCONTAINER is not set"
        );
        return;
    }

    let conn = test_db::db2_odbc_url();
    let source_conn = parse_source(&conn, None).unwrap();
    let queries = [CXQuery::naked(
        "select decfloat16_v, decfloat34_v, xml_v \
         from cx_db2_type_edge \
         where id = 1",
    )];

    let destination = get_arrow(&source_conn, None, &queries, None).unwrap();
    let mut batches = destination.arrow().unwrap();
    assert_eq!(batches.len(), 1);
    let rb = batches.pop().unwrap();
    assert_eq!(rb.num_rows(), 1);

    assert_eq!(
        rb.schema().field(0).data_type(),
        &arrow::datatypes::DataType::LargeUtf8
    );
    assert_eq!(
        rb.schema().field(1).data_type(),
        &arrow::datatypes::DataType::LargeUtf8
    );
    assert_eq!(
        rb.schema().field(2).data_type(),
        &arrow::datatypes::DataType::LargeBinary
    );
}

#[test]
fn test_db2_testcontainer_decfloat_fallback_preserves_strings() {
    let _ = env_logger::builder().is_test(true).try_init();

    if !use_db2_testcontainer() {
        eprintln!(
            "CONNECTORX_SKIP: skipping Db2 DECFLOAT fallback test: CONNECTORX_DB2_TESTCONTAINER is not set"
        );
        return;
    }

    let conn = test_db::db2_odbc_conn();
    let queries = [CXQuery::naked(
        "select decfloat16_v, decfloat34_v \
         from cx_db2_type_edge \
         order by id",
    )];

    let source = Db2Source::with_options(
        &conn,
        1,
        Db2Options {
            unknown_type_fallback_to_varchar: true,
            ..Db2Options::default()
        },
    )
    .unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher =
        Dispatcher::<_, _, Db2ArrowTransport>::new(source, &mut destination, &queries, None);
    dispatcher.run().unwrap();

    let mut result = destination.arrow().unwrap();
    assert_eq!(result.len(), 1);
    let rb = result.pop().unwrap();
    assert_eq!(rb.num_rows(), 2);
    assert_eq!(rb.num_columns(), 2);

    for index in 0..2 {
        assert_eq!(
            rb.schema().field(index).data_type(),
            &arrow::datatypes::DataType::LargeUtf8
        );
        assert_eq!(rb.column(index).null_count(), 1);
    }

    let decfloat16 = rb
        .column(0)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert!(decfloat16.value(0).contains("123.5"));
    assert!(decfloat16.is_null(1));

    let decfloat34 = rb
        .column(1)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert!(decfloat34.value(0).contains("9876543210.123456"));
    assert!(decfloat34.is_null(1));
}

#[test]
fn test_db2_get_arrow_route() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = db2_url() else {
        eprintln!("CONNECTORX_SKIP: skipping Db2 get_arrow test: DB2_URL is not set");
        return;
    };

    let source_conn = parse_source(&conn, None).unwrap();
    let queries = [basic_type_query()];
    let destination = get_arrow(&source_conn, None, &queries, None).unwrap();

    let result = destination.arrow().unwrap();
    assert_eq!(
        result[0].schema().field(2).data_type(),
        &arrow::datatypes::DataType::LargeUtf8
    );
    verify_arrow_results(result);
}

#[test]
fn test_db2_pre_execution_query_failure_is_reported() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = db2_url() else {
        eprintln!("CONNECTORX_SKIP: skipping Db2 pre-execution failure test: DB2_URL is not set");
        return;
    };

    let source_conn = parse_source(&conn, None).unwrap();
    let queries = [CXQuery::naked("select 1 as id from sysibm.sysdummy1")];
    let pre_execution_queries = ["select * from CX_CONNECTORX_PRE_EXEC_MISSING".to_string()];

    let err = match get_arrow(&source_conn, None, &queries, Some(&pre_execution_queries)) {
        Ok(_) => panic!("expected Db2 pre-execution query to fail"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("Db2 pre_execution_query[0]"), "{}", err);
    assert!(err.contains("CX_CONNECTORX_PRE_EXEC_MISSING"), "{}", err);
}

#[test]
fn test_db2_partition_query() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = db2_url() else {
        eprintln!("CONNECTORX_SKIP: skipping Db2 partition test: DB2_URL is not set");
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

#[test]
fn test_db2_query_wrapping_sql_shapes() {
    let query = db2_partition_edge_query();

    let count_sql = count_query(&query, &GenericDialect {}).unwrap().to_string();
    assert!(count_sql.contains("SELECT count(*) FROM ("));
    assert!(count_sql.contains("RISK_SCHEMA.RISK_RESULTS"));
    assert!(count_sql.contains("\"TradeId\""));
    assert!(count_sql.contains("\"select\""));
    assert!(count_sql.contains("date('2026-05-08')"));
    assert!(count_sql.contains("timestamp('2026-05-08 00:00:00')"));
    assert!(count_sql.contains("ORDER BY TRADE_ID"));
    assert!(count_sql.contains("FETCH FIRST 4 ROWS ONLY"));

    let range_sql =
        get_partition_range_query(query.as_str(), "TRADE_ID", &GenericDialect {}).unwrap();
    assert!(range_sql.contains("SELECT min(CXTMPTAB_RANGE.TRADE_ID)"));
    assert!(range_sql.contains("max(CXTMPTAB_RANGE.TRADE_ID)"));
    assert!(range_sql.contains("FROM ("));
    assert!(range_sql.contains("RISK_SCHEMA.RISK_RESULTS"));
    assert!(range_sql.contains("ORDER BY TRADE_ID"));
    assert!(range_sql.contains("FETCH FIRST 4 ROWS ONLY"));

    let part_sql =
        single_col_partition_query(query.as_str(), "TRADE_ID", 1, 3, &GenericDialect {}).unwrap();
    assert!(part_sql.contains("SELECT * FROM ("));
    assert!(part_sql.contains("RISK_SCHEMA.RISK_RESULTS"));
    assert!(part_sql.contains("ORDER BY TRADE_ID"));
    assert!(part_sql.contains("FETCH FIRST 4 ROWS ONLY"));
    assert!(part_sql.contains("1 <= CXTMPTAB_PART.TRADE_ID"));
    assert!(part_sql.contains("CXTMPTAB_PART.TRADE_ID < 3"));

    let offset_query = CXQuery::naked(
        "select TRADE_ID \
         from RISK_SCHEMA.RISK_RESULTS \
         order by TRADE_ID \
         offset 1 rows",
    );
    let offset_part_sql =
        single_col_partition_query(offset_query.as_str(), "TRADE_ID", 1, 3, &GenericDialect {})
            .unwrap();
    assert!(offset_part_sql.contains("ORDER BY TRADE_ID"));
    assert!(offset_part_sql.contains("OFFSET 1 ROWS"));

    let cte = db2_cte_partition_edge_query();
    let cte_count = count_query(&cte, &GenericDialect {}).unwrap().to_string();
    assert!(cte_count.contains("WITH q AS"));
    assert!(cte_count.contains("SELECT count(*) FROM ("));
    assert!(cte_count.contains("RISK_SCHEMA.RISK_RESULTS"));

    let cte_range =
        get_partition_range_query(cte.as_str(), "TRADE_ID", &GenericDialect {}).unwrap();
    assert!(cte_range.contains("WITH q AS"));
    assert!(cte_range.contains("SELECT min(CXTMPTAB_RANGE.TRADE_ID)"));
    assert!(cte_range.contains("FROM ("));
}

#[test]
fn test_db2_testcontainer_query_wrapping_count_range_and_partition() {
    let _ = env_logger::builder().is_test(true).try_init();

    if !use_db2_testcontainer() {
        eprintln!(
            "CONNECTORX_SKIP: skipping Db2 query wrapping test: CONNECTORX_DB2_TESTCONTAINER is not set"
        );
        return;
    }

    let conn = test_db::db2_odbc_url();
    let source_conn = parse_source(&conn, None).unwrap();
    let query = db2_partition_edge_query();

    let count = count_query(&query, &GenericDialect {}).unwrap();
    let destination = get_arrow(&source_conn, None, &[count], None).unwrap();
    assert_single_i64(destination.arrow().unwrap(), 4);

    let range = CXQuery::naked(
        get_partition_range_query(query.as_str(), "TRADE_ID", &GenericDialect {}).unwrap(),
    );
    let destination = get_arrow(&source_conn, None, &[range], None).unwrap();
    let mut batches = destination.arrow().unwrap();
    assert_eq!(batches.len(), 1);
    let rb = batches.pop().unwrap();
    assert_eq!(rb.num_rows(), 1);
    assert_eq!(rb.num_columns(), 2);
    assert_eq!(
        rb.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        rb.column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        4
    );

    let part = PartitionQuery::new(query.as_str(), "TRADE_ID", None, None, 2);
    let queries = partition(&part, &source_conn).unwrap();
    assert_eq!(queries.len(), 2);

    let destination = get_arrow(&source_conn, Some(query.to_string()), &queries, None).unwrap();
    let batches = destination.arrow().unwrap();
    let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert_eq!(rows, 4);
    assert_partition_trade_ids(&batches);
}

#[test]
fn test_db2_testcontainer_query_wrapping_cte_partition() {
    let _ = env_logger::builder().is_test(true).try_init();

    if !use_db2_testcontainer() {
        eprintln!(
            "CONNECTORX_SKIP: skipping Db2 CTE partition test: CONNECTORX_DB2_TESTCONTAINER is not set"
        );
        return;
    }

    let conn = test_db::db2_odbc_url();
    let source_conn = parse_source(&conn, None).unwrap();
    let query = db2_cte_partition_edge_query();

    let count = count_query(&query, &GenericDialect {}).unwrap();
    let destination = get_arrow(&source_conn, None, &[count], None).unwrap();
    assert_single_i64(destination.arrow().unwrap(), 4);

    let part = PartitionQuery::new(query.as_str(), "TRADE_ID", None, None, 2);
    let queries = partition(&part, &source_conn).unwrap();
    assert_eq!(queries.len(), 2);

    let destination = get_arrow(&source_conn, Some(query.to_string()), &queries, None).unwrap();
    let batches = destination.arrow().unwrap();
    let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert_eq!(rows, 4);
    assert_partition_trade_ids(&batches);
}

fn db2_partition_edge_query() -> CXQuery<String> {
    CXQuery::naked(
        "select TRADE_ID, \"TradeId\", \"select\", TRADE_LABEL, COB_DATE, CREATED_TS \
         from RISK_SCHEMA.RISK_RESULTS \
         where COB_DATE = date('2026-05-08') \
           and CREATED_TS >= timestamp('2026-05-08 00:00:00') \
         order by TRADE_ID \
         fetch first 4 rows only",
    )
}

fn db2_cte_partition_edge_query() -> CXQuery<String> {
    CXQuery::naked(
        "with q as ( \
             select TRADE_ID, \"TradeId\", \"select\", TRADE_LABEL, COB_DATE \
             from RISK_SCHEMA.RISK_RESULTS \
         ) \
         select TRADE_ID, \"TradeId\", \"select\", TRADE_LABEL \
         from q \
         where COB_DATE = date('2026-05-08') \
         order by TRADE_ID \
         fetch first 4 rows only",
    )
}

fn assert_single_i64(mut batches: Vec<RecordBatch>, expected: i64) {
    assert_eq!(batches.len(), 1);
    let rb = batches.pop().unwrap();
    assert_eq!(rb.num_rows(), 1);
    assert_eq!(rb.num_columns(), 1);
    assert_eq!(
        rb.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        expected
    );
}

fn assert_partition_trade_ids(batches: &[RecordBatch]) {
    let mut ids = Vec::new();
    for rb in batches {
        let col = rb.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for row in 0..col.len() {
            ids.push(col.value(row));
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3, 4]);
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
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .eq(&LargeStringArray::from(vec!["alpha", "beta"])));
}

/// Test that `get_arrow` (which routes through `db2_get_arrow`) preserves the exact
/// Arrow schema precision and scale for DECIMAL/NUMERIC columns.
///
/// This test requires a live DB2 ODBC connection specified via `DB2_URL`.
/// It is skipped silently when the environment variable is not set.
#[test]
fn test_db2_fast_path_decimal_precision_and_scale() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = db2_url() else {
        eprintln!("CONNECTORX_SKIP: skipping Db2 fast-path decimal test: DB2_URL is not set");
        return;
    };

    // Query two DECIMAL columns with different precision/scale.
    let queries = [CXQuery::naked(
        "select cast(123.4567 as decimal(18,4)) as d18_4, \
         cast(1234567.891011 as decimal(31,6)) as d31_6, \
         cast(99.99 as numeric(15,2)) as n15_2 \
         from sysibm.sysdummy1",
    )];

    let source_conn = parse_source(&conn, None).unwrap();
    let destination = get_arrow(&source_conn, None, &queries, None).unwrap();
    let mut batches = destination.arrow().unwrap();
    assert_eq!(batches.len(), 1);
    let rb = batches.pop().unwrap();
    assert_eq!(rb.num_rows(), 1);

    let schema = rb.schema();

    // --- d18_4: DECIMAL(18,4) ---
    assert_eq!(
        schema.field(0).data_type(),
        &arrow::datatypes::DataType::Decimal128(18, 4),
        "d18_4 field should be Decimal128(18, 4)"
    );
    let d18_4 = rb
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(d18_4.value(0), 1_234_567); // 123.4567 * 10^4

    // --- d31_6: DECIMAL(31,6) ---
    assert_eq!(
        schema.field(1).data_type(),
        &arrow::datatypes::DataType::Decimal128(31, 6),
        "d31_6 field should be Decimal128(31, 6)"
    );
    let d31_6 = rb
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(d31_6.value(0), 1_234_567_891_011); // 1234567.891011 * 10^6

    // --- n15_2: NUMERIC(15,2) ---
    assert_eq!(
        schema.field(2).data_type(),
        &arrow::datatypes::DataType::Decimal128(15, 2),
        "n15_2 field should be Decimal128(15, 2)"
    );
    let n15_2 = rb
        .column(2)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(n15_2.value(0), 9_999); // 99.99 * 10^2
}
