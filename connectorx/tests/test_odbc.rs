#![cfg(all(feature = "src_odbc", feature = "dst_arrow"))]

use std::sync::{Mutex, MutexGuard};

use arrow::{
    array::{
        Array, Decimal128Array, LargeBinaryArray, StringArray, Time64MicrosecondArray,
        TimestampMicrosecondArray,
    },
    util::display::array_value_to_string,
};
use chrono::NaiveDateTime;
use connectorx::{
    destinations::arrow::ArrowDestination,
    get_arrow::get_arrow,
    partition::{partition, PartitionQuery},
    prelude::*,
    sources::odbc::{odbc_conn_string, OdbcSource},
    sql::CXQuery,
    transports::OdbcArrowTransport,
};

mod test_db;

static ODBC_MAX_STR_LEN_LOCK: Mutex<()> = Mutex::new(());

fn lock_odbc_env() -> MutexGuard<'static, ()> {
    ODBC_MAX_STR_LEN_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(previous) = &self.previous {
            std::env::set_var(self.key, previous);
        } else {
            std::env::remove_var(self.key);
        }
    }
}

fn use_postgres_testcontainer() -> bool {
    std::env::var("CONNECTORX_ODBC_TESTCONTAINER").is_ok()
}

fn init_postgres_testcontainer() {
    test_db::postgres_odbc_url();
}

fn odbc_conn() -> Option<String> {
    if use_postgres_testcontainer() {
        return Some(test_db::postgres_odbc_conn());
    }
    std::env::var("ODBC_CONN").ok()
}

fn odbc_url() -> Option<String> {
    if use_postgres_testcontainer() {
        return Some(test_db::postgres_odbc_url());
    }
    std::env::var("ODBC_URL").ok()
}

fn odbc_query() -> Option<CXQuery<String>> {
    if use_postgres_testcontainer() {
        init_postgres_testcontainer();
    }
    std::env::var("ODBC_TEST_QUERY").ok().map(CXQuery::naked)
}

fn odbc_partition_query() -> Option<(String, String)> {
    if use_postgres_testcontainer() {
        init_postgres_testcontainer();
    }
    Some((
        std::env::var("ODBC_PARTITION_QUERY").ok()?,
        std::env::var("ODBC_PARTITION_COLUMN").ok()?,
    ))
}

fn assert_expected_rows(batches: &[arrow::record_batch::RecordBatch]) {
    if let Ok(expected) = std::env::var("ODBC_EXPECTED_ROWS") {
        let expected = expected.parse::<usize>().unwrap();
        let actual = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(actual, expected);
    }
}

fn assert_postgres_testcontainer_rows(batches: &[arrow::record_batch::RecordBatch]) {
    if batches.iter().any(|batch| batch.num_columns() < 3) {
        eprintln!("CONNECTORX_SKIP: skipping default PostgreSQL row assertion: query returned fewer than 3 columns");
        return;
    }

    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push((
                array_value_to_string(batch.column(0).as_ref(), row).unwrap(),
                array_value_to_string(batch.column(2).as_ref(), row).unwrap(),
            ));
        }
    }

    assert_eq!(
        rows,
        vec![
            ("1".to_string(), "alpha".to_string()),
            ("2".to_string(), "beta".to_string())
        ]
    );
}

#[test]
fn test_odbc_url_to_odbc_conn_string_escapes_values() {
    let conn = odbc_conn_string(
        "odbc://user%3Bname:pa%3Dss%7Dword@example.com:1234/db%3Bname?driver=My%7DDriver&ApplicationIntent=ReadOnly%3BStrict&server_key=Hostname",
    )
    .unwrap();

    assert_eq!(
        conn,
        "Driver={My}}Driver};Hostname=example.com;Port=1234;Database={db;name};UID={user;name};PWD={pa=ss}}word};ApplicationIntent={ReadOnly;Strict};"
    );
}

#[test]
fn test_odbc_url_to_odbc_conn_string_supports_dsn() {
    let conn = odbc_conn_string("odbc://user:pass@example.com/db?dsn=Warehouse").unwrap();

    assert_eq!(
        conn,
        "DSN=Warehouse;Server=example.com;Database=db;UID=user;PWD=pass;"
    );
}

#[test]
fn test_odbc_url_to_odbc_conn_string_requires_driver_or_dsn() {
    assert!(odbc_conn_string("odbc://example.com/db").is_err());
}

#[test]
fn test_odbc_url_to_odbc_conn_string_rejects_invalid_keys() {
    assert!(odbc_conn_string("odbc://example.com/db?driver=PostgreSQL&Bad%3BKey=value").is_err());
    assert!(
        odbc_conn_string("odbc://example.com/db?driver=PostgreSQL&server_key=Host%3Dname").is_err()
    );
}

#[test]
fn test_odbc_url_to_odbc_conn_string_keeps_raw_odbc_string() {
    let conn = "Driver={SQLite3};Database=/tmp/test.db;";
    assert_eq!(odbc_conn_string(conn).unwrap(), conn);
}

#[test]
fn test_odbc_arrow_route_with_raw_conn() {
    let _ = env_logger::builder().is_test(true).try_init();
    let _guard = lock_odbc_env();

    let (Some(conn), Some(query)) = (odbc_conn(), odbc_query()) else {
        eprintln!("CONNECTORX_SKIP: skipping ODBC integration test: ODBC_CONN and ODBC_TEST_QUERY are not set");
        return;
    };

    let queries = [query];
    let source = OdbcSource::new(&conn, 1).unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher =
        Dispatcher::<_, _, OdbcArrowTransport>::new(source, &mut destination, &queries, None);
    dispatcher.run().unwrap();

    let batches = destination.arrow().unwrap();
    assert!(!batches.is_empty());
    assert_expected_rows(&batches);
    if use_postgres_testcontainer() {
        assert_postgres_testcontainer_rows(&batches);
    }
}

#[test]
fn test_odbc_get_arrow_route() {
    let _ = env_logger::builder().is_test(true).try_init();
    let _guard = lock_odbc_env();

    let (Some(conn), Some(query)) = (odbc_url(), odbc_query()) else {
        eprintln!("CONNECTORX_SKIP: skipping ODBC get_arrow test: ODBC_URL and ODBC_TEST_QUERY are not set");
        return;
    };

    let source_conn = parse_source(&conn, None).unwrap();
    let destination = get_arrow(&source_conn, None, &[query], None).unwrap();
    let batches = destination.arrow().unwrap();
    assert!(!batches.is_empty());
    assert_expected_rows(&batches);
    if use_postgres_testcontainer() {
        assert_postgres_testcontainer_rows(&batches);
    }
}

#[test]
fn test_odbc_partition_query() {
    let _ = env_logger::builder().is_test(true).try_init();
    let _guard = lock_odbc_env();

    let (Some(conn), Some((query, column))) = (odbc_url(), odbc_partition_query()) else {
        eprintln!(
            "CONNECTORX_SKIP: skipping ODBC partition test: ODBC_URL, ODBC_PARTITION_QUERY, and ODBC_PARTITION_COLUMN are not set"
        );
        return;
    };

    let source_conn = parse_source(&conn, None).unwrap();
    let part = PartitionQuery::new(&query, &column, None, None, 2);
    let queries = partition(&part, &source_conn).unwrap();
    assert_eq!(queries.len(), 2);
}

#[test]
fn test_odbc_testcontainer_edge_types() {
    let _ = env_logger::builder().is_test(true).try_init();
    let _guard = lock_odbc_env();

    if !use_postgres_testcontainer() {
        eprintln!("CONNECTORX_SKIP: skipping ODBC edge type test: CONNECTORX_ODBC_TESTCONTAINER is not set");
        return;
    }

    let conn = test_db::postgres_odbc_url();
    let source_conn = parse_source(&conn, None).unwrap();
    let destination = get_arrow(
        &source_conn,
        None,
        &[CXQuery::naked(
            "select amount, created_at, event_time, payload, wide_text, nullable_text, long_text \
             from cx_odbc_edge order by id",
        )],
        None,
    )
    .unwrap();

    let mut batches = destination.arrow().unwrap();
    assert_eq!(batches.len(), 1);
    let batch = batches.pop().unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 7);

    let amount = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amount.value(0), 1_234_567_000_000);
    assert_eq!(amount.value(1), -90_001_000_000);

    let created_at = batch
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let expected_ts =
        NaiveDateTime::parse_from_str("2024-01-01 12:34:56.123456", "%Y-%m-%d %H:%M:%S%.f")
            .unwrap()
            .and_utc()
            .timestamp_micros();
    assert_eq!(created_at.value(0), expected_ts);

    let event_time = batch
        .column(2)
        .as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap();
    assert_eq!(event_time.value(0), (13 * 3600 + 14 * 60 + 15) * 1_000_000);

    let payload = batch
        .column(3)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert!(payload.eq(&LargeBinaryArray::from(vec![
        Some(&[0x00_u8, 0x01, 0x02, 0xff][..]),
        Some(&b"hello"[..]),
    ])));

    let wide_text = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(wide_text.value(0), "Grüße 東京");

    let nullable_text = batch
        .column(5)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(nullable_text.is_null(0));
    assert_eq!(nullable_text.value(1), "present");

    let long_text = batch
        .column(6)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(long_text.value(0).len(), 64);
}

#[test]
fn test_odbc_testcontainer_errors_on_truncated_text() {
    let _ = env_logger::builder().is_test(true).try_init();

    if !use_postgres_testcontainer() {
        eprintln!("CONNECTORX_SKIP: skipping ODBC truncation test: CONNECTORX_ODBC_TESTCONTAINER is not set");
        return;
    }

    let _guard = lock_odbc_env();
    let conn = test_db::postgres_odbc_url();
    let _env_guard = EnvGuard::set("ODBC_MAX_STR_LEN", "4");

    let source_conn = parse_source(&conn, None).unwrap();
    let destination = get_arrow(
        &source_conn,
        None,
        &[CXQuery::naked(
            "select nullable_text from cx_odbc_edge where id = 1",
        )],
        None,
    )
    .unwrap();
    let mut batches = destination.arrow().unwrap();
    let batch = batches.pop().unwrap();
    let nullable_text = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(nullable_text.is_null(0));

    let source_conn = parse_source(&conn, None).unwrap();
    let result = get_arrow(
        &source_conn,
        None,
        &[CXQuery::naked(
            "select long_text from cx_odbc_edge where id = 1",
        )],
        None,
    );

    let err = match result {
        Ok(_) => panic!("long text should fail when the ODBC text buffer truncates"),
        Err(err) => err,
    };
    let message = err.to_string();
    assert!(message.contains("truncated"), "{}", message);
    assert!(message.contains("ODBC_MAX_STR_LEN"), "{}", message);
}

#[test]
fn test_odbc_testcontainer_streaming_errors_on_truncated_text() {
    let _ = env_logger::builder().is_test(true).try_init();

    if !use_postgres_testcontainer() {
        eprintln!(
            "CONNECTORX_SKIP: skipping ODBC streaming truncation test: CONNECTORX_ODBC_TESTCONTAINER is not set"
        );
        return;
    }

    let _guard = lock_odbc_env();
    let conn = test_db::postgres_odbc_conn();
    let _env_guard = EnvGuard::set("ODBC_MAX_STR_LEN", "4");

    let queries = [CXQuery::naked(
        "select long_text from cx_odbc_edge where id = 1",
    )];
    let source = OdbcSource::new(&conn, 1).unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher =
        Dispatcher::<_, _, OdbcArrowTransport>::new(source, &mut destination, &queries, None);
    let result = dispatcher.run();

    let err = match result {
        Ok(_) => panic!("streaming route should fail when the ODBC text buffer truncates"),
        Err(err) => err,
    };
    let message = err.to_string();
    assert!(message.contains("truncated"), "{}", message);
    assert!(message.contains("ODBC_MAX_STR_LEN"), "{}", message);
}
