#![cfg(all(feature = "src_odbc", feature = "dst_arrow"))]

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

    let (Some(conn), Some(query)) = (odbc_conn(), odbc_query()) else {
        eprintln!("skipping ODBC integration test: ODBC_CONN and ODBC_TEST_QUERY are not set");
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
}

#[test]
fn test_odbc_get_arrow_route() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (Some(conn), Some(query)) = (odbc_url(), odbc_query()) else {
        eprintln!("skipping ODBC get_arrow test: ODBC_URL and ODBC_TEST_QUERY are not set");
        return;
    };

    let source_conn = parse_source(&conn, None).unwrap();
    let destination = get_arrow(&source_conn, None, &[query], None).unwrap();
    let batches = destination.arrow().unwrap();
    assert!(!batches.is_empty());
    assert_expected_rows(&batches);
}

#[test]
fn test_odbc_partition_query() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (Some(conn), Some((query, column))) = (odbc_url(), odbc_partition_query()) else {
        eprintln!(
            "skipping ODBC partition test: ODBC_URL, ODBC_PARTITION_QUERY, and ODBC_PARTITION_COLUMN are not set"
        );
        return;
    };

    let source_conn = parse_source(&conn, None).unwrap();
    let part = PartitionQuery::new(&query, &column, None, None, 2);
    let queries = partition(&part, &source_conn).unwrap();
    assert_eq!(queries.len(), 2);
}
