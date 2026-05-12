#![cfg(all(feature = "src_sybase", feature = "dst_arrow"))]

use std::convert::TryFrom;

use arrow::{
    array::Array,
    array::{
        BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int64Array,
        LargeBinaryArray, LargeStringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    },
    record_batch::RecordBatch,
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use connectorx::{
    destinations::arrow::ArrowDestination,
    get_arrow::get_arrow,
    partition::{partition, PartitionQuery},
    prelude::*,
    sources::sybase::{sybase_conn_string, SybaseSource, SybaseTypeSystem},
    sql::{count_query, get_partition_range_query, single_col_partition_query, CXQuery},
    transports::SybaseArrowTransport,
};
use odbc_api::{
    buffers::TextRowSet, environment, Connection, ConnectionOptions, Cursor, ResultSetMetadata,
};
use sqlparser::dialect::MsSqlDialect;

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

fn sybase_driver_matrix_conn() -> Option<String> {
    sybase_odbc_conn().or_else(|| sybase_url().and_then(|conn| sybase_conn_string(&conn).ok()))
}

#[derive(Debug)]
struct SybaseDriverMatrixReport {
    driver_keyword: Option<String>,
    tds_version: Option<String>,
    dbms_name: Option<String>,
    server_version: Option<String>,
    columns: Vec<SybaseDriverMatrixColumn>,
    case_errors: Vec<(String, String)>,
}

#[derive(Debug)]
struct SybaseDriverMatrixColumn {
    case_name: String,
    column_name: String,
    odbc_type_code: i16,
    odbc_type: String,
    column_size: Option<usize>,
    decimal_digits: i16,
    nullability: String,
    connectorx_type: String,
    buffer_policy: &'static str,
}

struct SybaseDriverMatrixCase {
    name: &'static str,
    query: &'static str,
}

impl SybaseDriverMatrixReport {
    fn to_markdown(&self) -> String {
        let mut out = String::new();
        out.push_str("# Sybase ODBC driver matrix\n\n");
        out.push_str(&format!(
            "- Driver keyword: {}\n",
            markdown_value(self.driver_keyword.as_deref())
        ));
        out.push_str(&format!(
            "- TDS version keyword: {}\n",
            markdown_value(self.tds_version.as_deref())
        ));
        out.push_str(&format!(
            "- ODBC DBMS name: {}\n",
            markdown_value(self.dbms_name.as_deref())
        ));
        out.push_str(&format!(
            "- Server version query: {}\n\n",
            markdown_value(self.server_version.as_deref())
        ));

        out.push_str("| case | column | ODBC code | ODBC type | size | scale | nullability | ConnectorX type | buffer policy |\n");
        out.push_str("| --- | --- | ---: | --- | ---: | ---: | --- | --- | --- |\n");
        for column in &self.columns {
            out.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                escape_markdown_cell(&column.case_name),
                escape_markdown_cell(&column.column_name),
                column.odbc_type_code,
                escape_markdown_cell(&column.odbc_type),
                column
                    .column_size
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                column.decimal_digits,
                escape_markdown_cell(&column.nullability),
                escape_markdown_cell(&column.connectorx_type),
                escape_markdown_cell(column.buffer_policy),
            ));
        }

        if !self.case_errors.is_empty() {
            out.push_str("\n| skipped case | error |\n");
            out.push_str("| --- | --- |\n");
            for (case, error) in &self.case_errors {
                out.push_str(&format!(
                    "| {} | {} |\n",
                    escape_markdown_cell(case),
                    escape_markdown_cell(error)
                ));
            }
        }

        out
    }
}

fn collect_sybase_driver_matrix(
    conn: &str,
) -> Result<SybaseDriverMatrixReport, Box<dyn std::error::Error>> {
    let env = environment()?;
    let conn_handle = env.connect_with_connection_string(conn, ConnectionOptions::default())?;
    let mut cases = sybase_driver_matrix_cases();
    if use_sybase_testcontainer() {
        cases.extend(sybase_driver_matrix_seeded_cases());
    }

    let mut report = SybaseDriverMatrixReport {
        driver_keyword: odbc_conn_keyword(conn, "Driver"),
        tds_version: odbc_conn_keyword(conn, "TDS_Version"),
        dbms_name: conn_handle.database_management_system_name().ok(),
        server_version: fetch_optional_text_scalar(&conn_handle, "select @@version as version")
            .ok()
            .flatten(),
        columns: Vec::new(),
        case_errors: Vec::new(),
    };

    for case in cases {
        match collect_sybase_driver_matrix_case(&conn_handle, case) {
            Ok(columns) => report.columns.extend(columns),
            Err(error) => report
                .case_errors
                .push((case.name.to_string(), error.to_string())),
        }
    }

    Ok(report)
}

fn sybase_driver_matrix_cases() -> Vec<&'static SybaseDriverMatrixCase> {
    static CASES: &[SybaseDriverMatrixCase] = &[
        SybaseDriverMatrixCase {
            name: "primitive_typed_buffers",
            query: "select convert(tinyint, 7) as tinyint_v, \
                    convert(smallint, -8) as smallint_v, \
                    convert(int, 9) as int_v, \
                    convert(bigint, 10) as bigint_v, \
                    convert(real, 1.25) as real_v, \
                    convert(float, 2.25) as float_v, \
                    convert(bit, 1) as bit_v",
        },
        SybaseDriverMatrixCase {
            name: "money_decimal_text_buffer",
            query: "select convert(money, 123.45) as money_v, \
                    convert(smallmoney, -12.34) as smallmoney_v, \
                    convert(numeric(18, 4), 123.4567) as numeric_v",
        },
        SybaseDriverMatrixCase {
            name: "temporal_text_buffer",
            query: "select convert(date, '2024-02-03') as date_v, \
                    convert(time, '03:04:05') as time_v, \
                    convert(bigtime, '03:04:05.123456') as bigtime_v, \
                    convert(datetime, '2024-02-03 04:05:06.123') as datetime_v, \
                    convert(bigdatetime, '2024-02-03 04:05:06.123456') as bigdatetime_v",
        },
        SybaseDriverMatrixCase {
            name: "binary_hex_text",
            query: "select convert(binary(4), 0x0304beef) as binary_v, \
                    convert(varbinary(4), 0x0304beef) as varbinary_v",
        },
        SybaseDriverMatrixCase {
            name: "unicode_text",
            query: "select convert(char(5), 'xy') as char_v, \
                    convert(varchar(32), 'plain varchar') as varchar_v, \
                    convert(text, 'long text value') as text_v, \
                    convert(unichar(16), N'Grusse') as unichar_v, \
                    convert(univarchar(64), N'Grüße Tokyo') as univarchar_v",
        },
    ];
    CASES.iter().collect()
}

fn sybase_driver_matrix_seeded_cases() -> Vec<&'static SybaseDriverMatrixCase> {
    static CASES: &[SybaseDriverMatrixCase] = &[
        SybaseDriverMatrixCase {
            name: "seeded_image_rowversion",
            query: "select fixed_bytes, variable_bytes, image_bytes, row_version \
                    from dbo.cx_odbc_binary_edge where id = 1",
        },
        SybaseDriverMatrixCase {
            name: "seeded_unitext_cast",
            query: "select long_univarchar_v, \
                    convert(univarchar(128), unitext_v) as unitext_as_univarchar \
                    from dbo.cx_odbc_unicode_edge where id = 1",
        },
    ];
    CASES.iter().collect()
}

fn collect_sybase_driver_matrix_case(
    conn: &Connection<'_>,
    case: &SybaseDriverMatrixCase,
) -> Result<Vec<SybaseDriverMatrixColumn>, Box<dyn std::error::Error>> {
    let Some(mut cursor) = conn.execute(case.query, (), None)? else {
        return Ok(Vec::new());
    };
    let num_cols = u16::try_from(cursor.num_result_cols()?)?;
    let mut columns = Vec::new();

    for column_number in 1..=num_cols {
        let column_name = cursor.col_name(column_number)?;
        let odbc_type = cursor.col_data_type(column_number)?;
        let nullability = cursor.col_nullability(column_number)?;
        let (connectorx_type, buffer_policy) =
            match SybaseTypeSystem::from_odbc(odbc_type, nullability, &column_name, false) {
                Ok(policy) => (
                    format!("{policy:?}"),
                    sybase_driver_matrix_buffer_policy(policy),
                ),
                Err(error) => (format!("unsupported: {error}"), "unsupported"),
            };

        columns.push(SybaseDriverMatrixColumn {
            case_name: case.name.to_string(),
            column_name,
            odbc_type_code: odbc_type.data_type().0,
            odbc_type: format!("{odbc_type:?}"),
            column_size: odbc_type.column_size().map(|value| value.get()),
            decimal_digits: odbc_type.decimal_digits(),
            nullability: format!("{nullability:?}"),
            connectorx_type,
            buffer_policy,
        });
    }

    Ok(columns)
}

fn sybase_driver_matrix_buffer_policy(ty: SybaseTypeSystem) -> &'static str {
    match ty {
        SybaseTypeSystem::TinyInt(_) => "typed U8 ODBC buffer -> Arrow Int64",
        SybaseTypeSystem::SmallInt(_) => "typed I16 ODBC buffer -> Arrow Int64",
        SybaseTypeSystem::Int(_) => "typed I32 ODBC buffer -> Arrow Int64",
        SybaseTypeSystem::BigInt(_) => "typed I64 ODBC buffer -> Arrow Int64",
        SybaseTypeSystem::Real(_) => "typed F32 ODBC buffer -> Arrow Float32",
        SybaseTypeSystem::Double(_) => "typed F64 ODBC buffer -> Arrow Float64",
        SybaseTypeSystem::Bit(_) => "typed bit ODBC buffer -> Arrow Boolean",
        SybaseTypeSystem::Numeric(..) | SybaseTypeSystem::Decimal(..) => {
            "text ODBC buffer -> Arrow Decimal128"
        }
        SybaseTypeSystem::Char(_) | SybaseTypeSystem::Varchar(_) | SybaseTypeSystem::Text(_) => {
            "text or wide-text ODBC buffer -> Arrow LargeUtf8"
        }
        SybaseTypeSystem::Binary(_) => "text/binary-compatible ODBC buffer -> Arrow LargeBinary",
        SybaseTypeSystem::Date(_) => "text ODBC buffer -> Arrow Date32",
        SybaseTypeSystem::Time(_) => "text ODBC buffer -> Arrow Time64(Microsecond)",
        SybaseTypeSystem::Timestamp(_) => "text ODBC buffer -> Arrow Timestamp(Microsecond)",
    }
}

fn fetch_optional_text_scalar(
    conn: &Connection<'_>,
    query: &str,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let Some(mut cursor) = conn.execute(query, (), None)? else {
        return Ok(None);
    };
    let buffer = TextRowSet::for_cursor(1, &mut cursor, Some(4096))?;
    let mut cursor = cursor.bind_buffer(buffer)?;
    let Some(batch) = cursor.fetch()? else {
        return Ok(None);
    };
    Ok(batch.at_as_str(0, 0)?.map(str::to_string))
}

fn odbc_conn_keyword(conn: &str, keyword: &str) -> Option<String> {
    odbc_conn_pairs(conn)
        .into_iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(keyword))
        .map(|(_, value)| value)
}

fn odbc_conn_pairs(conn: &str) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    let mut start = 0;
    let mut in_braces = false;
    let mut chars = conn.char_indices().peekable();

    while let Some((index, ch)) = chars.next() {
        match ch {
            '{' if !in_braces => in_braces = true,
            '}' if in_braces => {
                if matches!(chars.peek(), Some((_, '}'))) {
                    chars.next();
                } else {
                    in_braces = false;
                }
            }
            ';' if !in_braces => {
                push_odbc_conn_pair(&mut pairs, &conn[start..index]);
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }

    if start < conn.len() {
        push_odbc_conn_pair(&mut pairs, &conn[start..]);
    }

    pairs
}

fn push_odbc_conn_pair(pairs: &mut Vec<(String, String)>, item: &str) {
    let Some((key, value)) = item.split_once('=') else {
        return;
    };
    pairs.push((key.trim().to_string(), unescape_odbc_value(value.trim())));
}

fn unescape_odbc_value(value: &str) -> String {
    if value.starts_with('{') && value.ends_with('}') {
        value[1..value.len() - 1].replace("}}", "}")
    } else {
        value.to_string()
    }
}

fn markdown_value(value: Option<&str>) -> String {
    value
        .map(escape_markdown_cell)
        .unwrap_or_else(|| "unknown".to_string())
}

fn escape_markdown_cell(value: &str) -> String {
    value.replace('\n', " ").replace('|', "\\|")
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

#[test]
fn test_sybase_url_to_odbc_conn_string_rejects_duplicate_params() {
    let err = sybase_conn_string(
        "sybase://sa:sybase@127.0.0.1:5000/tempdb?driver=FreeTDS&Driver=BadDriver",
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("duplicate ODBC URL query parameter"));
    assert!(err.contains("driver"));

    let err = sybase_conn_string(
        "sybase://sa:sybase@127.0.0.1:5000/tempdb?tds_version=5.0&TDS_Version=7.4",
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("tds_version"));
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
    assert_eq!(
        rb.schema().field(2).data_type(),
        &arrow::datatypes::DataType::Decimal128(19, 4)
    );
    assert_eq!(money_v.value(0), 1_234_500);

    let smallmoney_v = rb
        .column(3)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(
        rb.schema().field(3).data_type(),
        &arrow::datatypes::DataType::Decimal128(10, 4)
    );
    assert_eq!(smallmoney_v.value(0), -123_400);

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
fn test_sybase_driver_matrix_metadata_report() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = sybase_driver_matrix_conn() else {
        eprintln!(
            "CONNECTORX_SKIP: skipping Sybase driver matrix metadata test: SYBASE_ODBC_CONN or SYBASE_URL is not set"
        );
        return;
    };

    let report = collect_sybase_driver_matrix(&conn).unwrap();
    eprintln!("{}", report.to_markdown());

    assert!(
        !report.columns.is_empty(),
        "Sybase driver matrix should record at least one ODBC-reported column"
    );
}

#[test]
fn test_sybase_testcontainer_temporal_type_family_and_nulls() {
    let _ = env_logger::builder().is_test(true).try_init();

    if !use_sybase_testcontainer() {
        eprintln!(
            "CONNECTORX_SKIP: skipping Sybase temporal family test: CONNECTORX_SYBASE_TESTCONTAINER is not set"
        );
        return;
    }

    let conn = test_db::sybase_odbc_conn();
    let queries = [CXQuery::naked(
        "select date_v, time_v, datetime_v, smalldatetime_v, bigtime_v, bigdatetime_v \
         from dbo.cx_odbc_temporal_edge order by id",
    )];

    let source = SybaseSource::new(&conn, 1).unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher =
        Dispatcher::<_, _, SybaseArrowTransport>::new(source, &mut destination, &queries, None);
    dispatcher.run().unwrap();

    let mut result = destination.arrow().unwrap();
    assert_eq!(result.len(), 1);
    let rb = result.pop().unwrap();
    assert_eq!(rb.num_rows(), 2);
    assert_eq!(rb.num_columns(), 6);

    let date_v = rb.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
    let expected_date = NaiveDate::from_ymd_opt(2024, 2, 3)
        .unwrap()
        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
    assert_eq!(date_v.value(0), expected_date);
    assert!(date_v.is_null(1));

    let time_v = rb
        .column(1)
        .as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap();
    assert_eq!(
        time_v.value(0),
        NaiveTime::parse_from_str("03:04:05", "%H:%M:%S")
            .unwrap()
            .num_seconds_from_midnight() as i64
            * 1_000_000
    );
    assert!(time_v.is_null(1));

    let datetime_v = rb
        .column(2)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(
        datetime_v.value(0),
        NaiveDateTime::parse_from_str("2024-02-03 04:05:06.123", "%Y-%m-%d %H:%M:%S%.f")
            .unwrap()
            .and_utc()
            .timestamp_micros()
    );
    assert!(datetime_v.is_null(1));

    let smalldatetime_v = rb
        .column(3)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(
        smalldatetime_v.value(0),
        NaiveDateTime::parse_from_str("2024-02-03 04:05:00", "%Y-%m-%d %H:%M:%S")
            .unwrap()
            .and_utc()
            .timestamp_micros()
    );
    assert!(smalldatetime_v.is_null(1));

    let bigtime_v = rb
        .column(4)
        .as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap();
    assert_eq!(
        bigtime_v.value(0),
        NaiveTime::parse_from_str("13:14:15.123456", "%H:%M:%S%.f")
            .unwrap()
            .num_seconds_from_midnight() as i64
            * 1_000_000
            + 123_456
    );
    assert!(bigtime_v.is_null(1));

    let bigdatetime_v = rb
        .column(5)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(
        bigdatetime_v.value(0),
        NaiveDateTime::parse_from_str("2024-02-03 04:05:06.123456", "%Y-%m-%d %H:%M:%S%.f")
            .unwrap()
            .and_utc()
            .timestamp_micros()
    );
    assert!(bigdatetime_v.is_null(1));
}

#[test]
fn test_sybase_testcontainer_timestamp_rowversion_is_binary() {
    let _ = env_logger::builder().is_test(true).try_init();

    if !use_sybase_testcontainer() {
        eprintln!(
            "CONNECTORX_SKIP: skipping Sybase timestamp rowversion test: CONNECTORX_SYBASE_TESTCONTAINER is not set"
        );
        return;
    }

    let conn = test_db::sybase_odbc_conn();
    let queries = [CXQuery::naked(
        "select row_version from dbo.cx_odbc_temporal_edge where id = 1",
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
    assert_eq!(rb.num_columns(), 1);
    assert_eq!(
        rb.schema().field(0).data_type(),
        &arrow::datatypes::DataType::LargeBinary
    );

    let row_version = rb
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert!(!row_version.value(0).is_empty());
}

#[test]
fn test_sybase_testcontainer_binary_image_and_rowversion_transport() {
    let _ = env_logger::builder().is_test(true).try_init();

    if !use_sybase_testcontainer() {
        eprintln!(
            "CONNECTORX_SKIP: skipping Sybase binary/image test: CONNECTORX_SYBASE_TESTCONTAINER is not set"
        );
        return;
    }

    let conn = test_db::sybase_odbc_conn();
    let queries = [CXQuery::naked(
        "select fixed_bytes, variable_bytes, image_bytes, row_version \
         from dbo.cx_odbc_binary_edge order by id",
    )];

    let source = SybaseSource::new(&conn, 1).unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher =
        Dispatcher::<_, _, SybaseArrowTransport>::new(source, &mut destination, &queries, None);
    dispatcher.run().unwrap();

    let mut result = destination.arrow().unwrap();
    assert_eq!(result.len(), 1);
    let rb = result.pop().unwrap();
    assert_sybase_binary_edge_batch(&rb);
}

#[test]
fn test_sybase_testcontainer_binary_image_and_rowversion_fast_path() {
    let _ = env_logger::builder().is_test(true).try_init();

    if !use_sybase_testcontainer() {
        eprintln!(
            "CONNECTORX_SKIP: skipping Sybase binary/image fast-path test: CONNECTORX_SYBASE_TESTCONTAINER is not set"
        );
        return;
    }

    let conn = test_db::sybase_odbc_url();
    let source_conn = parse_source(&conn, None).unwrap();
    let queries = [CXQuery::naked(
        "select fixed_bytes, variable_bytes, image_bytes, row_version \
         from dbo.cx_odbc_binary_edge order by id",
    )];
    let destination = get_arrow(&source_conn, None, &queries, None).unwrap();

    let mut batches = destination.arrow().unwrap();
    assert_eq!(batches.len(), 1);
    let rb = batches.pop().unwrap();
    assert_sybase_binary_edge_batch(&rb);
}

#[test]
fn test_sybase_testcontainer_unicode_text_transport() {
    let _ = env_logger::builder().is_test(true).try_init();

    if !use_sybase_testcontainer() {
        eprintln!(
            "CONNECTORX_SKIP: skipping Sybase Unicode text transport test: CONNECTORX_SYBASE_TESTCONTAINER is not set"
        );
        return;
    }

    let conn = test_db::sybase_odbc_conn();
    let queries = [sybase_unicode_edge_query()];

    let source = SybaseSource::new(&conn, 1).unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher =
        Dispatcher::<_, _, SybaseArrowTransport>::new(source, &mut destination, &queries, None);
    dispatcher.run().unwrap();

    let mut result = destination.arrow().unwrap();
    assert_eq!(result.len(), 1);
    let rb = result.pop().unwrap();
    assert_sybase_unicode_edge_batch(&rb);
}

#[test]
fn test_sybase_testcontainer_unicode_text_fast_path() {
    let _ = env_logger::builder().is_test(true).try_init();

    if !use_sybase_testcontainer() {
        eprintln!(
            "CONNECTORX_SKIP: skipping Sybase Unicode text fast-path test: CONNECTORX_SYBASE_TESTCONTAINER is not set"
        );
        return;
    }

    let conn = test_db::sybase_odbc_url();
    let source_conn = parse_source(&conn, None).unwrap();
    let queries = [sybase_unicode_edge_query()];
    let destination = get_arrow(&source_conn, None, &queries, None).unwrap();

    let mut batches = destination.arrow().unwrap();
    assert_eq!(batches.len(), 1);
    let rb = batches.pop().unwrap();
    assert_sybase_unicode_edge_batch(&rb);
}

#[cfg(feature = "src_odbc")]
#[test]
fn test_sybase_testcontainer_unicode_text_generic_odbc_fast_path() {
    let _ = env_logger::builder().is_test(true).try_init();

    if !use_sybase_testcontainer() {
        eprintln!(
            "CONNECTORX_SKIP: skipping Sybase generic ODBC Unicode text test: CONNECTORX_SYBASE_TESTCONTAINER is not set"
        );
        return;
    }

    let conn = test_db::sybase_odbc_conn();
    let source_conn = parse_source(&conn, None).unwrap();
    let queries = [sybase_unicode_edge_query()];
    let destination = get_arrow(&source_conn, None, &queries, None).unwrap();

    let mut batches = destination.arrow().unwrap();
    assert_eq!(batches.len(), 1);
    let rb = batches.pop().unwrap();
    assert_sybase_unicode_edge_batch(&rb);
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

    let result = destination.arrow().unwrap();
    assert_eq!(
        result[0].schema().field(2).data_type(),
        &arrow::datatypes::DataType::LargeUtf8
    );
    verify_arrow_results(result);
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

#[test]
fn test_sybase_query_wrapping_sql_shapes() {
    let query = sybase_partition_edge_query();

    let count_sql = count_query(&query, &MsSqlDialect {}).unwrap().to_string();
    assert!(count_sql.contains("SELECT count(*) FROM ("));
    assert!(count_sql.contains("TOP (4)"));
    assert!(count_sql.contains("[TradeId]"));
    assert!(count_sql.contains("[select]"));
    assert!(count_sql.contains("convert(datetime"));
    assert!(count_sql.contains("dbo.cx_odbc_partition_edge"));
    assert!(!count_sql.contains("ORDER BY"));

    let range_sql = get_partition_range_query(query.as_str(), "TradeId", &MsSqlDialect {}).unwrap();
    assert!(range_sql.contains("SELECT min(CXTMPTAB_RANGE.TradeId), max(CXTMPTAB_RANGE.TradeId)"));
    assert!(range_sql.contains("FROM ("));
    assert!(range_sql.contains("TOP (4)"));
    assert!(!range_sql.contains("ORDER BY"));

    let part_sql =
        single_col_partition_query(query.as_str(), "TradeId", 1, 3, &MsSqlDialect {}).unwrap();
    assert!(part_sql.contains("SELECT * FROM ("));
    assert!(part_sql.contains("TOP (4)"));
    assert!(part_sql.contains("ORDER BY [TradeId]"));
    assert!(part_sql.contains("1 <= CXTMPTAB_PART.TradeId"));
    assert!(part_sql.contains("CXTMPTAB_PART.TradeId < 3"));
}

#[test]
fn test_sybase_testcontainer_query_wrapping_count_range_and_partition() {
    let _ = env_logger::builder().is_test(true).try_init();

    if !use_sybase_testcontainer() {
        eprintln!(
            "CONNECTORX_SKIP: skipping Sybase query wrapping test: CONNECTORX_SYBASE_TESTCONTAINER is not set"
        );
        return;
    }

    let conn = test_db::sybase_odbc_url();
    let source_conn = parse_source(&conn, None).unwrap();
    let query = sybase_partition_edge_query();

    let count = count_query(&query, &MsSqlDialect {}).unwrap();
    let destination = get_arrow(&source_conn, None, &[count], None).unwrap();
    assert_single_i64(destination.arrow().unwrap(), 4);

    let range = CXQuery::naked(
        get_partition_range_query(query.as_str(), "TradeId", &MsSqlDialect {}).unwrap(),
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

    let part = PartitionQuery::new(query.as_str(), "TradeId", None, None, 2);
    let queries = partition(&part, &source_conn).unwrap();
    assert_eq!(queries.len(), 2);

    let destination = get_arrow(&source_conn, Some(query.to_string()), &queries, None).unwrap();
    let batches = destination.arrow().unwrap();
    let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert_eq!(rows, 4);
    assert_partition_trade_ids(&batches);
}

#[test]
fn test_sybase_testcontainer_query_wrapping_nested_subquery_partition() {
    let _ = env_logger::builder().is_test(true).try_init();

    if !use_sybase_testcontainer() {
        eprintln!(
            "CONNECTORX_SKIP: skipping Sybase nested partition test: CONNECTORX_SYBASE_TESTCONTAINER is not set"
        );
        return;
    }

    let conn = test_db::sybase_odbc_url();
    let source_conn = parse_source(&conn, None).unwrap();
    let query = CXQuery::naked(
        "select TradeId, trade_label from ( \
             select [TradeId] as TradeId, trade_label \
             from dbo.cx_odbc_partition_edge \
             where [TradeId] is not null \
         ) nested_q where TradeId <= 4 order by TradeId",
    );
    let part = PartitionQuery::new(query.as_str(), "TradeId", None, None, 2);
    let queries = partition(&part, &source_conn).unwrap();
    assert_eq!(queries.len(), 2);

    let destination = get_arrow(&source_conn, Some(query.to_string()), &queries, None).unwrap();
    let batches = destination.arrow().unwrap();
    let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert_eq!(rows, 4);
    assert_partition_trade_ids(&batches);
}

fn sybase_partition_edge_query() -> CXQuery<String> {
    CXQuery::naked(
        "select top 4 [TradeId] as TradeId, [select], trade_label, \
         convert(datetime, cob_date) as cob_date \
         from dbo.cx_odbc_partition_edge \
         where [TradeId] is not null \
         order by [TradeId]",
    )
}

fn sybase_unicode_edge_query() -> CXQuery<String> {
    CXQuery::naked(
        "select varchar_text, text_v, unichar_v, univarchar_v, long_univarchar_v, \
         convert(univarchar(128), unitext_v) as unitext_as_univarchar \
         from dbo.cx_odbc_unicode_edge order by id",
    )
}

fn assert_sybase_binary_edge_batch(rb: &RecordBatch) {
    assert_eq!(rb.num_rows(), 2);
    assert_eq!(rb.num_columns(), 4);
    for index in 0..rb.num_columns() {
        assert_eq!(
            rb.schema().field(index).data_type(),
            &arrow::datatypes::DataType::LargeBinary
        );
    }

    let fixed_bytes = rb
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(
        fixed_bytes.value(0),
        &[0x00_u8, 0x01, 0x02, 0x03, 0x04, 0x05, 0xfe, 0xff]
    );
    assert_eq!(fixed_bytes.value(0).len(), 8);
    assert!(fixed_bytes.is_null(1));

    let variable_bytes = rb
        .column(1)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(variable_bytes.value(0), &[0x10_u8, 0x20, 0x30, 0x40, 0x50]);
    assert_eq!(variable_bytes.value(0).len(), 5);
    assert!(variable_bytes.is_null(1));

    let image_bytes = rb
        .column(2)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(
        image_bytes.value(0),
        &[
            0x00_u8, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0xa0, 0xb0, 0xc0, 0xd0,
            0xe0, 0xff,
        ]
    );
    assert_eq!(image_bytes.value(0).len(), 16);
    assert!(image_bytes.is_null(1));

    let row_version = rb
        .column(3)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(row_version.value(0).len(), 8);
    assert_eq!(row_version.value(1).len(), 8);
}

fn assert_sybase_unicode_edge_batch(rb: &RecordBatch) {
    assert_eq!(rb.num_rows(), 2);
    assert_eq!(rb.num_columns(), 6);
    for index in 0..rb.num_columns() {
        assert_eq!(
            rb.schema().field(index).data_type(),
            &arrow::datatypes::DataType::LargeUtf8
        );
    }

    let varchar_text = rb
        .column(0)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert_eq!(varchar_text.value(0), "plain varchar");
    assert!(varchar_text.is_null(1));

    let text_v = rb
        .column(1)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert_eq!(text_v.value(0).len(), 1200);
    assert!(text_v.value(0).chars().all(|ch| ch == 't'));
    assert!(text_v.is_null(1));

    let unichar_v = rb
        .column(2)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert_eq!(unichar_v.value(0).trim_end(), "Grusse");
    assert!(unichar_v.is_null(1));

    let univarchar_v = rb
        .column(3)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert_eq!(univarchar_v.value(0), "Grüße Tokyo");
    assert!(univarchar_v.is_null(1));

    let long_univarchar_v = rb
        .column(4)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert_eq!(long_univarchar_v.value(0).len(), 1200);
    assert!(long_univarchar_v.value(0).chars().all(|ch| ch == 'u'));
    assert!(long_univarchar_v.is_null(1));

    let unitext_as_univarchar = rb
        .column(5)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert_eq!(unitext_as_univarchar.value(0), "Unitext Grüße Tokyo");
    assert!(unitext_as_univarchar.is_null(1));
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
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .eq(&BooleanArray::from(vec![true, false])));

    assert!(rb
        .column(2)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .eq(&LargeStringArray::from(vec!["alpha", "beta"])));
}

/// Test that `get_arrow` (which routes through `sybase_get_arrow`) preserves the exact
/// Arrow schema precision and scale for NUMERIC/DECIMAL columns.
///
/// This test requires a live Sybase ODBC connection specified via `SYBASE_URL`.
/// It is skipped silently when the environment variable is not set.
#[test]
fn test_sybase_fast_path_decimal_precision_and_scale() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(conn) = sybase_url() else {
        eprintln!("CONNECTORX_SKIP: skipping Sybase fast-path decimal test: SYBASE_URL is not set");
        return;
    };

    // Two NUMERIC columns with different precision/scale, and one DECIMAL.
    let queries = [CXQuery::naked(
        "select convert(numeric(18,4), 123.4567) as n18_4, \
         convert(decimal(18,4), 123.4567) as d18_4",
    )];

    let source_conn = parse_source(&conn, None).unwrap();
    let destination = get_arrow(&source_conn, None, &queries, None).unwrap();
    let mut batches = destination.arrow().unwrap();
    assert_eq!(batches.len(), 1);
    let rb = batches.pop().unwrap();
    assert_eq!(rb.num_rows(), 1);

    let schema = rb.schema();

    // --- n18_4: NUMERIC(18,4) ---
    assert_eq!(
        schema.field(0).data_type(),
        &arrow::datatypes::DataType::Decimal128(18, 4),
        "n18_4 field should be Decimal128(18, 4)"
    );
    let n18_4 = rb
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(n18_4.value(0), 1_234_567); // 123.4567 * 10^4

    // --- d18_4: DECIMAL(18,4) ---
    assert_eq!(
        schema.field(1).data_type(),
        &arrow::datatypes::DataType::Decimal128(18, 4),
        "d18_4 field should be Decimal128(18, 4)"
    );
    let d18_4 = rb
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(d18_4.value(0), 1_234_567); // 123.4567 * 10^4
}
