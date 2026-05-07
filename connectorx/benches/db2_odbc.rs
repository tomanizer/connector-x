#[cfg(not(all(feature = "src_db2", feature = "dst_arrow")))]
fn main() {
    eprintln!(
        "Enable src_db2 and dst_arrow to run this benchmark:\n\
         cargo bench -p connectorx --features 'src_db2 dst_arrow' --bench db2_odbc"
    );
}

#[cfg(all(feature = "src_db2", feature = "dst_arrow"))]
use connectorx::{get_arrow::get_arrow, prelude::parse_source, sql::CXQuery};
#[cfg(all(feature = "src_db2", feature = "dst_arrow"))]
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

#[cfg(all(feature = "src_db2", feature = "dst_arrow"))]
fn default_query() -> String {
    "select id, small_v, amount, name, created_at, flag from cx_db2_test".to_string()
}

#[cfg(all(feature = "src_db2", feature = "dst_arrow"))]
fn primitive_query() -> String {
    "select id, small_v, flag from cx_db2_test".to_string()
}

#[cfg(all(feature = "src_db2", feature = "dst_arrow"))]
fn benchmark_cases() -> Vec<(&'static str, String)> {
    match std::env::var("DB2_BENCH_QUERY") {
        Ok(query) => vec![("odbc_get_arrow", query)],
        Err(_) => vec![
            ("odbc_get_arrow", default_query()),
            ("odbc_get_arrow_primitives", primitive_query()),
        ],
    }
}

#[cfg(all(feature = "src_db2", feature = "dst_arrow"))]
fn bench_db2_odbc(c: &mut Criterion) {
    let Ok(conn) = std::env::var("DB2_URL") else {
        eprintln!("skipping Db2 ODBC benchmark: DB2_URL is not set");
        return;
    };

    let source_conn = parse_source(&conn, None).expect("parse DB2_URL");
    let estimated_rows = std::env::var("DB2_BENCH_ROWS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok());

    let mut group = c.benchmark_group("db2");
    if let Some(rows) = estimated_rows {
        group.throughput(Throughput::Elements(rows));
    }

    for (name, query) in benchmark_cases() {
        let queries = [CXQuery::naked(query)];
        group.bench_function(name, |b| {
            b.iter(|| {
                let destination =
                    get_arrow(black_box(&source_conn), None, black_box(&queries), None)
                        .expect("run Db2 ODBC benchmark query");
                black_box(destination.arrow().expect("collect Arrow batches"));
            });
        });
    }
    group.finish();
}

#[cfg(all(feature = "src_db2", feature = "dst_arrow"))]
criterion_group!(benches, bench_db2_odbc);
#[cfg(all(feature = "src_db2", feature = "dst_arrow"))]
criterion_main!(benches);
