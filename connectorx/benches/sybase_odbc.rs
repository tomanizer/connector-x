#[cfg(not(all(feature = "src_sybase", feature = "dst_arrow")))]
fn main() {
    eprintln!(
        "Enable src_sybase and dst_arrow to run this benchmark:\n\
         cargo bench -p connectorx --features 'src_sybase dst_arrow' --bench sybase_odbc"
    );
}

#[cfg(all(feature = "src_sybase", feature = "dst_arrow"))]
use connectorx::{get_arrow::get_arrow, prelude::parse_source, sql::CXQuery};
#[cfg(all(feature = "src_sybase", feature = "dst_arrow"))]
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

#[cfg(all(feature = "src_sybase", feature = "dst_arrow"))]
fn default_query() -> String {
    "select id, small_v, amount, name, created_at, flag from dbo.cx_sybase_test".to_string()
}

#[cfg(all(feature = "src_sybase", feature = "dst_arrow"))]
fn primitive_query() -> String {
    "select id, small_v, flag from dbo.cx_sybase_test".to_string()
}

#[cfg(all(feature = "src_sybase", feature = "dst_arrow"))]
fn benchmark_cases() -> Vec<(&'static str, String)> {
    match std::env::var("SYBASE_BENCH_QUERY") {
        Ok(query) => vec![("odbc_get_arrow", query)],
        Err(_) => vec![
            ("odbc_get_arrow", default_query()),
            ("odbc_get_arrow_primitives", primitive_query()),
        ],
    }
}

#[cfg(all(feature = "src_sybase", feature = "dst_arrow"))]
fn bench_sybase_odbc(c: &mut Criterion) {
    let Ok(conn) = std::env::var("SYBASE_URL") else {
        eprintln!("skipping Sybase ODBC benchmark: SYBASE_URL is not set");
        return;
    };

    let source_conn = parse_source(&conn, None).expect("parse SYBASE_URL");
    let estimated_rows = std::env::var("SYBASE_BENCH_ROWS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok());

    let mut group = c.benchmark_group("sybase");
    if let Some(rows) = estimated_rows {
        group.throughput(Throughput::Elements(rows));
    }

    for (name, query) in benchmark_cases() {
        let queries = [CXQuery::naked(query)];
        group.bench_function(name, |b| {
            b.iter(|| {
                let destination =
                    get_arrow(black_box(&source_conn), None, black_box(&queries), None)
                        .expect("run Sybase ODBC benchmark query");
                black_box(destination.arrow().expect("collect Arrow batches"));
            });
        });
    }
    group.finish();
}

#[cfg(all(feature = "src_sybase", feature = "dst_arrow"))]
criterion_group!(benches, bench_sybase_odbc);
#[cfg(all(feature = "src_sybase", feature = "dst_arrow"))]
criterion_main!(benches);
