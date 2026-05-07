#[cfg(not(all(feature = "src_odbc", feature = "dst_arrow")))]
fn main() {
    eprintln!(
        "Enable src_odbc and dst_arrow to run this benchmark:\n\
         cargo bench -p connectorx --features 'src_odbc dst_arrow fptr' --bench odbc"
    );
}

#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
#[path = "../tests/test_db.rs"]
mod test_db;

#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
use connectorx::{get_arrow::get_arrow, prelude::parse_source, sql::CXQuery};
#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
const DEFAULT_BENCH_ROWS: u64 = 100_000;

#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
fn odbc_url() -> Option<String> {
    std::env::var("ODBC_URL").ok().or_else(|| {
        std::env::var("CONNECTORX_ODBC_TESTCONTAINER")
            .is_ok()
            .then(|| {
                test_db::postgres_odbc_conn();
                test_db::postgres_odbc_url()
            })
    })
}

#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
fn bench_rows() -> u64 {
    std::env::var("ODBC_BENCH_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_BENCH_ROWS)
}

#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
fn batch_sizes() -> Vec<usize> {
    std::env::var("ODBC_BENCH_BATCH_SIZES")
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| part.trim().parse().ok())
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| vec![1024, 4096, 8192, 16384])
}

#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
fn benchmark_cases(rows: u64) -> Vec<(&'static str, String)> {
    match std::env::var("ODBC_BENCH_QUERY") {
        Ok(query) => vec![("custom", query)],
        Err(_) => vec![
            (
                "primitive",
                format!(
                    "select id, flag, int_v, bigint_v, real_v, double_v \
                     from cx_odbc_perf where id <= {rows}"
                ),
            ),
            (
                "mixed",
                format!(
                    "select id, flag, amount, name, payload, created_at \
                     from cx_odbc_perf where id <= {rows}"
                ),
            ),
        ],
    }
}

#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
fn bench_odbc(c: &mut Criterion) {
    let Some(conn) = odbc_url() else {
        eprintln!(
            "skipping generic ODBC benchmark: set ODBC_URL or CONNECTORX_ODBC_TESTCONTAINER=1"
        );
        return;
    };

    let rows = bench_rows();
    let source_conn = parse_source(&conn, None).expect("parse ODBC_URL");
    let mut group = c.benchmark_group("odbc");
    group.throughput(Throughput::Elements(rows));

    for batch_size in batch_sizes() {
        std::env::set_var("ODBC_BATCH_SIZE", batch_size.to_string());
        for (name, query) in benchmark_cases(rows) {
            let queries = [CXQuery::naked(query)];
            group.bench_with_input(
                BenchmarkId::new(name, batch_size),
                &queries,
                |b, queries| {
                    b.iter(|| {
                        let destination =
                            get_arrow(black_box(&source_conn), None, black_box(queries), None)
                                .expect("run generic ODBC benchmark query");
                        black_box(destination.arrow().expect("collect Arrow batches"));
                    });
                },
            );
        }
    }

    group.finish();
}

#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
criterion_group!(benches, bench_odbc);
#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
criterion_main!(benches);
