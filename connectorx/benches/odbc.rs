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
use connectorx::{
    get_arrow::{get_arrow, new_record_batch_iter_result},
    prelude::parse_source,
    sql::CXQuery,
};
#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
const DEFAULT_BENCH_ROWS: u64 = 100_000;
#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
const DEFAULT_PARTITIONS: usize = 4;

#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
struct BenchmarkCase {
    name: &'static str,
    query: String,
    partition_columns: Option<&'static str>,
}

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
fn partition_count() -> usize {
    std::env::var("ODBC_BENCH_PARTITIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_PARTITIONS)
}

#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
fn benchmark_cases(rows: u64) -> Vec<BenchmarkCase> {
    match std::env::var("ODBC_BENCH_QUERY") {
        Ok(query) => vec![BenchmarkCase {
            name: "custom",
            query,
            partition_columns: None,
        }],
        Err(_) => vec![
            BenchmarkCase {
                name: "primitive",
                query: format!(
                    "select id, flag, int_v, bigint_v, real_v, double_v \
                     from cx_odbc_perf where id <= {rows}"
                ),
                partition_columns: Some("id, flag, int_v, bigint_v, real_v, double_v"),
            },
            BenchmarkCase {
                name: "mixed",
                query: format!(
                    "select id, flag, amount, name, payload, payload_bytes, created_at \
                     from cx_odbc_perf where id <= {rows}"
                ),
                partition_columns: Some(
                    "id, flag, amount, name, payload, payload_bytes, created_at",
                ),
            },
        ],
    }
}

#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
fn single_query(query: &str) -> Vec<CXQuery<String>> {
    vec![CXQuery::naked(query)]
}

#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
fn partitioned_queries(columns: &str, rows: u64, partition_count: usize) -> Vec<CXQuery<String>> {
    let partition_count = partition_count.max(1) as u64;
    let chunk = rows.div_ceil(partition_count).max(1);

    (0..partition_count)
        .filter_map(|partition| {
            let min = partition * chunk + 1;
            if min > rows {
                return None;
            }
            let max = (min + chunk - 1).min(rows);
            Some(CXQuery::naked(format!(
                "select {columns} from cx_odbc_perf where id between {min} and {max}"
            )))
        })
        .collect()
}

#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
fn run_arrow_table(source_conn: &connectorx::prelude::SourceConn, queries: &[CXQuery<String>]) {
    let destination = get_arrow(black_box(source_conn), None, black_box(queries), None)
        .expect("run generic ODBC benchmark query");
    let batches = destination.arrow().expect("collect Arrow table batches");
    let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    black_box((batches, rows));
}

#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
fn run_arrow_stream(
    source_conn: &connectorx::prelude::SourceConn,
    queries: &[CXQuery<String>],
    batch_size: usize,
) {
    let mut iter = new_record_batch_iter_result(
        black_box(source_conn),
        None,
        black_box(queries),
        batch_size,
        None,
    )
    .expect("create generic ODBC Arrow stream iterator");

    let _ = iter.get_schema();
    iter.prepare();

    let mut batches = 0usize;
    let mut rows = 0usize;
    loop {
        match iter.next_batch_result() {
            Ok(Some(batch)) => {
                batches += 1;
                rows += batch.num_rows();
                black_box(batch);
            }
            Ok(None) => break,
            Err(error) => panic!("read generic ODBC Arrow stream batch: {}", error),
        }
    }
    black_box((batches, rows));
}

#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
fn bench_query_set(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    source_conn: &connectorx::prelude::SourceConn,
    batch_size: usize,
    case_name: &str,
    partition_name: &str,
    queries: &[CXQuery<String>],
) {
    group.bench_with_input(
        BenchmarkId::new(format!("table/{case_name}/{partition_name}"), batch_size),
        &queries,
        |b, queries| b.iter(|| run_arrow_table(source_conn, queries)),
    );

    group.bench_with_input(
        BenchmarkId::new(format!("stream/{case_name}/{partition_name}"), batch_size),
        &queries,
        |b, queries| b.iter(|| run_arrow_stream(source_conn, queries, batch_size)),
    );
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
    let partitions = partition_count();
    let source_conn = parse_source(&conn, None).expect("parse ODBC_URL");
    let mut group = c.benchmark_group("odbc");
    group.throughput(Throughput::Elements(rows));

    for batch_size in batch_sizes() {
        std::env::set_var("ODBC_BATCH_SIZE", batch_size.to_string());
        for case in benchmark_cases(rows) {
            let queries = single_query(&case.query);
            bench_query_set(
                &mut group,
                &source_conn,
                batch_size,
                case.name,
                "single",
                &queries,
            );

            if partitions > 1 {
                if let Some(columns) = case.partition_columns {
                    let partitioned = partitioned_queries(columns, rows, partitions);
                    bench_query_set(
                        &mut group,
                        &source_conn,
                        batch_size,
                        case.name,
                        &format!("partitioned-{partitions}"),
                        &partitioned,
                    );
                }
            }
        }
    }

    group.finish();
}

#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
criterion_group!(benches, bench_odbc);
#[cfg(all(feature = "src_odbc", feature = "dst_arrow"))]
criterion_main!(benches);
