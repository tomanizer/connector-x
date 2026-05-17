# Benchmarks

ConnectorX includes historical TPC-H benchmarks and a newer ODBC driver comparison benchmark for the ODBC-backed database paths.

## ODBC Driver Comparison

Use `scripts/odbc_driver_comparison.py` to compare end-to-end DataFrame reads across PostgreSQL, IBM Db2, and Sybase ASE. The benchmark compares:

* pandas over normal ODBC drivers through `pyodbc`
* pandas over SQLAlchemy when a database-specific SQLAlchemy URL is configured
* Polars over `pyodbc`
* Polars over `arrow-odbc`
* ConnectorX native routes returning Polars
* ConnectorX generic `odbc://` routes returning Polars

The benchmark executes each route in a child process. This keeps imports, previous DataFrames, and allocator state from contaminating memory numbers for later routes.

### Dependencies

Install the Python packages needed for the routes you want to benchmark:

```bash
python -m pip install pandas polars pyarrow pyodbc sqlalchemy arrow-odbc psutil
```

Build or install ConnectorX from the current checkout before running ConnectorX routes. For local development, use the same Python environment you use for ConnectorX tests.

### Connection Variables

Configure at least two routes per backend to get meaningful comparisons.

PostgreSQL:

```bash
export POSTGRES_URL="postgresql://connectorx:connectorx@127.0.0.1:5432/connectorx"
export POSTGRES_ODBC_CONN="Driver={PostgreSQL Unicode};Server=127.0.0.1;Port=5432;Database=connectorx;UID=connectorx;PWD=connectorx;"
export POSTGRES_GENERIC_ODBC_URL="odbc://connectorx:connectorx@127.0.0.1:5432/connectorx?driver=PostgreSQL%20Unicode"
```

Db2:

```bash
export DB2_URL="db2://db2inst1:password@127.0.0.1:50000/testdb?driver=IBM%20DB2%20ODBC%20DRIVER"
export DB2_ODBC_CONN="Driver={IBM DB2 ODBC DRIVER};Hostname=127.0.0.1;Port=50000;Protocol=TCPIP;Database=testdb;UID=db2inst1;PWD=password;"
export DB2_GENERIC_ODBC_URL="odbc://localhost/?odbc_connect=..."
```

Sybase:

```bash
export SYBASE_URL="sybase://sa:myPassword@127.0.0.1:5000/tempdb?driver=FreeTDS&tds_version=5.0&charset=UTF-8"
export SYBASE_ODBC_CONN="Driver={FreeTDS};Server=127.0.0.1;Port=5000;TDS_Version=5.0;Database=tempdb;UID=sa;PWD=myPassword;charset=UTF-8;"
export SYBASE_GENERIC_ODBC_URL="odbc://localhost/?odbc_connect=..."
```

Optional SQLAlchemy routes run only when a SQLAlchemy URL is explicitly configured:

```bash
export POSTGRES_SQLALCHEMY_URL="postgresql+psycopg2://connectorx:connectorx@127.0.0.1:5432/connectorx"
export DB2_SQLALCHEMY_URL="db2+ibm_db://db2inst1:password@127.0.0.1:50000/testdb"
```

There is no single portable SQLAlchemy ODBC URL that works well across all three databases. For cross-database ODBC baselines, prefer the `pyodbc` and `arrow-odbc` routes.

Generic ConnectorX ODBC URLs can be built either with URL-style driver fields or with a URL-encoded `odbc_connect` value.

On macOS, IBM's registered Db2 CLI driver may point at `libdb2.dylib`. That works for `pyodbc`, but Arrow-based readers can fail with an ODBC SQLLEN size mismatch. Use the Linux/Intel runner below to remove host-library drift from the Python baselines. Current IBM standalone `clidriver` packages expose `libdb2.so`; ConnectorX Db2 routes still need either a driver exposing the expected SQLLEN ABI, such as IBM's `libdb2o` where available, or a ConnectorX compatibility fix before they can produce authoritative Db2 ConnectorX timings.

### Containerized Linux Runner

Use the containerized benchmark runner when local ODBC driver libraries are not representative, especially for Db2 on macOS. The compose stack starts PostgreSQL, Sybase ASE, Db2, and a Linux x86_64 benchmark runner with unixODBC, psqlODBC, FreeTDS, and IBM's standalone Db2 `clidriver` from the `ibm_db` wheel registered inside the runner image.

```bash
just odbc-driver-comparison-container-smoke
just odbc-driver-comparison-container
```

The default container run prepares and reads 10,000 rows with three measured iterations and one warmup:

```bash
CX_DRIVER_COMPARE_PREPARE_ROWS=100000 \
CX_DRIVER_COMPARE_ROWS=100000 \
CX_DRIVER_COMPARE_ITERATIONS=5 \
just odbc-driver-comparison-container
```

Pass benchmark arguments after the recipe name to override the default matrix:

```bash
just odbc-driver-comparison-container --backend db2 --rows 100000 --iterations 5
```

Reports are written to `target/odbc-driver-comparison-container/` on the host. The runner image is rebuilt by the `just` target so the current checkout is copied into the Linux image before the benchmark runs. By default the runner builds the Python package with its normal feature set; set `CX_BENCH_CONTAINER_MATURIN_FEATURES` only when you need to test a custom `maturin --no-default-features --features ...` matrix. Set `CX_BENCH_CONTAINER_SKIP_BUILD=1` only when the image already contains a compatible built extension.

The bundled Sybase image uses `tempdb`; large synthetic loads can exhaust its log. For publication-sized Sybase runs, use a Sybase container or external ASE instance with a larger benchmark database and override `SYBASE_URL` and `SYBASE_ODBC_CONN`.

Db2 note: the bundled IBM `clidriver` passes ODBC smoke tests and the `pyodbc`/Polars baselines, but ConnectorX's Rust ODBC path currently reports an SQLLEN ABI mismatch with that `libdb2.so` driver. Container reports run with `--warn-only` by default, so Db2 ConnectorX route errors are recorded instead of hiding the remaining driver compatibility work. PostgreSQL and Sybase ConnectorX routes run normally in this stack.

### Prepared Benchmark Table

For comparable results across databases, prepare the same synthetic table on each backend:

```bash
scripts/odbc_driver_comparison.py \
  --backend postgres \
  --backend db2 \
  --backend sybase \
  --prepare-rows 100000 \
  --rows 100000 \
  --iterations 3 \
  --warmups 1 \
  --warn-only
```

The prepared table is `cx_bench_perf` for PostgreSQL and Db2 and `dbo.cx_bench_perf` for Sybase. The default benchmark cases read:

* `primitive`: integer, bigint, float, and double columns
* `mixed`: primitive columns plus decimal, text, binary, and timestamp columns

Use larger row counts for real performance work:

```bash
scripts/odbc_driver_comparison.py \
  --backend postgres \
  --backend db2 \
  --backend sybase \
  --prepare-rows 1000000 \
  --rows 1000000 \
  --iterations 5 \
  --warmups 1
```

### TPC-H Style Scan

To mirror ConnectorX's historical benchmark shape, load a TPC-H `lineitem` table and include the TPC-H case:

```bash
export TPCH_TABLE=lineitem
export TPCH_PARTITION_ON=l_orderkey

scripts/odbc_driver_comparison.py \
  --backend postgres \
  --backend db2 \
  --backend sybase \
  --include-tpch \
  --case tpch-lineitem \
  --iterations 5 \
  --warmups 1
```

Per-backend table and partition names can be overridden with `POSTGRES_TPCH_TABLE`, `DB2_TPCH_TABLE`, `SYBASE_TPCH_TABLE`, and matching `*_TPCH_PARTITION_ON` variables.

### Custom Workloads

Run one custom query with environment variables:

```bash
export CX_DRIVER_COMPARE_QUERY="select * from cx_bench_perf where id <= 500000"
export CX_DRIVER_COMPARE_PARTITION_ON=id
export CX_DRIVER_COMPARE_PARTITION_RANGE=1,500000

scripts/odbc_driver_comparison.py --backend db2 --iterations 5
```

Run a workload matrix with JSON:

```bash
export CX_DRIVER_COMPARE_CASES_JSON='[
  {
    "name": "narrow",
    "query": "select id, int_v, bigint_v from cx_bench_perf where id <= 1000000",
    "partition_on": "id",
    "partition_range": [1, 1000000],
    "expected_rows": 1000000
  },
  {
    "name": "wide",
    "query": "select * from cx_bench_perf where id <= 1000000",
    "partition_on": "id",
    "partition_range": [1, 1000000],
    "expected_rows": 1000000
  }
]'
```

Backend-specific variants use `POSTGRES_DRIVER_COMPARE_CASES_JSON`, `DB2_DRIVER_COMPARE_CASES_JSON`, or `SYBASE_DRIVER_COMPARE_CASES_JSON`.

### Outputs

By default, outputs are written under `target/odbc-driver-comparison/`:

* raw JSON with route timings, memory, package versions, and correctness summaries
* CSV summary for spreadsheets or plotting
* Markdown report with per-route speedups against `pandas-pyodbc`

Set `CX_DRIVER_COMPARE_OUTPUT_DIR` or pass `--output-dir` to choose a different location.

### Interpreting Results

Use median elapsed time and median rows/sec as the primary performance numbers. The report computes speedup against `pandas-pyodbc` when that route is present.

`polars-arrow-odbc` is the strongest generic ODBC baseline because it keeps data on the Arrow path. `pandas-pyodbc` is the conventional baseline most users recognize, but it is expected to spend more time converting row-oriented ODBC buffers into pandas blocks.

ConnectorX partitioned routes should only be compared with routes that read the same rows. They are intentionally allowed to use multiple database connections because that is a core part of ConnectorX's performance model.

For publication-quality numbers:

* run on an otherwise idle machine
* use local network placement or document cloud latency clearly
* run at least five measured iterations
* avoid tiny tables where connection setup dominates
* report driver manager and ODBC driver versions
* keep raw JSON artifacts with the report
