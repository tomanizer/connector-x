# ODBC

## Protocols

* `binary`: ConnectorX uses ODBC block cursors and Arrow transports.

## Connection Strings

Use a raw ODBC connection string when you need exact driver-specific keywords:

```python
import connectorx as cx

conn = "Driver={SQLite3};Database=/tmp/example.db;"
df = cx.read_sql(conn, "select * from example")
table = cx.read_sql(conn, "select * from example", return_type="arrow")
```

For URL-style configuration, use `odbc://` with either `driver=` or `dsn=`:

```python
import connectorx as cx

conn = "odbc://username:password@server:1433/database?driver=ODBC%20Driver%2018%20for%20SQL%20Server"
df = cx.read_sql(conn, "select * from dbo.lineitem")
table = cx.read_sql(conn, "select * from dbo.lineitem", return_type="arrow")
```

ConnectorX expands the URL into an ODBC connection string using `Driver` or `DSN`, `Server`, `Port`, `Database`, `UID`, and `PWD`. Additional URL query parameters are appended to the generated ODBC connection string. Use `server_key=Hostname` when a driver expects `Hostname` instead of `Server`.

All generated ODBC values are escaped when required, including `}` characters. Raw ODBC connection strings starting with `Driver=`, `DSN=`, `FileDSN=`, or `Database=` are passed through unchanged.

ODBC URL query parameter names are decoded and matched case-insensitively. Duplicate query parameter names are rejected with an error instead of using first-wins or last-wins behavior. Generic ODBC first-class URL parameters are `driver`, `dsn`, `server_key`, `odbc_connect`, `replace_invalid_utf16`, `replace_invalid_utf8`, `max_connections`, `login_timeout_secs`, and `query_timeout_secs`; other non-duplicate parameters are passed through to the ODBC driver connection string.

Python users can also build ODBC URLs with `ConnectionUrl`:

```python
from connectorx import ConnectionUrl

conn = ConnectionUrl(
    backend="odbc",
    driver="PostgreSQL Unicode",
    username="connectorx",
    password="connectorx",
    server="127.0.0.1",
    port=5432,
    database="connectorx",
)
```

For DSN-only connections, omit the server fields and pass `dsn=`. If the DSN still needs credentials, pass `username` and `password`; ConnectorX encodes them as `UID` and `PWD` ODBC options:

```python
conn = ConnectionUrl(backend="odbc", dsn="Warehouse DSN")

conn_with_credentials = ConnectionUrl(
    backend="odbc",
    dsn="Warehouse DSN",
    username="connectorx",
    password="connectorx",
)
```

The generic ODBC, Sybase, and Db2 Python paths use the Rust Arrow route:

* `return_type="pandas"` or the default `cx.read_sql(conn, query)` reads a complete Arrow table and converts it to pandas with `table.to_pandas(date_as_object=False, split_blocks=False)`. This path requires both pandas and pyarrow.
* `return_type="arrow"` returns the `pyarrow.Table` directly.
* `return_type="polars"` reads Arrow and converts to a Polars DataFrame.
* `return_type="modin"` and `return_type="dask"` use the same Arrow-to-pandas handoff before wrapping the pandas result.
* `return_type="arrow_stream"` returns a `pyarrow.RecordBatchReader` and is always explicit; ConnectorX does not implicitly materialize an open stream into pandas.

The lower-level row-wise pandas extension path is not supported for ODBC-family sources. Use the public `connectorx.read_sql` wrapper so generic ODBC, Sybase, and Db2 stay on the direct Arrow path.

## Route Selection

ConnectorX has three ODBC-backed routes:

| Database | Preferred route | Use generic `odbc://` when |
| --- | --- | --- |
| IBM Db2 | `db2://...` | you are validating a driver-specific issue, need an exact raw ODBC connection string, or are comparing behavior with another ODBC client. |
| Sybase / SAP ASE | `sybase://...` | you are validating a driver-specific issue, need an exact raw ODBC connection string, or are using an ASE-compatible driver before promoting it to the Sybase route. |
| Other ODBC databases | `odbc://...` or a raw ODBC connection string | this is the normal route for databases without a dedicated ConnectorX source. |

For Db2 and Sybase, prefer the dedicated route in production. The dedicated routes share the same direct Arrow ODBC fetch path as generic `odbc://`, so primitive, decimal, text, binary, and temporal columns are still fetched in ODBC batches and emitted as Arrow arrays without the older row-wise parser path. The dedicated routes add the database-specific policy on top: URL keyword normalization, source-specific timeout/buffer/fallback settings, source-specific unknown-type diagnostics, and the Db2 or Sybase SQL dialect used for count/range/partition query generation.

Arrow schemas are expected to match between a dedicated route and generic `odbc://` when the same ODBC driver reports the same standard metadata and the selected columns are supported by both policies. If a driver reports a vendor-specific type, the dedicated route may produce a source-specific error message or fallback policy while generic `odbc://` reports the generic ODBC policy. Cast vendor-specific columns in the query when exact cross-route schema equality matters.

Partitioning is expected to work on all three routes for queries that ConnectorX can wrap in the route dialect and whose partition column has non-NULL integer min/max bounds. Dedicated `db2://` and `sybase://` should be preferred for partitioned Db2 and ASE reads because they use the matching dialect policy; generic `odbc://` uses the generic ODBC wrapper and is best treated as a portability and comparison route for those databases.

## Runtime Dependencies

ConnectorX links against the platform ODBC manager. The ODBC driver for your database is a runtime dependency and is not bundled in ConnectorX wheels.

* Linux wheels are built against unixODBC. Runtime systems need the unixODBC manager libraries and the target database driver registered with unixODBC.
* macOS wheels are built against Homebrew `unixodbc`. Runtime systems need `unixodbc` and the target database driver installed locally.
* Windows wheels link to the Windows ODBC manager. Runtime systems need the target vendor ODBC driver installed and registered.

Database-specific ODBC drivers are not bundled in ConnectorX wheels. Examples: FreeTDS or SAP ASE SDK for Sybase, IBM Data Server Driver for ODBC and CLI for Db2, and psqlODBC for PostgreSQL-backed generic ODBC tests.

## Type Support

The ODBC-family connectors use one shared fetch and conversion layer. Standard ODBC-reported types are mapped as follows:

| ODBC-reported type | Generic ODBC | Db2 | Sybase |
| --- | --- | --- | --- |
| `SQL_TINYINT` | `u8` | `u8` if reported | `u8` |
| `SQL_SMALLINT` | `i16` | `i16` | `i16` |
| `SQL_INTEGER` | `i32` | `i32` | `i32` |
| `SQL_BIGINT` | `i64` | `i64` | `i64` |
| `SQL_REAL`, `SQL_FLOAT(<=24)` | `f32` | `f32` | `f32` |
| `SQL_DOUBLE`, `SQL_FLOAT(>24)` | `f64` | `f64` | `f64` |
| `SQL_NUMERIC`, `SQL_DECIMAL` | Arrow decimal via text buffer | Arrow decimal via text buffer | Arrow decimal via text buffer |
| `SQL_BIT` | `bool` | `bool` if reported | `bool` |
| char/varchar/long varchar and wide variants | Arrow `LargeUtf8` | Arrow `LargeUtf8` | Arrow `LargeUtf8` |
| binary/varbinary/long varbinary | Arrow large binary | Arrow large binary | Arrow large binary through text-compatible FreeTDS path |
| date/time/timestamp | Arrow date/time/timestamp | Arrow date/time/timestamp | Arrow date/time/timestamp through text-compatible FreeTDS path |
| unknown/vendor-specific | error by default; optional `String` fallback | error by default; optional `String` fallback | error by default; optional `String` fallback, except FreeTDS `TIME2` maps to time |

Nullability reported as unknown is treated as nullable. If a driver reports a value as nullable but later returns `NULL` for a non-null ConnectorX destination type, ConnectorX returns an error instead of fabricating a default.

Automatic partitioning for generic ODBC, Db2, and Sybase requires `MIN(partition_on)` and `MAX(partition_on)` to return non-NULL `i64` integer bounds. Empty strings, SQL `NULL`, decimal values, fractional values, and exponent notation are rejected with a partition-bound error instead of being coerced or truncated. Cast decimal partition columns to a suitable integer expression or pass an explicit `partition_range` only when that conversion is semantically correct.

Vendor-specific ODBC types may be reported as unknown or other. ConnectorX rejects those types by default so driver-specific values are not silently returned as strings. Cast them in the query to a supported standard type when you need a specific output type. For compatibility with older behavior, set the matching opt-in environment variable to `true`: `ODBC_TYPE_FALLBACK_TO_VARCHAR`, `DB2_TYPE_FALLBACK_TO_VARCHAR`, or `SYBASE_TYPE_FALLBACK_TO_VARCHAR`.

Narrow text buffers are decoded as UTF-8. Invalid UTF-8 sequences are rejected by default with an error that includes the source, column name, row index, and byte offset. Add `replace_invalid_utf8=true` to the ODBC, Db2, or Sybase URL only when you explicitly want invalid sequences replaced with U+FFFD. If a driver returns narrow text in a database or process code page, configure the driver/session to return UTF-8 or cast through a Unicode/wide-text path before reading into Arrow.

Wide text buffers are decoded as UTF-16. Invalid UTF-16 sequences are rejected by default with an error that includes the source, column name, row index, and byte offset. Add `replace_invalid_utf16=true` to the ODBC, Db2, or Sybase URL only when you explicitly want invalid sequences replaced with U+FFFD for compatibility with legacy data or driver encoding bugs.

Text, wide text, and binary buffers are checked after every fetch. If the ODBC driver reports that a value was truncated, ConnectorX returns an error that names the relevant max-length setting. Increase the setting or cast/substr the selected column in the query.

## Timeouts

ConnectorX can set ODBC login and statement timeouts for generic ODBC, Db2, and Sybase connections. Use URL options for per-source control:

```python
conn = "odbc://user:password@server/database?driver=PostgreSQL%20Unicode&login_timeout_secs=10&query_timeout_secs=120"
```

`login_timeout_secs` is passed to the ODBC connection attribute before login. `query_timeout_secs` is passed to each statement execution, including metadata, row-count, partition-range, and data-fetch queries. Both values must be positive integers in seconds. They are ConnectorX-only options and are not appended to generated ODBC connection strings.

Timeout enforcement ultimately depends on the ODBC driver. When a driver reports a standard timeout diagnostic such as `HYT00`, `HYT01`, or timeout text, ConnectorX returns a typed timeout error that includes the source name, configured timeout, and query for statement timeouts.

## Performance

The ODBC reader fetches rows in batches and binds primitive, binary, and temporal columns with typed ODBC buffers. Decimal and text columns use text buffers for driver compatibility.

ConnectorX uses the process-wide ODBC environment provided by `odbc-api` and shares it across generic ODBC, Db2, and Sybase connections. Each active query still uses its own ODBC connection, but concurrent ODBC connections are capped per source instance so partitioned reads do not open unbounded connections.

For `return_type="arrow"` and `return_type="arrow_stream"`, `pre_execution_query` statements are executed on each ODBC connection before ConnectorX runs metadata discovery or a partition query fetch. This supports session settings and temp objects used by the selected query. Partition-range discovery runs before the Arrow read path, so provide an explicit `partition_range` when range discovery also depends on session-local pre-execution setup.

For `return_type="arrow_stream"`, ConnectorX uses a bounded producer queue instead of buffering every fetched batch. The queue capacity is two `RecordBatch` values per permitted active ODBC connection, capped at 64 batches, so resident stream memory is driven by `batch_size`, selected column widths, and `max_connections`. Dropping the Python/Rust iterator cancels the stream producers; an in-flight ODBC fetch may still run until the driver returns or `query_timeout_secs` fires.

Tuning environment variables:

* `ODBC_BATCH_SIZE`: rows per ODBC block fetch. Defaults to `1024`. Recommended range is `1024` to `16384`; hard maximum is `65536`.
* `ODBC_MAX_STR_LEN`: maximum bytes bound per cell for ODBC text and binary buffers. Defaults to `1024`. Hard maximum is `67108864` bytes.
* `ODBC_MAX_CONNECTIONS`: maximum active ODBC connections per source instance. Defaults to the number of partition queries, with a minimum of `1`.
* `ODBC_LOGIN_TIMEOUT_SECS`: ODBC login timeout in seconds. Unset by default.
* `ODBC_QUERY_TIMEOUT_SECS`: ODBC statement timeout in seconds. Unset by default.
* `ODBC_TYPE_FALLBACK_TO_VARCHAR`: when `true`, map unknown or vendor-specific ODBC types to `String` instead of returning an error. Defaults to `false`.

`ODBC_BATCH_SIZE * ODBC_MAX_STR_LEN` must not exceed `268435456` bytes, which caps the per-column allocation for variable-width ODBC buffers. If a workload needs very large text, binary, or LOB cells, lower `ODBC_BATCH_SIZE` when raising `ODBC_MAX_STR_LEN`.

For URL-style generic ODBC, `max_connections=N`, `login_timeout_secs=N`, and `query_timeout_secs=N` override the matching environment variables for that source instance and are not passed through to the ODBC driver.

To benchmark the generic ODBC Arrow path against the PostgreSQL testcontainer fixture:

```bash
scripts/odbc_postgres_bench.sh --sample-size 10 --measurement-time 2 --warm-up-time 1
```

Useful benchmark controls:

* `ODBC_BENCH_ROWS`: number of rows read from the seeded benchmark table. Defaults to `100000`.
* `ODBC_BENCH_BATCH_SIZES`: comma-separated `ODBC_BATCH_SIZE` values to compare. Defaults to `1024,4096,8192,16384`.
* `ODBC_BENCH_QUERY`: custom benchmark query. When set, the benchmark runs only that query.

To compare ConnectorX ODBC-family routes against Polars `arrow-odbc`, use the Python correctness benchmark:

```bash
ODBC_URL="odbc://localhost/db?driver=PostgreSQL&server_key=Server&..." \
ODBC_CONN="Driver={PostgreSQL};Server=127.0.0.1;Database=postgres;UID=postgres;PWD=postgres;" \
scripts/odbc_arrow_compare.py --backend odbc
```

For Db2 and Sybase, configure both the dedicated ConnectorX URL and the raw ODBC connection used by `arrow-odbc`:

```bash
DB2_URL="db2://db2inst1:password@127.0.0.1:50000/testdb?driver=IBM%20DB2%20ODBC%20DRIVER" \
DB2_ODBC_CONN="Driver={IBM DB2 ODBC DRIVER};Hostname=127.0.0.1;Port=50000;Protocol=TCPIP;Database=testdb;UID=db2inst1;PWD=password;" \
scripts/odbc_arrow_compare.py --backend db2

SYBASE_URL="sybase://sa:sybase@127.0.0.1:5000/tempdb?driver=FreeTDS&tds_version=5.0" \
SYBASE_ODBC_CONN="Driver={FreeTDS};Server=127.0.0.1;Port=5000;TDS_Version=5.0;UID=sa;PWD=sybase;Database=tempdb;" \
scripts/odbc_arrow_compare.py --backend sybase
```

The script compares ConnectorX dedicated routes, ConnectorX generic `odbc://`, ConnectorX partitioned routes for partitionable cases, and `pl.read_database(..., connection=...)` through `arrow-odbc` where the required connection strings are configured. It reports wall-clock time, rows/sec, peak RSS delta when available, route partition count, schema, null counts, min/max summaries, and row hashes. By default it exits non-zero on correctness mismatches; pass `--warn-only` to keep timings while only warning about mismatches.

Useful controls:

* `CX_ODBC_COMPARE_BACKENDS`: comma-separated `odbc`, `db2`, and/or `sybase` when `--backend` is omitted.
* `CX_ODBC_COMPARE_ITERATIONS` and `CX_ODBC_COMPARE_WARMUPS`: measured and warmup iterations per route.
* `CX_ODBC_COMPARE_PARTITION_NUM`: ConnectorX partition count for partitionable cases. Defaults to `4`.
* `CX_ODBC_COMPARE_QUERY`, `CX_ODBC_COMPARE_PARTITION_ON`, and `CX_ODBC_COMPARE_PARTITION_RANGE`: run one custom query instead of the built-in edge cases.
* `CX_ODBC_COMPARE_CASES_JSON`: JSON array of `{ "name", "query", "partition_on", "partition_range" }` objects for a custom workload matrix.
* `CX_ODBC_COMPARE_ARROW_EXECUTE_OPTIONS_JSON`: JSON object passed as Polars `read_database(..., execute_options=...)` for the `arrow-odbc` route.
* `DB2_GENERIC_ODBC_URL` and `SYBASE_GENERIC_ODBC_URL`: override the generic ConnectorX `odbc://` route. If omitted, the script builds an `odbc_connect` URL from the raw ODBC connection string.

## Testing

For the preferred live test, run PostgreSQL through the Rust testcontainer helper and connect to it through psqlODBC:

```bash
scripts/odbc_postgres_live.sh
```

Prerequisites:

* Docker
* unixODBC
* psqlODBC registered as `PostgreSQL Unicode`

On Ubuntu, install the local ODBC dependencies with:

```bash
sudo apt-get install unixodbc unixodbc-dev odbc-postgresql
```

The same testcontainer-backed path can be run directly with:

```bash
CONNECTORX_ODBC_TESTCONTAINER=1 \
cargo test -p connectorx --no-default-features --features "src_odbc dst_arrow fptr" --test test_odbc
```

Generic ODBC integration tests are also environment-gated and can be pointed at any ODBC backend:

```bash
ODBC_CONN="Driver={SQLite3};Database=/tmp/example.db;" \
ODBC_TEST_QUERY="select 1 as id" \
cargo test -p connectorx --no-default-features --features "src_odbc dst_arrow fptr" --test test_odbc
```

Partition smoke tests additionally use `ODBC_URL`, `ODBC_PARTITION_QUERY`, and `ODBC_PARTITION_COLUMN`.
Set `ODBC_EXPECTED_ROWS` to assert the returned row count for live tests.

## CI And Live Coverage

ODBC-family coverage is split into three explicit categories:

| Coverage kind | Where it runs | Expected result without credentials |
| --- | --- | --- |
| Compile/unit coverage | `connector-rust-ci` for `src_odbc`, `src_db2`, and `src_sybase` | Tests print `CONNECTORX_SKIP:` for env-gated live cases and still pass. The job summary records the platform ODBC manager library and registered driver names reported by the runner where Python is available. |
| PostgreSQL ODBC testcontainer coverage | Ubuntu `connector-rust-ci` and manual `odbc-live` with `backend=postgres` or `backend=both` | Runs without repository secrets, using Docker testcontainers and psqlODBC. |
| Release wheel ODBC runtime smoke | `release` verification on Linux x86_64/aarch64, macOS Intel/ARM, and Windows for each Python wheel | Installs the built wheel, imports `connectorx`, loads the platform ODBC manager, allocates an ODBC environment handle, and uploads JSON metadata with driver discovery output. |
| Package index import smoke | Manual `import-test` across Linux, macOS, and Windows | Installs ConnectorX from the selected package index, imports `read_sql`, runs the same ODBC manager smoke test, and records summary/artifact metadata. |
| Secret-backed live coverage | Manual `odbc-live` with `backend=sybase`, `db2`, `odbc`, or `both` | Fails before tests with a clear error if the selected backend secrets are missing. |

The GitHub Actions job summary records which ODBC-family backends were skipped, exercised through PostgreSQL testcontainers, exercised against secret-backed live drivers, or smoke-tested at the ODBC manager layer. Runtime smoke artifacts are named `odbc-runtime-*` and include the OS, Python version, ODBC manager library candidate that loaded, `SQLAllocHandle(SQL_HANDLE_ENV)` result, and driver discovery output such as `odbcinst -j`/`odbcinst -q -d` on Unix-like runners or `Get-OdbcDriver` on Windows. In raw logs, grep for `CONNECTORX_SKIP:` to find env-gated tests that intentionally skipped, and `ODBC_COVERAGE:` for local `just` live-test output.

Support tiers for driver/platform claims:

| Tier | Meaning |
| --- | --- |
| Tested in no-secret CI | Generic ODBC with PostgreSQL through psqlODBC on Ubuntu, plus ODBC manager linkage smoke on release/import matrices. |
| Tested with repository secrets or manual dispatch | Sybase, Db2, and arbitrary generic ODBC targets configured through the `odbc-live` workflow. |
| Expected but user-validated | macOS and Windows database-specific drivers where CI proves the ODBC manager loads but the target commercial/vendor driver is supplied by the deployment. |

Repository secrets for the manual `odbc-live` workflow:

| Backend | Required secrets |
| --- | --- |
| Sybase | `SYBASE_ODBC_CONN` and/or `SYBASE_URL` |
| Db2 | `DB2_ODBC_CONN` and/or `DB2_URL` |
| Generic ODBC | `ODBC_TEST_QUERY` plus `ODBC_CONN` and/or `ODBC_URL`; optionally `ODBC_EXPECTED_ROWS` |

The manual workflow installs `unixodbc`, FreeTDS for Sybase, and IBM's Linux x64 Db2 ODBC/CLI driver registered as `IBM DB2 ODBC DRIVER`. The PostgreSQL testcontainer path installs `odbc-postgresql` and uses the registered `PostgreSQL Unicode` driver. If you use a different commercial ODBC driver, point the corresponding secret at that registered driver name or absolute driver library path.

Local live-test shortcuts:

```bash
just test-odbc-live postgres
just test-odbc-live sybase
just test-odbc-live db2
just test-odbc-live odbc
just test-odbc-live all
```

The `postgres` target uses the no-secret PostgreSQL ODBC testcontainer path. The other targets use the same environment variables as the manual workflow and print `CONNECTORX_SKIP:` when their credentials are not set.
