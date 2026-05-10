# ODBC

## Protocols

* `binary`: ConnectorX uses ODBC block cursors and Arrow transports.

## Connection Strings

Use a raw ODBC connection string when you need exact driver-specific keywords:

```python
import connectorx as cx

conn = "Driver={SQLite3};Database=/tmp/example.db;"
cx.read_sql(conn, "select * from example", return_type="arrow")
```

For URL-style configuration, use `odbc://` with either `driver=` or `dsn=`:

```python
import connectorx as cx

conn = "odbc://username:password@server:1433/database?driver=ODBC%20Driver%2018%20for%20SQL%20Server"
cx.read_sql(conn, "select * from dbo.lineitem", return_type="arrow")
```

ConnectorX expands the URL into an ODBC connection string using `Driver` or `DSN`, `Server`, `Port`, `Database`, `UID`, and `PWD`. Additional URL query parameters are appended to the generated ODBC connection string. Use `server_key=Hostname` when a driver expects `Hostname` instead of `Server`.

All generated ODBC values are escaped when required, including `}` characters. Raw ODBC connection strings starting with `Driver=`, `DSN=`, `FileDSN=`, or `Database=` are passed through unchanged.

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

The generic ODBC, Sybase, and Db2 Python paths use the Rust Arrow route. Use `return_type="arrow"`, `return_type="arrow_stream"`, or a downstream Arrow consumer. To get pandas today, read Arrow and call `table.to_pandas()` after installing `pyarrow`.

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
| char/varchar/long varchar and wide variants | UTF-8 `String` | UTF-8 `String` | UTF-8 `String` |
| binary/varbinary/long varbinary | Arrow large binary | Arrow large binary | Arrow large binary through text-compatible FreeTDS path |
| date/time/timestamp | Arrow date/time/timestamp | Arrow date/time/timestamp | Arrow date/time/timestamp through text-compatible FreeTDS path |
| unknown/vendor-specific | error by default; optional `String` fallback | error by default; optional `String` fallback | error by default; optional `String` fallback, except FreeTDS `TIME2` maps to time |

Nullability reported as unknown is treated as nullable. If a driver reports a value as nullable but later returns `NULL` for a non-null ConnectorX destination type, ConnectorX returns an error instead of fabricating a default.

Automatic partitioning for generic ODBC, Db2, and Sybase requires `MIN(partition_on)` and `MAX(partition_on)` to return non-NULL `i64` integer bounds. Empty strings, SQL `NULL`, decimal values, fractional values, and exponent notation are rejected with a partition-bound error instead of being coerced or truncated. Cast decimal partition columns to a suitable integer expression or pass an explicit `partition_range` only when that conversion is semantically correct.

Vendor-specific ODBC types may be reported as unknown or other. ConnectorX rejects those types by default so driver-specific values are not silently returned as strings. Cast them in the query to a supported standard type when you need a specific output type. For compatibility with older behavior, set the matching opt-in environment variable to `true`: `ODBC_TYPE_FALLBACK_TO_VARCHAR`, `DB2_TYPE_FALLBACK_TO_VARCHAR`, or `SYBASE_TYPE_FALLBACK_TO_VARCHAR`.

Wide text buffers are decoded as UTF-16. Invalid UTF-16 sequences are rejected by default with an error that includes the source, column name, row index, and byte offset. Add `replace_invalid_utf16=true` to the ODBC, Db2, or Sybase URL only when you explicitly want invalid sequences replaced with U+FFFD for compatibility with legacy data or driver encoding bugs.

Text, wide text, and binary buffers are checked after every fetch. If the ODBC driver reports that a value was truncated, ConnectorX returns an error that names the relevant max-length setting. Increase the setting or cast/substr the selected column in the query.

## Performance

The ODBC reader fetches rows in batches and binds primitive, binary, and temporal columns with typed ODBC buffers. Decimal and text columns use text buffers for driver compatibility.

Tuning environment variables:

* `ODBC_BATCH_SIZE`: rows per ODBC block fetch. Defaults to `1024`.
* `ODBC_MAX_STR_LEN`: maximum bytes bound per cell for ODBC text and binary buffers. Defaults to `1024`.
* `ODBC_TYPE_FALLBACK_TO_VARCHAR`: when `true`, map unknown or vendor-specific ODBC types to `String` instead of returning an error. Defaults to `false`.

To benchmark the generic ODBC Arrow path against the PostgreSQL testcontainer fixture:

```bash
scripts/odbc_postgres_bench.sh --sample-size 10 --measurement-time 2 --warm-up-time 1
```

Useful benchmark controls:

* `ODBC_BENCH_ROWS`: number of rows read from the seeded benchmark table. Defaults to `100000`.
* `ODBC_BENCH_BATCH_SIZES`: comma-separated `ODBC_BATCH_SIZE` values to compare. Defaults to `1024,4096,8192,16384`.
* `ODBC_BENCH_QUERY`: custom benchmark query. When set, the benchmark runs only that query.

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
| Compile/unit coverage | `connector-rust-ci` for `src_odbc`, `src_db2`, and `src_sybase` | Tests print `CONNECTORX_SKIP:` for env-gated live cases and still pass. |
| PostgreSQL ODBC testcontainer coverage | Ubuntu `connector-rust-ci` and manual `odbc-live` with `backend=postgres` or `backend=both` | Runs without repository secrets, using Docker testcontainers and psqlODBC. |
| Secret-backed live coverage | Manual `odbc-live` with `backend=sybase`, `db2`, `odbc`, or `both` | Fails before tests with a clear error if the selected backend secrets are missing. |

The GitHub Actions job summary records which ODBC-family backends were skipped, exercised through PostgreSQL testcontainers, or exercised against secret-backed live drivers. In raw logs, grep for `CONNECTORX_SKIP:` to find env-gated tests that intentionally skipped, and `ODBC_COVERAGE:` for local `just` live-test output.

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
