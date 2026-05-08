# ODBC

## Protocols

* `binary`: ConnectorX uses ODBC block cursors and Arrow transports.

## Connection Strings

Use a raw ODBC connection string when you need exact driver-specific keywords:

```python
conn = "Driver={SQLite3};Database=/tmp/example.db;"
```

For URL-style configuration, use `odbc://` with either `driver=` or `dsn=`:

```python
conn = "odbc://username:password@server:1433/database?driver=ODBC%20Driver%2018%20for%20SQL%20Server"
```

ConnectorX expands the URL into an ODBC connection string using `Driver` or `DSN`, `Server`, `Port`, `Database`, `UID`, and `PWD`. Additional URL query parameters are appended to the generated ODBC connection string. Use `server_key=Hostname` when a driver expects `Hostname` instead of `Server`.

All generated ODBC values are escaped when required, including `}` characters. Raw ODBC connection strings starting with `Driver=`, `DSN=`, `FileDSN=`, or `Database=` are passed through unchanged.

## Runtime Dependencies

ConnectorX links against the platform ODBC manager. The ODBC driver for your database is a runtime dependency and is not bundled in ConnectorX wheels.

* Linux: install `unixodbc`, `unixodbc-dev`, and your database driver.
* macOS: install `unixodbc` with Homebrew and your database driver.
* Windows: install/register the vendor ODBC driver with the Windows ODBC driver manager.

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
| unknown/vendor-specific | `String` fallback | `String` fallback | `String` fallback, except FreeTDS `TIME2` maps to time |

Nullability reported as unknown is treated as nullable. If a driver reports a value as nullable but later returns `NULL` for a non-null ConnectorX destination type, ConnectorX returns an error instead of fabricating a default.

Vendor-specific ODBC types may be reported as unknown or other. Cast them in the query to a supported standard type when you need a specific output type.

Text, wide text, and binary buffers are checked after every fetch. If the ODBC driver reports that a value was truncated, ConnectorX returns an error that names the relevant max-length setting. Increase the setting or cast/substr the selected column in the query.

## Performance

The ODBC reader fetches rows in batches and binds primitive, binary, and temporal columns with typed ODBC buffers. Decimal and text columns use text buffers for driver compatibility.

Tuning environment variables:

* `ODBC_BATCH_SIZE`: rows per ODBC block fetch. Defaults to `1024`.
* `ODBC_MAX_STR_LEN`: maximum bytes bound per cell for ODBC text and binary buffers. Defaults to `1024`.

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
