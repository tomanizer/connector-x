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

All generated ODBC values are brace-escaped, including `}` characters. Raw ODBC connection strings are passed through unchanged.

## Runtime Dependencies

ConnectorX links against the platform ODBC manager. The ODBC driver for your database is a runtime dependency and is not bundled in ConnectorX wheels.

* Linux: install `unixodbc`, `unixodbc-dev`, and your database driver.
* macOS: install `unixodbc` with Homebrew and your database driver.
* Windows: install/register the vendor ODBC driver with the Windows ODBC driver manager.

## Type Support

The generic ODBC connector maps standard ODBC-reported types:

* Integer: `tinyint`, `smallint`, `integer`, `bigint`
* Floating point: `real`, `float`, `double`
* Decimal: `numeric`, `decimal`
* Boolean: `bit`
* Text: character, varchar, long varchar, and wide-character variants
* Binary: binary, varbinary, and long varbinary
* Temporal: date, time, timestamp

Vendor-specific ODBC types may be reported as unknown or other. Cast them in the query to a supported standard type when needed.

## Performance

The ODBC reader fetches rows in batches and binds primitive, binary, and temporal columns with typed ODBC buffers. Decimal and text columns use text buffers for driver compatibility.

Tuning environment variables:

* `ODBC_BATCH_SIZE`: rows per ODBC block fetch. Defaults to `1024`.
* `ODBC_MAX_STR_LEN`: maximum bytes bound per cell for ODBC text and binary buffers. Defaults to `1024`.

## Testing

For the preferred live test, run PostgreSQL in Docker and connect to it through psqlODBC:

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

Generic ODBC integration tests are also environment-gated and can be pointed at any ODBC backend:

```bash
ODBC_CONN="Driver={SQLite3};Database=/tmp/example.db;" \
ODBC_TEST_QUERY="select 1 as id" \
cargo test -p connectorx --no-default-features --features "src_odbc dst_arrow fptr" --test test_odbc
```

Partition smoke tests additionally use `ODBC_URL`, `ODBC_PARTITION_QUERY`, and `ODBC_PARTITION_COLUMN`.
Set `ODBC_EXPECTED_ROWS` to assert the returned row count for live tests.
