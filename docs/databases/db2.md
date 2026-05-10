# IBM Db2

## Protocol

* `binary`: ConnectorX uses the same public protocol label as other sources, but the Db2 implementation currently reads through ODBC block cursors.

## Connection String

```python
import connectorx as cx

conn = "db2://username:password@server:50000/database?driver=IBM%20DB2%20ODBC%20DRIVER"
table = cx.read_sql(conn, "select * from schema.table", return_type="arrow")
```

The `driver` query parameter can be an ODBC driver name from `odbcinst.ini` or an absolute driver library path. URL-encode absolute paths:

```python
conn = "db2://db2inst1:password@127.0.0.1:50000/testdb?driver=%2Fopt%2Fibm%2Fdb2%2Fclidriver%2Flib%2Flibdb2o.so"
```

Python users can construct the same URL with `ConnectionUrl`:

```python
from connectorx import ConnectionUrl

conn = ConnectionUrl(
    backend="db2",
    username="db2inst1",
    password="password",
    server="127.0.0.1",
    port=50000,
    database="testdb",
    database_options={"driver": "IBM DB2 ODBC DRIVER"},
)
```

ConnectorX expands this URL into an ODBC connection string using `Driver`, `Hostname`, `Port`, `Protocol`, `Database`, `UID`, and `PWD`. `Protocol` defaults to `TCPIP`. Generated values are brace-escaped, including `}` characters. A raw ODBC connection string starting with `Driver=`, `DSN=`, `FileDSN=`, or `Database=` is also accepted.

Additional URL query parameters are appended to the ODBC connection string, so settings such as `Security=SSL` can be passed through.

`replace_invalid_utf16=true` is a ConnectorX-only URL option. It is not passed to the Db2 ODBC driver. By default, ConnectorX rejects invalid UTF-16 returned through ODBC wide text buffers; use this option only when you explicitly want invalid sequences replaced with U+FFFD.

## Driver Setup

ConnectorX links against the platform ODBC manager. The Db2 ODBC/CLI driver is a runtime dependency and is not bundled in ConnectorX wheels.

### Linux

Install `unixodbc`/`unixodbc-dev`, then install and register the IBM Data Server Driver for ODBC and CLI (`clidriver`):

```bash
sudo apt-get install unixodbc unixodbc-dev
```

### macOS

```bash
brew install unixodbc
```

Install IBM's `clidriver` separately and reference its driver name or library path in `DB2_URL`.

### Windows

Windows provides the ODBC driver manager. Install IBM Data Server Driver Package or IBM Data Server Runtime Client and reference the registered Db2 ODBC driver by name.

## Supported Types

The ODBC path currently maps these Db2 types:

* Integer: `smallint`, `integer`, `bigint`
* Floating point: `real`, `double`, `float`
* Decimal: `numeric`, `decimal`
* Boolean: ODBC `SQL_BIT` when reported by the driver
* Text: `char`, `varchar`, `clob`, wide-character variants reported by ODBC
* Binary: `binary`, `varbinary`, `blob`
* Date/time: `date`, `time`, `timestamp`

Db2 `DECFLOAT`, `XML`, graphic string, and platform-specific types may be reported by the ODBC driver as generic or vendor-specific types. ConnectorX rejects unknown/vendor-specific ODBC types by default. Cast them in the query to a supported type when you need a specific output type, or set `DB2_TYPE_FALLBACK_TO_VARCHAR=true` to opt into the older string fallback behavior.

Db2 graphic and wide-character buffers are decoded as UTF-16 when returned through ODBC wide text buffers. Invalid UTF-16 is an error by default and reports source, column name, row index, and byte offset. Add `replace_invalid_utf16=true` to the Db2 URL only for explicit replacement-character compatibility.

See the ODBC-family type matrix in `docs/databases/odbc.md` for the shared runtime mapping, strict unknown-type handling, fallback opt-in, and truncation behavior.

## Performance Tuning

The ODBC reader fetches rows in batches and binds primitive columns with typed ODBC buffers. Integer, floating-point, binary, temporal, and `SQL_BIT` columns avoid text conversion in the hot path. Decimal and text columns use text buffers for driver compatibility.

The defaults are tuned for throughput over small memory use:

* `DB2_BATCH_SIZE`: rows per ODBC block fetch. Defaults to `1024`.
* `DB2_MAX_STR_LEN`: maximum bytes bound per cell for ODBC text and binary buffers. Defaults to `1024`.
* `DB2_MAX_CONNECTIONS`: maximum active Db2 ODBC connections per source instance. Defaults to the number of partition queries, with a minimum of `1`.
* `DB2_TYPE_FALLBACK_TO_VARCHAR`: when `true`, map unknown or vendor-specific ODBC types to `String` instead of returning an error. Defaults to `false`.

Increase `DB2_BATCH_SIZE` for wide network latency or large scans. Set `max_connections=N` on the Db2 URL, or `DB2_MAX_CONNECTIONS`, when partition count is higher than the number of server connections you want ConnectorX to hold concurrently. Increase `DB2_MAX_STR_LEN` when selected character, decimal, or binary columns can exceed the default bound.
If the ODBC driver reports truncation for a text, decimal, or binary value, ConnectorX returns an error instead of returning partial data.

## Testing And Benchmarking

A local Db2 Community container can be started with Docker:

```bash
just start-db2-docker
```

The container uses IBM's `icr.io/db2_community/db2` image, accepts the container license with `LICENSE=accept`, creates `testdb`, persists data in the `connectorx-db2-data` Docker volume, and exposes Db2 on port `50000`. On Apple Silicon, Docker runs the `linux/amd64` image through emulation because IBM does not publish an arm64 Db2 image.

Seed the benchmark table with:

```bash
just seed-db2-docker
```

Verify that a Linux amd64 ODBC client can reach the container and read the seeded table:

```bash
just check-db2-linux-odbc
```

Run the ConnectorX Db2 integration tests inside the Db2 container with IBM's full Db2 client stack:

```bash
just test-db2-docker
```

This path uses `/opt/ibm/db2/V12.1/lib64/libdb2o.so` from the full Db2 image and sources the initialized Db2 profile before running Rust tests. On macOS arm64, the IBM `macarm64_odbc_cli` package can validate Db2 CLI connectivity, but it does not currently include the `libdb2o` library IBM documents for 64-bit unixODBC driver managers. In practice, that can surface as `SQLLEN` conversion failures in Rust ODBC clients.

A Db2 ODBC connection can be tested with:

```bash
DB2_ODBC_CONN="Driver={IBM DB2 ODBC DRIVER};Hostname=127.0.0.1;Port=50000;Protocol=TCPIP;Database=testdb;UID=db2inst1;PWD=password;" \
cargo test -p connectorx --features "src_db2 dst_arrow" --test test_db2
```

Run the ODBC benchmark with:

```bash
DB2_URL="db2://db2inst1:password@127.0.0.1:50000/testdb?driver=IBM%20DB2%20ODBC%20DRIVER" \
DB2_BENCH_QUERY="select * from cx_db2_test" \
DB2_BENCH_ROWS=10000 \
cargo bench -p connectorx --features "src_db2 dst_arrow" --bench db2_odbc
```
