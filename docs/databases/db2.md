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

`max_connections=N`, `login_timeout_secs=N`, and `query_timeout_secs=N` are also ConnectorX-only URL options. `login_timeout_secs` configures the ODBC login timeout, and `query_timeout_secs` configures the statement timeout used for metadata, row-count, partition-range, and data-fetch queries. Both timeout values must be positive integers in seconds. Driver support varies, but standard ODBC timeout diagnostics are returned as typed ConnectorX timeout errors.

Db2 URL query parameter names are decoded and matched case-insensitively. Duplicate query parameter names are rejected with an error instead of using first-wins or last-wins behavior. First-class Db2 URL parameters are `driver`, `protocol`, `replace_invalid_utf16`, `max_connections`, `login_timeout_secs`, and `query_timeout_secs`; other non-duplicate parameters are passed through to the Db2 ODBC driver connection string.

## Dedicated Versus Generic ODBC Route

Use `db2://` for Db2 production reads. It uses the same direct Arrow ODBC batch fetch path as generic `odbc://`, but keeps Db2-specific behavior in the places where it matters:

* URL construction uses Db2 keywords such as `Hostname`, `Protocol`, and `Database`.
* `DB2_*` environment variables and URL options control Db2 buffer size, batch size, connection limits, timeouts, UTF-16 replacement, and unknown-type fallback without affecting other ODBC sources in the same process.
* Unknown or vendor-specific Db2 types produce Db2-specific diagnostics and mention `DB2_TYPE_FALLBACK_TO_VARCHAR`.
* Partition count, range, and predicate queries are generated through the Db2 route policy.

Use generic `odbc://` for Db2 mainly as a comparison or troubleshooting route: for example, when you need to pass an exact raw ODBC connection string through `odbc_connect`, compare ConnectorX with another ODBC client, or isolate whether a problem is in the driver metadata versus the Db2 route policy. For supported columns reported with standard ODBC metadata, `db2://` and `odbc://` are expected to produce the same Arrow schema, row count, null counts, and values.

The route-comparison live tests can be run with both source features enabled:

```bash
DB2_URL="db2://db2inst1:YOUR_PASSWORD@127.0.0.1:50000/testdb?driver=IBM%20DB2%20ODBC%20DRIVER" \
DB2_ODBC_CONN="Driver={IBM DB2 ODBC DRIVER};Hostname=127.0.0.1;Port=50000;Protocol=TCPIP;Database=testdb;UID=db2inst1;PWD=YOUR_PASSWORD;" \
cargo test -p connectorx --no-default-features --features "src_db2 src_odbc dst_arrow fptr" --test test_odbc_route_compare -- db2
```

Set `DB2_GENERIC_ODBC_URL` instead of `DB2_ODBC_CONN` when you want the comparison to use a hand-written generic `odbc://` URL.

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
* Text: `char`, `varchar`, `clob`, `dbclob`, `graphic`, `vargraphic`, and wide-character variants when reported as standard ODBC text or wide-text metadata
* Binary: `binary`, `varbinary`, `blob`
* Date/time: `date`, `time`, `timestamp`

Db2 `DECFLOAT`, `XML`, `ROWID`, and platform-specific types may be reported by the ODBC driver as vendor-specific `SQL_DECFLOAT`, `SQL_XML`, `SQL_ROWID`, or other non-standard ODBC type codes. ConnectorX rejects unknown/vendor-specific ODBC types by default. Cast them in the query to a supported type when you need a specific output type, or set `DB2_TYPE_FALLBACK_TO_VARCHAR=true` to opt into the older string fallback behavior.

The initial DB2-specific policy is:

| Db2 type | ConnectorX policy |
| --- | --- |
| `DECIMAL(p,s)` / `NUMERIC(p,s)` | Arrow `Decimal128(p,s)` using the precision and scale reported by ODBC. |
| `DECFLOAT(16)` / `DECFLOAT(34)` | Strict unsupported when reported as `SQL_DECFLOAT`; cast to `varchar` or opt into `DB2_TYPE_FALLBACK_TO_VARCHAR=true` for string output. Scientific notation is passed through as text in fallback mode rather than parsed as fixed-scale decimal. |
| `XML` | Strict unsupported when reported as `SQL_XML`; use `xmlserialize(... as varchar(n))` for text output. |
| `CLOB` / `DBCLOB` | Arrow `LargeUtf8` when reported as standard long text or wide long text. Values are fetched through the configured text buffer. |
| `BLOB` | Arrow `LargeBinary` when reported as standard long binary. Values are fetched through the configured binary buffer. |
| `GRAPHIC` / `VARGRAPHIC` | Arrow `LargeUtf8` when reported as ODBC wide text. |
| `ROWID` | Supported as `LargeBinary` or `LargeUtf8` only if the driver reports standard binary or text metadata. Vendor-specific `SQL_ROWID` follows the strict/fallback unknown-type policy. |

Db2 graphic and wide-character buffers are decoded as UTF-16 when returned through ODBC wide text buffers. Invalid UTF-16 is an error by default and reports source, column name, row index, and byte offset. Add `replace_invalid_utf16=true` to the Db2 URL only for explicit replacement-character compatibility.

`DB2_MAX_STR_LEN` bounds the per-cell buffer used for text, binary, decimal-as-text, CLOB, DBCLOB, and BLOB values. ConnectorX detects ODBC truncation indicators and returns an error asking you to raise `DB2_MAX_STR_LEN` or cast/substr the column. Piecewise LOB streaming is not implemented yet, so very large LOB extraction should use an explicit cast/substr window or a larger buffer sized for the workload.

See the ODBC-family type matrix in `docs/databases/odbc.md` for the shared runtime mapping, strict unknown-type handling, fallback opt-in, and truncation behavior.

## Query Wrapping And Partitioning

ConnectorX generates DB2 SQL for row counts, partition min/max ranges, and partition predicates by using the shared SQL wrapper with `sqlparser`'s `GenericDialect`. The DB2 test suite covers these generated shapes:

* schema-qualified tables such as `RISK_SCHEMA.RISK_RESULTS`
* double-quoted identifiers, including mixed-case names such as `"TradeId"`
* reserved-word column names quoted with double quotes, such as `"select"`
* `DATE(...)` and `TIMESTAMP(...)` constructor expressions
* `WITH` / CTE queries
* nested derived-table wrapping for count, range, and partition predicates
* `ORDER BY` in the source query
* `FETCH FIRST n ROWS ONLY` in the source query

Queries that use DB2 syntax outside these tested shapes may still rely on ConnectorX's string-composition fallback when `sqlparser` cannot parse the source SQL. Keep partition columns projected by the source query, and prefer simple integer partition keys for partitioned extraction.

## Performance Tuning

The ODBC reader fetches rows in batches and binds primitive columns with typed ODBC buffers. Integer, floating-point, binary, temporal, and `SQL_BIT` columns avoid text conversion in the hot path. Decimal and text columns use text buffers for driver compatibility.

The defaults are tuned for throughput over small memory use:

* `DB2_BATCH_SIZE`: rows per ODBC block fetch. Defaults to `1024`. Recommended range is `1024` to `16384`; hard maximum is `65536`.
* `DB2_MAX_STR_LEN`: maximum bytes bound per cell for ODBC text and binary buffers. Defaults to `1024`. Hard maximum is `67108864` bytes.
* `DB2_MAX_CONNECTIONS`: maximum active Db2 ODBC connections per source instance. Defaults to the number of partition queries, with a minimum of `1`.
* `DB2_LOGIN_TIMEOUT_SECS`: ODBC login timeout in seconds. Unset by default.
* `DB2_QUERY_TIMEOUT_SECS`: ODBC statement timeout in seconds. Unset by default.
* `DB2_TYPE_FALLBACK_TO_VARCHAR`: when `true`, map unknown or vendor-specific ODBC types to `String` instead of returning an error. Defaults to `false`.

`DB2_BATCH_SIZE * DB2_MAX_STR_LEN` must not exceed `268435456` bytes, which caps the per-column allocation for variable-width ODBC buffers. Increase `DB2_BATCH_SIZE` for wide network latency or large scans. Set `max_connections=N` on the Db2 URL, or `DB2_MAX_CONNECTIONS`, when partition count is higher than the number of server connections you want ConnectorX to hold concurrently. Set `login_timeout_secs=N` or `query_timeout_secs=N` on the Db2 URL for source-specific timeouts, or use the matching environment variables as defaults. Increase `DB2_MAX_STR_LEN` when selected character, decimal, or binary columns can exceed the default bound; lower `DB2_BATCH_SIZE` when raising `DB2_MAX_STR_LEN` for large LOB cells.
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

Compare ConnectorX `db2://`, ConnectorX generic `odbc://`, partitioned ConnectorX, and Polars `arrow-odbc` with:

```bash
DB2_URL="db2://db2inst1:password@127.0.0.1:50000/testdb?driver=IBM%20DB2%20ODBC%20DRIVER" \
DB2_ODBC_CONN="Driver={IBM DB2 ODBC DRIVER};Hostname=127.0.0.1;Port=50000;Protocol=TCPIP;Database=testdb;UID=db2inst1;PWD=password;" \
scripts/odbc_arrow_compare.py --backend db2
```

The comparison script fails by default on schema, null-count, row-count, row-hash, or value mismatches and reports timings for each route.
