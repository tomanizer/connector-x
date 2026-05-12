# Sybase

## Protocol

* `binary`: ConnectorX uses the same public protocol label as other sources, but the Sybase implementation currently reads through ODBC block cursors.

## Connection String

```python
import connectorx as cx

conn = "sybase://username:password@server:5000/database?driver=FreeTDS&tds_version=5.0"
table = cx.read_sql(conn, "select * from dbo.lineitem", return_type="arrow")
```

The `driver` query parameter can be an ODBC driver name from `odbcinst.ini` or an absolute driver library path. URL-encode absolute paths:

```python
conn = "sybase://sa:sybase@127.0.0.1:5000/tempdb?driver=%2Fopt%2Fhomebrew%2Flib%2Flibtdsodbc.so"
```

Python users can construct the same URL with `ConnectionUrl`:

```python
from connectorx import ConnectionUrl

conn = ConnectionUrl(
    backend="sybase",
    username="sa",
    password="sybase",
    server="127.0.0.1",
    port=5000,
    database="tempdb",
    database_options={"driver": "FreeTDS", "tds_version": "5.0"},
)
```

`tds_version` defaults to `5.0`, which is the usual value for Sybase ASE through FreeTDS.
Generated ODBC values are brace-escaped, including `}` characters. Raw ODBC connection strings starting with `Driver=`, `DSN=`, `FileDSN=`, or `Database=` are passed through unchanged.

`replace_invalid_utf16=true` is a ConnectorX-only URL option. It is not passed to the Sybase ODBC driver. By default, ConnectorX rejects invalid UTF-16 returned through ODBC wide text buffers; use this option only when you explicitly want invalid sequences replaced with U+FFFD.

`max_connections=N`, `login_timeout_secs=N`, and `query_timeout_secs=N` are also ConnectorX-only URL options. `login_timeout_secs` configures the ODBC login timeout, and `query_timeout_secs` configures the statement timeout used for metadata, row-count, partition-range, and data-fetch queries. Both timeout values must be positive integers in seconds. Driver support varies, but standard ODBC timeout diagnostics are returned as typed ConnectorX timeout errors.

Sybase URL query parameter names are decoded and matched case-insensitively. Duplicate query parameter names are rejected with an error instead of using first-wins or last-wins behavior. First-class Sybase URL parameters are `driver`, `tds_version`, `replace_invalid_utf16`, `max_connections`, `login_timeout_secs`, and `query_timeout_secs`; other non-duplicate parameters are passed through to the Sybase ODBC driver connection string.

## Dedicated Versus Generic ODBC Route

Use `sybase://` for Sybase/SAP ASE production reads. It uses the same direct Arrow ODBC batch fetch path as generic `odbc://`, but keeps ASE-specific behavior in the places where it matters:

* URL construction uses ASE/FreeTDS-oriented keywords such as `Server`, `Port`, `TDS_Version`, and `Database`.
* `SYBASE_*` environment variables and URL options control Sybase buffer size, batch size, connection limits, timeouts, UTF-16 replacement, and unknown-type fallback without affecting other ODBC sources in the same process.
* Sybase-specific type policy covers common ASE and FreeTDS behavior such as `money`, `smallmoney`, `bigtime`, `bigdatetime`, rowversion-like `timestamp`, and binary values returned through text-compatible FreeTDS buffers.
* Partition count, range, and predicate queries are generated through the Sybase/T-SQL route policy.

Use generic `odbc://` for Sybase mainly as a comparison or troubleshooting route: for example, when validating a new SAP ASE or FreeTDS driver configuration, passing an exact raw ODBC connection string through `odbc_connect`, or isolating whether a problem is in driver metadata versus Sybase route policy. For supported columns reported with standard ODBC metadata, `sybase://` and `odbc://` are expected to produce the same Arrow schema, row count, null counts, and values.

The route-comparison live tests can be run with both source features enabled:

```bash
SYBASE_URL="sybase://sa:YOUR_PASSWORD@127.0.0.1:5000/tempdb?driver=FreeTDS&tds_version=5.0" \
SYBASE_ODBC_CONN="Driver={FreeTDS};Server=127.0.0.1;Port=5000;TDS_Version=5.0;UID=sa;PWD=YOUR_PASSWORD;Database=tempdb;" \
cargo test -p connectorx --no-default-features --features "src_sybase src_odbc dst_arrow fptr" --test test_odbc_route_compare -- sybase
```

Set `SYBASE_GENERIC_ODBC_URL` instead of `SYBASE_ODBC_CONN` when you want the comparison to use a hand-written generic `odbc://` URL.

## Driver Matrix And Diagnostics

Sybase ODBC behavior depends on the driver stack. ConnectorX keeps the Sybase route on the same batched ODBC/Arrow path as generic `odbc://`, but the Sybase type policy is intentionally driver-aware for the ASE cases that FreeTDS exposes differently from standard ODBC metadata.

| Driver stack | Status | Coverage | Known behavior |
| --- | --- | --- | --- |
| FreeTDS through unixODBC, `TDS_Version=5.0`, ASE 16 testcontainer | Verified in live tests | Primitive typed buffers, money/decimal, temporal values, binary/image/rowversion, Unicode text, query wrapping, partitioning, and route comparison against generic ODBC | `bigtime` is commonly reported as SQL Server `TIME2`; binary-family values may arrive through text-compatible hex buffers; `money` and `smallmoney` report decimal/numeric metadata and use the decimal text-buffer path; `unichar`/`univarchar` report wide-text metadata; `unitext` should be projected with an explicit text cast when text output is required. |
| FreeTDS against other ASE versions | Expected but not fully enumerated | Run the diagnostic matrix and route-comparison tests against the target server | The same policies should apply, but temporal precision, LOB display sizes, and Unicode metadata can vary by server and FreeTDS version. |
| SAP ASE ODBC driver | Unverified | No committed live fixture yet | Expected to work when it reports standard ODBC metadata for supported Sybase types. ConnectorX does not currently claim verified behavior for SAP-specific reporting of `bigtime`, `bigdatetime`, rowversion-like `timestamp`, `unitext`, or large LOB bounds. |
| iODBC or Windows ODBC manager with SAP/third-party drivers | Unverified | No committed live fixture yet | Validate metadata, timeout behavior, and truncation diagnostics before relying on production extraction. |

The diagnostic test prints a Markdown matrix of the ODBC metadata reported by the connected driver and the ConnectorX policy selected for each column:

```bash
SYBASE_ODBC_CONN="Driver={FreeTDS};Server=127.0.0.1;Port=5000;TDS_Version=5.0;UID=sa;PWD=YOUR_PASSWORD;Database=tempdb;" \
cargo test -p connectorx --no-default-features --features "src_sybase dst_arrow fptr" --test test_sybase -- test_sybase_driver_matrix_metadata_report --nocapture
```

When `CONNECTORX_SYBASE_TESTCONTAINER=1` is set, the matrix also includes the seeded `image`, rowversion-like `timestamp`, long `univarchar`, and `unitext`-cast cases from `scripts/odbc_sybase.sql`. Without the seeded tables it still records expression-based primitive, money, decimal, temporal, binary, and Unicode cases.

The output includes the parsed `Driver` and `TDS_Version` connection keywords where present, the ODBC DBMS name, `@@version` when the server allows it, the active `SYBASE_TYPE_FALLBACK_TO_VARCHAR` policy, ODBC type code, column size, decimal digits, nullability, ConnectorX type policy, and the buffer policy. This keeps new driver-specific expectations additive: add a new diagnostic case or expected row for the driver instead of rewriting the core Sybase type-system tests.

## Driver Setup

ConnectorX links against the platform ODBC manager. The Sybase driver is a runtime dependency and is not bundled in ConnectorX wheels.

### Linux

Debian/Ubuntu:

```bash
sudo apt-get install unixodbc unixodbc-dev freetds-bin freetds-dev
```

RHEL/CentOS/Fedora:

```bash
sudo yum install unixODBC unixODBC-devel freetds freetds-devel
```

### macOS

```bash
brew install unixodbc freetds
```

Common FreeTDS driver paths are `/opt/homebrew/lib/libtdsodbc.so` on Apple Silicon Homebrew and `/usr/local/lib/libtdsodbc.so` on Intel Homebrew.

### Windows

Windows provides the ODBC driver manager. Install a Sybase-compatible ODBC driver separately, such as SAP ASE SDK/ODBC or another vendor driver, and reference it by driver name in the `driver` query parameter. FreeTDS is also available through some third-party package managers, but production Windows deployments should use a driver distribution you can support operationally.

## Supported Types

The ODBC path currently maps these Sybase/ASE types:

* Integer: `tinyint`, `smallint`, `int`, `bigint`
* Floating point: `real`, `float`
* Decimal and money: `numeric`, `decimal`, `money`, `smallmoney`
* Boolean: `bit`
* Text: `char`, `varchar`, `text`, `unichar`, `univarchar`
* Binary: `binary`, `varbinary`, `image`
* Date/time: `date`, `time`, `datetime`, `smalldatetime`, `bigtime`, `bigdatetime`

`char`, `varchar`, and `text` map to Arrow `LargeUtf8`. Sybase Unicode types reported through ODBC wide-character metadata, including `unichar` and `univarchar`, also map to Arrow `LargeUtf8`. If a driver reports `nchar` or `nvarchar` as ODBC `WCHAR`/`WVARCHAR`, ConnectorX applies the same mapping; SAP ASE deployments should verify whether those names are supported aliases for the configured server and driver.

`unitext` may be reported by FreeTDS as binary UCS-2 bytes. Cast it to `varchar` or `univarchar` in the query if you need text output. The Sybase live tests cover `unitext` through an explicit `convert(univarchar(...), unitext_column)` projection.

Sybase `timestamp` is a rowversion-like binary value, not a wall-clock timestamp. When the ODBC driver reports it as `binary` or `varbinary`, ConnectorX returns `LargeBinary`. Cast it explicitly in the query if your driver exposes a non-standard representation and you need a different output type.

Sybase `binary`, `varbinary`, `image`, and rowversion-like `timestamp` values map to Arrow `LargeBinary`. FreeTDS commonly returns binary-family values through text buffers as ASCII hex; ConnectorX hex-decodes those values before producing Arrow arrays. Drivers that expose true ODBC binary buffers are passed through as raw bytes. `image` is treated as a bounded ODBC binary value, so very large values are still subject to the configured `SYBASE_MAX_STR_LEN` buffer limit and truncation checks.

FreeTDS reports ASE `time` and `bigtime` through the SQL Server `TIME2` extension on common ASE 16 configurations; ConnectorX maps that metadata to Arrow `Time64(Microsecond)`. `datetime`, `smalldatetime`, and `bigdatetime` map to Arrow `Timestamp(Microsecond)`, with the precision bounded by the source type and driver formatting.

Sybase Unicode text buffers are decoded as UTF-16 when returned through ODBC wide text buffers. Invalid UTF-16 is an error by default and reports source, column name, row index, and byte offset. Add `replace_invalid_utf16=true` to the Sybase URL only for explicit replacement-character compatibility.

ASE may reject expressions like `convert(bit, null)` because the untyped `NULL` literal is treated as `VOID TYPE`. Use a typed expression such as a table column, parameter, or `case` expression when selecting nullable `bit` values.

ConnectorX rejects unknown/vendor-specific ODBC types by default. Cast them in the query to a supported type when you need a specific output type, or set `SYBASE_TYPE_FALLBACK_TO_VARCHAR=true` to opt into the older string fallback behavior. The known FreeTDS `TIME2` extension is still mapped to time.

See the ODBC-family type matrix in `docs/databases/odbc.md` for the shared runtime mapping, strict unknown-type handling, fallback opt-in, and truncation behavior.

## Query Wrapping And Partitioning

Sybase count, partition-range, and partition predicates are generated with ConnectorX's T-SQL-compatible wrapping path. The live Sybase tests cover schema-qualified tables, bracketed identifiers, reserved-word columns, `top`, `order by`, nested subqueries, `convert(datetime, ...)`, and nullable partition columns. Query shapes outside that set should be validated against the target ASE server and ODBC driver before relying on partitioned extraction in production.

## Performance Tuning

The ODBC reader fetches rows in batches and binds primitive columns with typed ODBC buffers. Integer, floating-point, and `bit` columns avoid text conversion in the hot path. Decimal, date/time, text, and binary columns still use text buffers for driver compatibility.

The defaults are tuned for throughput over small memory use:

* `SYBASE_BATCH_SIZE`: rows per ODBC block fetch. Defaults to `1024`. Recommended range is `1024` to `16384`; hard maximum is `65536`.
* `SYBASE_MAX_STR_LEN`: maximum text bytes bound per cell in ODBC text buffers. Defaults to `1024`. Hard maximum is `67108864` bytes.
* `SYBASE_MAX_CONNECTIONS`: maximum active Sybase ODBC connections per source instance. Defaults to the number of partition queries, with a minimum of `1`.
* `SYBASE_LOGIN_TIMEOUT_SECS`: ODBC login timeout in seconds. Unset by default.
* `SYBASE_QUERY_TIMEOUT_SECS`: ODBC statement timeout in seconds. Unset by default.
* `SYBASE_TYPE_FALLBACK_TO_VARCHAR`: when `true`, map unknown or vendor-specific ODBC types to `String` instead of returning an error. Defaults to `false`.

`SYBASE_BATCH_SIZE * SYBASE_MAX_STR_LEN` must not exceed `268435456` bytes, which caps the per-column allocation for variable-width ODBC buffers. Increase `SYBASE_BATCH_SIZE` for wide network latency or large scans. Set `max_connections=N` on the Sybase URL, or `SYBASE_MAX_CONNECTIONS`, when partition count is higher than the number of server connections you want ConnectorX to hold concurrently. Set `login_timeout_secs=N` or `query_timeout_secs=N` on the Sybase URL for source-specific timeouts, or use the matching environment variables as defaults. Increase `SYBASE_MAX_STR_LEN` only when selected character, decimal, date/time, or binary columns can exceed the default bound; lower `SYBASE_BATCH_SIZE` when raising `SYBASE_MAX_STR_LEN` for large LOB cells.
If the ODBC driver reports truncation for a text-compatible value, ConnectorX returns an error instead of returning partial data.

## Testing And Benchmarking

A local FreeTDS connection can be tested with:

```bash
SYBASE_ODBC_CONN="Driver=/path/to/libtdsodbc.so;Server=127.0.0.1;Port=5000;TDS_Version=5.0;UID=sa;PWD=sybase;Database=tempdb;" \
cargo test -p connectorx --features "src_sybase dst_arrow" --test test_sybase
```

Run the ODBC benchmark with:

```bash
SYBASE_URL="sybase://sa:sybase@127.0.0.1:5000/tempdb?driver=%2Fpath%2Fto%2Flibtdsodbc.so" \
SYBASE_BENCH_QUERY="select * from dbo.cx_sybase_test" \
SYBASE_BENCH_ROWS=10000 \
cargo bench -p connectorx --features "src_sybase dst_arrow" --bench sybase_odbc
```

Compare ConnectorX `sybase://`, ConnectorX generic `odbc://`, partitioned ConnectorX, and Polars `arrow-odbc` with:

```bash
SYBASE_URL="sybase://sa:sybase@127.0.0.1:5000/tempdb?driver=FreeTDS&tds_version=5.0" \
SYBASE_ODBC_CONN="Driver={FreeTDS};Server=127.0.0.1;Port=5000;TDS_Version=5.0;UID=sa;PWD=sybase;Database=tempdb;" \
scripts/odbc_arrow_compare.py --backend sybase
```

The comparison script includes Sybase edge cases for money/decimal, temporal values, binary/image, Unicode text, rowversion-like binary values, and partitioned reads where the seeded tables are available. It fails by default on schema, null-count, row-count, row-hash, or value mismatches and reports timings for each route.
