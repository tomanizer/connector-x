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

`unitext` may be reported by FreeTDS as binary UCS-2 bytes. Cast it to `varchar` or `univarchar` in the query if you need text output.

Sybase Unicode text buffers are decoded as UTF-16 when returned through ODBC wide text buffers. Invalid UTF-16 is an error by default and reports source, column name, row index, and byte offset. Add `replace_invalid_utf16=true` to the Sybase URL only for explicit replacement-character compatibility.

ASE may reject expressions like `convert(bit, null)` because the untyped `NULL` literal is treated as `VOID TYPE`. Use a typed expression such as a table column, parameter, or `case` expression when selecting nullable `bit` values.

ConnectorX rejects unknown/vendor-specific ODBC types by default. Cast them in the query to a supported type when you need a specific output type, or set `SYBASE_TYPE_FALLBACK_TO_VARCHAR=true` to opt into the older string fallback behavior. The known FreeTDS `TIME2` extension is still mapped to time.

See the ODBC-family type matrix in `docs/databases/odbc.md` for the shared runtime mapping, strict unknown-type handling, fallback opt-in, and truncation behavior.

## Performance Tuning

The ODBC reader fetches rows in batches and binds primitive columns with typed ODBC buffers. Integer, floating-point, and `bit` columns avoid text conversion in the hot path. Decimal, date/time, text, and binary columns still use text buffers for driver compatibility.

The defaults are tuned for throughput over small memory use:

* `SYBASE_BATCH_SIZE`: rows per ODBC block fetch. Defaults to `1024`.
* `SYBASE_MAX_STR_LEN`: maximum text bytes bound per cell in ODBC text buffers. Defaults to `1024`.
* `SYBASE_MAX_CONNECTIONS`: maximum active Sybase ODBC connections per source instance. Defaults to the number of partition queries, with a minimum of `1`.
* `SYBASE_TYPE_FALLBACK_TO_VARCHAR`: when `true`, map unknown or vendor-specific ODBC types to `String` instead of returning an error. Defaults to `false`.

Increase `SYBASE_BATCH_SIZE` for wide network latency or large scans. Set `max_connections=N` on the Sybase URL, or `SYBASE_MAX_CONNECTIONS`, when partition count is higher than the number of server connections you want ConnectorX to hold concurrently. Increase `SYBASE_MAX_STR_LEN` only when selected character, decimal, date/time, or binary columns can exceed the default bound.
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
