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

ASE may reject expressions like `convert(bit, null)` because the untyped `NULL` literal is treated as `VOID TYPE`. Use a typed expression such as a table column, parameter, or `case` expression when selecting nullable `bit` values.

See the ODBC-family type matrix in `docs/databases/odbc.md` for the shared runtime mapping, unknown-type fallback, and truncation behavior.

## Route Selection: `sybase://` vs `odbc://`

Prefer `sybase://` for SAP ASE / Sybase workloads.

* `sybase://` and `odbc://` share the same ODBC fetch core, but the dedicated Sybase route adds Sybase-specific connection-string defaults (`tds_version=5.0`), FreeTDS/ASE type handling, and the Sybase partitioning dialect.
* `sybase://` is the safer default for correctness because ConnectorX's Sybase type system recognizes ASE/FreeTDS quirks such as the `TIME2` extension and intentionally uses text-compatible buffers for temporal and binary values that are commonly reported in driver-specific ways.
* Use `odbc://` when you need an exact DSN or vendor-specific ODBC keyword set, and keep the query to standard SQL/ODBC-reported types if you want the closest match.

Practical expectations:

1. **Type fidelity:** prefer `sybase://`. Standard ODBC-reported primitive/text/decimal types are expected to match `odbc://`, but ASE-specific types and FreeTDS quirks are more likely to behave correctly on the dedicated route.
2. **Partitioning:** prefer `sybase://`. The dedicated route uses the T-SQL/MS SQL dialect rewriter; `odbc://` uses the generic SQL rewriter, which is fine for simple ANSI-style queries but is not expected to cover every ASE-specific query form.
3. **Performance:** both routes share the same block-cursor fetch engine, but the Sybase route deliberately favors compatibility for temporal/binary-heavy queries. Benchmark both routes on your actual driver and schema if raw throughput is the deciding factor.
4. **Connection-string safety:** generated values are brace-escaped on both routes, but `sybase://` keeps the structured URL form and default TDS settings. Raw ODBC strings passed through `odbc:///?odbc_connect=...` are trusted as-is.

## Performance Tuning

The ODBC reader fetches rows in batches and binds primitive columns with typed ODBC buffers. Integer, floating-point, and `bit` columns avoid text conversion in the hot path. Decimal, date/time, text, and binary columns still use text buffers for driver compatibility.

The defaults are tuned for throughput over small memory use:

* `SYBASE_BATCH_SIZE`: rows per ODBC block fetch. Defaults to `1024`.
* `SYBASE_MAX_STR_LEN`: maximum text bytes bound per cell in ODBC text buffers. Defaults to `1024`.

Increase `SYBASE_BATCH_SIZE` for wide network latency or large scans. Increase `SYBASE_MAX_STR_LEN` only when selected character, decimal, date/time, or binary columns can exceed the default bound.
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

To compare the dedicated Sybase route, the generic ODBC route, and partitioned extraction from a Polars caller, run the same query through each route:

```bash
SYBASE_URL="sybase://sa:sybase@127.0.0.1:5000/tempdb?driver=%2Fpath%2Fto%2Flibtdsodbc.so" \
SYBASE_ODBC_URL="odbc:///?odbc_connect=Driver%3D%7B%2Fpath%2Fto%2Flibtdsodbc.so%7D%3BServer%3D127.0.0.1%3BPort%3D5000%3BTDS_Version%3D5.0%3BUID%3Dsa%3BPWD%3Dsybase%3BDatabase%3Dtempdb%3B" \
SYBASE_BENCH_QUERY="select id, name, amount from dbo.cx_sybase_test order by id" \
python - <<'PY'
import os
import statistics
import time
import polars as pl

query = os.environ["SYBASE_BENCH_QUERY"]
cases = [
    ("sybase:// dedicated", os.environ["SYBASE_URL"], {}),
    ("odbc:// generic", os.environ["SYBASE_ODBC_URL"], {}),
    ("sybase:// partitioned", os.environ["SYBASE_URL"], {"partition_on": "id", "partition_num": 4}),
]

for label, uri, extra in cases:
    timings = []
    for _ in range(5):
        start = time.perf_counter()
        df = pl.read_database_uri(query, uri, engine="connectorx", **extra)
        timings.append(time.perf_counter() - start)
    print(label, "rows=", df.height, "median_s=", round(statistics.median(timings), 4), "runs=", [round(v, 4) for v in timings])
PY
```

Only add a generic-ODBC partitioned case after verifying that your query parses and partitions cleanly through the generic route.
