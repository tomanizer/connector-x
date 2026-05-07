# Sybase

## Protocol

* `binary`: ConnectorX uses the same public protocol label as other sources, but the Sybase implementation currently reads through ODBC block cursors.

## Connection String

```python
conn = "sybase://username:password@server:5000/database?driver=FreeTDS&tds_version=5.0"
```

The `driver` query parameter can be an ODBC driver name from `odbcinst.ini` or an absolute driver library path. URL-encode absolute paths:

```python
conn = "sybase://sa:sybase@127.0.0.1:5000/tempdb?driver=%2Fopt%2Fhomebrew%2Flib%2Flibtdsodbc.so"
```

`tds_version` defaults to `5.0`, which is the usual value for Sybase ASE through FreeTDS.

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

## Performance Tuning

The ODBC reader fetches rows in batches and binds primitive columns with typed ODBC buffers. Integer, floating-point, and `bit` columns avoid text conversion in the hot path. Decimal, date/time, text, and binary columns still use text buffers for driver compatibility.

The defaults are tuned for throughput over small memory use:

* `SYBASE_BATCH_SIZE`: rows per ODBC block fetch. Defaults to `1024`.
* `SYBASE_MAX_STR_LEN`: maximum text bytes bound per cell in ODBC text buffers. Defaults to `1024`.

Increase `SYBASE_BATCH_SIZE` for wide network latency or large scans. Increase `SYBASE_MAX_STR_LEN` only when selected character, decimal, date/time, or binary columns can exceed the default bound.

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
