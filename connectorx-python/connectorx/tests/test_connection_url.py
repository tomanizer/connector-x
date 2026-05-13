import urllib.parse

import pytest

import connectorx as cx
from .. import ConnectionUrl, rewrite_conn


class FakeArrowTable:
    def __init__(self) -> None:
        self.pandas = FakePandasFrame()
        self.to_pandas_kwargs = None

    def to_pandas(self, **kwargs):
        self.to_pandas_kwargs = kwargs
        return self.pandas


class FakePandasFrame:
    def __init__(self) -> None:
        self.set_index_calls = []

    def set_index(self, column, inplace=False) -> None:
        self.set_index_calls.append((column, inplace))


def test_connection_url_builds_generic_odbc_driver_url() -> None:
    conn = ConnectionUrl(
        backend="odbc",
        driver="ODBC Driver 18 for SQL Server",
        username="user",
        password="pa;ss}",
        server="db.example.com",
        port=1433,
        database="warehouse",
        database_options={"ApplicationIntent": "ReadOnly", "server_key": "Hostname"},
    )

    parsed = urllib.parse.urlparse(conn)
    query = urllib.parse.parse_qs(parsed.query)

    assert parsed.scheme == "odbc"
    assert parsed.username == "user"
    assert urllib.parse.unquote(parsed.password or "") == "pa;ss}"
    assert parsed.hostname == "db.example.com"
    assert parsed.port == 1433
    assert parsed.path == "/warehouse"
    assert query["driver"] == ["ODBC Driver 18 for SQL Server"]
    assert query["ApplicationIntent"] == ["ReadOnly"]
    assert query["server_key"] == ["Hostname"]


def test_connection_url_builds_generic_odbc_dsn_url() -> None:
    conn = ConnectionUrl(
        backend="odbc",
        dsn="Warehouse DSN",
        username="user",
        password="secret}",
        database_options={"Trace": "No"},
    )

    parsed = urllib.parse.urlparse(conn)
    query = urllib.parse.parse_qs(parsed.query)

    assert parsed.scheme == "odbc"
    assert parsed.netloc == ""
    assert parsed.path == "/"
    assert query["dsn"] == ["Warehouse DSN"]
    assert query["UID"] == ["user"]
    assert query["PWD"] == ["secret}"]
    assert query["Trace"] == ["No"]


def test_connection_url_rejects_ambiguous_generic_odbc_identity() -> None:
    with pytest.raises(ValueError, match="exactly one of driver or dsn"):
        ConnectionUrl(backend="odbc")

    with pytest.raises(ValueError, match="exactly one of driver or dsn"):
        ConnectionUrl(backend="odbc", driver="PostgreSQL Unicode", dsn="Warehouse")


def test_raw_odbc_connection_string_keeps_binary_protocol() -> None:
    conn, protocol = rewrite_conn("Driver={SQLite3};Database=/tmp/example.db;")

    assert conn == "Driver={SQLite3};Database=/tmp/example.db;"
    assert protocol == "binary"


def test_rewrite_conn_keeps_none_backward_compatibility() -> None:
    conn, protocol = rewrite_conn(None)

    assert conn is None
    assert protocol == "binary"


@pytest.mark.parametrize(
    "conn",
    [
        "odbc://user:password@server:1433/database?driver=ODBC%20Driver",
        "db2://user:password@server:50000/database?driver=IBM%20DB2%20ODBC%20DRIVER",
        "sybase://user:password@server:5000/database?driver=FreeTDS",
        "Driver={SQLite3};Database=/tmp/example.db;",
        ConnectionUrl(
            backend="odbc",
            dsn="Warehouse DSN",
            username="user",
            password="secret",
        ),
    ],
)
def test_odbc_family_default_pandas_uses_arrow_path(monkeypatch, conn) -> None:
    calls = []
    arrow_table = FakeArrowTable()

    def fake_read_sql(conn, return_type, **kwargs):
        calls.append((conn, return_type, kwargs))
        return "arrow-result"

    monkeypatch.setattr(cx, "try_import_module", lambda name: object())
    monkeypatch.setattr(cx, "_read_sql", fake_read_sql)
    monkeypatch.setattr(cx, "reconstruct_arrow", lambda result: arrow_table)

    df = cx.read_sql(conn, "select 1;", index_col="id")

    assert df is arrow_table.pandas
    assert calls == [
        (
            str(conn),
            "arrow",
            {
                "queries": ["select 1"],
                "protocol": "binary",
                "partition_query": None,
                "pre_execution_queries": None,
            },
        )
    ]
    assert arrow_table.to_pandas_kwargs == {
        "date_as_object": False,
        "split_blocks": False,
    }
    assert df.set_index_calls == [("id", True)]


def test_non_odbc_default_pandas_keeps_pandas_transport(monkeypatch) -> None:
    calls = []
    frame = FakePandasFrame()

    def fake_read_sql(conn, return_type, **kwargs):
        calls.append((conn, return_type, kwargs))
        return "pandas-result"

    monkeypatch.setattr(cx, "try_import_module", lambda name: object())
    monkeypatch.setattr(cx, "_read_sql", fake_read_sql)
    monkeypatch.setattr(cx, "reconstruct_pandas", lambda result: frame)

    df = cx.read_sql("postgresql://user:password@server:5432/database", "select 1;")

    assert df is frame
    assert calls == [
        (
            "postgresql://user:password@server:5432/database",
            "pandas",
            {
                "queries": ["select 1"],
                "protocol": "binary",
                "partition_query": None,
                "pre_execution_queries": None,
            },
        )
    ]


def test_odbc_family_arrow_stream_stays_explicit(monkeypatch) -> None:
    calls = []
    record_batch_reader = object()

    def fake_read_sql(conn, return_type, **kwargs):
        calls.append((conn, return_type, kwargs))
        return "stream-result"

    monkeypatch.setattr(cx, "_read_sql", fake_read_sql)
    monkeypatch.setattr(cx, "reconstruct_arrow_rb", lambda result: record_batch_reader)

    result = cx.read_sql(
        "db2://user:password@server:50000/database?driver=IBM%20DB2%20ODBC%20DRIVER",
        "select 1;",
        return_type="arrow_stream",
        batch_size=123,
    )

    assert result is record_batch_reader
    assert calls == [
        (
            "db2://user:password@server:50000/database?driver=IBM%20DB2%20ODBC%20DRIVER",
            "arrow_stream",
            {
                "queries": ["select 1"],
                "protocol": "binary",
                "partition_query": None,
                "pre_execution_queries": None,
                "batch_size": 123,
            },
        )
    ]
