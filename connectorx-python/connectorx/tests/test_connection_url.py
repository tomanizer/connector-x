import urllib.parse

import pytest

from .. import ConnectionUrl, rewrite_conn


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
