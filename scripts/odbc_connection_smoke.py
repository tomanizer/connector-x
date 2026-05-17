#!/usr/bin/env python3
"""Smoke-test ODBC benchmark connection environment variables."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class Probe:
    backend: str
    env_name: str
    query: str


PROBES = (
    Probe("postgres", "POSTGRES_ODBC_CONN", "select 1"),
    Probe("sybase", "SYBASE_ODBC_CONN", "select 1"),
    Probe("db2", "DB2_ODBC_CONN", "select 1 from sysibm.sysdummy1"),
)


def main() -> int:
    try:
        import pyodbc
    except ImportError:
        print("pyodbc is not installed in this Python environment", file=sys.stderr)
        return 2

    failures = []
    for probe in PROBES:
        conn_str = os.environ.get(probe.env_name)
        if not conn_str:
            print(f"SKIP {probe.backend}: {probe.env_name} is not set")
            continue

        try:
            conn = pyodbc.connect(conn_str, autocommit=True, timeout=10)
            try:
                cursor = conn.cursor()
                cursor.execute(probe.query)
                row = cursor.fetchone()
                print(f"OK   {probe.backend}: {probe.env_name} returned {row[0]!r}")
            finally:
                conn.close()
        except Exception as exc:
            failures.append((probe.backend, probe.env_name, exc))
            print(f"FAIL {probe.backend}: {probe.env_name}: {exc}", file=sys.stderr)

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
