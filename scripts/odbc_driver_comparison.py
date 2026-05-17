#!/usr/bin/env python3
"""Compare DataFrame ingestion routes for ODBC-backed databases.

The benchmark is designed for end-to-end user-visible reads. It compares:

* pandas over normal ODBC drivers through pyodbc
* pandas over SQLAlchemy where a SQLAlchemy URL is explicitly configured
* Polars over pyodbc
* Polars over arrow-odbc
* ConnectorX native routes returning Polars
* ConnectorX generic ODBC routes returning Polars

Each route runs in a child process so process RSS and imports from previous
routes do not pollute later measurements.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.metadata
import json
import math
import multiprocessing as mp
import os
import platform
import queue as queue_module
import statistics
import subprocess
import sys
import time
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any


try:
    import resource
except ImportError:  # pragma: no cover - Windows.
    resource = None


BACKENDS = ("postgres", "db2", "sybase")
DEFAULT_ROWS = 100_000
DEFAULT_ITERATIONS = 3
DEFAULT_WARMUPS = 1
DEFAULT_PARTITIONS = 4
DEFAULT_PREPARE_BATCH_SIZE = 1_000
BENCH_TABLE = {
    "postgres": "cx_bench_perf",
    "db2": "cx_bench_perf",
    "sybase": "dbo.cx_bench_perf",
}
PRIMITIVE_COLUMNS = "id, flag, int_v, bigint_v, real_v, double_v"
MIXED_COLUMNS = (
    "id, flag, int_v, bigint_v, real_v, double_v, amount, name, "
    "payload, payload_bytes, created_at"
)
PACKAGE_NAMES = (
    "connectorx",
    "pandas",
    "polars",
    "pyarrow",
    "pyodbc",
    "sqlalchemy",
    "arrow-odbc",
)


@dataclass(frozen=True)
class BackendConfig:
    name: str
    connectorx_url: str | None
    generic_odbc_url: str | None
    raw_odbc_conn: str | None
    sqlalchemy_url: str | None


@dataclass(frozen=True)
class Case:
    name: str
    query: str
    partition_on: str | None = None
    partition_range: tuple[int, int] | None = None
    expected_rows: int | None = None
    sort_by: str | None = None


@dataclass(frozen=True)
class RouteSpec:
    backend: str
    name: str
    kind: str
    connection: str
    partitioned: bool = False


def env(name: str) -> str | None:
    value = os.environ.get(name)
    return value if value else None


def first_env(*names: str) -> str | None:
    for name in names:
        value = env(name)
        if value:
            return value
    return None


def positive_int(value: str | None, default: int) -> int:
    if not value:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def parse_partition_range(value: str | None) -> tuple[int, int] | None:
    if not value:
        return None
    try:
        low, high = value.split(",", 1)
    except ValueError:
        raise ValueError(f"invalid partition range {value!r}; expected 'low,high'") from None
    return int(low.strip()), int(high.strip())


def parse_json_cases(raw: str, env_name: str) -> list[Case]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON in {env_name}: {exc}") from exc
    if not isinstance(payload, list):
        raise ValueError(f"{env_name} must be a JSON array")

    cases = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise ValueError(f"{env_name}[{index}] must be an object")
        name = item.get("name")
        query = item.get("query")
        if not isinstance(name, str) or not name:
            raise ValueError(f"{env_name}[{index}].name must be a non-empty string")
        if not isinstance(query, str) or not query:
            raise ValueError(f"{env_name}[{index}].query must be a non-empty string")
        partition_range = item.get("partition_range")
        if partition_range is not None:
            if (
                not isinstance(partition_range, list)
                or len(partition_range) != 2
                or not all(isinstance(value, int) for value in partition_range)
            ):
                raise ValueError(
                    f"{env_name}[{index}].partition_range must be a two-item integer array"
                )
            partition_range = (partition_range[0], partition_range[1])
        expected_rows = item.get("expected_rows")
        if expected_rows is not None and not isinstance(expected_rows, int):
            raise ValueError(f"{env_name}[{index}].expected_rows must be an integer")
        cases.append(
            Case(
                name=name,
                query=query,
                partition_on=item.get("partition_on"),
                partition_range=partition_range,
                expected_rows=expected_rows,
                sort_by=item.get("sort_by"),
            )
        )
    return cases


def backend_config(name: str) -> BackendConfig:
    prefix = name.upper()
    if name == "postgres":
        connectorx_url = first_env("POSTGRES_URL", "POSTGRES_CONNECTORX_URL")
        raw_odbc_conn = first_env("POSTGRES_ODBC_CONN", "ODBC_CONN")
        generic_url = first_env("POSTGRES_GENERIC_ODBC_URL", "ODBC_URL")
        sqlalchemy_url = first_env("POSTGRES_SQLALCHEMY_URL", "POSTGRES_SQLALCHEMY_ODBC_URL")
    else:
        connectorx_url = first_env(f"{prefix}_URL", f"{prefix}_CONNECTORX_URL")
        raw_odbc_conn = first_env(f"{prefix}_ODBC_CONN")
        generic_url = first_env(f"{prefix}_GENERIC_ODBC_URL")
        sqlalchemy_url = first_env(f"{prefix}_SQLALCHEMY_URL", f"{prefix}_SQLALCHEMY_ODBC_URL")

    return BackendConfig(
        name=name,
        connectorx_url=connectorx_url,
        generic_odbc_url=generic_url,
        raw_odbc_conn=raw_odbc_conn,
        sqlalchemy_url=sqlalchemy_url,
    )


def load_custom_cases(backend: str) -> list[Case] | None:
    prefix = backend.upper()
    query = first_env(f"{prefix}_DRIVER_COMPARE_QUERY", "CX_DRIVER_COMPARE_QUERY")
    if query:
        return [
            Case(
                name="custom",
                query=query,
                partition_on=first_env(
                    f"{prefix}_DRIVER_COMPARE_PARTITION_ON",
                    "CX_DRIVER_COMPARE_PARTITION_ON",
                ),
                partition_range=parse_partition_range(
                    first_env(
                        f"{prefix}_DRIVER_COMPARE_PARTITION_RANGE",
                        "CX_DRIVER_COMPARE_PARTITION_RANGE",
                    )
                ),
                expected_rows=positive_int(
                    first_env(
                        f"{prefix}_DRIVER_COMPARE_EXPECTED_ROWS",
                        "CX_DRIVER_COMPARE_EXPECTED_ROWS",
                    ),
                    0,
                )
                or None,
                sort_by=first_env(f"{prefix}_DRIVER_COMPARE_SORT_BY", "CX_DRIVER_COMPARE_SORT_BY"),
            )
        ]

    cases_json = first_env(
        f"{prefix}_DRIVER_COMPARE_CASES_JSON",
        "CX_DRIVER_COMPARE_CASES_JSON",
    )
    if cases_json:
        return parse_json_cases(
            cases_json,
            f"{prefix}_DRIVER_COMPARE_CASES_JSON or CX_DRIVER_COMPARE_CASES_JSON",
        )
    return None


def default_cases(backend: str, rows: int, include_tpch: bool) -> list[Case]:
    custom = load_custom_cases(backend)
    if custom is not None:
        return custom

    prefix = backend.upper()
    table = first_env(f"{prefix}_BENCH_TABLE", "CX_DRIVER_COMPARE_TABLE") or BENCH_TABLE[backend]
    partition_on = first_env(f"{prefix}_BENCH_PARTITION_ON", "CX_DRIVER_COMPARE_PARTITION_ON") or "id"
    cases = [
        Case(
            name="primitive",
            query=f"select {PRIMITIVE_COLUMNS} from {table} where id <= {rows}",
            partition_on=partition_on,
            partition_range=(1, rows),
            expected_rows=rows,
            sort_by=partition_on,
        ),
        Case(
            name="mixed",
            query=f"select {MIXED_COLUMNS} from {table} where id <= {rows}",
            partition_on=partition_on,
            partition_range=(1, rows),
            expected_rows=rows,
            sort_by=partition_on,
        ),
    ]

    tpch_table = first_env(f"{prefix}_TPCH_TABLE", "TPCH_TABLE")
    if include_tpch or tpch_table:
        tpch_table = tpch_table or "lineitem"
        tpch_partition_on = (
            first_env(f"{prefix}_TPCH_PARTITION_ON", "TPCH_PARTITION_ON") or "l_orderkey"
        )
        cases.append(
            Case(
                name="tpch-lineitem",
                query=f"select * from {tpch_table}",
                partition_on=tpch_partition_on,
                sort_by=tpch_partition_on,
            )
        )
    return cases


def build_routes(
    config: BackendConfig,
    case: Case,
    include_partitioned: bool,
    route_filter: set[str] | None,
) -> list[RouteSpec]:
    routes: list[RouteSpec] = []

    def add(route: RouteSpec) -> None:
        if route_filter is None or route.name in route_filter or route.kind in route_filter:
            routes.append(route)

    if config.raw_odbc_conn:
        add(RouteSpec(config.name, "pandas-pyodbc", "pandas-pyodbc", config.raw_odbc_conn))
        add(RouteSpec(config.name, "polars-pyodbc", "polars-pyodbc", config.raw_odbc_conn))
        add(RouteSpec(config.name, "polars-arrow-odbc", "polars-arrow-odbc", config.raw_odbc_conn))
    if config.sqlalchemy_url:
        add(
            RouteSpec(
                config.name,
                "pandas-sqlalchemy",
                "pandas-sqlalchemy",
                config.sqlalchemy_url,
            )
        )
    if config.connectorx_url:
        add(
            RouteSpec(
                config.name,
                f"connectorx-{config.name}",
                "connectorx",
                config.connectorx_url,
            )
        )
        if include_partitioned and case.partition_on:
            add(
                RouteSpec(
                    config.name,
                    f"connectorx-{config.name}-partitioned",
                    "connectorx",
                    config.connectorx_url,
                    partitioned=True,
                )
            )
    if config.generic_odbc_url:
        add(
            RouteSpec(
                config.name,
                "connectorx-odbc",
                "connectorx",
                config.generic_odbc_url,
            )
        )
        if include_partitioned and case.partition_on:
            add(
                RouteSpec(
                    config.name,
                    "connectorx-odbc-partitioned",
                    "connectorx",
                    config.generic_odbc_url,
                    partitioned=True,
                )
            )
    return routes


def arrow_execute_options() -> dict[str, Any] | None:
    payload = env("CX_DRIVER_COMPARE_ARROW_EXECUTE_OPTIONS_JSON")
    if not payload:
        return None
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"invalid JSON in CX_DRIVER_COMPARE_ARROW_EXECUTE_OPTIONS_JSON: {exc}"
        ) from exc
    if not isinstance(parsed, dict):
        raise ValueError("CX_DRIVER_COMPARE_ARROW_EXECUTE_OPTIONS_JSON must be an object")
    return parsed


def read_route(route: RouteSpec, case: Case, partition_num: int) -> Any:
    if route.kind == "pandas-pyodbc":
        import pandas as pd
        import pyodbc

        conn = pyodbc.connect(route.connection, autocommit=True)
        try:
            return pd.read_sql_query(case.query, conn)
        finally:
            conn.close()

    if route.kind == "pandas-sqlalchemy":
        import pandas as pd
        import sqlalchemy as sa

        engine = sa.create_engine(route.connection)
        try:
            with engine.connect() as conn:
                return pd.read_sql_query(case.query, conn)
        finally:
            engine.dispose()

    if route.kind == "polars-pyodbc":
        import polars as pl
        import pyodbc

        conn = pyodbc.connect(route.connection, autocommit=True)
        try:
            return pl.read_database(query=case.query, connection=conn)
        finally:
            conn.close()

    if route.kind == "polars-arrow-odbc":
        import polars as pl

        kwargs: dict[str, Any] = {"query": case.query, "connection": route.connection}
        execute_options = arrow_execute_options()
        if execute_options is not None:
            kwargs["execute_options"] = execute_options
        return pl.read_database(**kwargs)

    if route.kind == "connectorx":
        import connectorx as cx

        kwargs: dict[str, Any] = {"return_type": "polars"}
        if route.partitioned:
            kwargs["partition_on"] = case.partition_on
            kwargs["partition_num"] = partition_num
            if case.partition_range is not None:
                kwargs["partition_range"] = case.partition_range
        return cx.read_sql(route.connection, case.query, **kwargs)

    raise ValueError(f"unknown route kind: {route.kind}")


def to_polars(frame: Any) -> Any:
    import polars as pl

    module = frame.__class__.__module__
    if module.startswith("polars"):
        return frame
    if module.startswith("pandas"):
        return pl.from_pandas(frame)
    if module.startswith("pyarrow"):
        return pl.from_arrow(frame)
    raise TypeError(f"cannot summarize frame of type {type(frame)!r}")


def stringify(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def frame_min_max(df: Any) -> dict[str, dict[str, str | None]]:
    values: dict[str, dict[str, str | None]] = {}
    for column in df.columns:
        series = df[column]
        try:
            values[column] = {
                "min": stringify(series.min()),
                "max": stringify(series.max()),
            }
        except Exception:
            continue
    return values


def sample_hash(df: Any, sample_rows: int = 5) -> str | None:
    if df.height == 0 or df.width == 0:
        return None
    try:
        samples = []
        for row in df.head(sample_rows).to_dicts():
            samples.append({key: stringify(value) for key, value in row.items()})
        if df.height > sample_rows:
            for row in df.tail(sample_rows).to_dicts():
                samples.append({key: stringify(value) for key, value in row.items()})
        payload = json.dumps(samples, sort_keys=True, default=str).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()
    except Exception:
        return None


def summarize_frame(frame: Any) -> dict[str, Any]:
    df = to_polars(frame)
    null_row = df.null_count().to_dicts()[0] if df.width else {}
    estimated_size_mb = None
    try:
        estimated_size_mb = df.estimated_size() / (1024 * 1024)
    except Exception:
        pass
    return {
        "rows": int(df.height),
        "cols": int(df.width),
        "columns": list(df.columns),
        "schema": {name: str(dtype) for name, dtype in df.schema.items()},
        "null_counts": {key: int(value) for key, value in null_row.items()},
        "min_max": frame_min_max(df),
        "sample_hash": sample_hash(df),
        "estimated_frame_mb": estimated_size_mb,
    }


def peak_rss_mb() -> float | None:
    if resource is not None:
        try:
            value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        except Exception:
            value = None
        if value is not None:
            if platform.system() == "Darwin":
                return value / (1024 * 1024)
            return value / 1024

    try:
        import psutil

        return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
    except Exception:
        return None


def run_worker(
    route_payload: dict[str, Any],
    case_payload: dict[str, Any],
    iterations: int,
    warmups: int,
    partition_num: int,
    queue: Any,
) -> None:
    route = RouteSpec(**route_payload)
    case = Case(**case_payload)
    try:
        for _ in range(warmups):
            read_route(route, case, partition_num)

        samples = []
        best_summary = None
        best_elapsed = math.inf
        for index in range(iterations):
            start = time.perf_counter()
            frame = read_route(route, case, partition_num)
            elapsed = time.perf_counter() - start
            summary = summarize_frame(frame)
            del frame
            samples.append(
                {
                    "iteration": index + 1,
                    "elapsed_s": elapsed,
                    "rows": summary["rows"],
                    "rows_per_s": summary["rows"] / elapsed if elapsed > 0 else 0.0,
                }
            )
            if elapsed < best_elapsed:
                best_elapsed = elapsed
                best_summary = summary

        queue.put(
            {
                "ok": True,
                "iterations": samples,
                "summary": best_summary,
                "peak_rss_mb": peak_rss_mb(),
            }
        )
    except BaseException as exc:  # pragma: no cover - exercised by live routes.
        queue.put(
            {
                "ok": False,
                "error": str(exc),
                "traceback": traceback.format_exc(),
                "peak_rss_mb": peak_rss_mb(),
            }
        )


def run_route_in_child(
    route: RouteSpec,
    case: Case,
    iterations: int,
    warmups: int,
    partition_num: int,
    timeout_secs: int | None,
) -> dict[str, Any]:
    # ODBC drivers are C libraries and may not be fork-safe after prepare has
    # initialized driver-manager state in the parent process.
    context = mp.get_context("spawn")
    queue = context.Queue()
    process = context.Process(
        target=run_worker,
        args=(
            asdict(route),
            asdict(case),
            iterations,
            warmups,
            partition_num,
            queue,
        ),
    )
    started_at = datetime.now(timezone.utc)
    process.start()
    process.join(timeout_secs if timeout_secs and timeout_secs > 0 else None)
    finished_at = datetime.now(timezone.utc)
    if process.is_alive():
        process.terminate()
        process.join(10)
        return {
            "ok": False,
            "error": f"route timed out after {timeout_secs}s",
            "traceback": None,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "exit_code": process.exitcode,
        }

    payload: dict[str, Any]
    try:
        payload = queue.get_nowait()
    except queue_module.Empty:
        payload = {
            "ok": False,
            "error": f"worker exited without a result; exit code {process.exitcode}",
            "traceback": None,
        }
    payload["started_at"] = started_at.isoformat()
    payload["finished_at"] = finished_at.isoformat()
    payload["exit_code"] = process.exitcode
    return payload


def route_result(
    backend: str,
    case: Case,
    route: RouteSpec,
    worker_payload: dict[str, Any],
    partition_num: int,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "backend": backend,
        "case": case.name,
        "route": route.name,
        "route_kind": route.kind,
        "partitioned": route.partitioned,
        "partition_num": partition_num if route.partitioned else None,
        "query": case.query,
        "expected_rows": case.expected_rows,
        "partition_on": case.partition_on if route.partitioned else None,
        "status": "ok" if worker_payload.get("ok") else "error",
        "error": worker_payload.get("error"),
        "traceback": worker_payload.get("traceback"),
        "started_at": worker_payload.get("started_at"),
        "finished_at": worker_payload.get("finished_at"),
        "exit_code": worker_payload.get("exit_code"),
        "peak_rss_mb": worker_payload.get("peak_rss_mb"),
    }
    if not worker_payload.get("ok"):
        return result

    iterations = worker_payload["iterations"]
    elapsed_values = [item["elapsed_s"] for item in iterations]
    summary = worker_payload["summary"]
    median_elapsed = statistics.median(elapsed_values)
    best_elapsed = min(elapsed_values)
    mean_elapsed = statistics.mean(elapsed_values)
    rows = summary["rows"]
    result.update(
        {
            "rows": rows,
            "cols": summary["cols"],
            "elapsed_s_median": median_elapsed,
            "elapsed_s_best": best_elapsed,
            "elapsed_s_mean": mean_elapsed,
            "rows_per_s_median": rows / median_elapsed if median_elapsed > 0 else 0.0,
            "rows_per_s_best": rows / best_elapsed if best_elapsed > 0 else 0.0,
            "estimated_frame_mb": summary.get("estimated_frame_mb"),
            "schema": summary["schema"],
            "columns": summary["columns"],
            "null_counts": summary["null_counts"],
            "min_max": summary["min_max"],
            "sample_hash": summary["sample_hash"],
            "iterations": iterations,
        }
    )
    return result


def execute_ignore(cursor: Any, statement: str) -> None:
    try:
        cursor.execute(statement)
    except Exception:
        try:
            cursor.connection.rollback()
        except Exception:
            pass
        pass


def prepare_backend(
    config: BackendConfig,
    rows: int,
    batch_size: int,
    fast_executemany: bool,
) -> dict[str, Any]:
    if not config.raw_odbc_conn:
        return {
            "backend": config.name,
            "status": "skipped",
            "message": "raw ODBC connection is not configured",
        }

    import pyodbc

    started = time.perf_counter()
    conn = pyodbc.connect(config.raw_odbc_conn, autocommit=config.name == "sybase")
    cursor = conn.cursor()
    try:
        if config.name == "postgres":
            execute_ignore(cursor, "drop table if exists cx_bench_perf")
            cursor.execute(
                """
                create table cx_bench_perf (
                    id integer primary key,
                    flag integer not null,
                    int_v integer not null,
                    bigint_v bigint not null,
                    real_v real not null,
                    double_v double precision not null,
                    amount numeric(18, 4) not null,
                    name varchar(64) not null,
                    payload varchar(128) not null,
                    payload_bytes bytea not null,
                    created_at timestamp not null
                )
                """
            )
            table = "cx_bench_perf"
        elif config.name == "db2":
            execute_ignore(cursor, "drop table cx_bench_perf")
            cursor.execute(
                """
                create table cx_bench_perf (
                    id integer not null primary key,
                    flag integer not null,
                    int_v integer not null,
                    bigint_v bigint not null,
                    real_v real not null,
                    double_v double not null,
                    amount decimal(18, 4) not null,
                    name varchar(64) not null,
                    payload varchar(128) not null,
                    payload_bytes varbinary(64) not null,
                    created_at timestamp not null
                )
                """
            )
            table = "cx_bench_perf"
        elif config.name == "sybase":
            execute_ignore(
                cursor,
                "if object_id('dbo.cx_bench_perf') is not null drop table dbo.cx_bench_perf",
            )
            cursor.execute(
                """
                create table dbo.cx_bench_perf (
                    id int not null primary key,
                    flag int not null,
                    int_v int not null,
                    bigint_v bigint not null,
                    real_v real not null,
                    double_v float not null,
                    amount numeric(18, 4) not null,
                    name varchar(64) not null,
                    payload varchar(128) not null,
                    payload_bytes image not null,
                    created_at datetime not null
                )
                """
            )
            table = "dbo.cx_bench_perf"
        else:
            raise ValueError(f"unsupported prepare backend: {config.name}")
        conn.commit()

        if fast_executemany and hasattr(cursor, "fast_executemany"):
            cursor.fast_executemany = True

        insert_sql = (
            f"insert into {table} "
            "(id, flag, int_v, bigint_v, real_v, double_v, amount, name, "
            "payload, payload_bytes, created_at) "
            "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        inserted = 0
        base_time = datetime(2024, 1, 1)
        for start in range(1, rows + 1, batch_size):
            stop = min(rows, start + batch_size - 1)
            batch = []
            for row_id in range(start, stop + 1):
                binary_value = pyodbc.Binary(bytes([row_id % 256]) * 64)
                batch.append(
                    (
                        row_id,
                        row_id % 2,
                        row_id * 3,
                        row_id * 100_000,
                        row_id / 3.0,
                        row_id / 7.0,
                        (Decimal(row_id) / Decimal("11")).quantize(Decimal("0.0001")),
                        f"name-{row_id}",
                        "x" * 64,
                        binary_value,
                        base_time + timedelta(seconds=row_id),
                    )
                )
            cursor.executemany(insert_sql, batch)
            conn.commit()
            inserted += len(batch)
        return {
            "backend": config.name,
            "status": "ok",
            "rows": inserted,
            "elapsed_s": time.perf_counter() - started,
            "table": table,
        }
    finally:
        cursor.close()
        conn.close()


def selected_backends(args: argparse.Namespace) -> list[str]:
    requested = args.backend
    if not requested:
        env_backends = env("CX_DRIVER_COMPARE_BACKENDS")
        requested = env_backends.split(",") if env_backends else ["all"]
    requested = [item.strip().lower() for item in requested if item.strip()]
    invalid = [item for item in requested if item not in BACKENDS and item != "all"]
    if invalid:
        valid = ", ".join((*BACKENDS, "all"))
        raise ValueError(f"invalid backend(s): {', '.join(invalid)}; valid values: {valid}")
    if "all" in requested:
        configured = []
        for backend in BACKENDS:
            config = backend_config(backend)
            if any(
                (
                    config.connectorx_url,
                    config.generic_odbc_url,
                    config.raw_odbc_conn,
                    config.sqlalchemy_url,
                )
            ):
                configured.append(backend)
        return configured
    return requested


def run_git(args: list[str]) -> str | None:
    try:
        result = subprocess.run(
            ["git", *args],
            capture_output=True,
            text=True,
            check=False,
            errors="replace",
        )
    except OSError:
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def package_versions() -> dict[str, str | None]:
    versions = {}
    for package in PACKAGE_NAMES:
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = None
    return versions


def registered_odbc_drivers() -> list[str]:
    try:
        result = subprocess.run(
            ["odbcinst", "-q", "-d"],
            capture_output=True,
            text=True,
            check=False,
            errors="replace",
        )
    except OSError:
        return []
    if result.returncode != 0:
        return []
    return [line.strip()[1:-1] for line in result.stdout.splitlines() if line.strip()]


def collect_metadata(
    args: argparse.Namespace,
    configs: dict[str, BackendConfig],
) -> dict[str, Any]:
    uname = platform.uname()
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "command": " ".join(sys.argv),
        "rows": args.rows,
        "iterations": args.iterations,
        "warmups": args.warmups,
        "partition_num": args.partition_num,
        "include_tpch": args.include_tpch,
        "configured_backends": {
            name: {
                "connectorx_url": bool(config.connectorx_url),
                "generic_odbc_url": bool(config.generic_odbc_url),
                "raw_odbc_conn": bool(config.raw_odbc_conn),
                "sqlalchemy_url": bool(config.sqlalchemy_url),
            }
            for name, config in configs.items()
        },
        "os": {
            "system": uname.system,
            "release": uname.release,
            "version": uname.version,
            "machine": uname.machine,
        },
        "cpu": {
            "processor": platform.processor(),
            "python_reported_cpu_count": os.cpu_count(),
        },
        "python": sys.version,
        "packages": package_versions(),
        "registered_odbc_drivers": registered_odbc_drivers(),
        "git": {
            "branch": run_git(["branch", "--show-current"]),
            "commit": run_git(["rev-parse", "HEAD"]),
        },
    }


def compare_group(results: list[dict[str, Any]]) -> list[str]:
    ok_results = [result for result in results if result["status"] == "ok"]
    if len(ok_results) < 2:
        return []
    preferred = ["pandas-pyodbc", "polars-arrow-odbc"]
    reference = None
    for name in preferred:
        reference = next((result for result in ok_results if result["route"] == name), None)
        if reference is not None:
            break
    if reference is None:
        reference = ok_results[0]

    failures = []
    for result in ok_results:
        expected_rows = result.get("expected_rows")
        if expected_rows is not None and result.get("rows") != expected_rows:
            failures.append(
                f"{result['backend']}/{result['case']} {result['route']}: row count "
                f"differs from expected_rows ({result.get('rows')} != {expected_rows})"
            )
        if result is reference:
            continue
        prefix = f"{result['backend']}/{result['case']} {result['route']}"
        if result.get("rows") != reference.get("rows"):
            failures.append(
                f"{prefix}: row count differs from {reference['route']} "
                f"({result.get('rows')} != {reference.get('rows')})"
            )
        if result.get("cols") != reference.get("cols"):
            failures.append(
                f"{prefix}: column count differs from {reference['route']} "
                f"({result.get('cols')} != {reference.get('cols')})"
            )
        if result.get("columns") != reference.get("columns"):
            failures.append(f"{prefix}: columns differ from {reference['route']}")
        if result.get("null_counts") != reference.get("null_counts"):
            failures.append(f"{prefix}: null counts differ from {reference['route']}")
    return failures


def collect_mismatches(results: list[dict[str, Any]]) -> list[str]:
    mismatches = []
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for result in results:
        groups.setdefault((result["backend"], result["case"]), []).append(result)
    for group in groups.values():
        mismatches.extend(compare_group(group))
    return mismatches


def format_float(value: Any, digits: int = 3) -> str:
    if value is None:
        return ""
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def format_rate(value: Any) -> str:
    if value is None:
        return ""
    value = float(value)
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.2f}k"
    return f"{value:.2f}"


def speedup_label(route: dict[str, Any], baseline: dict[str, Any] | None) -> str:
    if (
        baseline is None
        or baseline.get("elapsed_s_median") is None
        or route.get("elapsed_s_median") is None
        or route["elapsed_s_median"] <= 0
    ):
        return ""
    return f"{baseline['elapsed_s_median'] / route['elapsed_s_median']:.2f}x"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_csv(path: Path, results: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "backend",
        "case",
        "route",
        "route_kind",
        "partitioned",
        "partition_num",
        "status",
        "expected_rows",
        "rows",
        "cols",
        "elapsed_s_median",
        "elapsed_s_best",
        "elapsed_s_mean",
        "rows_per_s_median",
        "rows_per_s_best",
        "peak_rss_mb",
        "estimated_frame_mb",
        "error",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow({field: result.get(field) for field in fieldnames})


def markdown_table(results: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Route | Status | Rows | Median s | Rows/s | Peak RSS MB | Frame MB | Speedup vs pandas-pyodbc |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    ok_results = [result for result in results if result["status"] == "ok"]
    baseline = next((result for result in ok_results if result["route"] == "pandas-pyodbc"), None)
    sorted_results = sorted(
        results,
        key=lambda item: (
            item["status"] != "ok",
            item.get("elapsed_s_median") if item.get("elapsed_s_median") is not None else math.inf,
            item["route"],
        ),
    )
    for result in sorted_results:
        status = result["status"]
        route = result["route"]
        if result.get("partitioned"):
            route = f"{route} ({result.get('partition_num')} partitions)"
        lines.append(
            "| `{route}` | {status} | {rows} | {elapsed} | {rate} | {rss} | {frame_mb} | {speedup} |".format(
                route=route,
                status=status,
                rows=result.get("rows") or "",
                elapsed=format_float(result.get("elapsed_s_median")),
                rate=format_rate(result.get("rows_per_s_median")),
                rss=format_float(result.get("peak_rss_mb"), 1),
                frame_mb=format_float(result.get("estimated_frame_mb"), 1),
                speedup=speedup_label(result, baseline),
            )
        )
    return lines


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    metadata = payload["metadata"]
    prepare_results = payload.get("prepare") or []
    results = payload["results"]
    mismatches = payload["mismatches"]
    lines = [
        "# ODBC Driver Comparison Benchmark",
        "",
        "This report compares DataFrame ingestion routes over normal ODBC drivers and ConnectorX.",
        "",
        "## Environment",
        "",
        f"- Generated at: `{metadata['generated_at']}`",
        f"- Git commit: `{metadata['git'].get('commit') or 'unknown'}`",
        f"- Git branch: `{metadata['git'].get('branch') or 'unknown'}`",
        f"- OS: `{metadata['os']['system']} {metadata['os']['release']} {metadata['os']['machine']}`",
        f"- Python: `{metadata['python'].split()[0]}`",
        f"- Rows per prepared case: `{metadata['rows']}`",
        f"- Iterations: `{metadata['iterations']}` measured, `{metadata['warmups']}` warmup",
        f"- ConnectorX partition count: `{metadata['partition_num']}`",
        "",
        "Package versions:",
        "",
        "| Package | Version |",
        "| --- | --- |",
    ]
    for package, version in metadata["packages"].items():
        lines.append(f"| `{package}` | `{version or 'not installed'}` |")

    if prepare_results:
        lines.extend(
            [
                "",
                "## Prepared Data",
                "",
                "| Backend | Status | Table | Rows | Elapsed s |",
                "| --- | --- | --- | ---: | ---: |",
            ]
        )
        for prepared in prepare_results:
            lines.append(
                "| `{backend}` | {status} | `{table}` | {rows} | {elapsed} |".format(
                    backend=prepared.get("backend"),
                    status=prepared.get("status"),
                    table=prepared.get("table") or "",
                    rows=prepared.get("rows") or "",
                    elapsed=format_float(prepared.get("elapsed_s")),
                )
            )

    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for result in results:
        groups.setdefault((result["backend"], result["case"]), []).append(result)

    lines.extend(["", "## Results", ""])
    for (backend, case), group in sorted(groups.items()):
        lines.extend([f"### {backend} / {case}", ""])
        lines.extend(markdown_table(group))
        lines.append("")

    route_errors = [result for result in results if result["status"] != "ok"]
    if route_errors:
        lines.extend(
            [
                "## Route Errors",
                "",
                "| Backend | Case | Route | Error |",
                "| --- | --- | --- | --- |",
            ]
        )
        for result in route_errors:
            error = str(result.get("error") or "").replace("\n", " ")
            lines.append(
                f"| `{result['backend']}` | `{result['case']}` | `{result['route']}` | {error} |"
            )
        lines.append("")

    lines.extend(["## Correctness Checks", ""])
    if mismatches:
        for mismatch in mismatches:
            lines.append(f"- {mismatch}")
    else:
        lines.append("No row-count, column-count, column-order, or null-count mismatches were detected.")

    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- `pandas-sqlalchemy` only runs when a database-specific SQLAlchemy URL is configured.",
            "- `polars-arrow-odbc` uses Polars' ODBC/Arrow path and is the strongest generic ODBC baseline.",
            "- ConnectorX routes return Polars directly with `connectorx.read_sql(..., return_type=\"polars\")`.",
            "- Connection setup is included in every timed route, matching the end-to-end user call shape.",
            "- For publication-quality numbers, run the same workload on an otherwise idle machine and compare medians from repeated benchmark invocations.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backend",
        action="append",
        choices=BACKENDS + ("all",),
        help="Backend to run. May be repeated. Defaults to CX_DRIVER_COMPARE_BACKENDS or all configured backends.",
    )
    parser.add_argument("--case", action="append", help="Case name to run. Defaults to all cases.")
    parser.add_argument(
        "--route",
        action="append",
        help="Route name or kind to run. May be repeated. Defaults to all configured routes.",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=positive_int(env("CX_DRIVER_COMPARE_ROWS"), DEFAULT_ROWS),
        help="Rows read by the default prepared-table cases.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=positive_int(env("CX_DRIVER_COMPARE_ITERATIONS"), DEFAULT_ITERATIONS),
        help="Measured iterations per route.",
    )
    parser.add_argument(
        "--warmups",
        type=int,
        default=positive_int(env("CX_DRIVER_COMPARE_WARMUPS"), DEFAULT_WARMUPS),
        help="Warmup iterations per route.",
    )
    parser.add_argument(
        "--partition-num",
        type=int,
        default=positive_int(env("CX_DRIVER_COMPARE_PARTITION_NUM"), DEFAULT_PARTITIONS),
        help="ConnectorX partition count for partitioned routes.",
    )
    parser.add_argument("--include-tpch", action="store_true", help="Include a TPC-H lineitem scan case.")
    parser.add_argument("--no-partitioned", action="store_true", help="Skip ConnectorX partitioned routes.")
    parser.add_argument(
        "--prepare-rows",
        type=int,
        default=positive_int(env("CX_DRIVER_COMPARE_PREPARE_ROWS"), 0),
        help="Drop/create/fill cx_bench_perf through pyodbc before running.",
    )
    parser.add_argument(
        "--prepare-batch-size",
        type=int,
        default=positive_int(
            env("CX_DRIVER_COMPARE_PREPARE_BATCH_SIZE"),
            DEFAULT_PREPARE_BATCH_SIZE,
        ),
        help="Rows inserted per pyodbc batch while preparing benchmark data.",
    )
    parser.add_argument(
        "--prepare-fast-executemany",
        action="store_true",
        help="Enable pyodbc fast_executemany while loading benchmark data.",
    )
    parser.add_argument(
        "--route-timeout-secs",
        type=int,
        default=positive_int(env("CX_DRIVER_COMPARE_ROUTE_TIMEOUT_SECS"), 0),
        help="Optional timeout per route/case process. 0 disables the timeout.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(env("CX_DRIVER_COMPARE_OUTPUT_DIR") or "target/odbc-driver-comparison"),
    )
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--output-csv", type=Path)
    parser.add_argument("--output-markdown", type=Path)
    parser.add_argument(
        "--warn-only",
        action="store_true",
        help="Exit zero even when correctness mismatches or route errors are present.",
    )
    return parser.parse_args()


def run(args: argparse.Namespace) -> int:
    if args.rows < 1:
        raise ValueError("--rows must be at least 1")
    if args.iterations < 1:
        raise ValueError("--iterations must be at least 1")
    if args.warmups < 0:
        raise ValueError("--warmups must be zero or greater")
    if args.partition_num < 1:
        raise ValueError("--partition-num must be at least 1")

    backends = selected_backends(args)
    if not backends:
        print("No configured backends found. Set POSTGRES_URL/ODBC_CONN, DB2_URL, or SYBASE_URL.", file=sys.stderr)
        return 2

    configs = {backend: backend_config(backend) for backend in backends}
    route_filter = set(args.route) if args.route else None
    prepare_results = []
    if args.prepare_rows:
        for backend in backends:
            print(f"Preparing {backend} benchmark table with {args.prepare_rows} rows...")
            prepare_results.append(
                prepare_backend(
                    configs[backend],
                    args.prepare_rows,
                    args.prepare_batch_size,
                    args.prepare_fast_executemany,
                )
            )

    results = []
    for backend in backends:
        config = configs[backend]
        cases = default_cases(backend, args.rows, args.include_tpch)
        for case in cases:
            if args.case and case.name not in args.case:
                continue
            routes = build_routes(
                config,
                case,
                include_partitioned=not args.no_partitioned,
                route_filter=route_filter,
            )
            if not routes:
                print(f"SKIP {backend}/{case.name}: no configured routes")
                continue
            for route in routes:
                print(f"RUN {backend}/{case.name}/{route.name}")
                worker_payload = run_route_in_child(
                    route,
                    case,
                    args.iterations,
                    args.warmups,
                    args.partition_num,
                    args.route_timeout_secs,
                )
                result = route_result(backend, case, route, worker_payload, args.partition_num)
                results.append(result)
                if result["status"] == "ok":
                    print(
                        "  rows={rows} median={elapsed}s rows/s={rate}".format(
                            rows=result["rows"],
                            elapsed=format_float(result["elapsed_s_median"]),
                            rate=format_rate(result["rows_per_s_median"]),
                        )
                    )
                else:
                    print(f"  ERROR {result.get('error')}", file=sys.stderr)

    if not results:
        print("No benchmark routes ran. Check route configuration.", file=sys.stderr)
        return 2

    mismatches = collect_mismatches(results)
    metadata = collect_metadata(args, configs)
    payload = {
        "schema_version": 1,
        "metadata": metadata,
        "prepare": prepare_results,
        "mismatches": mismatches,
        "results": results,
    }

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_json = args.output_json or args.output_dir / f"odbc-driver-comparison-{timestamp}.json"
    output_csv = args.output_csv or args.output_dir / f"odbc-driver-comparison-{timestamp}.csv"
    output_markdown = (
        args.output_markdown or args.output_dir / f"odbc-driver-comparison-{timestamp}.md"
    )
    write_json(output_json, payload)
    write_csv(output_csv, results)
    write_markdown(output_markdown, payload)

    print(f"Wrote JSON: {output_json}")
    print(f"Wrote CSV: {output_csv}")
    print(f"Wrote Markdown report: {output_markdown}")

    if mismatches:
        print("\nCorrectness mismatches:", file=sys.stderr)
        for mismatch in mismatches:
            print(f"- {mismatch}", file=sys.stderr)

    route_errors = [result for result in results if result["status"] != "ok"]
    if (mismatches or route_errors) and not args.warn_only:
        return 1
    return 0


def main() -> int:
    args = parse_args()
    try:
        return run(args)
    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
