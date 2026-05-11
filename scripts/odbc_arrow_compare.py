#!/usr/bin/env python3
"""Compare ConnectorX ODBC-family routes with Polars arrow-odbc.

The script is intentionally dependency-light: it expects a Python environment
with polars, connectorx, and arrow-odbc available. Configure routes with
environment variables, for example:

  DB2_URL="db2://..." DB2_ODBC_CONN="Driver={IBM DB2 ODBC DRIVER};..." \
    scripts/odbc_arrow_compare.py --backend db2

  SYBASE_URL="sybase://..." SYBASE_ODBC_CONN="Driver={FreeTDS};..." \
    scripts/odbc_arrow_compare.py --backend sybase

Generic ODBC can use ODBC_URL and ODBC_CONN. For DB2/Sybase, the generic ODBC
route defaults to an odbc_connect URL built from the raw ODBC connection string
unless DB2_GENERIC_ODBC_URL or SYBASE_GENERIC_ODBC_URL is set.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable
from urllib.parse import quote


try:
    import resource
except ImportError:  # pragma: no cover - Windows.
    resource = None


pl: Any = None


BACKENDS = ("db2", "sybase", "odbc")


def load_polars() -> None:
    global pl
    if pl is not None:
        return
    try:
        import polars as polars_module
    except ImportError as exc:  # pragma: no cover - exercised by users.
        raise SystemExit(
            "polars is required. Install connectorx-python with the polars extra "
            "and install arrow-odbc for the baseline route."
        ) from exc
    pl = polars_module


@dataclass(frozen=True)
class BackendConfig:
    name: str
    dedicated_url: str | None
    generic_url: str | None
    raw_odbc_conn: str | None


@dataclass(frozen=True)
class Case:
    name: str
    query: str
    partition_on: str | None = None
    partition_range: tuple[int, int] | None = None


@dataclass(frozen=True)
class Route:
    name: str
    runner: Callable[[], pl.DataFrame]
    partitions: int | None = None


@dataclass
class RunResult:
    backend: str
    case: str
    route: str
    rows: int
    cols: int
    elapsed_s: float
    rows_per_s: float
    peak_rss_mb: float | None
    schema: dict[str, str]
    null_counts: dict[str, int]
    min_max: dict[str, dict[str, str | None]]
    row_hash: int | None
    partitions: int | None


def env(name: str) -> str | None:
    value = os.environ.get(name)
    return value if value else None


def raw_odbc_url(raw_conn: str) -> str:
    return "odbc://localhost/?odbc_connect=" + quote(raw_conn, safe="")


def backend_config(name: str) -> BackendConfig:
    prefix = name.upper()
    if name == "odbc":
        raw_conn = env("ODBC_CONN") or env("ODBC_ODBC_CONN")
        generic_url = env("ODBC_URL")
        if generic_url is None and raw_conn is not None:
            generic_url = raw_odbc_url(raw_conn)
        return BackendConfig(
            name=name,
            dedicated_url=None,
            generic_url=generic_url,
            raw_odbc_conn=raw_conn,
        )

    raw_conn = env(f"{prefix}_ODBC_CONN")
    generic_url = env(f"{prefix}_GENERIC_ODBC_URL")
    if generic_url is None and raw_conn is not None:
        generic_url = raw_odbc_url(raw_conn)
    return BackendConfig(
        name=name,
        dedicated_url=env(f"{prefix}_URL"),
        generic_url=generic_url,
        raw_odbc_conn=raw_conn,
    )


def parse_partition_range(value: str | None) -> tuple[int, int] | None:
    if not value:
        return None
    try:
        low, high = value.split(",", 1)
    except ValueError:
        raise ValueError(
            f"Invalid partition range format: {value!r}. Expected 'low,high'."
        ) from None
    return int(low.strip()), int(high.strip())


def parse_positive_int(value: str, name: str) -> int:
    try:
        parsed = int(value)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {value!r}") from None
    if parsed < 1:
        raise ValueError(f"{name} must be at least 1, got {parsed}")
    return parsed


def parse_case_partition_range(raw_range: Any, env_name: str) -> tuple[int, int] | None:
    if raw_range is None:
        return None
    if (
        not isinstance(raw_range, list)
        or len(raw_range) != 2
        or not all(isinstance(value, int) for value in raw_range)
    ):
        raise ValueError(
            f"{env_name} partition_range must be a two-item integer array, "
            f"got {raw_range!r}"
        )
    return raw_range[0], raw_range[1]


def load_custom_cases(backend: str) -> list[Case] | None:
    prefix = backend.upper()
    query = env(f"{prefix}_COMPARE_QUERY") or env("CX_ODBC_COMPARE_QUERY")
    if query:
        return [
            Case(
                name="custom",
                query=query,
                partition_on=env(f"{prefix}_COMPARE_PARTITION_ON")
                or env("CX_ODBC_COMPARE_PARTITION_ON"),
                partition_range=parse_partition_range(
                    env(f"{prefix}_COMPARE_PARTITION_RANGE")
                    or env("CX_ODBC_COMPARE_PARTITION_RANGE")
                ),
            )
        ]

    cases_json = env(f"{prefix}_COMPARE_CASES_JSON") or env("CX_ODBC_COMPARE_CASES_JSON")
    if not cases_json:
        return None
    cases_env_name = f"{prefix}_COMPARE_CASES_JSON or CX_ODBC_COMPARE_CASES_JSON"
    try:
        payload = json.loads(cases_json)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON in {cases_env_name}: {exc}"
        ) from exc
    if not isinstance(payload, list):
        raise ValueError(
            f"{cases_env_name} must be a JSON array of objects with name and query"
        )
    cases = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise ValueError(f"{cases_env_name}[{index}] must be an object")
        name = item.get("name")
        query_value = item.get("query")
        if not isinstance(name, str) or not name:
            raise ValueError(f"{cases_env_name}[{index}].name must be a non-empty string")
        if not isinstance(query_value, str) or not query_value:
            raise ValueError(f"{cases_env_name}[{index}].query must be a non-empty string")
        partition_on = item.get("partition_on")
        if partition_on is not None and not isinstance(partition_on, str):
            raise ValueError(f"{cases_env_name}[{index}].partition_on must be a string")
        cases.append(
            Case(
                name=name,
                query=query_value,
                partition_on=partition_on,
                partition_range=parse_case_partition_range(
                    item.get("partition_range"),
                    f"{cases_env_name}[{index}]",
                ),
            )
        )
    return cases


def default_cases(backend: str) -> list[Case]:
    custom = load_custom_cases(backend)
    if custom is not None:
        return custom

    if backend == "db2":
        return [
            Case(
                "edge",
                "select id, amount, created_at, event_time, payload, wide_text, "
                "nullable_text, long_text from cx_odbc_edge order by id",
            ),
            Case(
                "partition",
                "select TRADE_ID, COB_DATE, CREATED_TS, TRADE_LABEL "
                "from RISK_SCHEMA.RISK_RESULTS where TRADE_ID is not null",
                partition_on="TRADE_ID",
            ),
        ]
    if backend == "sybase":
        return [
            Case(
                "edge",
                "select id, amount, created_at, event_time, payload, wide_text, "
                "nullable_text, long_text from dbo.cx_odbc_edge order by id",
            ),
            Case(
                "temporal",
                "select id, date_v, time_v, datetime_v, smalldatetime_v, "
                "bigtime_v, bigdatetime_v from dbo.cx_odbc_temporal_edge order by id",
            ),
            Case(
                "binary",
                "select id, fixed_bytes, variable_bytes, image_bytes, row_version "
                "from dbo.cx_odbc_binary_edge order by id",
            ),
            Case(
                "unicode",
                "select id, varchar_text, text_v, unichar_v, univarchar_v, "
                "long_univarchar_v, unitext_v from dbo.cx_odbc_unicode_edge order by id",
            ),
            Case(
                "partition",
                "select TradeId, [select], trade_label, cob_date "
                "from dbo.cx_odbc_partition_edge where TradeId is not null",
                partition_on="TradeId",
            ),
        ]
    odbc_compare_rows = parse_positive_int(env("ODBC_COMPARE_ROWS") or "100000", "ODBC_COMPARE_ROWS")
    return [
        Case(
            "edge",
            "select id, amount, created_at, event_time, payload, wide_text, "
            "nullable_text, long_text from cx_odbc_edge order by id",
        ),
        Case(
            "perf",
            "select id, flag, int_v, bigint_v, real_v, double_v, amount, "
            "name, payload, created_at from cx_odbc_perf where id <= "
            + str(odbc_compare_rows),
            partition_on="id",
        ),
    ]


def arrow_execute_options() -> dict[str, Any] | None:
    payload = env("CX_ODBC_COMPARE_ARROW_EXECUTE_OPTIONS_JSON")
    if not payload:
        return None
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON in CX_ODBC_COMPARE_ARROW_EXECUTE_OPTIONS_JSON: {exc}"
        ) from exc


def read_connectorx(
    uri: str,
    query: str,
    partition_on: str | None = None,
    partition_range: tuple[int, int] | None = None,
    partition_num: int | None = None,
) -> pl.DataFrame:
    kwargs: dict[str, Any] = {"engine": "connectorx"}
    if partition_on is not None:
        kwargs["partition_on"] = partition_on
        kwargs["partition_num"] = partition_num
        if partition_range is not None:
            kwargs["partition_range"] = partition_range
    return pl.read_database_uri(query=query, uri=uri, **kwargs)


def read_arrow_odbc(raw_conn: str, query: str) -> pl.DataFrame:
    kwargs: dict[str, Any] = {"query": query, "connection": raw_conn}
    execute_options = arrow_execute_options()
    if execute_options is not None:
        kwargs["execute_options"] = execute_options
    return pl.read_database(**kwargs)


def build_routes(
    config: BackendConfig,
    case: Case,
    partition_num: int,
    include_partitioned: bool,
) -> list[Route]:
    routes: list[Route] = []
    if config.dedicated_url:
        routes.append(
            Route(
                f"connectorx-{config.name}",
                lambda uri=config.dedicated_url, query=case.query: read_connectorx(uri, query),
            )
        )
        if include_partitioned and case.partition_on:
            routes.append(
                Route(
                    f"connectorx-{config.name}-partitioned",
                    lambda uri=config.dedicated_url, query=case.query: read_connectorx(
                        uri,
                        query,
                        case.partition_on,
                        case.partition_range,
                        partition_num,
                    ),
                    partitions=partition_num,
                )
            )
    if config.generic_url:
        routes.append(
            Route(
                "connectorx-odbc",
                lambda uri=config.generic_url, query=case.query: read_connectorx(uri, query),
            )
        )
        if include_partitioned and case.partition_on:
            routes.append(
                Route(
                    "connectorx-odbc-partitioned",
                    lambda uri=config.generic_url, query=case.query: read_connectorx(
                        uri,
                        query,
                        case.partition_on,
                        case.partition_range,
                        partition_num,
                    ),
                    partitions=partition_num,
                )
            )
    if config.raw_odbc_conn:
        routes.append(
            Route(
                "arrow-odbc",
                lambda raw=config.raw_odbc_conn, query=case.query: read_arrow_odbc(raw, query),
            )
        )
    return routes


def peak_rss_mb() -> float | None:
    if resource is None:
        return None
    try:
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:
        return None
    if platform.system() == "Darwin":
        return value / (1024 * 1024)
    return value / 1024


def schema_signature(df: pl.DataFrame) -> dict[str, str]:
    return {name: str(dtype) for name, dtype in df.schema.items()}


def null_counts(df: pl.DataFrame) -> dict[str, int]:
    row = df.null_count().to_dicts()[0] if df.width else {}
    return {key: int(value) for key, value in row.items()}


def stringify_scalar(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def min_max(df: pl.DataFrame) -> dict[str, dict[str, str | None]]:
    values: dict[str, dict[str, str | None]] = {}
    for column in df.columns:
        series = df[column]
        try:
            values[column] = {
                "min": stringify_scalar(series.min()),
                "max": stringify_scalar(series.max()),
            }
        except Exception:
            continue
    return values


def row_hash(df: pl.DataFrame) -> int | None:
    try:
        return int(df.hash_rows(seed=0).sum())
    except Exception:
        return None


def sort_columns(df: pl.DataFrame, sort_by: str | None) -> list[str]:
    requested = []
    if sort_by:
        requested = [column.strip() for column in sort_by.split(",") if column.strip()]
    existing = [column for column in requested if column in df.columns]
    existing.extend(column for column in df.columns if column not in existing)
    return existing


def canonical(df: pl.DataFrame, sort_by: str | None) -> pl.DataFrame:
    columns = sort_columns(df, sort_by)
    if columns:
        try:
            return df.sort(columns)
        except Exception:
            return df
    return df


def measure(route: Route, backend: str, case: Case, iterations: int, warmups: int) -> tuple[RunResult, pl.DataFrame]:
    for _ in range(warmups):
        route.runner()

    best_elapsed = float("inf")
    best_df: pl.DataFrame | None = None
    before_rss = peak_rss_mb()
    for _ in range(iterations):
        start = time.perf_counter()
        df = route.runner()
        elapsed = time.perf_counter() - start
        if elapsed < best_elapsed:
            best_elapsed = elapsed
            best_df = df

    assert best_df is not None
    after_rss = peak_rss_mb()
    peak_delta = None
    if before_rss is not None and after_rss is not None:
        peak_delta = max(0.0, after_rss - before_rss)
    rows = best_df.height
    return (
        RunResult(
            backend=backend,
            case=case.name,
            route=route.name,
            rows=rows,
            cols=best_df.width,
            elapsed_s=best_elapsed,
            rows_per_s=(rows / best_elapsed) if best_elapsed > 0 else 0.0,
            peak_rss_mb=peak_delta,
            schema=schema_signature(best_df),
            null_counts=null_counts(best_df),
            min_max=min_max(best_df),
            row_hash=row_hash(best_df),
            partitions=route.partitions,
        ),
        best_df,
    )


def compare_frames(
    reference_name: str,
    reference: pl.DataFrame,
    candidate_name: str,
    candidate: pl.DataFrame,
    sort_by: str | None,
) -> list[str]:
    failures: list[str] = []
    left = canonical(reference, sort_by)
    right = canonical(candidate, sort_by)
    if left.height != right.height:
        failures.append(f"row count differs from {reference_name}: {left.height} != {right.height}")
    if left.columns != right.columns:
        failures.append(f"columns differ from {reference_name}: {left.columns} != {right.columns}")
    if schema_signature(left) != schema_signature(right):
        failures.append(
            f"schema differs from {reference_name}: "
            f"{schema_signature(left)} != {schema_signature(right)}"
        )
    if null_counts(left) != null_counts(right):
        failures.append(
            f"null counts differ from {reference_name}: "
            f"{null_counts(left)} != {null_counts(right)}"
        )
    if min_max(left) != min_max(right):
        failures.append(
            f"min/max summaries differ from {reference_name}: "
            f"{min_max(left)} != {min_max(right)}"
        )
    left_hash = row_hash(left)
    right_hash = row_hash(right)
    if left_hash is not None and right_hash is not None and left_hash != right_hash:
        failures.append(f"row hash differs from {reference_name}: {left_hash} != {right_hash}")
    try:
        if left.columns == right.columns and left.shape == right.shape and not left.equals(right):
            failures.append(f"values differ from {reference_name}: DataFrame.equals returned false")
    except Exception as exc:
        failures.append(f"value comparison against {reference_name} failed: {exc}")
    if failures:
        return [f"{candidate_name}: {failure}" for failure in failures]
    return []


def print_result(result: RunResult) -> None:
    mem = "n/a" if result.peak_rss_mb is None else f"{result.peak_rss_mb:.1f}"
    partitions = "-" if result.partitions is None else str(result.partitions)
    print(
        f"{result.backend:7} {result.case:12} {result.route:30} "
        f"rows={result.rows:<8} cols={result.cols:<4} "
        f"time={result.elapsed_s:8.3f}s rows/s={result.rows_per_s:12.1f} "
        f"peak_rss_delta_mb={mem:>8} partitions={partitions}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backend",
        action="append",
        choices=BACKENDS + ("all",),
        help="Backend to run. May be repeated. Defaults to CX_ODBC_COMPARE_BACKENDS or all configured backends.",
    )
    parser.add_argument("--case", action="append", help="Case name to run. Defaults to all cases.")
    parser.add_argument(
        "--iterations",
        type=int,
        default=int(os.environ.get("CX_ODBC_COMPARE_ITERATIONS", "3")),
        help="Measured iterations per route. Best elapsed time is reported.",
    )
    parser.add_argument(
        "--warmups",
        type=int,
        default=int(os.environ.get("CX_ODBC_COMPARE_WARMUPS", "1")),
        help="Warmup iterations per route.",
    )
    parser.add_argument(
        "--partition-num",
        type=int,
        default=int(os.environ.get("CX_ODBC_COMPARE_PARTITION_NUM", "4")),
        help="ConnectorX partitions for partitionable cases.",
    )
    parser.add_argument(
        "--no-partitioned",
        action="store_true",
        help="Skip ConnectorX partitioned routes.",
    )
    parser.add_argument(
        "--sort-by",
        default=os.environ.get("CX_ODBC_COMPARE_SORT_BY"),
        help="Column used to sort before comparing values. Defaults to case partition column, then no sort.",
    )
    parser.add_argument(
        "--warn-only",
        action="store_true",
        help="Print correctness mismatches but exit zero.",
    )
    parser.add_argument(
        "--output-json",
        help="Write run results and mismatches as JSON.",
    )
    return parser.parse_args()


def selected_backends(args: argparse.Namespace) -> list[str]:
    requested = args.backend
    if not requested:
        env_backends = env("CX_ODBC_COMPARE_BACKENDS")
        requested = env_backends.split(",") if env_backends else ["all"]
    requested = [item.strip().lower() for item in requested]
    invalid = [item for item in requested if item and item not in BACKENDS and item != "all"]
    if invalid:
        valid = ", ".join((*BACKENDS, "all"))
        raise ValueError(f"Invalid backend(s): {', '.join(invalid)}. Valid values: {valid}")
    if "all" in requested:
        return [backend for backend in BACKENDS if route_count(backend_config(backend)) > 0]
    return [item for item in requested if item]


def route_count(config: BackendConfig) -> int:
    return sum(
        value is not None
        for value in (config.dedicated_url, config.generic_url, config.raw_odbc_conn)
    )


def main() -> int:
    args = parse_args()
    try:
        return run(args)
    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2


def run(args: argparse.Namespace) -> int:
    results: list[RunResult] = []
    mismatches: list[str] = []
    ran_anything = False

    for backend in selected_backends(args):
        config = backend_config(backend)
        if route_count(config) < 2:
            print(
                f"SKIP {backend}: configure at least two routes, including the raw ODBC "
                f"connection for arrow-odbc, to compare correctness.",
                file=sys.stderr,
            )
            continue

        for case in default_cases(backend):
            if args.case and case.name not in args.case:
                continue
            routes = build_routes(
                config,
                case,
                args.partition_num,
                include_partitioned=not args.no_partitioned,
            )
            if len(routes) < 2:
                continue

            load_polars()
            frames: dict[str, pl.DataFrame] = {}
            for route in routes:
                result, df = measure(route, backend, case, args.iterations, args.warmups)
                results.append(result)
                frames[route.name] = df
                print_result(result)
                ran_anything = True

            reference_name = "arrow-odbc" if "arrow-odbc" in frames else routes[0].name
            reference = frames[reference_name]
            sort_by = args.sort_by or case.partition_on
            for name, df in frames.items():
                if name == reference_name:
                    continue
                mismatches.extend(compare_frames(reference_name, reference, name, df, sort_by))

    if not ran_anything:
        print("No benchmark cases ran. Check connection environment variables.", file=sys.stderr)
        return 2

    if mismatches:
        print("\nCorrectness mismatches:", file=sys.stderr)
        for mismatch in mismatches:
            print(f"- {mismatch}", file=sys.stderr)
    else:
        print("\nCorrectness: all compared routes matched.")

    if args.output_json:
        payload = {
            "results": [result.__dict__ for result in results],
            "mismatches": mismatches,
        }
        with open(args.output_json, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")

    return 1 if mismatches and not args.warn_only else 0


if __name__ == "__main__":
    raise SystemExit(main())
