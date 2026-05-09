#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any

try:
    import resource
except ImportError:  # pragma: no cover - not available on Windows
    resource = None  # type: ignore[assignment]


COMMON_COMPARE_PREFIX = "CX_ODBC_COMPARE"


@dataclass(frozen=True)
class BackendSpec:
    name: str
    compare_prefix: str
    connectorx_uri_env: str
    arrow_odbc_conn_env: str
    batch_envs: tuple[str, ...]
    max_len_envs: tuple[str, ...]


BACKENDS: dict[str, BackendSpec] = {
    "db2": BackendSpec(
        name="db2",
        compare_prefix="DB2",
        connectorx_uri_env="DB2_URL",
        arrow_odbc_conn_env="DB2_ODBC_CONN",
        batch_envs=("DB2_BATCH_SIZE",),
        max_len_envs=("DB2_MAX_STR_LEN",),
    ),
    "sybase": BackendSpec(
        name="sybase",
        compare_prefix="SYBASE",
        connectorx_uri_env="SYBASE_URL",
        arrow_odbc_conn_env="SYBASE_ODBC_CONN",
        batch_envs=("SYBASE_BATCH_SIZE",),
        max_len_envs=("SYBASE_MAX_STR_LEN",),
    ),
    "odbc": BackendSpec(
        name="odbc",
        compare_prefix="ODBC",
        connectorx_uri_env="ODBC_URL",
        arrow_odbc_conn_env="ODBC_CONN",
        batch_envs=("ODBC_BATCH_SIZE",),
        max_len_envs=("ODBC_MAX_STR_LEN",),
    ),
}


@dataclass(frozen=True)
class Workload:
    name: str
    query: str
    sort_columns: tuple[str, ...] = ()
    partition_on: str | None = None
    partition_range: tuple[int, int] | None = None
    partition_num: int | None = None

    @property
    def supports_partitioning(self) -> bool:
        return (
            self.partition_on is not None
            and self.partition_range is not None
            and self.partition_num is not None
        )


def env_first(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value is not None and value.strip():
            return value.strip()
    return None


def parse_csv_columns(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def parse_int(raw: str | None) -> int | None:
    if raw is None or not raw.strip():
        return None
    return int(raw.strip())


def parse_int_pair(raw: str | None) -> tuple[int, int] | None:
    if raw is None or not raw.strip():
        return None
    parts = [part.strip() for part in raw.split(",", 1)]
    if len(parts) != 2 or not all(parts):
        raise ValueError(f"expected '<min>,<max>' integer pair, got {raw!r}")
    return int(parts[0]), int(parts[1])


def workload_env_names(spec: BackendSpec, suffix: str) -> tuple[str, str]:
    return f"{spec.compare_prefix}_COMPARE_{suffix}", f"{COMMON_COMPARE_PREFIX}_{suffix}"


def load_workloads(spec: BackendSpec) -> list[Workload]:
    workloads: list[Workload] = []

    for workload_name in ("MIXED", "WIDE"):
        query = env_first(*workload_env_names(spec, f"{workload_name}_QUERY"))
        if not query:
            continue
        sort_columns = parse_csv_columns(
            env_first(*workload_env_names(spec, f"{workload_name}_SORT_COLUMNS"))
        )
        workloads.append(
            Workload(
                name=workload_name.lower(),
                query=query,
                sort_columns=sort_columns,
            )
        )

    partition_query = env_first(*workload_env_names(spec, "PARTITION_QUERY"))
    partition_on = env_first(*workload_env_names(spec, "PARTITION_COLUMN"))
    partition_range_raw = env_first(*workload_env_names(spec, "PARTITION_RANGE"))
    partition_num_raw = env_first(*workload_env_names(spec, "PARTITION_NUM"))
    partition_sort_columns = parse_csv_columns(
        env_first(*workload_env_names(spec, "PARTITION_SORT_COLUMNS"))
    )

    partition_values = {
        "query": partition_query,
        "column": partition_on,
        "range": partition_range_raw,
        "num": partition_num_raw,
    }
    if any(value is not None for value in partition_values.values()):
        missing = [name for name, value in partition_values.items() if value is None]
        if missing:
            raise ValueError(
                f"incomplete partition workload for {spec.name}: missing {', '.join(missing)}"
            )
        workloads.append(
            Workload(
                name="partition",
                query=partition_query or "",
                sort_columns=partition_sort_columns,
                partition_on=partition_on,
                partition_range=parse_int_pair(partition_range_raw),
                partition_num=parse_int(partition_num_raw),
            )
        )

    return workloads


def best_effort_peak_rss_mib() -> float | None:
    if resource is None:
        return None
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return usage / (1024 * 1024)
    return usage / 1024


def ensure_polars() -> Any:
    try:
        import polars as pl
    except ImportError as exc:  # pragma: no cover - import failure only in runtime
        raise RuntimeError(
            "polars is required to run this benchmark. Install it in the connectorx-python environment."
        ) from exc
    return pl


def ensure_arrow_odbc() -> None:
    if importlib.util.find_spec("arrow_odbc") is None:
        raise RuntimeError(
            "arrow-odbc is required for pl.read_database over ODBC connection strings. "
            "Install it before running this benchmark."
        )


def normalize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: normalize_value(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [normalize_value(item) for item in value]
    if isinstance(value, bytes):
        return value.hex()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except TypeError:
            pass
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def sort_frame(df: Any, sort_columns: tuple[str, ...]) -> Any:
    if sort_columns:
        return df.sort(list(sort_columns))
    return df


def dataframe_hash(df: Any) -> int | None:
    try:
        return int(df.hash_rows().sum())
    except Exception:
        return None


def collect_decimal_fields(arrow_schema: Any) -> dict[str, dict[str, int]]:
    import pyarrow.types as patypes

    decimals: dict[str, dict[str, int]] = {}
    for field in arrow_schema:
        if patypes.is_decimal(field.type):
            decimals[field.name] = {
                "precision": field.type.precision,
                "scale": field.type.scale,
            }
    return decimals


def collect_minmax(df: Any, arrow_schema: Any) -> dict[str, dict[str, Any]]:
    import pyarrow.types as patypes

    stats: dict[str, dict[str, Any]] = {}
    for field in arrow_schema:
        field_type = field.type
        if not (
            patypes.is_decimal(field_type)
            or patypes.is_integer(field_type)
            or patypes.is_floating(field_type)
            or patypes.is_date(field_type)
            or patypes.is_time(field_type)
            or patypes.is_timestamp(field_type)
        ):
            continue
        series = df.get_column(field.name)
        stats[field.name] = {
            "min": normalize_value(series.min()),
            "max": normalize_value(series.max()),
        }
    return stats


def summarize_frame(df: Any, workload: Workload, sample_size: int) -> dict[str, Any]:
    ordered = sort_frame(df, workload.sort_columns)
    arrow_table = ordered.to_arrow()
    null_counts_df = ordered.null_count().to_dicts()
    null_counts = null_counts_df[0] if null_counts_df else {}
    return {
        "row_count": ordered.height,
        "column_names": list(ordered.columns),
        "polars_schema": {
            name: str(dtype) for name, dtype in ordered.schema.items()
        },
        "arrow_schema": [str(field) for field in arrow_table.schema],
        "null_counts": normalize_value(null_counts),
        "row_hash_sum": dataframe_hash(ordered),
        "sample_rows": normalize_value(ordered.head(sample_size).to_dicts()),
        "minmax": collect_minmax(ordered, arrow_table.schema),
        "decimal_fields": collect_decimal_fields(arrow_table.schema),
    }


def compare_summaries(reference: dict[str, Any], candidate: dict[str, Any]) -> list[str]:
    mismatches: list[str] = []
    for field in (
        "row_count",
        "column_names",
        "polars_schema",
        "arrow_schema",
        "null_counts",
        "row_hash_sum",
        "sample_rows",
        "minmax",
        "decimal_fields",
    ):
        if reference.get(field) != candidate.get(field):
            mismatches.append(field)
    return mismatches


def run_connectorx_route(
    spec: BackendSpec,
    workload: Workload,
    sample_size: int,
    partitioned: bool,
) -> dict[str, Any]:
    pl = ensure_polars()
    uri = os.environ[spec.connectorx_uri_env]
    start = perf_counter()
    frame = pl.read_database_uri(
        query=workload.query,
        uri=uri,
        engine="connectorx",
        partition_on=workload.partition_on if partitioned else None,
        partition_range=workload.partition_range if partitioned else None,
        partition_num=workload.partition_num if partitioned else None,
    )
    elapsed = perf_counter() - start
    return {
        "rows": frame.height,
        "elapsed_s": elapsed,
        "rows_per_s": frame.height / elapsed if elapsed > 0 else None,
        "peak_rss_mib": best_effort_peak_rss_mib(),
        "connections_or_partitions": workload.partition_num if partitioned else 1,
        "settings": {
            env: os.environ.get(env)
            for env in (*spec.batch_envs, *spec.max_len_envs)
            if os.environ.get(env) is not None
        },
        "summary": summarize_frame(frame, workload, sample_size),
    }


def run_arrow_odbc_route(
    spec: BackendSpec,
    workload: Workload,
    sample_size: int,
    arrow_batch_size: int | None,
) -> dict[str, Any]:
    pl = ensure_polars()
    ensure_arrow_odbc()
    conn = os.environ[spec.arrow_odbc_conn_env]
    start = perf_counter()
    if arrow_batch_size:
        batches = list(
            pl.read_database(
                query=workload.query,
                connection=conn,
                iter_batches=True,
                batch_size=arrow_batch_size,
            )
        )
        frame = pl.concat(batches, rechunk=True) if batches else pl.DataFrame()
    else:
        frame = pl.read_database(query=workload.query, connection=conn)
    elapsed = perf_counter() - start
    settings = {}
    if arrow_batch_size is not None:
        settings["ARROW_ODBC_BATCH_SIZE"] = arrow_batch_size
    return {
        "rows": frame.height,
        "elapsed_s": elapsed,
        "rows_per_s": frame.height / elapsed if elapsed > 0 else None,
        "peak_rss_mib": best_effort_peak_rss_mib(),
        "connections_or_partitions": 1,
        "settings": settings,
        "summary": summarize_frame(frame, workload, sample_size),
    }


def choose_reference(successful_routes: list[dict[str, Any]]) -> dict[str, Any]:
    by_name = {route["route"]: route for route in successful_routes}
    for preferred in ("arrow-odbc", "connectorx-single", "connectorx-partitioned"):
        if preferred in by_name:
            return by_name[preferred]
    return successful_routes[0]


def print_results(results: list[dict[str, Any]]) -> None:
    rows = [
        [
            result["backend"],
            result["workload"],
            result["route"],
            result["status"],
            str(result.get("rows", "")),
            format(result.get("elapsed_s"), ".4f") if result.get("elapsed_s") is not None else "",
            format(result.get("rows_per_s"), ".2f") if result.get("rows_per_s") is not None else "",
            format(result.get("peak_rss_mib"), ".1f") if result.get("peak_rss_mib") is not None else "",
            str(result.get("connections_or_partitions", "")),
            json.dumps(result.get("settings", {}), sort_keys=True),
            ",".join(result.get("mismatches", ())),
        ]
        for result in results
    ]
    headers = [
        "backend",
        "workload",
        "route",
        "status",
        "rows",
        "elapsed_s",
        "rows_per_s",
        "peak_rss_mib",
        "connections",
        "settings",
        "mismatches",
    ]
    widths = [
        max(len(header), *(len(row[index]) for row in rows)) if rows else len(header)
        for index, header in enumerate(headers)
    ]
    print(" | ".join(header.ljust(widths[index]) for index, header in enumerate(headers)))
    print("-+-".join("-" * width for width in widths))
    for row in rows:
        print(" | ".join(value.ljust(widths[index]) for index, value in enumerate(row)))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare Polars ConnectorX ODBC-family routes against Polars read_database "
            "with arrow-odbc-backed ODBC connection strings."
        )
    )
    parser.add_argument(
        "--backend",
        action="append",
        choices=sorted(BACKENDS),
        help="Backend to run. Defaults to every backend with configured connection env vars.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=5,
        help="Number of normalized sample rows to store and compare per route.",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        help="Optional path to write the full machine-readable result payload.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    arrow_batch_size = parse_int(os.environ.get("ARROW_ODBC_BATCH_SIZE"))
    requested_backends = args.backend or list(BACKENDS)
    results: list[dict[str, Any]] = []
    comparison_ran = False
    comparison_made = False
    comparison_failed = False

    for backend_name in requested_backends:
        spec = BACKENDS[backend_name]
        try:
            workloads = load_workloads(spec)
        except ValueError as exc:
            results.append(
                {
                    "backend": spec.name,
                    "workload": "*",
                    "route": "configuration",
                    "status": "error",
                    "error": str(exc),
                    "mismatches": [],
                }
            )
            comparison_failed = True
            continue

        if not workloads:
            results.append(
                {
                    "backend": spec.name,
                    "workload": "*",
                    "route": "configuration",
                    "status": "skip",
                    "error": (
                        f"no workloads configured; set {spec.compare_prefix}_COMPARE_* or "
                        f"{COMMON_COMPARE_PREFIX}_* query env vars"
                    ),
                    "mismatches": [],
                }
            )
            continue

        connectorx_available = env_first(spec.connectorx_uri_env) is not None
        arrow_available = env_first(spec.arrow_odbc_conn_env) is not None
        if not connectorx_available and not arrow_available:
            results.append(
                {
                    "backend": spec.name,
                    "workload": "*",
                    "route": "configuration",
                    "status": "skip",
                    "error": (
                        f"set {spec.connectorx_uri_env} and/or {spec.arrow_odbc_conn_env} "
                        "to run this backend"
                    ),
                    "mismatches": [],
                }
            )
            continue

        for workload in workloads:
            workload_results: list[dict[str, Any]] = []
            comparison_ran = True

            if connectorx_available:
                for route_name, partitioned in (
                    ("connectorx-single", False),
                    ("connectorx-partitioned", True),
                ):
                    if partitioned and not workload.supports_partitioning:
                        continue
                    try:
                        route = run_connectorx_route(
                            spec=spec,
                            workload=workload,
                            sample_size=args.sample_size,
                            partitioned=partitioned,
                        )
                        workload_results.append(
                            {
                                "backend": spec.name,
                                "workload": workload.name,
                                "route": route_name,
                                "status": "ok",
                                **route,
                                "mismatches": [],
                            }
                        )
                    except Exception as exc:
                        workload_results.append(
                            {
                                "backend": spec.name,
                                "workload": workload.name,
                                "route": route_name,
                                "status": "error",
                                "error": str(exc),
                                "mismatches": [],
                            }
                        )
                        comparison_failed = True
            else:
                workload_results.append(
                    {
                        "backend": spec.name,
                        "workload": workload.name,
                        "route": "connectorx-single",
                        "status": "skip",
                        "error": f"{spec.connectorx_uri_env} is not set",
                        "mismatches": [],
                    }
                )

            if arrow_available:
                try:
                    route = run_arrow_odbc_route(
                        spec=spec,
                        workload=workload,
                        sample_size=args.sample_size,
                        arrow_batch_size=arrow_batch_size,
                    )
                    workload_results.append(
                        {
                            "backend": spec.name,
                            "workload": workload.name,
                            "route": "arrow-odbc",
                            "status": "ok",
                            **route,
                            "mismatches": [],
                        }
                    )
                except Exception as exc:
                    workload_results.append(
                        {
                            "backend": spec.name,
                            "workload": workload.name,
                            "route": "arrow-odbc",
                            "status": "error",
                            "error": str(exc),
                            "mismatches": [],
                        }
                    )
                    comparison_failed = True
            else:
                workload_results.append(
                    {
                        "backend": spec.name,
                        "workload": workload.name,
                        "route": "arrow-odbc",
                        "status": "skip",
                        "error": f"{spec.arrow_odbc_conn_env} is not set",
                        "mismatches": [],
                    }
                )

            successful = [
                result
                for result in workload_results
                if result["status"] == "ok" and result.get("summary") is not None
            ]
            if len(successful) >= 2:
                comparison_made = True
                reference = choose_reference(successful)
                for result in successful:
                    if result is reference:
                        continue
                    mismatches = compare_summaries(
                        reference["summary"],
                        result["summary"],
                    )
                    result["mismatches"] = mismatches
                    if mismatches:
                        comparison_failed = True

            results.extend(workload_results)

    print_results(results)

    for result in results:
        if result["status"] == "error":
            print(
                f"ERROR [{result['backend']}:{result['workload']}:{result['route']}]: {result['error']}",
                file=sys.stderr,
            )
        elif result.get("mismatches"):
            print(
                f"MISMATCH [{result['backend']}:{result['workload']}:{result['route']}]: "
                f"{', '.join(result['mismatches'])}",
                file=sys.stderr,
            )
        elif result["status"] == "skip":
            print(
                f"SKIP [{result['backend']}:{result['workload']}:{result['route']}]: {result['error']}",
                file=sys.stderr,
            )

    if args.json_out:
        args.json_out.write_text(json.dumps(results, indent=2, sort_keys=True), encoding="utf-8")

    if comparison_failed:
        return 1
    if not comparison_ran or not comparison_made:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
