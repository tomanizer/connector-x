#!/usr/bin/env python3
"""Build comparable ODBC performance baseline artifacts from Criterion output."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_ROWS = 100_000
DEFAULT_PARTITIONS = 4
DEFAULT_BATCH_SIZES = [1024, 4096, 8192, 16384]
DEFAULT_FEATURES = "src_odbc dst_arrow fptr"
NS_PER_SECOND = 1_000_000_000.0

APPROX_BYTES_PER_ROW = {
    "primitive": 32,
    "mixed": 176,
}


def positive_int(value: str | None, default: int) -> int:
    if not value:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def parse_csv_ints(value: str | None) -> list[int]:
    if not value:
        return []
    values = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            parsed = int(part)
        except ValueError:
            continue
        if parsed > 0:
            values.append(parsed)
    return values


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


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def seconds_from_ns(value: float | int | None) -> float | None:
    if value is None:
        return None
    return float(value) / NS_PER_SECOND


def estimate_seconds(estimates: dict[str, Any], key: str) -> dict[str, float | None]:
    payload = estimates.get(key) or {}
    confidence = payload.get("confidence_interval") or {}
    return {
        "point": seconds_from_ns(payload.get("point_estimate")),
        "lower": seconds_from_ns(confidence.get("lower_bound")),
        "upper": seconds_from_ns(confidence.get("upper_bound")),
    }


def parse_path(path: Path, criterion_dir: Path) -> dict[str, Any]:
    relative = path.relative_to(criterion_dir)
    parts = list(relative.parts)
    if len(parts) < 4:
        raise ValueError(f"unexpected Criterion path: {relative}")

    group = parts[0]
    id_parts = parts[1:-2]
    batch_size = None
    if id_parts:
        try:
            batch_size = int(id_parts[-1])
            id_parts = id_parts[:-1]
        except ValueError:
            batch_size = None

    mode = None
    case = None
    partitioning = None
    if len(id_parts) >= 3 and id_parts[0] in {"table", "stream"}:
        mode = id_parts[0]
        case = id_parts[1]
        partitioning = id_parts[2]
    elif id_parts:
        label_parts = id_parts[0].split("_", 2)
        if len(label_parts) == 3 and label_parts[0] in {"table", "stream"}:
            mode, case, partitioning = label_parts
        else:
            mode = "table"
            case = id_parts[0]
            partitioning = "single"

    return {
        "group": group,
        "benchmark_id": "/".join(parts[:-2]),
        "mode": mode,
        "case": case,
        "partitioning": partitioning,
        "batch_size": batch_size,
    }


def partition_count(partitioning: str | None) -> int:
    if not partitioning or partitioning == "single":
        return 1
    prefix = "partitioned-"
    if partitioning.startswith(prefix):
        try:
            return max(1, int(partitioning[len(prefix) :]))
        except ValueError:
            return 1
    return 1


def ceil_div(left: int, right: int) -> int:
    return (left + right - 1) // right


def estimated_batch_count(rows: int, batch_size: int | None, partitions: int) -> int | None:
    if not batch_size or batch_size <= 0:
        return None
    if partitions <= 1:
        return ceil_div(rows, batch_size)

    chunk = max(1, ceil_div(rows, partitions))
    batches = 0
    for partition in range(partitions):
        minimum = partition * chunk + 1
        if minimum > rows:
            break
        maximum = min(rows, minimum + chunk - 1)
        batches += ceil_div(maximum - minimum + 1, batch_size)
    return batches


def rows_per_second(rows: int, seconds: float | None) -> float | None:
    if seconds is None or seconds <= 0:
        return None
    return rows / seconds


def collect_benchmarks(criterion_dir: Path, rows: int) -> list[dict[str, Any]]:
    benchmarks = []
    for path in sorted(criterion_dir.glob("odbc/**/new/estimates.json")):
        dimensions = parse_path(path, criterion_dir)
        estimates = load_json(path)
        mean = estimate_seconds(estimates, "mean")
        median = estimate_seconds(estimates, "median")
        partitions = partition_count(dimensions["partitioning"])
        batch_count = estimated_batch_count(rows, dimensions["batch_size"], partitions)
        approx_bytes = APPROX_BYTES_PER_ROW.get(dimensions["case"])
        median_rows_per_second = rows_per_second(rows, median["point"])
        mean_rows_per_second = rows_per_second(rows, mean["point"])
        approx_bytes_per_second = (
            median_rows_per_second * approx_bytes
            if median_rows_per_second is not None and approx_bytes is not None
            else None
        )

        benchmarks.append(
            {
                **dimensions,
                "rows": rows,
                "partitions": partitions,
                "estimated_batch_count": batch_count,
                "peak_memory_mb": None,
                "mean_seconds": mean["point"],
                "mean_seconds_lower": mean["lower"],
                "mean_seconds_upper": mean["upper"],
                "median_seconds": median["point"],
                "median_seconds_lower": median["lower"],
                "median_seconds_upper": median["upper"],
                "mean_rows_per_second": mean_rows_per_second,
                "median_rows_per_second": median_rows_per_second,
                "approx_bytes_per_row": approx_bytes,
                "approx_bytes_per_second": approx_bytes_per_second,
                "criterion_estimates_path": str(path),
            }
        )
    return benchmarks


def collect_metadata(args: argparse.Namespace, runtime: list[Any]) -> dict[str, Any]:
    uname = platform.uname()
    manager = None
    drivers = []
    if runtime:
        first = runtime[0] if isinstance(runtime[0], dict) else {}
        manager = (first.get("manager") or {}).get("library")
        drivers = ((first.get("drivers") or {}).get("names")) or []

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "backend": args.backend,
        "features": args.features,
        "rows": args.rows,
        "batch_sizes": args.batch_sizes,
        "requested_partitions": args.partitions,
        "odbc_driver": args.driver
        or os.environ.get("ODBC_POSTGRES_DRIVER")
        or os.environ.get("ODBC_DRIVER"),
        "odbc_driver_manager": manager,
        "registered_odbc_drivers": drivers,
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
        "git": {
            "branch": run_git(["branch", "--show-current"]),
            "commit": run_git(["rev-parse", "HEAD"]),
            "github_sha": os.environ.get("GITHUB_SHA"),
            "github_ref": os.environ.get("GITHUB_REF"),
        },
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_csv(path: Path, benchmarks: list[dict[str, Any]], metadata: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "backend",
        "os",
        "machine",
        "driver",
        "driver_manager",
        "features",
        "group",
        "benchmark_id",
        "mode",
        "case",
        "partitioning",
        "batch_size",
        "partitions",
        "rows",
        "estimated_batch_count",
        "peak_memory_mb",
        "median_seconds",
        "median_rows_per_second",
        "mean_seconds",
        "mean_rows_per_second",
        "approx_bytes_per_row",
        "approx_bytes_per_second",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for benchmark in benchmarks:
            row = {
                "backend": metadata["backend"],
                "os": metadata["os"]["system"],
                "machine": metadata["os"]["machine"],
                "driver": metadata.get("odbc_driver"),
                "driver_manager": metadata.get("odbc_driver_manager"),
                "features": metadata["features"],
            }
            for field in fieldnames:
                if field not in row:
                    row[field] = benchmark.get(field)
            writer.writerow(row)


def format_rate(value: float | None) -> str:
    if value is None:
        return "n/a"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.2f}k"
    return f"{value:.2f}"


def format_ms(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 1000:.2f}"


def write_summary(path: Path, benchmarks: list[dict[str, Any]], metadata: dict[str, Any]) -> None:
    rows = sorted(
        benchmarks,
        key=lambda item: (
            item.get("mode") or "",
            item.get("case") or "",
            item.get("partitioning") or "",
            item.get("batch_size") or math.inf,
        ),
    )
    lines = [
        "### ODBC performance baseline",
        f"- Backend: `{metadata['backend']}`",
        f"- Driver: `{metadata.get('odbc_driver') or 'unknown'}`",
        f"- Driver manager: `{metadata.get('odbc_driver_manager') or 'unknown'}`",
        f"- Rows per benchmark: `{metadata['rows']}`",
        "",
        "| Mode | Case | Partitioning | Batch size | Median ms | Median rows/s |",
        "| --- | --- | --- | ---: | ---: | ---: |",
    ]
    for benchmark in rows[:24]:
        lines.append(
            "| `{mode}` | `{case}` | `{partitioning}` | {batch_size} | {median_ms} | {rows_per_second} |".format(
                mode=benchmark.get("mode") or "unknown",
                case=benchmark.get("case") or "unknown",
                partitioning=benchmark.get("partitioning") or "unknown",
                batch_size=benchmark.get("batch_size") or "n/a",
                median_ms=format_ms(benchmark.get("median_seconds")),
                rows_per_second=format_rate(benchmark.get("median_rows_per_second")),
            )
        )
    with path.open("a", encoding="utf-8") as summary:
        summary.write("\n".join(lines))
        summary.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--criterion-dir", type=Path, default=Path("target/criterion"))
    parser.add_argument("--runtime-json", type=Path, action="append", default=[])
    parser.add_argument("--arrow-compare-json", type=Path, action="append", default=[])
    parser.add_argument("--output-json", type=Path, default=Path("odbc-performance-baseline.json"))
    parser.add_argument("--output-csv", type=Path, default=Path("odbc-performance-baseline.csv"))
    parser.add_argument("--summary", type=Path)
    parser.add_argument("--backend", default="odbc-postgres-testcontainer")
    parser.add_argument("--features", default=DEFAULT_FEATURES)
    parser.add_argument("--driver")
    parser.add_argument(
        "--rows",
        type=int,
        default=positive_int(os.environ.get("ODBC_BENCH_ROWS"), DEFAULT_ROWS),
    )
    parser.add_argument(
        "--batch-sizes",
        type=int,
        nargs="*",
        default=parse_csv_ints(os.environ.get("ODBC_BENCH_BATCH_SIZES"))
        or DEFAULT_BATCH_SIZES,
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=positive_int(os.environ.get("ODBC_BENCH_PARTITIONS"), DEFAULT_PARTITIONS),
    )
    parser.add_argument("--allow-empty", action="store_true")
    args = parser.parse_args()

    runtime = [load_json(path) for path in args.runtime_json if path.exists()]
    arrow_compare = [load_json(path) for path in args.arrow_compare_json if path.exists()]
    benchmarks = collect_benchmarks(args.criterion_dir, args.rows)
    metadata = collect_metadata(args, runtime)

    if not benchmarks and not args.allow_empty:
        print(
            f"no ODBC Criterion estimates found under {args.criterion_dir}",
            file=sys.stderr,
        )
        return 1

    payload = {
        "schema_version": 1,
        "metadata": metadata,
        "runtime": runtime,
        "arrow_compare": arrow_compare,
        "benchmarks": benchmarks,
    }
    write_json(args.output_json, payload)
    write_csv(args.output_csv, benchmarks, metadata)
    if args.summary:
        write_summary(args.summary, benchmarks, metadata)

    print(
        f"wrote {len(benchmarks)} ODBC benchmark rows to "
        f"{args.output_json} and {args.output_csv}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
