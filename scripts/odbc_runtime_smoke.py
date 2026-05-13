#!/usr/bin/env python3
"""Smoke-test platform ODBC runtime availability for ConnectorX wheels."""

from __future__ import annotations

import argparse
import ctypes
import ctypes.util
import json
import platform
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any


SQL_HANDLE_ENV = 1
SQL_SUCCESS = 0
SQL_SUCCESS_WITH_INFO = 1


def run_command(command: list[str]) -> dict[str, Any]:
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
    except OSError as exc:
        return {
            "command": command,
            "available": False,
            "error": str(exc),
        }

    return {
        "command": command,
        "available": True,
        "returncode": result.returncode,
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
    }


def odbc_library_candidates() -> list[str]:
    system = platform.system()
    candidates: list[str | None]
    if system == "Windows":
        candidates = [ctypes.util.find_library("odbc32"), "odbc32"]
    elif system == "Darwin":
        candidates = [
            ctypes.util.find_library("odbc"),
            "/opt/homebrew/lib/libodbc.dylib",
            "/usr/local/lib/libodbc.dylib",
            "libodbc.dylib",
        ]
    else:
        candidates = [
            ctypes.util.find_library("odbc"),
            "libodbc.so.2",
            "libodbc.so",
        ]

    unique = []
    for candidate in candidates:
        if candidate and candidate not in unique:
            unique.append(candidate)
    return unique


def load_library(candidate: str) -> ctypes.CDLL:
    if platform.system() == "Windows" and hasattr(ctypes, "WinDLL"):
        return ctypes.WinDLL(candidate)  # type: ignore[attr-defined]
    return ctypes.CDLL(candidate)


def load_odbc_manager() -> tuple[ctypes.CDLL | None, dict[str, Any]]:
    attempts = []
    for candidate in odbc_library_candidates():
        try:
            library = load_library(candidate)
            return library, {
                "loaded": True,
                "library": candidate,
                "attempts": attempts,
            }
        except OSError as exc:
            attempts.append({"library": candidate, "error": str(exc)})

    return None, {
        "loaded": False,
        "library": None,
        "attempts": attempts,
    }


def allocate_environment(library: ctypes.CDLL | None) -> dict[str, Any]:
    if library is None:
        return {"ok": False, "error": "ODBC manager library could not be loaded"}

    handle = ctypes.c_void_p()
    sql_alloc_handle = library.SQLAllocHandle
    sql_alloc_handle.argtypes = [
        ctypes.c_short,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    sql_alloc_handle.restype = ctypes.c_short

    ret = sql_alloc_handle(SQL_HANDLE_ENV, None, ctypes.byref(handle))
    ok = ret in (SQL_SUCCESS, SQL_SUCCESS_WITH_INFO) and bool(handle.value)

    if ok:
        sql_free_handle = library.SQLFreeHandle
        sql_free_handle.argtypes = [ctypes.c_short, ctypes.c_void_p]
        sql_free_handle.restype = ctypes.c_short
        free_ret = sql_free_handle(SQL_HANDLE_ENV, handle)
    else:
        free_ret = None

    return {
        "ok": ok,
        "returncode": ret,
        "handle_allocated": bool(handle.value),
        "free_returncode": free_ret,
    }


def discover_posix_drivers() -> dict[str, Any]:
    odbcinst = shutil.which("odbcinst")
    if not odbcinst:
        return {"tool": "odbcinst", "available": False}

    return {
        "tool": odbcinst,
        "available": True,
        "environment": run_command([odbcinst, "-j"]),
        "drivers": run_command([odbcinst, "-q", "-d"]),
    }


def discover_windows_drivers() -> dict[str, Any]:
    shell = shutil.which("pwsh") or shutil.which("powershell")
    if not shell:
        return {"tool": "powershell", "available": False}

    command = [
        shell,
        "-NoProfile",
        "-Command",
        "Get-OdbcDriver | Select-Object -First 50 Name,Platform | ConvertTo-Json -Depth 3",
    ]
    return {
        "tool": shell,
        "available": True,
        "drivers": run_command(command),
    }


def parse_driver_names(discovery: dict[str, Any]) -> list[str]:
    if platform.system() == "Windows":
        stdout = discovery.get("drivers", {}).get("stdout") or ""
        if not stdout:
            return []
        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError:
            return []
        rows = payload if isinstance(payload, list) else [payload]
        return [
            row.get("Name", "")
            for row in rows
            if isinstance(row, dict) and row.get("Name")
        ]

    stdout = discovery.get("drivers", {}).get("stdout") or ""
    names = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("[") and line.endswith("]"):
            names.append(line[1:-1])
    return names


def collect() -> dict[str, Any]:
    library, manager = load_odbc_manager()
    allocation = allocate_environment(library)
    discovery = (
        discover_windows_drivers()
        if platform.system() == "Windows"
        else discover_posix_drivers()
    )
    driver_names = parse_driver_names(discovery)

    return {
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "python": sys.version,
        },
        "manager": manager,
        "environment_allocation": allocation,
        "driver_discovery": discovery,
        "drivers": {
            "count": len(driver_names),
            "names": driver_names,
        },
    }


def write_summary(path: str, data: dict[str, Any]) -> None:
    drivers = data["drivers"]["names"]
    first_drivers = ", ".join(f"`{name}`" for name in drivers[:10]) or "none reported"
    lines = [
        "### ODBC runtime smoke",
        f"- Platform: `{data['platform']['system']} {data['platform']['machine']}`",
        f"- ODBC manager library: `{data['manager']['library']}`",
        "- `SQLAllocHandle(SQL_HANDLE_ENV)`: "
        + ("success" if data["environment_allocation"]["ok"] else "failed"),
        f"- Driver discovery tool: `{data['driver_discovery'].get('tool')}`",
        f"- Registered drivers reported: {data['drivers']['count']} ({first_drivers})",
    ]
    with open(path, "a", encoding="utf-8") as summary:
        summary.write("\n".join(lines))
        summary.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--json", type=Path, help="Write collected ODBC metadata to this file"
    )
    parser.add_argument("--summary", help="Append a Markdown summary to this file")
    args = parser.parse_args()

    data = collect()
    print(json.dumps(data, indent=2))

    if args.json:
        args.json.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    if args.summary:
        write_summary(args.summary, data)

    return 0 if data["environment_allocation"]["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
