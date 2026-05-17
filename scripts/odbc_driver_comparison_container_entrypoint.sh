#!/usr/bin/env bash
set -euo pipefail

workspace="${CONNECTORX_BENCH_ROOT:-/workspace}"
cd "$workspace"

export PATH="${VIRTUAL_ENV:-/opt/connectorx-bench-venv}/bin:${PATH}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$workspace/target/odbc-benchmark-runner-build}"
export CX_DRIVER_COMPARE_OUTPUT_DIR="${CX_DRIVER_COMPARE_OUTPUT_DIR:-$workspace/target/odbc-driver-comparison-container}"
if [ -z "${CX_DRIVER_COMPARE_ARROW_EXECUTE_OPTIONS_JSON:-}" ]; then
    export CX_DRIVER_COMPARE_ARROW_EXECUTE_OPTIONS_JSON='{"max_text_size": 4096, "max_binary_size": 65536}'
fi
export PYTHONPATH="$workspace/connectorx-python:${PYTHONPATH:-}"
export DB2_CLIENT_PROFILE_PATH="${DB2_CLIENT_PROFILE_PATH:-/home/db2bench/sqllib/db2profile}"
export DB2_CLI_DRIVER_LIB_DIR="${DB2_CLI_DRIVER_LIB_DIR:-/home/db2bench/sqllib/lib64}"
if [ "${DB2_PROFILE:-}" = "$DB2_CLIENT_PROFILE_PATH" ]; then
    unset DB2_PROFILE
fi

refresh_db2_client_path() {
    if [ -f "$DB2_CLIENT_PROFILE_PATH" ] && [ "${CONNECTORX_DB2_PROFILE_LOADED:-0}" != "1" ]; then
        # IBM's libdb2o driver expects the Db2 client instance environment.
        # shellcheck disable=SC1090
        . "$DB2_CLIENT_PROFILE_PATH"
        export CONNECTORX_DB2_PROFILE_LOADED=1
    fi
    if [ -d "$DB2_CLI_DRIVER_LIB_DIR" ]; then
        export LD_LIBRARY_PATH="$DB2_CLI_DRIVER_LIB_DIR:${LD_LIBRARY_PATH:-}"
    fi
    local compacted=""
    local part
    local old_ifs="$IFS"
    IFS=:
    for part in ${LD_LIBRARY_PATH:-}; do
        if [ -n "$part" ] && [ -d "$part" ]; then
            case ":$compacted:" in
                *":$part:"*) ;;
                *) compacted="${compacted:+$compacted:}$part" ;;
            esac
        fi
    done
    IFS="$old_ifs"
    export LD_LIBRARY_PATH="$compacted"
}

refresh_db2_client_path

chown_outputs() {
    if [ -n "${HOST_UID:-}" ] && [ -n "${HOST_GID:-}" ]; then
        chown -R "$HOST_UID:$HOST_GID" "$CX_DRIVER_COMPARE_OUTPUT_DIR" 2>/dev/null || true
        chown "$HOST_UID:$HOST_GID" connectorx-python/connectorx/connectorx*.so 2>/dev/null || true
    fi
}
trap chown_outputs EXIT

odbc_connect_url() {
    python - "$1" <<'PY'
from urllib.parse import quote
import sys

print("odbc://localhost/?odbc_connect=" + quote(sys.argv[1], safe=""))
PY
}

export POSTGRES_GENERIC_ODBC_URL="${POSTGRES_GENERIC_ODBC_URL:-$(odbc_connect_url "${POSTGRES_ODBC_CONN:-}")}"
export SYBASE_GENERIC_ODBC_URL="${SYBASE_GENERIC_ODBC_URL:-$(odbc_connect_url "${SYBASE_ODBC_CONN:-}")}"
export DB2_GENERIC_ODBC_URL="${DB2_GENERIC_ODBC_URL:-$(odbc_connect_url "${DB2_ODBC_CONN:-}")}"

print_runtime() {
    echo "Registered ODBC drivers:"
    odbcinst -q -d || true
    echo
    echo "Db2 CLI driver libraries:"
    if [ -d "$DB2_CLI_DRIVER_LIB_DIR" ]; then
        local db2_lib_dir
        db2_lib_dir="$(readlink -f "$DB2_CLI_DRIVER_LIB_DIR" || printf '%s\n' "$DB2_CLI_DRIVER_LIB_DIR")"
        find "$db2_lib_dir" -maxdepth 1 \
            \( -name 'libdb2.so*' -o -name 'libdb2o.so*' -o -name 'libdb2clixml4c.so*' \) \
            -print | sort || true
    fi
    echo
}

build_connectorx() {
    if [ "${CX_BENCH_CONTAINER_SKIP_BUILD:-0}" = "1" ]; then
        return
    fi
    cd "$workspace/connectorx-python"
    if [ -n "${CX_BENCH_CONTAINER_MATURIN_FEATURES:-}" ]; then
        maturin develop --release --no-default-features --features "$CX_BENCH_CONTAINER_MATURIN_FEATURES"
    else
        maturin develop --release
    fi
    cd "$workspace"
}

wait_for_odbc() {
    local deadline
    deadline=$((SECONDS + ${CX_BENCH_CONTAINER_WAIT_SECS:-1800}))
    until refresh_db2_client_path && python scripts/odbc_connection_smoke.py; do
        if [ "$SECONDS" -ge "$deadline" ]; then
            echo "Timed out waiting for ODBC benchmark databases." >&2
            return 1
        fi
        sleep 10
    done
}

run_benchmark() {
    mkdir -p "$CX_DRIVER_COMPARE_OUTPUT_DIR"
    local args=("$@")
    if [ "${#args[@]}" -eq 0 ]; then
        args=(
            --backend postgres
            --backend sybase
            --backend db2
            --prepare-rows "${CX_DRIVER_COMPARE_PREPARE_ROWS:-10000}"
            --rows "${CX_DRIVER_COMPARE_ROWS:-10000}"
            --iterations "${CX_DRIVER_COMPARE_ITERATIONS:-3}"
            --warmups "${CX_DRIVER_COMPARE_WARMUPS:-1}"
            --route-timeout-secs "${CX_DRIVER_COMPARE_ROUTE_TIMEOUT_SECS:-600}"
            --output-dir "$CX_DRIVER_COMPARE_OUTPUT_DIR"
            --warn-only
        )
    fi
    python scripts/odbc_driver_comparison.py "${args[@]}"
}

command="${1:-benchmark}"
case "$command" in
    bash|sh)
        exec "$@"
        ;;
    smoke)
        print_runtime
        build_connectorx
        wait_for_odbc
        ;;
    benchmark)
        shift || true
        print_runtime
        build_connectorx
        wait_for_odbc
        run_benchmark "$@"
        ;;
    --*)
        print_runtime
        build_connectorx
        wait_for_odbc
        run_benchmark "$@"
        ;;
    *)
        exec "$@"
        ;;
esac
