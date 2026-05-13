#!/usr/bin/env bash
set -euo pipefail

driver_name="${ODBC_POSTGRES_DRIVER:-PostgreSQL Unicode}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the Postgres ODBC benchmark" >&2
  exit 1
fi

if ! command -v odbcinst >/dev/null 2>&1; then
  echo "odbcinst is required. Install unixODBC and psqlODBC first." >&2
  exit 1
fi

if ! odbcinst -q -d | grep -Fqxi "[${driver_name}]"; then
  echo "ODBC driver '${driver_name}' is not registered." >&2
  echo "Registered drivers:" >&2
  odbcinst -q -d >&2 || true
  exit 1
fi

export CONNECTORX_ODBC_TESTCONTAINER=1
export ODBC_POSTGRES_DRIVER="${driver_name}"
export ODBC_BENCH_ROWS="${ODBC_BENCH_ROWS:-100000}"
export ODBC_BENCH_BATCH_SIZES="${ODBC_BENCH_BATCH_SIZES:-1024,4096,8192,16384}"
export ODBC_BENCH_PARTITIONS="${ODBC_BENCH_PARTITIONS:-4}"

cargo bench -p connectorx \
  --no-default-features \
  --features "src_odbc dst_arrow fptr" \
  --bench odbc \
  -- "$@"
