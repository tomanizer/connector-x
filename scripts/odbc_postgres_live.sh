#!/usr/bin/env bash
set -euo pipefail

container_name="${ODBC_POSTGRES_CONTAINER:-connectorx-odbc-postgres}"
driver_name="${ODBC_POSTGRES_DRIVER:-PostgreSQL Unicode}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the Postgres ODBC live test" >&2
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

if docker ps -a --format '{{.Names}}' | grep -qx "${container_name}"; then
  echo "removing stale ${container_name} container from the previous ODBC live test" >&2
  docker rm -f "${container_name}" >/dev/null
fi

export CONNECTORX_ODBC_TESTCONTAINER=1
export ODBC_POSTGRES_DRIVER="${driver_name}"
cargo test -p connectorx --no-default-features --features "src_odbc dst_arrow fptr" --test test_odbc -- --nocapture
