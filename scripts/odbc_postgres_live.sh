#!/usr/bin/env bash
set -euo pipefail

container_name="${ODBC_POSTGRES_CONTAINER:-connectorx-odbc-postgres}"
postgres_image="${ODBC_POSTGRES_IMAGE:-postgres:16}"
postgres_port="${ODBC_POSTGRES_PORT:-5432}"
driver_name="${ODBC_POSTGRES_DRIVER:-PostgreSQL Unicode}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the Postgres ODBC live test" >&2
  exit 1
fi

if ! command -v odbcinst >/dev/null 2>&1; then
  echo "odbcinst is required. Install unixODBC and psqlODBC first." >&2
  exit 1
fi

if ! odbcinst -q -d | grep -qi "^\[${driver_name}\]$"; then
  echo "ODBC driver '${driver_name}' is not registered." >&2
  echo "Registered drivers:" >&2
  odbcinst -q -d >&2 || true
  exit 1
fi

if docker ps -a --format '{{.Names}}' | grep -qx "${container_name}"; then
  docker rm -f "${container_name}" >/dev/null
fi

docker run -d \
  --name "${container_name}" \
  -e POSTGRES_USER=connectorx \
  -e POSTGRES_PASSWORD=connectorx \
  -e POSTGRES_DB=connectorx \
  -p "${postgres_port}:5432" \
  "${postgres_image}" >/dev/null

cleanup() {
  if [ "${ODBC_POSTGRES_KEEP:-0}" != "1" ]; then
    docker rm -f "${container_name}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

for _ in $(seq 1 60); do
  if docker exec "${container_name}" pg_isready -U connectorx -d connectorx >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

docker exec -i "${container_name}" \
  psql -U connectorx -d connectorx \
  < "$(dirname "$0")/odbc_postgres.sql"

export ODBC_CONN="Driver={${driver_name}};Server=127.0.0.1;Port=${postgres_port};Database=connectorx;UID=connectorx;PWD=connectorx;"
export ODBC_URL="odbc://connectorx:connectorx@127.0.0.1:${postgres_port}/connectorx?driver=$(printf '%s' "${driver_name}" | sed 's/ /%20/g')"
export ODBC_TEST_QUERY="select id, flag, name from cx_odbc_test order by id"
export ODBC_PARTITION_QUERY="select id, flag, name from cx_odbc_test"
export ODBC_PARTITION_COLUMN="id"
export ODBC_EXPECTED_ROWS="2"

cargo test -p connectorx --no-default-features --features "src_odbc dst_arrow fptr" --test test_odbc -- --nocapture
