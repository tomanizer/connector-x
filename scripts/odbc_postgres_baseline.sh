#!/usr/bin/env bash
set -euo pipefail

artifact_dir="${ODBC_BENCH_ARTIFACT_DIR:-target/odbc-performance}"
runtime_json="${artifact_dir}/odbc-runtime.json"
baseline_json="${artifact_dir}/odbc-performance-baseline.json"
baseline_csv="${artifact_dir}/odbc-performance-baseline.csv"

export ODBC_POSTGRES_DRIVER="${ODBC_POSTGRES_DRIVER:-PostgreSQL Unicode}"
export ODBC_BENCH_ROWS="${ODBC_BENCH_ROWS:-100000}"
export ODBC_BENCH_BATCH_SIZES="${ODBC_BENCH_BATCH_SIZES:-1024,4096,8192,16384}"
export ODBC_BENCH_PARTITIONS="${ODBC_BENCH_PARTITIONS:-4}"

mkdir -p "${artifact_dir}"

python3 scripts/odbc_runtime_smoke.py --json "${runtime_json}"

scripts/odbc_postgres_bench.sh "$@"

summary_args=()
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  summary_args=(--summary "${GITHUB_STEP_SUMMARY}")
fi

python3 scripts/odbc_bench_report.py \
  --runtime-json "${runtime_json}" \
  --output-json "${baseline_json}" \
  --output-csv "${baseline_csv}" \
  "${summary_args[@]}"

echo "ODBC performance baseline JSON: ${baseline_json}"
echo "ODBC performance baseline CSV: ${baseline_csv}"
