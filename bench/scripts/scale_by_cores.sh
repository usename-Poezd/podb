#!/usr/bin/env bash
# Runs db_bench against db_engine started with several --cores values, to
# measure throughput/latency scaling. Each core count gets a fresh scratch
# data-dir (so main.cpp's topology-mismatch guard never triggers) and runs
# sequentially (one server at a time, so runs don't contend for CPU).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DB_ENGINE="${REPO_ROOT}/build/src/db_engine"
DB_BENCH="${REPO_ROOT}/build/bench/db_bench"

CORE_COUNTS=(1 2 4 8)
PORT=19906
BENCH_ARGS=(--mode kv --duration 20 --threads 32 --keyspace 100000 --value-size 128 --read-ratio 0.5)
CSV_OUT="${REPO_ROOT}/scaling_results.csv"

if [[ ! -x "${DB_ENGINE}" || ! -x "${DB_BENCH}" ]]; then
  echo "error: db_engine/db_bench not found under ${REPO_ROOT}/build — run 'make build' first" >&2
  exit 1
fi

rm -f "${CSV_OUT}"
BASE_DATA_DIR="$(mktemp -d)"
trap 'rm -rf "${BASE_DATA_DIR}"' EXIT

for cores in "${CORE_COUNTS[@]}"; do
  data_dir="${BASE_DATA_DIR}/cores_${cores}"
  mkdir -p "${data_dir}"

  echo "=== cores=${cores} === (server log: ${data_dir}/server.log)"
  # db_engine printf-logs every single request/response in its hot path;
  # left un-redirected this both floods the caller's terminal and, worse,
  # forces the server through slow line-buffered TTY writes on every RPC,
  # which measurably caps achievable throughput. Route it to a file instead
  # (fully buffered) so the benchmark measures the server, not the terminal.
  "${DB_ENGINE}" --cores "${cores}" --port "${PORT}" --data-dir "${data_dir}" \
      > "${data_dir}/server.log" 2>&1 &
  server_pid=$!

  # A bare TCP connect can succeed before the gRPC service is registered
  # (that happens after WAL recovery in main.cpp), so probe with db_bench
  # itself instead.
  ready=0
  for _ in $(seq 1 50); do
    if "${DB_BENCH}" --address "127.0.0.1:${PORT}" --mode kv --requests 1 --threads 1 >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 0.2
  done
  if [[ "${ready}" -ne 1 ]]; then
    echo "error: db_engine did not become ready for cores=${cores}" >&2
    kill "${server_pid}" 2>/dev/null || true
    wait "${server_pid}" 2>/dev/null || true
    exit 1
  fi

  "${DB_BENCH}" --address "127.0.0.1:${PORT}" "${BENCH_ARGS[@]}" --label "cores=${cores}" --csv "${CSV_OUT}"

  kill "${server_pid}" 2>/dev/null || true
  wait "${server_pid}" 2>/dev/null || true
done

echo
echo "Results written to ${CSV_OUT}"
column -s, -t "${CSV_OUT}"
