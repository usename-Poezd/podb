# db_bench

Standalone gRPC load-testing client for `db_engine`. It is a pure client — it
talks to an already-running server over the `Database` gRPC service and
never touches server-internal code.

Built alongside the server by the normal build (`make build`); the binary
lands at `./build/bench/db_bench` (the server binary is `./build/src/db_engine`).

## Execution model

Each `--threads` worker owns its own gRPC channel + stub and issues
requests in a closed loop (blocking call → immediately issue the next).
This keeps the tool simple, but it means a momentary server stall makes a
thread wait rather than keep enqueueing at a fixed rate ("coordinated
omission") — tail-latency numbers under saturation should be read with that
in mind. gRPC's own transport-level retries are disabled on the channel so
they can't interfere with `db_bench`'s own `write_write_conflict` retry
logic.

## Modes

- `--mode kv` — non-transactional `Get`/`Set`, mixed by `--read-ratio`.
- `--mode tx` — `BeginTransaction → Execute×<--tx-ops> → Commit`, retrying
  an `Execute` up to `--max-retries` times on `write_write_conflict`
  (optionally honoring the server's `retry_after_ms` backoff hint). A tx
  gets exactly one `Commit` attempt; any other failure rolls back and is
  counted as `other_abort`.

## Flags

| Flag | Default | Purpose |
|---|---|---|
| `--address, -a` | `127.0.0.1:9906` | server target |
| `--mode, -m` | `kv` | `kv` or `tx` |
| `--duration, -d` | `10` (s) | measured-phase length; mutually exclusive with `--requests` |
| `--requests, -n` | unset | fixed attempt count instead of time-based stop |
| `--threads, -t` | `hardware_concurrency()` | worker/connection count |
| `--warmup-seconds` | `2` | discarded warmup phase, `0` disables |
| `--keyspace, -k` | `100000` | distinct key count, uniform |
| `--value-size` | `128` | fixed SET payload size in bytes |
| `--read-ratio` | `0.5` | fraction of ops that are GET (both modes) |
| `--tx-ops` | `4` | Execute calls per tx (tx mode) |
| `--tx-priority` | `0` | `BeginTxRequest.priority` (0 = server default) |
| `--max-retries` | `5` | Execute conflict-retry cap |
| `--honor-retry-after` | `true` | sleep server's `retry_after_ms` hint before retrying |
| `--rpc-timeout-ms` | `5000` | per-RPC deadline |
| `--csv` | unset | append a summary row to this file (header written if new) |
| `--label` | `""` | free-form tag written to the CSV row (e.g. `cores=4`) |

## Examples

```bash
# Non-transactional throughput/latency
./build/bench/db_bench --mode kv --duration 20 --threads 32 --keyspace 100000

# Transaction lifecycle, low contention (large keyspace)
./build/bench/db_bench --mode tx --duration 20 --threads 32 --keyspace 100000 --tx-ops 4

# Transaction lifecycle, forced contention (small keyspace) — measures conflict/abort rate
./build/bench/db_bench --mode tx --duration 20 --threads 32 --keyspace 200 --tx-ops 4
```

## Manual smoke test

1. `./build/src/db_engine --cores 2 --port 19906 --data-dir /tmp/podb_bench_smoke`
2. `./build/bench/db_bench --address 127.0.0.1:19906 --mode kv --duration 3 --threads 4`
   → nonzero QPS, `p50 < p99 < p999`, zero RPC errors, `get_ok + set_ok == total`.
3. `./build/bench/db_bench --address 127.0.0.1:19906 --mode tx --duration 3 --threads 4 --tx-ops 4 --keyspace 50`
   → `committed + conflict_abort + other_abort == total_attempts`, `retry_count > 0`,
   commit-only latency lower than full tx-cycle latency.
4. Run `db_bench` against a port with no server listening → fails fast within
   `--rpc-timeout-ms`, doesn't hang.

## Scaling by core count (throughput/latency vs. `--cores`)

`scripts/scale_by_cores.sh` starts `db_engine` at several `--cores` values
(fresh scratch data-dir each time), runs `db_bench` against each, and
collects results into a CSV:

```bash
./bench/scripts/scale_by_cores.sh
column -s, -t scaling_results.csv
```

Edit the `CORE_COUNTS` / `BENCH_ARGS` variables at the top of the script to
change the sweep. Run it once with a large `--keyspace` (low contention) for
throughput/latency scaling, and separately with a small `--keyspace --mode
tx` to see how conflict rate changes with core count.
