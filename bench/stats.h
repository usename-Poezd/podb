#pragma once

#include <cstdint>
#include <vector>

namespace bench {

struct ThreadStats {
  std::vector<uint32_t> get_us;
  std::vector<uint32_t> set_us;
  std::vector<uint32_t> tx_execute_us;
  std::vector<uint32_t> tx_cycle_all_us;
  std::vector<uint32_t> tx_cycle_committed_us;
  std::vector<uint32_t> commit_us;

  uint64_t get_ok = 0;
  uint64_t get_rpc_err = 0;
  uint64_t set_ok = 0;
  uint64_t set_app_err = 0;
  uint64_t set_rpc_err = 0;

  uint64_t tx_committed = 0;
  uint64_t tx_conflict_abort = 0;
  uint64_t tx_other_abort = 0;
  uint64_t retry_count = 0;
  uint64_t timeout_count = 0;
};

// Nearest-rank percentile: index = ceil(p * N), 1-indexed. p in [0,1].
// Returns 0.0 for an empty sample set.
double Percentile(const std::vector<uint32_t> &sorted_us, double p);

struct LatencyReport {
  uint64_t count = 0;
  double qps = 0.0;
  double p50_ms = 0.0;
  double p95_ms = 0.0;
  double p99_ms = 0.0;
  double p999_ms = 0.0;
};

// Sorts a copy of samples_us (microseconds) and derives count/qps/percentiles.
LatencyReport BuildLatencyReport(std::vector<uint32_t> samples_us, double duration_s);

// Concatenates latency samples and sums counters across threads.
ThreadStats MergeStats(const std::vector<ThreadStats> &per_thread);

}  // namespace bench
