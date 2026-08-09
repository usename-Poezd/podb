#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

#include "options.h"
#include "stats.h"

namespace bench {

// Absolute time boundaries and (optionally) a shared request budget, computed
// once by the orchestrator before spawning worker threads so no shared
// mutable state is touched on the hot path other than the atomic budget.
struct RunControl {
  std::chrono::steady_clock::time_point warmup_end;
  std::chrono::steady_clock::time_point measure_end;   // only consulted when remaining_requests == nullptr
  std::atomic<int64_t> *remaining_requests = nullptr;  // non-null => --requests mode
};

// Each worker owns its own gRPC channel/stub and Workload instance, runs a
// discarded warmup phase, then a measured phase, writing samples into `stats`.
void RunKvWorker(const Options &opts, const RunControl &control, ThreadStats &stats);
void RunTxWorker(const Options &opts, const RunControl &control, ThreadStats &stats);

}  // namespace bench
