#include "stats.h"

#include <algorithm>
#include <cmath>

namespace bench {

double Percentile(const std::vector<uint32_t> &sorted_us, double p) {
  if (sorted_us.empty()) return 0.0;
  const double clamped_p = std::clamp(p, 0.0, 1.0);
  const size_t n = sorted_us.size();
  size_t idx = static_cast<size_t>(std::ceil(clamped_p * static_cast<double>(n)));
  idx = std::clamp<size_t>(idx, 1, n);
  return static_cast<double>(sorted_us[idx - 1]);
}

LatencyReport BuildLatencyReport(std::vector<uint32_t> samples_us, double duration_s) {
  LatencyReport report;
  report.count = samples_us.size();
  report.qps = duration_s > 0.0 ? static_cast<double>(report.count) / duration_s : 0.0;

  std::sort(samples_us.begin(), samples_us.end());
  report.p50_ms = Percentile(samples_us, 0.50) / 1000.0;
  report.p95_ms = Percentile(samples_us, 0.95) / 1000.0;
  report.p99_ms = Percentile(samples_us, 0.99) / 1000.0;
  report.p999_ms = Percentile(samples_us, 0.999) / 1000.0;
  return report;
}

ThreadStats MergeStats(const std::vector<ThreadStats> &per_thread) {
  ThreadStats out;

  size_t get_n = 0, set_n = 0, exec_n = 0, cycle_n = 0, cycle_committed_n = 0, commit_n = 0;
  for (const auto &s : per_thread) {
    get_n += s.get_us.size();
    set_n += s.set_us.size();
    exec_n += s.tx_execute_us.size();
    cycle_n += s.tx_cycle_all_us.size();
    cycle_committed_n += s.tx_cycle_committed_us.size();
    commit_n += s.commit_us.size();
  }
  out.get_us.reserve(get_n);
  out.set_us.reserve(set_n);
  out.tx_execute_us.reserve(exec_n);
  out.tx_cycle_all_us.reserve(cycle_n);
  out.tx_cycle_committed_us.reserve(cycle_committed_n);
  out.commit_us.reserve(commit_n);

  for (const auto &s : per_thread) {
    out.get_us.insert(out.get_us.end(), s.get_us.begin(), s.get_us.end());
    out.set_us.insert(out.set_us.end(), s.set_us.begin(), s.set_us.end());
    out.tx_execute_us.insert(out.tx_execute_us.end(), s.tx_execute_us.begin(), s.tx_execute_us.end());
    out.tx_cycle_all_us.insert(out.tx_cycle_all_us.end(), s.tx_cycle_all_us.begin(), s.tx_cycle_all_us.end());
    out.tx_cycle_committed_us.insert(out.tx_cycle_committed_us.end(), s.tx_cycle_committed_us.begin(),
                                      s.tx_cycle_committed_us.end());
    out.commit_us.insert(out.commit_us.end(), s.commit_us.begin(), s.commit_us.end());

    out.get_ok += s.get_ok;
    out.get_rpc_err += s.get_rpc_err;
    out.set_ok += s.set_ok;
    out.set_app_err += s.set_app_err;
    out.set_rpc_err += s.set_rpc_err;
    out.tx_committed += s.tx_committed;
    out.tx_conflict_abort += s.tx_conflict_abort;
    out.tx_other_abort += s.tx_other_abort;
    out.retry_count += s.retry_count;
    out.timeout_count += s.timeout_count;
  }
  return out;
}

}  // namespace bench
