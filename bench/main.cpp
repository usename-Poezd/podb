#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "options.h"
#include "stats.h"
#include "worker.h"

namespace {

using bench::LatencyReport;
using bench::Mode;
using bench::Options;
using bench::ThreadStats;

void PrintConfig(const Options &opts, int resolved_threads) {
  std::cout << "db_bench config:\n"
            << "  address        = " << opts.address << "\n"
            << "  mode           = " << (opts.mode == Mode::kKv ? "kv" : "tx") << "\n"
            << "  threads        = " << resolved_threads << "\n";
  if (opts.requests > 0) {
    std::cout << "  requests       = " << opts.requests << "\n";
  } else {
    std::cout << "  duration_s     = " << opts.duration_s << "\n";
  }
  std::cout << "  warmup_s       = " << opts.warmup_s << "\n"
            << "  keyspace       = " << opts.keyspace << "\n"
            << "  value_size     = " << opts.value_size << "\n"
            << "  read_ratio     = " << opts.read_ratio << "\n";
  if (opts.mode == Mode::kTx) {
    std::cout << "  tx_ops         = " << opts.tx_ops << "\n"
              << "  tx_priority    = " << opts.tx_priority << "\n"
              << "  max_retries    = " << opts.max_retries << "\n"
              << "  honor_retry_after = " << (opts.honor_retry_after ? "true" : "false") << "\n";
  }
  std::cout << "  rpc_timeout_ms = " << opts.rpc_timeout_ms << "\n\n";
}

void PrintLatencyHeader() {
  std::cout << std::left << std::setw(24) << "metric" << std::right << std::setw(10) << "count"
            << std::setw(12) << "qps" << std::setw(10) << "p50ms" << std::setw(10) << "p95ms" << std::setw(10)
            << "p99ms" << std::setw(10) << "p999ms" << "\n";
}

void PrintLatencyRow(const std::string &name, const LatencyReport &r) {
  std::cout << std::left << std::setw(24) << name << std::right << std::setw(10) << r.count << std::setw(12)
            << std::fixed << std::setprecision(1) << r.qps << std::setw(10) << std::setprecision(3) << r.p50_ms
            << std::setw(10) << r.p95_ms << std::setw(10) << r.p99_ms << std::setw(10) << r.p999_ms << "\n";
}

void AppendCsv(const Options &opts, int resolved_threads, double measured_duration_s, const ThreadStats &totals,
               const LatencyReport &primary, const LatencyReport &commit_report) {
  const bool file_exists = std::ifstream(opts.csv_path).good();
  std::ofstream out(opts.csv_path, std::ios::app);
  if (!file_exists) {
    out << "label,timestamp,mode,address,threads,duration_s,keyspace,value_size,read_ratio,"
           "tx_ops,total_attempts,committed,conflict_aborts,other_aborts,retries,qps,"
           "p50_ms,p95_ms,p99_ms,p999_ms,commit_p50_ms,commit_p99_ms\n";
  }

  const uint64_t total_attempts = opts.mode == Mode::kTx
      ? totals.tx_committed + totals.tx_conflict_abort + totals.tx_other_abort
      : totals.get_ok + totals.get_rpc_err + totals.set_ok + totals.set_app_err + totals.set_rpc_err;
  const auto now_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

  out << opts.label << ',' << now_t << ',' << (opts.mode == Mode::kKv ? "kv" : "tx") << ',' << opts.address << ','
      << resolved_threads << ',' << measured_duration_s << ',' << opts.keyspace << ',' << opts.value_size << ','
      << opts.read_ratio << ',' << opts.tx_ops << ',' << total_attempts << ',' << totals.tx_committed << ','
      << totals.tx_conflict_abort << ',' << totals.tx_other_abort << ',' << totals.retry_count << ','
      << primary.qps << ',' << primary.p50_ms << ',' << primary.p95_ms << ',' << primary.p99_ms << ','
      << primary.p999_ms << ',' << commit_report.p50_ms << ',' << commit_report.p99_ms << '\n';
}

}  // namespace

int main(int argc, char **argv) {
  const std::vector<std::string> args(argv + 1, argv + argc);
  bench::ParseResult parsed = bench::ParseArgs(args);

  if (parsed.outcome == bench::ParseOutcome::kHelpRequested) {
    std::cout << parsed.help_text << "\n";
    return 0;
  }
  if (parsed.outcome == bench::ParseOutcome::kError) {
    std::cerr << "Error: " << parsed.error_message << "\n";
    return 1;
  }

  const Options opts = parsed.options;
  const int threads = opts.threads > 0
      ? opts.threads
      : static_cast<int>(std::max(1u, std::thread::hardware_concurrency()));

  const auto now = std::chrono::steady_clock::now();
  bench::RunControl control;
  control.warmup_end = now + std::chrono::seconds(opts.warmup_s);
  control.measure_end = control.warmup_end + std::chrono::seconds(opts.duration_s);

  std::atomic<int64_t> remaining{opts.requests};
  control.remaining_requests = opts.requests > 0 ? &remaining : nullptr;

  std::vector<ThreadStats> per_thread(threads);
  std::vector<std::thread> workers;
  workers.reserve(threads);
  for (int i = 0; i < threads; ++i) {
    workers.emplace_back([&opts, &control, &per_thread, i]() {
      if (opts.mode == Mode::kKv) {
        bench::RunKvWorker(opts, control, per_thread[i]);
      } else {
        bench::RunTxWorker(opts, control, per_thread[i]);
      }
    });
  }
  for (auto &t : workers) t.join();

  double measured_duration_s =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - control.warmup_end).count();
  if (measured_duration_s <= 0.0) measured_duration_s = 1e-9;

  const ThreadStats totals = bench::MergeStats(per_thread);

  PrintConfig(opts, threads);
  PrintLatencyHeader();

  LatencyReport primary_report;
  LatencyReport commit_report;

  if (opts.mode == Mode::kKv) {
    const auto get_report = bench::BuildLatencyReport(totals.get_us, measured_duration_s);
    const auto set_report = bench::BuildLatencyReport(totals.set_us, measured_duration_s);
    std::vector<uint32_t> combined = totals.get_us;
    combined.insert(combined.end(), totals.set_us.begin(), totals.set_us.end());
    const auto combined_report = bench::BuildLatencyReport(combined, measured_duration_s);

    PrintLatencyRow("get", get_report);
    PrintLatencyRow("set", set_report);
    PrintLatencyRow("combined", combined_report);
    primary_report = combined_report;

    std::cout << "\nErrors: get_rpc_err=" << totals.get_rpc_err << " set_app_err=" << totals.set_app_err
              << " set_rpc_err=" << totals.set_rpc_err << " timeouts=" << totals.timeout_count << "\n";
  } else {
    const auto exec_report = bench::BuildLatencyReport(totals.tx_execute_us, measured_duration_s);
    const auto cycle_all_report = bench::BuildLatencyReport(totals.tx_cycle_all_us, measured_duration_s);
    const auto cycle_committed_report = bench::BuildLatencyReport(totals.tx_cycle_committed_us, measured_duration_s);
    commit_report = bench::BuildLatencyReport(totals.commit_us, measured_duration_s);

    PrintLatencyRow("tx-execute", exec_report);
    PrintLatencyRow("tx-cycle-all", cycle_all_report);
    PrintLatencyRow("tx-cycle-committed", cycle_committed_report);
    PrintLatencyRow("commit-only", commit_report);
    primary_report = cycle_all_report;

    const uint64_t total_attempts = totals.tx_committed + totals.tx_conflict_abort + totals.tx_other_abort;
    const double committed_pct = total_attempts ? 100.0 * static_cast<double>(totals.tx_committed) / static_cast<double>(total_attempts) : 0.0;
    const double conflict_pct = total_attempts ? 100.0 * static_cast<double>(totals.tx_conflict_abort) / static_cast<double>(total_attempts) : 0.0;
    const double other_pct = total_attempts ? 100.0 * static_cast<double>(totals.tx_other_abort) / static_cast<double>(total_attempts) : 0.0;
    const double avg_retries = total_attempts ? static_cast<double>(totals.retry_count) / static_cast<double>(total_attempts) : 0.0;

    std::cout << "\nTx outcomes: total=" << total_attempts << " committed=" << totals.tx_committed << " ("
              << std::fixed << std::setprecision(1) << committed_pct << "%)"
              << " conflict_abort=" << totals.tx_conflict_abort << " (" << conflict_pct << "%)"
              << " other_abort=" << totals.tx_other_abort << " (" << other_pct << "%)"
              << " retries=" << totals.retry_count << " avg_retries_per_tx=" << std::setprecision(3) << avg_retries
              << "\n";
  }

  if (!opts.csv_path.empty()) {
    AppendCsv(opts, threads, measured_duration_s, totals, primary_report, commit_report);
  }

  return 0;
}
