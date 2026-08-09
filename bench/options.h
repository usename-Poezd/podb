#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace bench {

enum class Mode { kKv, kTx };

struct Options {
  std::string address = "127.0.0.1:9906";
  Mode mode = Mode::kKv;
  int duration_s = 10;
  int64_t requests = 0;  // 0 = unset, use duration-based stop instead
  int threads = 0;       // 0 = resolve to hardware_concurrency at runtime
  int warmup_s = 2;
  int64_t keyspace = 100000;
  uint32_t value_size = 128;
  double read_ratio = 0.5;
  int tx_ops = 4;
  uint32_t tx_priority = 0;
  int max_retries = 5;
  bool honor_retry_after = true;
  int rpc_timeout_ms = 5000;
  std::string csv_path;
  std::string label;
};

enum class ParseOutcome { kOk, kHelpRequested, kError };

struct ParseResult {
  ParseOutcome outcome = ParseOutcome::kError;
  Options options;
  std::string error_message;  // set when outcome == kError
  std::string help_text;      // set when outcome == kHelpRequested
};

// Parses args (NOT including argv[0]). Pure function of its input, so it's
// unit-testable without a live process/argv.
ParseResult ParseArgs(const std::vector<std::string>& args);

}  // namespace bench
