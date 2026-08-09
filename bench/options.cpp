#include "options.h"

#include <sstream>

#include <boost/program_options.hpp>

namespace bench {

namespace po = boost::program_options;

namespace {

po::options_description BuildDescription(Options &defaults) {
  po::options_description desc("db_bench options");
  desc.add_options()
      ("help,h", "Show this help message")
      ("address,a", po::value<std::string>(&defaults.address)->default_value(defaults.address),
       "Server address host:port")
      ("mode,m", po::value<std::string>()->default_value("kv"),
       "Workload mode: kv or tx")
      ("duration,d", po::value<int>(&defaults.duration_s)->default_value(defaults.duration_s),
       "Measured phase duration in seconds")
      ("requests,n", po::value<int64_t>(&defaults.requests)->default_value(defaults.requests),
       "Fixed attempt count instead of duration-based stop (0 = use --duration)")
      ("threads,t", po::value<int>(&defaults.threads)->default_value(defaults.threads),
       "Worker/connection thread count (0 = hardware_concurrency)")
      ("warmup-seconds", po::value<int>(&defaults.warmup_s)->default_value(defaults.warmup_s),
       "Discarded warmup phase length in seconds")
      ("keyspace,k", po::value<int64_t>(&defaults.keyspace)->default_value(defaults.keyspace),
       "Number of distinct keys, uniformly sampled")
      ("value-size", po::value<uint32_t>(&defaults.value_size)->default_value(defaults.value_size),
       "SET payload size in bytes")
      ("read-ratio", po::value<double>(&defaults.read_ratio)->default_value(defaults.read_ratio),
       "Fraction of ops that are reads, in [0,1]")
      ("tx-ops", po::value<int>(&defaults.tx_ops)->default_value(defaults.tx_ops),
       "Execute calls per transaction (tx mode)")
      ("tx-priority", po::value<uint32_t>(&defaults.tx_priority)->default_value(defaults.tx_priority),
       "BeginTransaction priority (0 = server default)")
      ("max-retries", po::value<int>(&defaults.max_retries)->default_value(defaults.max_retries),
       "Max Execute retries on write_write_conflict")
      ("honor-retry-after", po::value<bool>(&defaults.honor_retry_after)->default_value(defaults.honor_retry_after),
       "Sleep the server's retry_after_ms hint before retrying")
      ("rpc-timeout-ms", po::value<int>(&defaults.rpc_timeout_ms)->default_value(defaults.rpc_timeout_ms),
       "Per-RPC deadline in milliseconds")
      ("csv", po::value<std::string>(&defaults.csv_path)->default_value(""),
       "Append a summary row to this CSV file")
      ("label", po::value<std::string>(&defaults.label)->default_value(""),
       "Free-form label written to the CSV row");
  return desc;
}

}  // namespace

ParseResult ParseArgs(const std::vector<std::string> &args) {
  ParseResult result;
  Options opts;
  po::options_description desc = BuildDescription(opts);

  po::variables_map vm;
  try {
    po::store(po::command_line_parser(args).options(desc).run(), vm);
    po::notify(vm);
  } catch (const std::exception &e) {
    result.outcome = ParseOutcome::kError;
    result.error_message = e.what();
    return result;
  }

  if (vm.count("help")) {
    std::ostringstream oss;
    oss << desc;
    result.outcome = ParseOutcome::kHelpRequested;
    result.help_text = oss.str();
    return result;
  }

  const std::string mode_str = vm["mode"].as<std::string>();
  if (mode_str == "kv") {
    opts.mode = Mode::kKv;
  } else if (mode_str == "tx") {
    opts.mode = Mode::kTx;
  } else {
    result.error_message = "--mode must be 'kv' or 'tx', got '" + mode_str + "'";
    return result;
  }

  const bool duration_explicit = !vm["duration"].defaulted();
  if (opts.requests > 0 && duration_explicit) {
    result.error_message = "--duration and --requests are mutually exclusive";
    return result;
  }
  if (opts.requests < 0) {
    result.error_message = "--requests must be >= 0";
    return result;
  }
  if (opts.duration_s < 1 && opts.requests <= 0) {
    result.error_message = "--duration must be >= 1";
    return result;
  }
  if (opts.read_ratio < 0.0 || opts.read_ratio > 1.0) {
    result.error_message = "--read-ratio must be within [0,1]";
    return result;
  }
  if (opts.threads < 0) {
    result.error_message = "--threads must be >= 0";
    return result;
  }
  if (opts.keyspace < 1) {
    result.error_message = "--keyspace must be >= 1";
    return result;
  }
  if (opts.tx_ops < 1) {
    result.error_message = "--tx-ops must be >= 1";
    return result;
  }
  if (opts.max_retries < 0) {
    result.error_message = "--max-retries must be >= 0";
    return result;
  }
  if (opts.rpc_timeout_ms < 1) {
    result.error_message = "--rpc-timeout-ms must be >= 1";
    return result;
  }
  if (opts.warmup_s < 0) {
    result.error_message = "--warmup-seconds must be >= 0";
    return result;
  }

  result.outcome = ParseOutcome::kOk;
  result.options = opts;
  return result;
}

}  // namespace bench
