#include <gtest/gtest.h>

#include "options.h"
#include "stats.h"

namespace bench {
namespace {

TEST(bench_stats, PercentileNearestRank) {
  const std::vector<uint32_t> data = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
  EXPECT_DOUBLE_EQ(Percentile(data, 0.50), 50.0);
  EXPECT_DOUBLE_EQ(Percentile(data, 0.90), 90.0);
  EXPECT_DOUBLE_EQ(Percentile(data, 1.0), 100.0);
  EXPECT_DOUBLE_EQ(Percentile(data, 0.01), 10.0);
}

TEST(bench_stats, PercentileEmpty) {
  const std::vector<uint32_t> data;
  EXPECT_DOUBLE_EQ(Percentile(data, 0.5), 0.0);
}

TEST(bench_stats, BuildLatencyReportComputesQpsAndPercentiles) {
  const std::vector<uint32_t> samples = {100, 200, 300, 400};  // microseconds
  const auto report = BuildLatencyReport(samples, 2.0);
  EXPECT_EQ(report.count, 4u);
  EXPECT_DOUBLE_EQ(report.qps, 2.0);
  EXPECT_DOUBLE_EQ(report.p50_ms, 0.2);
  EXPECT_DOUBLE_EQ(report.p999_ms, 0.4);
}

TEST(bench_stats, BuildLatencyReportZeroDurationYieldsZeroQps) {
  const std::vector<uint32_t> samples = {100};
  const auto report = BuildLatencyReport(samples, 0.0);
  EXPECT_DOUBLE_EQ(report.qps, 0.0);
}

TEST(bench_stats, MergeStatsConcatenatesAndSumsCounters) {
  ThreadStats a;
  a.get_us = {1, 2};
  a.get_ok = 2;
  a.tx_committed = 3;
  ThreadStats b;
  b.get_us = {3};
  b.get_ok = 1;
  b.tx_committed = 1;

  const auto merged = MergeStats({a, b});
  EXPECT_EQ(merged.get_us.size(), 3u);
  EXPECT_EQ(merged.get_ok, 3u);
  EXPECT_EQ(merged.tx_committed, 4u);
}

TEST(bench_options, ParseArgsRejectsUnknownMode) {
  const auto result = ParseArgs({"--mode", "bogus"});
  EXPECT_EQ(result.outcome, ParseOutcome::kError);
}

TEST(bench_options, ParseArgsRejectsDurationAndRequestsTogether) {
  const auto result = ParseArgs({"--duration", "5", "--requests", "100"});
  EXPECT_EQ(result.outcome, ParseOutcome::kError);
}

TEST(bench_options, ParseArgsRejectsOutOfRangeReadRatio) {
  const auto result = ParseArgs({"--read-ratio", "1.5"});
  EXPECT_EQ(result.outcome, ParseOutcome::kError);
}

TEST(bench_options, ParseArgsAcceptsValidRequestsOnly) {
  const auto result = ParseArgs({"--requests", "500"});
  ASSERT_EQ(result.outcome, ParseOutcome::kOk);
  EXPECT_EQ(result.options.requests, 500);
}

TEST(bench_options, ParseArgsHelpRequested) {
  const auto result = ParseArgs({"--help"});
  EXPECT_EQ(result.outcome, ParseOutcome::kHelpRequested);
}

TEST(bench_options, ParseArgsDefaultsToKvMode) {
  const auto result = ParseArgs({});
  ASSERT_EQ(result.outcome, ParseOutcome::kOk);
  EXPECT_EQ(result.options.mode, Mode::kKv);
}

}  // namespace
}  // namespace bench
