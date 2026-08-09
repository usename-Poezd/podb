#pragma once

#include <cstdint>
#include <random>
#include <string>

namespace bench {

// Per-thread key/value generator. Not thread-safe — each worker thread owns
// its own instance.
class Workload {
 public:
  Workload(int64_t keyspace, uint32_t value_size, double read_ratio);

  std::string NextKey();
  bool NextIsRead();

  // Fixed-content buffer reused for every SET call, so RNG/allocation cost
  // never becomes the client-side bottleneck (the server doesn't inspect
  // value content).
  const std::string &ValueBuffer() const { return value_buffer_; }

 private:
  int64_t keyspace_;
  double read_ratio_;
  std::mt19937_64 rng_;
  std::uniform_int_distribution<int64_t> key_dist_;
  std::uniform_real_distribution<double> ratio_dist_;
  std::string value_buffer_;
};

}  // namespace bench
