#include "workload.h"

#include <algorithm>

namespace bench {

Workload::Workload(int64_t keyspace, uint32_t value_size, double read_ratio)
    : keyspace_(std::max<int64_t>(keyspace, 1)),
      read_ratio_(read_ratio),
      rng_(std::random_device{}()),
      key_dist_(0, keyspace_ - 1),
      ratio_dist_(0.0, 1.0),
      value_buffer_(value_size, 'A') {}

std::string Workload::NextKey() { return "key:" + std::to_string(key_dist_(rng_)); }

bool Workload::NextIsRead() { return ratio_dist_(rng_) < read_ratio_; }

}  // namespace bench
