#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace db {

struct alignas(8) Slot {
  uint32_t next_free_index = 0;
  uint32_t generation = 0;
};

class SlabAllocator {
public:
  explicit SlabAllocator(size_t capacity = 65536) : slots_(capacity), free_head_(0) {
    for (size_t i = 0; i < capacity; ++i) {
      slots_[i].next_free_index = static_cast<uint32_t>(i + 1);
    }
  }

  SlabAllocator(const SlabAllocator &) = delete;
  SlabAllocator &operator=(const SlabAllocator &) = delete;

  uint64_t Allocate() {
    if (free_head_ >= slots_.size()) {
      throw std::runtime_error("SlabAllocator capacity exhausted!");
    }
    uint32_t index = free_head_;
    free_head_ = slots_[index].next_free_index;
    uint64_t request_id = (static_cast<uint64_t>(slots_[index].generation) << 32) | index;
    return request_id;
  }

  void GetAndFree(uint64_t request_id) {
    uint32_t index = static_cast<uint32_t>(request_id & 0xFFFFFFFF);
    uint32_t generation = static_cast<uint32_t>(request_id >> 32);
    if (index >= slots_.size() || slots_[index].generation != generation) {
      return;
    }
    slots_[index].generation++;
    slots_[index].next_free_index = free_head_;
    free_head_ = index;
  }

private:
  std::vector<Slot> slots_;
  uint32_t free_head_;
};

} // namespace db
