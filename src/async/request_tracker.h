#pragma once

#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

#include "core/slab_allocator.h"
#include "core/task.h"

namespace db {

class RequestTracker {
public:
  explicit RequestTracker(size_t capacity = 65536)
      : slab_(capacity), response_slots_(capacity), completion_handlers_(capacity) {}

  uint64_t AllocSlot() {
    return slab_.Allocate();
  }

  void SetCompletion(uint32_t index, std::function<void()> handler) {
    completion_handlers_[index] = std::move(handler);
  }

  Task GetResponse(uint32_t index) {
    return std::move(response_slots_[index]);
  }

  void Fulfill(uint64_t request_id, Task response) {
    uint32_t index = static_cast<uint32_t>(request_id & 0xFFFFFFFF);
    if (index < response_slots_.size()) {
      response_slots_[index] = std::move(response);
    }
    slab_.GetAndFree(request_id);
    if (index < completion_handlers_.size() && completion_handlers_[index]) {
      auto handler = std::move(completion_handlers_[index]);
      completion_handlers_[index] = nullptr;
      handler();
    }
  }

private:
  SlabAllocator slab_;
  std::vector<Task> response_slots_;
  std::vector<std::function<void()>> completion_handlers_;
};

}
