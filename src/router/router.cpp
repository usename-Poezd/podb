#include "router/router.h"

#include <cstdio>
#include <utility>

#include "core/worker.h"

namespace db {

Router::Router(int local_core_id, std::vector<Worker *> all_workers,
               std::function<void(Task)> local_execute)
    : local_core_id_(local_core_id), num_cores_(static_cast<int>(all_workers.size())),
      all_workers_(std::move(all_workers)), local_execute_(std::move(local_execute)) {}

int Router::RouteKey(const std::string &key) const noexcept {
  return static_cast<int>(std::hash<std::string>{}(key) % static_cast<std::size_t>(num_cores_));
}

void Router::RouteTask(Task task) {
  const int target = RouteKey(task.key);
  const uint32_t rid = static_cast<uint32_t>(task.request_id & 0xFFFFFFFF);
  if (target == local_core_id_) {
    std::printf("[Core %d] ROUT %s \"%.20s\" rid=%-4u → local\n", local_core_id_,
                TaskTypeName(task.type), task.key.c_str(), rid);
    local_execute_(std::move(task));
  } else {
    std::printf("[Core %d] ROUT %s \"%.20s\" rid=%-4u → Core %d\n", local_core_id_,
                TaskTypeName(task.type), task.key.c_str(), rid, target);
    all_workers_[target]->PushTask(std::move(task));
  }
}

} // namespace db
