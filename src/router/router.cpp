#include "router/router.h"

#include <cstdio>
#include <utility>

#include "core/worker.h"

namespace db {

Router::Router(int local_core_id, std::vector<Worker *> all_workers, StorageEngine &local_storage)
    : local_core_id_(local_core_id), num_cores_(static_cast<int>(all_workers.size())),
      all_workers_(std::move(all_workers)), local_storage_(local_storage),
      executor_(local_storage_, local_core_id_) {}

int Router::RouteKey(const std::string &key) const noexcept {
  return static_cast<int>(std::hash<std::string>{}(key) % static_cast<std::size_t>(num_cores_));
}

void Router::RouteTask(Task task) {
  const int target = RouteKey(task.key);
  const uint32_t rid = static_cast<uint32_t>(task.request_id & 0xFFFFFFFF);
  if (target == local_core_id_) {
    std::printf("[Core %d] ROUT %s \"%.20s\" rid=%-4u → local\n", local_core_id_,
                TaskTypeName(task.type), task.key.c_str(), rid);
    HandleLocally(std::move(task));
  } else {
    std::printf("[Core %d] ROUT %s \"%.20s\" rid=%-4u → Core %d\n", local_core_id_,
                TaskTypeName(task.type), task.key.c_str(), rid, target);
    all_workers_[target]->PushTask(std::move(task));
  }
}

void Router::HandleLocally(Task task) {
  Task resp = executor_.Execute(std::move(task));
  if (resp.reply_to_core >= 0 && resp.reply_to_core < static_cast<int>(all_workers_.size())) {
    all_workers_[resp.reply_to_core]->PushTask(std::move(resp));
  }
}

} // namespace db
