#include "router.h"

#include <cstdio>
#include <utility>

#include "worker.h"

namespace db {

Router::Router(int local_core_id, std::vector<Worker *> all_workers, StorageEngine &local_storage)
    : local_core_id_(local_core_id), num_cores_(static_cast<int>(all_workers.size())),
      all_workers_(std::move(all_workers)), local_storage_(local_storage) {}

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
  Task response;
  switch (task.type) {
  case TaskType::SET_REQUEST:
    local_storage_.Set(task.key, task.value);
    std::printf("[Core %d] EXEC SET \"%.20s\" = \"%.20s\" → OK   reply→Core %d\n", local_core_id_,
                task.key.c_str(), task.value.c_str(), task.reply_to_core);
    response.type = TaskType::SET_RESPONSE;
    response.key = std::move(task.key);
    response.success = true;
    break;
  case TaskType::GET_REQUEST: {
    auto result = local_storage_.Get(task.key);
    bool found = result.has_value();
    std::printf("[Core %d] EXEC GET \"%.20s\"        → %-5s reply→Core %d\n", local_core_id_,
                task.key.c_str(), found ? "FOUND" : "MISS", task.reply_to_core);
    response.type = TaskType::GET_RESPONSE;
    response.key = std::move(task.key);
    response.found = found;
    if (result)
      response.value = std::move(*result);
    break;
  }
  default:
    return;
  }
  response.request_id = task.request_id;
  response.reply_to_core = task.reply_to_core;
  if (task.reply_to_core >= 0 && task.reply_to_core < static_cast<int>(all_workers_.size())) {
    all_workers_[task.reply_to_core]->PushTask(std::move(response));
  }
}

} // namespace db
