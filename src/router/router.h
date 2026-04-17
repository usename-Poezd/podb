#pragma once

#include <string>
#include <vector>

#include "core/task.h"
#include "execution/kv_executor.h"
#include "storage/storage_engine.h"

namespace db {

class Worker;

class Router {
public:
  Router(int local_core_id, std::vector<Worker *> all_workers, StorageEngine &local_storage);

  [[nodiscard]] int RouteKey(const std::string &key) const noexcept;

  void RouteTask(Task task);

private:
  void HandleLocally(Task task);

  int local_core_id_;
  int num_cores_;
  std::vector<Worker *> all_workers_;
  StorageEngine &local_storage_;
  KvExecutor executor_;
};

} // namespace db
