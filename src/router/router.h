#pragma once

#include <functional>
#include <string>
#include <vector>

#include "core/task.h"

namespace db {

class Worker;

class Router {
public:
  Router(int local_core_id, std::vector<Worker *> all_workers,
         std::function<void(Task)> local_execute);

  [[nodiscard]] int RouteKey(const std::string &key) const noexcept;

  void RouteTask(Task task);

  /// Отправить task напрямую на конкретный core (минуя key-based routing).
  /// Используется для finalize операций, где target core известен заранее.
  void SendToCore(int target_core, Task task);

private:
  int local_core_id_;
  int num_cores_;
  std::vector<Worker *> all_workers_;
  std::function<void(Task)> local_execute_;
};

} // namespace db
