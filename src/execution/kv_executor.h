#pragma once

#include <cstdio>
#include <utility>

#include "core/task.h"
#include "storage/storage_engine.h"

namespace db {

class KvExecutor {
public:
  KvExecutor(StorageEngine &storage, int core_id)
      : storage_(storage), core_id_(core_id) {}

  Task Execute(Task request) {
    Task response;
    switch (request.type) {
    case TaskType::SET_REQUEST:
      storage_.Set(request.key, request.value);
      std::printf("[Core %d] EXEC SET \"%.20s\" = \"%.20s\" → OK   reply→Core %d\n",
                  core_id_, request.key.c_str(), request.value.c_str(), request.reply_to_core);
      response.type = TaskType::SET_RESPONSE;
      response.key = std::move(request.key);
      response.success = true;
      break;
    case TaskType::GET_REQUEST: {
      auto result = storage_.Get(request.key);
      bool found = result.has_value();
      std::printf("[Core %d] EXEC GET \"%.20s\"        → %-5s reply→Core %d\n",
                  core_id_, request.key.c_str(), found ? "FOUND" : "MISS", request.reply_to_core);
      response.type = TaskType::GET_RESPONSE;
      response.key = std::move(request.key);
      response.found = found;
      if (result)
        response.value = std::move(*result);
      break;
    }
    default:
      return response;
    }
    response.request_id = request.request_id;
    response.reply_to_core = request.reply_to_core;
    return response;
  }

private:
  StorageEngine &storage_;
  int core_id_;
};

} // namespace db
