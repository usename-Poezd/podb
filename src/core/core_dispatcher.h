#pragma once

#include "core/task.h"
#include "router/router.h"
#include "handlers/grpc_handler.h"

namespace db {

class CoreDispatcher {
public:
  CoreDispatcher(Router& router, GrpcHandler& handler)
      : router_(router), handler_(handler) {}

  void Dispatch(Task task) {
    if (task.IsResponse()) {
      handler_.ResumeCoroutine(task.request_id, std::move(task));
    } else {
      router_.RouteTask(std::move(task));
    }
  }

private:
  Router& router_;
  GrpcHandler& handler_;
};

}
