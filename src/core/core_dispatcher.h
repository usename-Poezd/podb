#pragma once

#include "core/task.h"
#include "handlers/grpc_handler.h"
#include "transaction/tx_coordinator.h"

namespace db {

class CoreDispatcher {
public:
  CoreDispatcher(Router& router, GrpcHandler& handler, TxCoordinator& tx_coordinator)
      : router_(router), handler_(handler), tx_coordinator_(tx_coordinator) {}

  void Dispatch(Task task) {
    if (task.IsTxPrepareResponse()) {
      tx_coordinator_.HandlePrepareResponse(std::move(task));
    } else if (task.IsTxFinalizeResponse()) {
      tx_coordinator_.HandleFinalizeResponse(std::move(task));
    } else if (task.IsResponse()) {
      handler_.ResumeCoroutine(task.request_id, std::move(task));
    } else if (task.IsTxControl()) {
      tx_coordinator_.HandleControl(std::move(task));
    } else if (task.IsTxExecute()) {
      tx_coordinator_.HandleExecute(std::move(task));
    } else {
      router_.RouteTask(std::move(task));
    }
  }

private:
  Router& router_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
  GrpcHandler& handler_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
  TxCoordinator& tx_coordinator_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
};

}  // namespace db
