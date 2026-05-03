#include "transaction/tx_coordinator.h"

namespace db {

TxCoordinator::TxCoordinator(Router& router,
                             std::function<void(uint64_t, Task)> resume_fn)
    : router_(router), resume_fn_(std::move(resume_fn)) {}

void TxCoordinator::HandleControl(Task task) {
  switch (task.type) {
  case TaskType::TX_BEGIN_REQUEST:
    HandleBegin(task);
    break;
  case TaskType::TX_COMMIT_REQUEST:
    HandleCommit(task);
    break;
  case TaskType::TX_ROLLBACK_REQUEST:
    HandleRollback(task);
    break;
  case TaskType::TX_HEARTBEAT_REQUEST:
    HandleHeartbeat(task);
    break;
  default:
    break;
  }
}

void TxCoordinator::HandleExecute(Task task) {
  const auto tx_it = tx_table_.find(task.tx_id);
  if (tx_it == tx_table_.end()) {
    Task response;
    response.type = TaskType::TX_EXECUTE_RESPONSE;
    response.success = false;
    response.tx_id = task.tx_id;
    response.error_message = "tx_not_found";
    SendResponse(task.request_id, std::move(response));
    return;
  }

  if (tx_it->second.state != TxState::ACTIVE) {
    Task response;
    response.type = TaskType::TX_EXECUTE_RESPONSE;
    response.success = false;
    response.tx_id = task.tx_id;
    response.error_message = "tx_not_active";
    SendResponse(task.request_id, std::move(response));
    return;
  }

  // Stage 2: сохраняем TX тип, добавляем snapshot_ts из TxRecord.
  task.snapshot_ts = tx_it->second.snapshot_ts;

  // Для SET операций — трекаем participant core.
  if (task.type == TaskType::TX_EXECUTE_SET_REQUEST) {
    const int target = router_.RouteKey(task.key);
    tx_it->second.participant_cores.insert(target);
  }

  router_.RouteTask(std::move(task));
}

void TxCoordinator::HandleBegin(Task& task) {
  const uint64_t tx_id = next_tx_id_++;
  const uint64_t snapshot_ts = next_snapshot_ts_++;
  const uint64_t now_ts = next_snapshot_ts_++;

  tx_table_[tx_id] = TxRecord{
      .tx_id = tx_id,
      .snapshot_ts = snapshot_ts,
      .state = TxState::ACTIVE,
      .created_ts = now_ts,
      .last_heartbeat_ts = now_ts,
      .participant_cores = {},
  };

  Task response;
  response.type = TaskType::TX_BEGIN_RESPONSE;
  response.success = true;
  response.tx_id = tx_id;
  response.snapshot_ts = snapshot_ts;
  SendResponse(task.request_id, std::move(response));
}

void TxCoordinator::HandleCommit(Task& task) {
  Task response;
  response.type = TaskType::TX_COMMIT_RESPONSE;
  response.tx_id = task.tx_id;

  const auto tx_it = tx_table_.find(task.tx_id);
  if (tx_it == tx_table_.end()) {
    response.success = false;
    response.error_message = "tx_not_found";
    SendResponse(task.request_id, std::move(response));
    return;
  }

  if (tx_it->second.state != TxState::ACTIVE) {
    response.success = false;
    response.error_message = "tx_not_active";
    SendResponse(task.request_id, std::move(response));
    return;
  }

  tx_it->second.state = TxState::COMMITTED;
  const auto& participants = tx_it->second.participant_cores;

  if (participants.empty()) {
    response.success = true;
    SendResponse(task.request_id, std::move(response));
    return;
  }

  const uint64_t commit_ts = next_snapshot_ts_++;

  PendingFinalize pending_finalize;
  pending_finalize.client_request_id = task.request_id;
  pending_finalize.tx_id = task.tx_id;
  pending_finalize.remaining = static_cast<int>(participants.size());
  pending_finalize.is_commit = true;
  pending_finalizes_[task.tx_id] = pending_finalize;

  for (int core : participants) {
    Task finalize_task;
    finalize_task.type = TaskType::TX_FINALIZE_COMMIT_REQUEST;
    finalize_task.tx_id = task.tx_id;
    finalize_task.commit_ts = commit_ts;
    finalize_task.reply_to_core = 0;
    router_.SendToCore(core, std::move(finalize_task));
  }
}

void TxCoordinator::HandleRollback(Task& task) {
  Task response;
  response.type = TaskType::TX_ROLLBACK_RESPONSE;
  response.tx_id = task.tx_id;

  const auto tx_it = tx_table_.find(task.tx_id);
  if (tx_it == tx_table_.end()) {
    response.success = false;
    response.error_message = "tx_not_found";
    SendResponse(task.request_id, std::move(response));
    return;
  }

  if (tx_it->second.state != TxState::ACTIVE) {
    response.success = false;
    response.error_message = "tx_not_active";
    SendResponse(task.request_id, std::move(response));
    return;
  }

  tx_it->second.state = TxState::ABORTED;
  const auto& participants = tx_it->second.participant_cores;

  if (participants.empty()) {
    response.success = true;
    SendResponse(task.request_id, std::move(response));
    return;
  }

  PendingFinalize pending_finalize;
  pending_finalize.client_request_id = task.request_id;
  pending_finalize.tx_id = task.tx_id;
  pending_finalize.remaining = static_cast<int>(participants.size());
  pending_finalize.is_commit = false;
  pending_finalizes_[task.tx_id] = pending_finalize;

  for (int core : participants) {
    Task finalize_task;
    finalize_task.type = TaskType::TX_FINALIZE_ABORT_REQUEST;
    finalize_task.tx_id = task.tx_id;
    finalize_task.reply_to_core = 0;
    router_.SendToCore(core, std::move(finalize_task));
  }
}

void TxCoordinator::HandleHeartbeat(Task& task) {
  Task response;
  response.type = TaskType::TX_HEARTBEAT_RESPONSE;
  response.tx_id = task.tx_id;

  const auto tx_record_it = tx_table_.find(task.tx_id);
  if (tx_record_it == tx_table_.end()) {
    response.success = false;
    response.error_message = "tx_not_found";
    SendResponse(task.request_id, std::move(response));
    return;
  }

  if (tx_record_it->second.state != TxState::ACTIVE) {
    response.success = false;
    response.error_message = "tx_not_active";
    SendResponse(task.request_id, std::move(response));
    return;
  }

  tx_record_it->second.last_heartbeat_ts = next_snapshot_ts_++;
  SendResponse(task.request_id, std::move(response));
}

void TxCoordinator::HandleFinalizeResponse(
    Task task) {  // NOLINT(performance-unnecessary-value-param)
  auto pending_finalize_it = pending_finalizes_.find(task.tx_id);
  if (pending_finalize_it == pending_finalizes_.end()) {
    return;
  }

  pending_finalize_it->second.remaining--;
  if (pending_finalize_it->second.remaining <= 0) {
    Task response;
    response.tx_id = pending_finalize_it->second.tx_id;
    response.success = true;
    if (pending_finalize_it->second.is_commit) {
      response.type = TaskType::TX_COMMIT_RESPONSE;
    } else {
      response.type = TaskType::TX_ROLLBACK_RESPONSE;
    }

    const uint64_t request_id = pending_finalize_it->second.client_request_id;
    pending_finalizes_.erase(pending_finalize_it);
    SendResponse(request_id, std::move(response));
  }
}

void TxCoordinator::SendResponse(uint64_t request_id, Task response) {
  resume_fn_(request_id, std::move(response));
}

}  // namespace db
