#include "transaction/tx_coordinator.h"

namespace db {

TxCoordinator::TxCoordinator(Router& router,
                             std::function<void(uint64_t, Task)> resume_fn,
                             WalWriter* wal)
    : router_(router), resume_fn_(std::move(resume_fn)), wal_(wal) {}

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

  if (wal_) {
    WalRecord rec;
    rec.type = WalRecordType::TX_BEGIN;
    rec.tx_id = tx_id;
    rec.snapshot_ts = snapshot_ts;
    wal_->Append(std::move(rec));
  }

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

  const auto& participants = tx_it->second.participant_cores;

  if (participants.empty()) {
    tx_it->second.state = TxState::COMMITTED;
    response.success = true;
    SendResponse(task.request_id, std::move(response));
    return;
  }

  tx_it->second.state = TxState::PREPARING;

  PendingPrepare pending_prepare;
  pending_prepare.client_request_id = task.request_id;
  pending_prepare.tx_id = task.tx_id;
  pending_prepare.remaining = static_cast<int>(participants.size());
  pending_prepares_[task.tx_id] = pending_prepare;

  for (int core : participants) {
    Task prepare_task;
    prepare_task.type = TaskType::TX_PREPARE_REQUEST;
    prepare_task.tx_id = task.tx_id;
    prepare_task.snapshot_ts = tx_it->second.snapshot_ts;
    prepare_task.reply_to_core = 0;
    router_.SendToCore(core, std::move(prepare_task));
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

  if (wal_) {
    WalRecord rec;
    rec.type = WalRecordType::ABORT_DECISION;
    rec.tx_id = task.tx_id;
    wal_->Append(std::move(rec));
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
  pending_finalize.client_response_type = TaskType::TX_ROLLBACK_RESPONSE;
  pending_finalizes_[task.tx_id] = pending_finalize;

  for (int core : participants) {
    Task finalize_task;
    finalize_task.type = TaskType::TX_FINALIZE_ABORT_REQUEST;
    finalize_task.tx_id = task.tx_id;
    finalize_task.reply_to_core = 0;
    router_.SendToCore(core, std::move(finalize_task));
  }
}

void TxCoordinator::HandlePrepareResponse(
    Task task) {  // NOLINT(performance-unnecessary-value-param)
  auto pending_prepare_it = pending_prepares_.find(task.tx_id);
  if (pending_prepare_it == pending_prepares_.end()) {
    return;
  }

  if (!task.success) {
    pending_prepare_it->second.any_no = true;
    if (pending_prepare_it->second.first_error.empty()) {
      pending_prepare_it->second.first_error = std::move(task.error_message);
    }
  }

  pending_prepare_it->second.remaining--;
  if (pending_prepare_it->second.remaining > 0) {
    return;
  }

  auto tx_it = tx_table_.find(task.tx_id);
  if (tx_it == tx_table_.end()) {
    pending_prepares_.erase(pending_prepare_it);
    return;
  }

  const auto& participants = tx_it->second.participant_cores;
  PendingFinalize pending_finalize;
  pending_finalize.client_request_id = pending_prepare_it->second.client_request_id;
  pending_finalize.tx_id = pending_prepare_it->second.tx_id;
  pending_finalize.remaining = static_cast<int>(participants.size());
  pending_finalize.client_response_type = TaskType::TX_COMMIT_RESPONSE;

  if (!pending_prepare_it->second.any_no) {
    const uint64_t commit_ts = next_snapshot_ts_++;
    if (wal_) {
      WalRecord rec;
      rec.type = WalRecordType::COMMIT_DECISION;
      rec.tx_id = task.tx_id;
      rec.commit_ts = commit_ts;
      wal_->Append(std::move(rec));
      wal_->Sync();
    }
    tx_it->second.state = TxState::COMMITTED;
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
  } else {
    if (wal_) {
      WalRecord rec;
      rec.type = WalRecordType::ABORT_DECISION;
      rec.tx_id = task.tx_id;
      wal_->Append(std::move(rec));
    }
    tx_it->second.state = TxState::ABORTED;
    pending_finalize.is_commit = false;
    pending_finalize.error_message =
        pending_prepare_it->second.first_error.empty()
            ? "prepare_rejected"
            : std::move(pending_prepare_it->second.first_error);
    pending_finalizes_[task.tx_id] = pending_finalize;

    for (int core : participants) {
      Task finalize_task;
      finalize_task.type = TaskType::TX_FINALIZE_ABORT_REQUEST;
      finalize_task.tx_id = task.tx_id;
      finalize_task.reply_to_core = 0;
      router_.SendToCore(core, std::move(finalize_task));
    }
  }

  pending_prepares_.erase(pending_prepare_it);
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
    response.type = pending_finalize_it->second.client_response_type;
    response.success = pending_finalize_it->second.error_message.empty();
    if (!response.success) {
      response.error_message = pending_finalize_it->second.error_message;
    }

    const uint64_t request_id = pending_finalize_it->second.client_request_id;
    pending_finalizes_.erase(pending_finalize_it);
    SendResponse(request_id, std::move(response));
  }
}

void TxCoordinator::LoadRecoveredState(
    std::unordered_map<uint64_t, TxRecord> recovered_tx_table,
    uint64_t next_tx_id, uint64_t next_snapshot_ts) {
  tx_table_ = std::move(recovered_tx_table);
  next_tx_id_ = next_tx_id;
  next_snapshot_ts_ = next_snapshot_ts;
}

void TxCoordinator::ResolveInDoubt() {
  for (auto& [tx_id, record] : tx_table_) {
    if (record.participant_cores.empty()) {
      continue;
    }

    if (record.state == TxState::COMMITTED) {
      PendingFinalize pf;
      pf.client_request_id = 0;
      pf.tx_id = tx_id;
      pf.remaining = static_cast<int>(record.participant_cores.size());
      pf.is_commit = true;
      pf.client_response_type = TaskType::TX_COMMIT_RESPONSE;
      pending_finalizes_[tx_id] = pf;

      for (int core : record.participant_cores) {
        Task finalize_task;
        finalize_task.type = TaskType::TX_FINALIZE_COMMIT_REQUEST;
        finalize_task.tx_id = tx_id;
        finalize_task.commit_ts = record.snapshot_ts;
        finalize_task.reply_to_core = 0;
        router_.SendToCore(core, std::move(finalize_task));
      }
    } else if (record.state == TxState::PREPARING ||
               record.state == TxState::ACTIVE) {
      record.state = TxState::ABORTED;
      if (wal_) {
        WalRecord wal_rec;
        wal_rec.type = WalRecordType::ABORT_DECISION;
        wal_rec.tx_id = tx_id;
        wal_->Append(std::move(wal_rec));
      }

      PendingFinalize pf;
      pf.client_request_id = 0;
      pf.tx_id = tx_id;
      pf.remaining = static_cast<int>(record.participant_cores.size());
      pf.is_commit = false;
      pf.client_response_type = TaskType::TX_COMMIT_RESPONSE;
      pending_finalizes_[tx_id] = pf;

      for (int core : record.participant_cores) {
        Task finalize_task;
        finalize_task.type = TaskType::TX_FINALIZE_ABORT_REQUEST;
        finalize_task.tx_id = tx_id;
        finalize_task.reply_to_core = 0;
        router_.SendToCore(core, std::move(finalize_task));
      }
    }
  }
}

void TxCoordinator::SendResponse(uint64_t request_id, Task response) {
  resume_fn_(request_id, std::move(response));
}

}  // namespace db
