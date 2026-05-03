#pragma once

#include <cstdio>
#include <utility>

#include "core/task.h"
#include "storage/storage_engine.h"
#include "storage/versioned_value.h"
#include "wal/wal_writer.h"

namespace db {

class KvExecutor {
public:
  KvExecutor(StorageEngine &storage, int core_id, WalWriter *wal = nullptr)
      : storage_(storage), core_id_(core_id), wal_(wal) {}

  Task Execute(Task request) {
    Task response;
    switch (request.type) {
    case TaskType::SET_REQUEST: {
      const std::size_t value_size = request.value.size();
      storage_.Set(request.key, std::move(request.value));
      std::printf("[Core %d] EXEC SET \"%.20s\" size=%zu → OK   reply→Core %d\n",
                  core_id_, request.key.c_str(), value_size, request.reply_to_core);
      response.type = TaskType::SET_RESPONSE;
      response.key = std::move(request.key);
      response.success = true;
      break;
    }
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
    case TaskType::TX_EXECUTE_GET_REQUEST: {
      // MVCC чтение: own intent → latest committed ≤ snapshot_ts
      auto result = storage_.MvccGet(request.key, request.snapshot_ts, request.tx_id);
      std::printf("[Core %d] EXEC TX_GET \"%.20s\" tx=%lu snap=%lu → %-5s reply→Core %d\n",
                  core_id_, request.key.c_str(), request.tx_id, request.snapshot_ts,
                  result.found ? "FOUND" : "MISS", request.reply_to_core);
      response.type = TaskType::TX_EXECUTE_RESPONSE;
      response.key = std::move(request.key);
      response.tx_id = request.tx_id;
      response.found = result.found;
      response.success = true;
      if (result.found)
        response.value = std::move(result.value);
      break;
    }
    case TaskType::TX_EXECUTE_SET_REQUEST: {
      BinaryValue value_copy = request.value;
      // MVCC запись intent
      auto result = storage_.WriteIntent(request.key, std::move(request.value), request.tx_id);
      if (result == WriteIntentResult::OK && wal_) {
        WalRecord rec;
        rec.type = WalRecordType::INTENT;
        rec.tx_id = request.tx_id;
        rec.key = request.key;
        rec.value = std::move(value_copy);
        rec.is_deleted = false;
        wal_->Append(std::move(rec));
      }
      std::printf("[Core %d] EXEC TX_SET \"%.20s\" tx=%lu → %s reply→Core %d\n",
                  core_id_, request.key.c_str(), request.tx_id,
                  result == WriteIntentResult::OK ? "OK" : "CONFLICT",
                  request.reply_to_core);
      response.type = TaskType::TX_EXECUTE_RESPONSE;
      response.key = std::move(request.key);
      response.tx_id = request.tx_id;
      if (result == WriteIntentResult::OK) {
        response.success = true;
      } else {
        response.success = false;
        response.error_message = "write_write_conflict";
      }
      break;
    }
    case TaskType::TX_FINALIZE_COMMIT_REQUEST: {
      if (wal_) {
        WalRecord rec;
        rec.type = WalRecordType::COMMIT_FINALIZE;
        rec.tx_id = request.tx_id;
        rec.commit_ts = request.commit_ts;
        wal_->Append(std::move(rec));
      }
      // Финализация commit: promote intents → committed
      storage_.CommitTransaction(request.tx_id, request.commit_ts);
      std::printf("[Core %d] EXEC FIN_COMMIT tx=%lu commit_ts=%lu\n",
                  core_id_, request.tx_id, request.commit_ts);
      response.type = TaskType::TX_FINALIZE_COMMIT_RESPONSE;
      response.tx_id = request.tx_id;
      response.success = true;
      break;
    }
    case TaskType::TX_PREPARE_REQUEST: {
      auto result = storage_.ValidatePrepare(request.tx_id);
      // WAL логирование PREPARE только при YES-голосе; fdatasync гарантирует durability
      if (result.can_commit && wal_) {
        WalRecord rec;
        rec.type = WalRecordType::PREPARE;
        rec.tx_id = request.tx_id;
        rec.vote_yes = true;
        wal_->Append(std::move(rec));
        wal_->Sync();  // CRITICAL: fdatasync сбрасывает PREPARE + все предшествующие INTENT
      }
      std::printf("[Core %d] EXEC PREPARE tx=%lu → %s\n",
                  core_id_, request.tx_id,
                  result.can_commit ? "YES" : "NO");
      response.type = TaskType::TX_PREPARE_RESPONSE;
      response.tx_id = request.tx_id;
      response.success = result.can_commit;
      if (!result.can_commit)
        response.error_message = std::move(result.reason);
      break;
    }
    case TaskType::TX_FINALIZE_ABORT_REQUEST: {
      if (wal_) {
        WalRecord rec;
        rec.type = WalRecordType::ABORT_FINALIZE;
        rec.tx_id = request.tx_id;
        wal_->Append(std::move(rec));
      }
      storage_.AbortTransaction(request.tx_id);
      std::printf("[Core %d] EXEC FIN_ABORT tx=%lu\n", core_id_, request.tx_id);
      response.type = TaskType::TX_FINALIZE_ABORT_RESPONSE;
      response.tx_id = request.tx_id;
      response.success = true;
      break;
    }
    case TaskType::GC_REQUEST: {
      const size_t removed = storage_.GarbageCollect(request.snapshot_ts);
      std::printf("[Core %d] GC watermark=%lu removed=%zu\n",
                  core_id_, request.snapshot_ts, removed);
      response.type = TaskType::GC_RESPONSE;
      response.success = true;
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
  WalWriter *wal_{nullptr};
};

}  // namespace db
