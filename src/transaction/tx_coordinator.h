#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <unordered_map>
#include <unordered_set>

#include "core/clock.h"
#include "core/task.h"
#include "router/router.h"
#include "wal/wal_record.h"
#include "wal/wal_writer.h"

namespace db {

/// Состояние транзакции
enum class TxState : uint8_t {
  ACTIVE,
  PREPARING,  // PREPARE разослан, ожидаем голоса участников
  COMMITTED,
  ABORTED,
};

/// Запись о транзакции в таблице координатора
struct TxRecord {
  uint64_t tx_id{0};
  uint64_t snapshot_ts{0};
  TxState state{TxState::ACTIVE};
  uint64_t created_ts{0};
  uint64_t last_heartbeat_ts{0};
  std::unordered_set<int> participant_cores;  // Cores с intents для этой tx
  Clock::TimePoint created_time{};
  Clock::TimePoint last_heartbeat_time{};
};

/// Координатор транзакций на Core 0.
///
/// Управляет жизненным циклом транзакций: Begin, Execute, Commit, Rollback,
/// Heartbeat. Stage 1: Execute просто делает обычный Get/Set через Router —
/// без MVCC, без intents. Это routing plumbing для будущего MVCC (Stage 2).
class TxCoordinator {
 public:
  /// @param router Ссылка на Router для маршрутизации Execute операций
  /// @param resume_fn Callback для возврата ответа в GrpcHandler
  /// (через request_id)
  TxCoordinator(Router& router, std::function<void(uint64_t, Task)> resume_fn,
                WalWriter* wal = nullptr, Clock* clock = nullptr,
                std::chrono::milliseconds lease_timeout =
                    std::chrono::seconds(30),
                std::chrono::milliseconds stuck_timeout =
                    std::chrono::seconds(10));

  /// Обработка control-операций (Begin/Commit/Rollback/Heartbeat).
  /// Эти операции не покидают Core 0.
  void HandleControl(Task task);

  /// Обработка Execute операций (TX_EXECUTE_GET/SET_REQUEST).
  /// Валидирует tx, добавляет snapshot_ts и маршрутизирует через Router.
  void HandleExecute(Task task);

  /// Обработка ответов финализации от participant cores.
  void HandleFinalizeResponse(Task task);  // NOLINT(performance-unnecessary-value-param)

  /// Обработка голосов PREPARE от participant cores.
  void HandlePrepareResponse(Task task);  // NOLINT(performance-unnecessary-value-param)

  void ReapStaleTransactions();
  [[nodiscard]] uint64_t GetMinActiveSnapshot() const;

  /// Загрузить восстановленное состояние coordinator из WAL replay.
  void LoadRecoveredState(std::unordered_map<uint64_t, TxRecord> recovered_tx_table,
                          uint64_t next_tx_id, uint64_t next_snapshot_ts);

  /// Разрешить in-doubt транзакции после recovery.
  void ResolveInDoubt();

 private:
  /// Ожидание завершения финализации на participant cores
  struct PendingFinalize {
    uint64_t client_request_id{0};  // request_id клиентского Commit/Rollback
    uint64_t tx_id{0};
    int remaining{0};               // Сколько acks ещё ждём
    bool is_commit{true};           // true=commit, false=rollback
    TaskType client_response_type{TaskType::TX_COMMIT_RESPONSE};
    std::string error_message;
    Clock::TimePoint created_time{};  // for stuck-timeout detection
  };

  /// Ожидание голосов участников на фазе PREPARE
  struct PendingPrepare {
    uint64_t client_request_id{0};
    uint64_t tx_id{0};
    int remaining{0};
    bool any_no{false};
    std::string first_error;
    Clock::TimePoint created_time{};  // for stuck-timeout detection
  };

  void HandleBegin(Task& task);
  void HandleCommit(Task& task);
  void HandleRollback(Task& task);
  void HandleHeartbeat(Task& task);

  /// Отправляет ответ обратно в GrpcHandler
  void SendResponse(uint64_t request_id, Task response);

  Router& router_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
  std::function<void(uint64_t, Task)> resume_fn_;
  WalWriter* wal_{nullptr};
  Clock* clock_{nullptr};
  SteadyClock default_clock_;
  std::chrono::milliseconds lease_timeout_;
  std::chrono::milliseconds stuck_timeout_;
  static constexpr uint64_t kReaperSentinel = UINT64_MAX;

  /// Таблица активных и завершённых транзакций
  std::unordered_map<uint64_t, TxRecord> tx_table_;

  std::unordered_map<uint64_t, PendingPrepare> pending_prepares_;

  std::unordered_map<uint64_t, PendingFinalize> pending_finalizes_;

  /// Счётчик tx_id. Начинается с 1 (tx_id=0 зарезервирован как sentinel).
  uint64_t next_tx_id_{1};

  /// Монотонный счётчик snapshot timestamps.
  uint64_t next_snapshot_ts_{1};
};

}  // namespace db
