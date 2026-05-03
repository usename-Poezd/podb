#pragma once

#include <cstdint>
#include <string>

#include "core/types.h"

namespace db {

enum class TaskType : uint8_t {
  SET_REQUEST = 0,
  GET_REQUEST = 1,
  SET_RESPONSE = 2,
  GET_RESPONSE = 3,
  // 4-9 зарезервированы для будущих не-транзакционных типов (DELETE и т.д.)
  TX_BEGIN_REQUEST = 10,
  TX_BEGIN_RESPONSE = 11,
  TX_EXECUTE_GET_REQUEST = 12,   // Execute GET внутри транзакции
  TX_EXECUTE_SET_REQUEST = 13,   // Execute SET внутри транзакции
  TX_EXECUTE_RESPONSE = 14,      // Ответ на Execute (GET или SET)
  TX_COMMIT_REQUEST = 15,
  TX_COMMIT_RESPONSE = 16,
  TX_ROLLBACK_REQUEST = 17,
  TX_ROLLBACK_RESPONSE = 18,
  TX_HEARTBEAT_REQUEST = 19,
  TX_HEARTBEAT_RESPONSE = 20,
  TX_FINALIZE_COMMIT_REQUEST = 21,   // Запрос на финализацию commit на participant core
  TX_FINALIZE_COMMIT_RESPONSE = 22,  // Ответ о завершении commit финализации
  TX_FINALIZE_ABORT_REQUEST = 23,    // Запрос на финализацию abort на participant core
  TX_PREPARE_REQUEST = 24,           // OCC validation запрос на participant core
  TX_PREPARE_RESPONSE = 25,          // Голос участника (YES/NO) обратно координатору
  TX_FINALIZE_ABORT_RESPONSE = 26,   // Ack финализации abort (замена TX_EXECUTE_RESPONSE хака)
};

inline const char *TaskTypeName(TaskType t) noexcept {
  switch (t) {
  case TaskType::GET_REQUEST:
    return "GET ";
  case TaskType::SET_REQUEST:
    return "SET ";
  case TaskType::GET_RESPONSE:
    return "GET←";
  case TaskType::SET_RESPONSE:
    return "SET←";
  case TaskType::TX_BEGIN_REQUEST:
    return "TXBG";
  case TaskType::TX_BEGIN_RESPONSE:
    return "TXBG←";
  case TaskType::TX_EXECUTE_GET_REQUEST:
    return "TXGT";
  case TaskType::TX_EXECUTE_SET_REQUEST:
    return "TXST";
  case TaskType::TX_EXECUTE_RESPONSE:
    return "TXEX←";
  case TaskType::TX_COMMIT_REQUEST:
    return "TXCM";
  case TaskType::TX_COMMIT_RESPONSE:
    return "TXCM←";
  case TaskType::TX_ROLLBACK_REQUEST:
    return "TXRB";
  case TaskType::TX_ROLLBACK_RESPONSE:
    return "TXRB←";
  case TaskType::TX_HEARTBEAT_REQUEST:
    return "TXHB";
  case TaskType::TX_HEARTBEAT_RESPONSE:
    return "TXHB←";
  case TaskType::TX_FINALIZE_COMMIT_REQUEST:
    return "TXFC";
  case TaskType::TX_FINALIZE_COMMIT_RESPONSE:
    return "TXFC←";
  case TaskType::TX_FINALIZE_ABORT_REQUEST:
    return "TXFA";
  case TaskType::TX_PREPARE_REQUEST:
    return "TXPR";
  case TaskType::TX_PREPARE_RESPONSE:
    return "TXPR←";
  case TaskType::TX_FINALIZE_ABORT_RESPONSE:
    return "TXFA←";
  }
  return "???";
}

struct Task {
  TaskType type{TaskType::GET_REQUEST};

  // --- Request payload ---
  std::string key;
  BinaryValue value;

  // --- Response payload ---
  bool found{false};
  bool success{true};

  // --- Routing/correlation ---
  uint64_t request_id{0};
  int reply_to_core{-1};

  // --- Транзакционные поля ---
  uint64_t tx_id{0};             // 0 = не в транзакции (sentinel)
  uint64_t snapshot_ts{0};       // snapshot timestamp для Snapshot Isolation
  uint64_t commit_ts{0};         // Timestamp фиксации для finalize операций
  std::string error_message;     // Детали ошибки (tx_not_found, tx_not_active и т.д.)

  // Helper methods
  bool IsRequest() const noexcept {
    return type == TaskType::GET_REQUEST || type == TaskType::SET_REQUEST ||
           type == TaskType::TX_BEGIN_REQUEST ||
           type == TaskType::TX_EXECUTE_GET_REQUEST ||
           type == TaskType::TX_EXECUTE_SET_REQUEST ||
           type == TaskType::TX_COMMIT_REQUEST ||
           type == TaskType::TX_ROLLBACK_REQUEST ||
           type == TaskType::TX_HEARTBEAT_REQUEST ||
           type == TaskType::TX_FINALIZE_COMMIT_REQUEST ||
           type == TaskType::TX_FINALIZE_ABORT_REQUEST ||
           type == TaskType::TX_PREPARE_REQUEST;
  }

  bool IsResponse() const noexcept {
    return type == TaskType::GET_RESPONSE || type == TaskType::SET_RESPONSE ||
           type == TaskType::TX_BEGIN_RESPONSE ||
           type == TaskType::TX_EXECUTE_RESPONSE ||
           type == TaskType::TX_COMMIT_RESPONSE ||
           type == TaskType::TX_ROLLBACK_RESPONSE ||
           type == TaskType::TX_HEARTBEAT_RESPONSE ||
           type == TaskType::TX_FINALIZE_COMMIT_RESPONSE ||
           type == TaskType::TX_PREPARE_RESPONSE ||
           type == TaskType::TX_FINALIZE_ABORT_RESPONSE;
  }

  /// Транзакционные control-операции (обрабатываются TxCoordinator на Core 0)
  bool IsTxControl() const noexcept {
    return type == TaskType::TX_BEGIN_REQUEST ||
           type == TaskType::TX_COMMIT_REQUEST ||
           type == TaskType::TX_ROLLBACK_REQUEST ||
           type == TaskType::TX_HEARTBEAT_REQUEST;
  }

  /// Транзакционные execute-операции (маршрутизируются через TxCoordinator → Router)
  bool IsTxExecute() const noexcept {
    return type == TaskType::TX_EXECUTE_GET_REQUEST ||
           type == TaskType::TX_EXECUTE_SET_REQUEST ||
           type == TaskType::TX_EXECUTE_RESPONSE;
  }

  /// Финализация транзакции на participant core (commit/abort intents)
  bool IsTxFinalize() const noexcept {
    return type == TaskType::TX_FINALIZE_COMMIT_REQUEST ||
           type == TaskType::TX_FINALIZE_ABORT_REQUEST;
  }

  bool IsTxPrepare() const noexcept {
    return type == TaskType::TX_PREPARE_REQUEST;
  }

  bool IsTxPrepareResponse() const noexcept {
    return type == TaskType::TX_PREPARE_RESPONSE;
  }

  bool IsTxFinalizeResponse() const noexcept {
    return type == TaskType::TX_FINALIZE_COMMIT_RESPONSE ||
           type == TaskType::TX_FINALIZE_ABORT_RESPONSE;
  }

  const char *OpName() const noexcept {
    switch (type) {
    case TaskType::GET_REQUEST:
    case TaskType::GET_RESPONSE:
    case TaskType::TX_EXECUTE_GET_REQUEST:
      return "GET";
    case TaskType::SET_REQUEST:
    case TaskType::SET_RESPONSE:
    case TaskType::TX_EXECUTE_SET_REQUEST:
      return "SET";
    case TaskType::TX_BEGIN_REQUEST:
    case TaskType::TX_BEGIN_RESPONSE:
      return "BEGIN";
    case TaskType::TX_EXECUTE_RESPONSE:
      return "EXEC";
    case TaskType::TX_COMMIT_REQUEST:
    case TaskType::TX_COMMIT_RESPONSE:
      return "COMMIT";
    case TaskType::TX_ROLLBACK_REQUEST:
    case TaskType::TX_ROLLBACK_RESPONSE:
      return "ROLLBACK";
    case TaskType::TX_HEARTBEAT_REQUEST:
    case TaskType::TX_HEARTBEAT_RESPONSE:
      return "HEARTBEAT";
    case TaskType::TX_FINALIZE_COMMIT_REQUEST:
    case TaskType::TX_FINALIZE_COMMIT_RESPONSE:
      return "FIN_COMMIT";
    case TaskType::TX_FINALIZE_ABORT_REQUEST:
    case TaskType::TX_FINALIZE_ABORT_RESPONSE:
      return "FIN_ABORT";
    case TaskType::TX_PREPARE_REQUEST:
    case TaskType::TX_PREPARE_RESPONSE:
      return "PREPARE";
    }
    return "???";
  }
};

}  // namespace db
