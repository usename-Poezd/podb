#pragma once

#include <cstdint>
#include <string>

#include "core/types.h"

namespace db {

/// Одна версия значения в MVCC цепочке.
/// Может быть committed version или intent (незафиксированная запись транзакции).
struct VersionedValue {
  uint64_t commit_ts{0};      // Timestamp фиксации (0 = intent, ещё не committed)
  BinaryValue value;           // Значение
  uint64_t tx_id{0};          // ID транзакции-владельца (0 = нет транзакции)
  bool is_intent{false};      // true = незафиксированный intent текущей транзакции
  bool is_deleted{false};     // true = tombstone (удаление ключа)
};

/// Результат MVCC чтения
struct MvccReadResult {
  bool found{false};          // Найдена ли видимая версия
  BinaryValue value;           // Значение (пусто если !found)
  bool is_deleted{false};     // Версия является tombstone
};

/// Результат записи intent
enum class WriteIntentResult : uint8_t {
  OK,                         // Intent успешно создан/обновлён
  WRITE_CONFLICT,             // Конфликт: другая транзакция уже имеет intent на этот ключ
};

}  // namespace db
