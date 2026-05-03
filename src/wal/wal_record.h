#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "core/types.h"

namespace db {

static constexpr uint32_t kWalMagic = 0xDB4C4F47;
static constexpr size_t kWalHeaderSize = 32;

// Типы WAL-записей: фазы 2PC-транзакций и служебные операции.
enum class WalRecordType : uint8_t {
  TX_BEGIN = 1,
  INTENT = 2,
  PREPARE = 3,
  COMMIT_DECISION = 4,
  ABORT_DECISION = 5,
  COMMIT_FINALIZE = 6,
  ABORT_FINALIZE = 7,
  CHECKPOINT = 8,
};

struct WalRecord {
  WalRecordType type{WalRecordType::TX_BEGIN};
  uint64_t tx_id{0};
  uint64_t lsn{0};

  // Поля, специфичные для типа записи
  uint64_t snapshot_ts{0};   // TX_BEGIN
  uint64_t commit_ts{0};     // COMMIT_DECISION, COMMIT_FINALIZE
  std::string key;           // INTENT
  BinaryValue value;         // INTENT
  bool is_deleted{false};    // INTENT
  bool vote_yes{false};      // PREPARE
  uint64_t snapshot_lsn{0};  // CHECKPOINT

  // Бинарная сериализация: заголовок(32B) + payload + CRC32c (маскированный)
  [[nodiscard]] std::vector<std::byte> Serialize() const;

  // Десериализация с проверкой magic и CRC32c; nullopt при любом несоответствии
  [[nodiscard]] static std::optional<WalRecord> Deserialize(const std::byte *data, size_t size);
};

}  // namespace db
