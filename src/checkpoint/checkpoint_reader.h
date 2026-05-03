#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "storage/storage_engine.h"

namespace db {

/// Чтение per-core snapshot с диска и восстановление в StorageEngine.
class CheckpointReader {
public:
  struct SnapshotHeader {
    uint32_t core_id{0};
    uint32_t num_cores{0};
    uint64_t layout_epoch{0};
    uint64_t wal_lsn{0};
    uint64_t entry_count{0};
  };

  /// Прочитать только заголовок snapshot (для проверки topology).
  [[nodiscard]] static std::optional<SnapshotHeader> ReadHeader(
      const std::string& path);

  /// Загрузить snapshot в StorageEngine (Clear + RestoreCommitted для каждого entry).
  static SnapshotHeader Load(const std::string& path,
                             StorageEngine& storage);
};

}  // namespace db
