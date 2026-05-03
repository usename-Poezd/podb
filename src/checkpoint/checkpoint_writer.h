#pragma once

#include <cstdint>
#include <string>

#include "storage/storage_engine.h"

namespace db {

/// Атомарная запись per-core snapshot на диск.
/// Паттерн: write tmp → fdatasync → rename → fsync(dir).
class CheckpointWriter {
public:
  struct TopologyMeta {
    uint32_t num_cores{0};
    uint64_t layout_epoch{0};
  };

  /// Записать atomic snapshot для одного core.
  /// Паттерн: write tmp → fdatasync → rename → fsync(dir).
  static void Write(const StorageEngine& storage,
                    const std::string& path,
                    int core_id,
                    const TopologyMeta& topo,
                    uint64_t wal_lsn);
};

}  // namespace db
