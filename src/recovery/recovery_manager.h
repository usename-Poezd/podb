#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "checkpoint/checkpoint_reader.h"
#include "checkpoint/checkpoint_writer.h"
#include "storage/storage_engine.h"
#include "transaction/tx_coordinator.h"
#include "wal/wal_reader.h"
#include "wal/wal_record.h"

namespace db {

class RecoveryManager {
public:
  struct TopologyMeta {
    uint32_t num_cores{0};
    uint64_t layout_epoch{0};
  };

  struct RecoveredCoordinatorState {
    std::unordered_map<uint64_t, TxRecord> tx_table;
    uint64_t max_tx_id{0};
    uint64_t max_snapshot_ts{0};
  };

  /// Записать topology metadata файл.
  static void WriteTopologyMeta(const std::string& data_dir,
                                const TopologyMeta& meta);

  /// Прочитать topology metadata. nullopt если файл не существует.
  [[nodiscard]] static std::optional<TopologyMeta>
  ReadTopologyMeta(const std::string& data_dir);

  /// Phase 0: Валидация topology. Пустая строка = OK, иначе ошибка.
  [[nodiscard]] static std::string ValidateTopology(
      const std::string& data_dir, int configured_cores);

  /// Phase 1: Per-core recovery. Загрузить snapshot + replay WAL tail.
  /// Возвращает макс LSN из всех применённых записей.
  static uint64_t RecoverCore(int core_id,
                              const std::string& data_dir,
                              StorageEngine& storage);

  /// Phase 2: Coordinator recovery. Replay Core 0 WAL → восстановить tx_table.
  [[nodiscard]] static RecoveredCoordinatorState
  RecoverCoordinator(const std::string& data_dir);

  /// Repartition: recover old topology, resolve все txs, rehash в новую topology.
  /// Populates new_storages with committed data under new hash routing.
  /// Пишет fresh snapshots. Удаляет old WAL files.
  static void Repartition(const std::string& data_dir,
                          uint32_t old_num_cores,
                          uint32_t new_num_cores,
                          std::vector<std::unique_ptr<StorageEngine>>& new_storages);

  /// Утилита: путь к WAL файлу для core.
  [[nodiscard]] static std::string WalPath(const std::string& data_dir, int core_id);

  /// Утилита: путь к snapshot файлу для core.
  [[nodiscard]] static std::string SnapPath(const std::string& data_dir, int core_id);

  /// Утилита: путь к topology metadata.
  [[nodiscard]] static std::string TopologyPath(const std::string& data_dir);
};

}  // namespace db
