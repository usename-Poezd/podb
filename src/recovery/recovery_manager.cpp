#include "recovery/recovery_manager.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include "wal/crc32c.h"

namespace db {

namespace {

constexpr uint32_t kTopologyMagic = 0xDB544F50U;
constexpr uint32_t kTopologyVersion = 1U;
constexpr size_t kTopologySize = 24;

constexpr size_t kOffMagic = 0;
constexpr size_t kOffVersion = 4;
constexpr size_t kOffNumCores = 8;
constexpr size_t kOffLayoutEpoch = 12;
constexpr size_t kOffCrc = 20;

void WriteAll(int file_fd, const void* src_buf, size_t byte_count) {
  auto view = std::span<const uint8_t>(static_cast<const uint8_t*>(src_buf), byte_count);
  while (!view.empty()) {
    const ssize_t written = ::write(file_fd, view.data(), view.size());
    if (written < 0) {
      throw std::runtime_error(std::string("write failed: ") + std::strerror(errno));
    }
    view = view.subspan(static_cast<size_t>(written));
  }
}

void ReadExact(int file_fd, void* dst_buf, size_t byte_count) {
  auto view = std::span<uint8_t>(static_cast<uint8_t*>(dst_buf), byte_count);
  while (!view.empty()) {
    const ssize_t bytes_read = ::read(file_fd, view.data(), view.size());
    if (bytes_read <= 0) {
      throw std::runtime_error("unexpected EOF reading topology metadata");
    }
    view = view.subspan(static_cast<size_t>(bytes_read));
  }
}

template <typename FieldT>
FieldT ReadField(const std::array<uint8_t, kTopologySize>& buffer, size_t offset) {
  FieldT value{};
  std::memcpy(&value, buffer.data() + offset, sizeof(FieldT));
  return value;
}

uint32_t ComputeTopologyCrc(const std::array<uint8_t, kTopologySize>& buffer) {
  return Crc32c(buffer.data(), kOffCrc);
}

void UpdateMax(uint64_t candidate, uint64_t& current_max) {
  current_max = std::max(current_max, candidate);
}

}  // namespace

void RecoveryManager::WriteTopologyMeta(const std::string& data_dir,
                                        const TopologyMeta& meta) {
  std::array<uint8_t, kTopologySize> buffer{};
  std::memcpy(buffer.data() + kOffMagic, &kTopologyMagic, sizeof(kTopologyMagic));
  std::memcpy(buffer.data() + kOffVersion, &kTopologyVersion, sizeof(kTopologyVersion));
  std::memcpy(buffer.data() + kOffNumCores, &meta.num_cores, sizeof(meta.num_cores));
  std::memcpy(buffer.data() + kOffLayoutEpoch, &meta.layout_epoch,
              sizeof(meta.layout_epoch));

  const uint32_t masked_crc = Crc32cMask(ComputeTopologyCrc(buffer));
  std::memcpy(buffer.data() + kOffCrc, &masked_crc, sizeof(masked_crc));

  const std::string path = TopologyPath(data_dir);
  const int file_fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (file_fd < 0) {
    throw std::runtime_error(std::string("open failed: ") + std::strerror(errno));
  }

  try {
    WriteAll(file_fd, buffer.data(), buffer.size());
    if (::fdatasync(file_fd) != 0) {
      throw std::runtime_error(std::string("fdatasync failed: ") + std::strerror(errno));
    }
  } catch (...) {
    ::close(file_fd);
    throw;
  }

  ::close(file_fd);
}

std::optional<RecoveryManager::TopologyMeta>
RecoveryManager::ReadTopologyMeta(const std::string& data_dir) {
  const std::string path = TopologyPath(data_dir);
  if (!std::filesystem::exists(path)) {
    return std::nullopt;
  }

  const int file_fd = ::open(path.c_str(), O_RDONLY);
  if (file_fd < 0) {
    throw std::runtime_error(std::string("open failed: ") + std::strerror(errno));
  }

  std::array<uint8_t, kTopologySize> buffer{};
  try {
    ReadExact(file_fd, buffer.data(), buffer.size());
  } catch (...) {
    ::close(file_fd);
    throw;
  }
  ::close(file_fd);

  const uint32_t magic = ReadField<uint32_t>(buffer, kOffMagic);
  if (magic != kTopologyMagic) {
    throw std::runtime_error("invalid topology metadata magic");
  }

  const uint32_t version = ReadField<uint32_t>(buffer, kOffVersion);
  if (version != kTopologyVersion) {
    throw std::runtime_error("unsupported topology metadata version");
  }

  const uint32_t stored_masked_crc = ReadField<uint32_t>(buffer, kOffCrc);
  const uint32_t computed_crc = ComputeTopologyCrc(buffer);
  if (computed_crc != Crc32cUnmask(stored_masked_crc)) {
    throw std::runtime_error("topology metadata CRC mismatch");
  }

  return TopologyMeta{
      .num_cores = ReadField<uint32_t>(buffer, kOffNumCores),
      .layout_epoch = ReadField<uint64_t>(buffer, kOffLayoutEpoch),
  };
}

std::string RecoveryManager::ValidateTopology(const std::string& data_dir,
                                              int configured_cores) {
  const auto meta = ReadTopologyMeta(data_dir);
  if (!meta.has_value()) {
    return "";
  }

  if (meta->num_cores != static_cast<uint32_t>(configured_cores)) {
    return "topology mismatch: metadata num_cores=" + std::to_string(meta->num_cores) +
           ", configured_cores=" + std::to_string(configured_cores);
  }

  return "";
}

uint64_t RecoveryManager::RecoverCore(int core_id,
                                      const std::string& data_dir,
                                      StorageEngine& storage) {
  uint64_t wal_lsn = 0;
  uint64_t max_lsn = 0;
  const std::string snap_path = SnapPath(data_dir, core_id);
  if (std::filesystem::exists(snap_path)) {
    const auto header = CheckpointReader::Load(snap_path, storage);
    wal_lsn = header.wal_lsn;
  }

  const std::string wal_path = WalPath(data_dir, core_id);
  if (!std::filesystem::exists(wal_path)) {
    return max_lsn;
  }

  WalReader reader(wal_path);
  for (const auto& rec : reader.ReadAll(wal_lsn)) {
    switch (rec.type) {
    case WalRecordType::INTENT:
      storage.WriteIntent(rec.key, rec.value, rec.tx_id);
      max_lsn = std::max(max_lsn, rec.lsn);
      break;
    case WalRecordType::COMMIT_FINALIZE:
      storage.CommitTransaction(rec.tx_id, rec.commit_ts);
      max_lsn = std::max(max_lsn, rec.lsn);
      break;
    case WalRecordType::ABORT_FINALIZE:
      storage.AbortTransaction(rec.tx_id);
      max_lsn = std::max(max_lsn, rec.lsn);
      break;
    case WalRecordType::TX_BEGIN:
    case WalRecordType::PREPARE:
    case WalRecordType::COMMIT_DECISION:
    case WalRecordType::ABORT_DECISION:
    case WalRecordType::CHECKPOINT:
      break;
    }
  }

  return max_lsn;
}

RecoveryManager::RecoveredCoordinatorState
RecoveryManager::RecoverCoordinator(const std::string& data_dir) {
  RecoveredCoordinatorState state;
  const std::string wal_path = WalPath(data_dir, 0);
  if (!std::filesystem::exists(wal_path)) {
    return state;
  }

  WalReader reader(wal_path);
  for (const auto& rec : reader.ReadAll(0)) {
    UpdateMax(rec.tx_id, state.max_tx_id);
    UpdateMax(rec.snapshot_ts, state.max_snapshot_ts);
    UpdateMax(rec.commit_ts, state.max_snapshot_ts);

    switch (rec.type) {
    case WalRecordType::TX_BEGIN: {
      TxRecord& tx = state.tx_table[rec.tx_id];
      tx.tx_id = rec.tx_id;
      tx.snapshot_ts = rec.snapshot_ts;
      tx.state = TxState::ACTIVE;
      break;
    }
    case WalRecordType::COMMIT_DECISION: {
      TxRecord& tx = state.tx_table[rec.tx_id];
      tx.tx_id = rec.tx_id;
      tx.commit_ts = rec.commit_ts;
      tx.state = TxState::COMMITTED;
      break;
    }
    case WalRecordType::ABORT_DECISION: {
      TxRecord& tx = state.tx_table[rec.tx_id];
      tx.tx_id = rec.tx_id;
      tx.state = TxState::ABORTED;
      break;
    }
    case WalRecordType::INTENT:
    case WalRecordType::PREPARE:
    case WalRecordType::COMMIT_FINALIZE:
    case WalRecordType::ABORT_FINALIZE:
    case WalRecordType::CHECKPOINT:
      break;
    }
  }

  for (auto& [tx_id, tx] : state.tx_table) {
    (void)tx_id;
    if (tx.state == TxState::ACTIVE) {
      tx.state = TxState::ABORTED;
    }
  }

  return state;
}

void RecoveryManager::Repartition(
    const std::string& data_dir,
    uint32_t old_num_cores,
    uint32_t new_num_cores,
    std::vector<std::unique_ptr<StorageEngine>>& new_storages) {
  new_storages.resize(new_num_cores);
  for (uint32_t core_id = 0; core_id < new_num_cores; ++core_id) {
    if (!new_storages[core_id]) {
      new_storages[core_id] = std::make_unique<StorageEngine>();
    } else {
      new_storages[core_id]->Clear();
    }
  }

  std::vector<std::unique_ptr<StorageEngine>> old_storages(old_num_cores);
  for (uint32_t core_id = 0; core_id < old_num_cores; ++core_id) {
    old_storages[core_id] = std::make_unique<StorageEngine>();
    RecoverCore(static_cast<int>(core_id), data_dir, *old_storages[core_id]);
  }

  const auto coordinator_state = RecoverCoordinator(data_dir);
  for (const auto& [tx_id, tx] : coordinator_state.tx_table) {
    for (const auto& storage : old_storages) {
      switch (tx.state) {
      case TxState::COMMITTED:
        storage->CommitTransaction(tx_id, tx.commit_ts);
        break;
      case TxState::ACTIVE:
      case TxState::PREPARING:
      case TxState::ABORTED:
        storage->AbortTransaction(tx_id);
        break;
      }
    }
  }

  for (const auto& storage : old_storages) {
    storage->ForEachLatestCommitted(
        [&](const std::string& key, const BinaryValue& value, uint64_t commit_ts,
            bool is_deleted) {
          if (is_deleted) {
            return;
          }

          const uint32_t new_owner = std::hash<std::string>{}(key) % new_num_cores;
          new_storages[new_owner]->RestoreCommitted(key, value, commit_ts, false);
        });
  }

  const auto old_topology = ReadTopologyMeta(data_dir);
  const uint64_t new_epoch = old_topology.has_value() ? old_topology->layout_epoch + 1 : 1;
  for (uint32_t core_id = 0; core_id < new_num_cores; ++core_id) {
    CheckpointWriter::Write(*new_storages[core_id], SnapPath(data_dir, static_cast<int>(core_id)),
                            static_cast<int>(core_id), {new_num_cores, new_epoch}, 0);
  }

  for (uint32_t core_id = 0; core_id < std::max(old_num_cores, new_num_cores); ++core_id) {
    std::filesystem::remove(WalPath(data_dir, static_cast<int>(core_id)));
  }

  for (uint32_t core_id = new_num_cores; core_id < old_num_cores; ++core_id) {
    std::filesystem::remove(SnapPath(data_dir, static_cast<int>(core_id)));
  }

  WriteTopologyMeta(data_dir, {new_num_cores, new_epoch});
}

std::string RecoveryManager::WalPath(const std::string& data_dir, int core_id) {
  return data_dir + "/core_" + std::to_string(core_id) + ".wal";
}

std::string RecoveryManager::SnapPath(const std::string& data_dir, int core_id) {
  return data_dir + "/core_" + std::to_string(core_id) + ".snap";
}

std::string RecoveryManager::TopologyPath(const std::string& data_dir) {
  return data_dir + "/topology.meta";
}

}  // namespace db
