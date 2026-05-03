#include "wal/wal_record.h"

#include <cstring>

#include "wal/crc32c.h"

namespace db {

// Макет заголовка (kWalHeaderSize = 32 байта):
//   [0-3]   magic       uint32_t
//   [4-7]   record_size uint32_t  (полный размер записи включая заголовок)
//   [8-15]  lsn         uint64_t
//   [16]    type        uint8_t
//   [17-19] padding     (нули)
//   [20-27] tx_id       uint64_t
//   [28-31] crc32c      uint32_t  (маскированный CRC; при вычислении = 0)

std::vector<std::byte> WalRecord::Serialize() const {
  size_t payload_size = 0;
  switch (type) {
  case WalRecordType::TX_BEGIN:
    payload_size = 8;
    break;
  case WalRecordType::INTENT:
    payload_size = 4 + key.size() + 4 + value.size() + 1;
    break;
  case WalRecordType::PREPARE:
    payload_size = 1;
    break;
  case WalRecordType::COMMIT_DECISION:
  case WalRecordType::COMMIT_FINALIZE:
    payload_size = 8;
    break;
  case WalRecordType::ABORT_DECISION:
  case WalRecordType::ABORT_FINALIZE:
    payload_size = 0;
    break;
  case WalRecordType::CHECKPOINT:
    payload_size = 8;
    break;
  }

  const size_t total_size = kWalHeaderSize + payload_size;
  std::vector<std::byte> buf(total_size, std::byte{0});

  const uint32_t magic = kWalMagic;
  const uint32_t record_size = static_cast<uint32_t>(total_size);
  const auto type_byte = static_cast<uint8_t>(type);

  std::memcpy(buf.data() + 0, &magic, 4);
  std::memcpy(buf.data() + 4, &record_size, 4);
  std::memcpy(buf.data() + 8, &lsn, 8);
  std::memcpy(buf.data() + 16, &type_byte, 1);
  std::memcpy(buf.data() + 20, &tx_id, 8);
  // [28-31] crc32c — заполняется после вычисления

  size_t offset = kWalHeaderSize;
  switch (type) {
  case WalRecordType::TX_BEGIN:
    std::memcpy(buf.data() + offset, &snapshot_ts, 8);
    break;
  case WalRecordType::INTENT: {
    const uint32_t key_sz = static_cast<uint32_t>(key.size());
    std::memcpy(buf.data() + offset, &key_sz, 4);
    offset += 4;
    if (!key.empty()) {
      std::memcpy(buf.data() + offset, key.data(), key.size());
      offset += key.size();
    }
    const uint32_t val_sz = static_cast<uint32_t>(value.size());
    std::memcpy(buf.data() + offset, &val_sz, 4);
    offset += 4;
    if (!value.empty()) {
      std::memcpy(buf.data() + offset, value.data(), value.size());
      offset += value.size();
    }
    const uint8_t del = is_deleted ? 1 : 0;
    std::memcpy(buf.data() + offset, &del, 1);
    break;
  }
  case WalRecordType::PREPARE: {
    const uint8_t vote = vote_yes ? 1 : 0;
    std::memcpy(buf.data() + offset, &vote, 1);
    break;
  }
  case WalRecordType::COMMIT_DECISION:
  case WalRecordType::COMMIT_FINALIZE:
    std::memcpy(buf.data() + offset, &commit_ts, 8);
    break;
  case WalRecordType::ABORT_DECISION:
  case WalRecordType::ABORT_FINALIZE:
    break;
  case WalRecordType::CHECKPOINT:
    std::memcpy(buf.data() + offset, &snapshot_lsn, 8);
    break;
  }

  // CRC32c по всему буферу (crc поле = 0), результат маскируем и вставляем
  const uint32_t masked = Crc32cMask(Crc32c(buf.data(), total_size));
  std::memcpy(buf.data() + 28, &masked, 4);

  return buf;
}

std::optional<WalRecord> WalRecord::Deserialize(const std::byte *data, size_t size) {
  if (size < kWalHeaderSize) {
    return std::nullopt;
  }

  uint32_t magic;
  std::memcpy(&magic, data + 0, 4);
  if (magic != kWalMagic) {
    return std::nullopt;
  }

  uint32_t record_size;
  std::memcpy(&record_size, data + 4, 4);
  if (static_cast<size_t>(record_size) != size) {
    return std::nullopt;
  }

  uint64_t lsn;
  std::memcpy(&lsn, data + 8, 8);

  uint8_t type_byte;
  std::memcpy(&type_byte, data + 16, 1);

  uint64_t tx_id;
  std::memcpy(&tx_id, data + 20, 8);

  uint32_t stored_masked;
  std::memcpy(&stored_masked, data + 28, 4);

  // Копируем буфер, обнуляем crc-поле, вычисляем CRC32c
  std::vector<std::byte> buf(data, data + size);
  const uint32_t zero_crc = 0;
  std::memcpy(buf.data() + 28, &zero_crc, 4);
  if (Crc32c(buf.data(), size) != Crc32cUnmask(stored_masked)) {
    return std::nullopt;
  }

  WalRecord rec;
  rec.type = static_cast<WalRecordType>(type_byte);
  rec.lsn = lsn;
  rec.tx_id = tx_id;

  size_t offset = kWalHeaderSize;
  switch (rec.type) {
  case WalRecordType::TX_BEGIN:
    if (offset + 8 > size) return std::nullopt;
    std::memcpy(&rec.snapshot_ts, data + offset, 8);
    break;
  case WalRecordType::INTENT: {
    if (offset + 4 > size) return std::nullopt;
    uint32_t key_sz;
    std::memcpy(&key_sz, data + offset, 4);
    offset += 4;
    if (offset + key_sz > size) return std::nullopt;
    rec.key.resize(key_sz);
    if (key_sz > 0) {
      std::memcpy(rec.key.data(), data + offset, key_sz);
    }
    offset += key_sz;
    if (offset + 4 > size) return std::nullopt;
    uint32_t val_sz;
    std::memcpy(&val_sz, data + offset, 4);
    offset += 4;
    if (offset + val_sz + 1 > size) return std::nullopt;
    rec.value.resize(val_sz);
    if (val_sz > 0) {
      std::memcpy(rec.value.data(), data + offset, val_sz);
    }
    offset += val_sz;
    uint8_t del;
    std::memcpy(&del, data + offset, 1);
    rec.is_deleted = (del != 0);
    break;
  }
  case WalRecordType::PREPARE: {
    if (offset + 1 > size) return std::nullopt;
    uint8_t vote;
    std::memcpy(&vote, data + offset, 1);
    rec.vote_yes = (vote != 0);
    break;
  }
  case WalRecordType::COMMIT_DECISION:
  case WalRecordType::COMMIT_FINALIZE:
    if (offset + 8 > size) return std::nullopt;
    std::memcpy(&rec.commit_ts, data + offset, 8);
    break;
  case WalRecordType::ABORT_DECISION:
  case WalRecordType::ABORT_FINALIZE:
    break;
  case WalRecordType::CHECKPOINT:
    if (offset + 8 > size) return std::nullopt;
    std::memcpy(&rec.snapshot_lsn, data + offset, 8);
    break;
  default:
    return std::nullopt;
  }

  return rec;
}

}  // namespace db
