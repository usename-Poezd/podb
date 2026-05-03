#include "checkpoint/checkpoint_reader.h"

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <span>
#include <stdexcept>

#include "wal/crc32c.h"

namespace db {

namespace {

constexpr uint32_t kMagic      = 0xDB534E50U;
constexpr size_t   kHeaderSize = 44;

// Байтовые смещения полей в 44-байтном заголовке
constexpr size_t kOffMagic       = 0;
constexpr size_t kOffCoreId      = 8;
constexpr size_t kOffNumCores    = 12;
constexpr size_t kOffLayoutEpoch = 16;
constexpr size_t kOffWalLsn      = 24;
constexpr size_t kOffEntryCount  = 32;
constexpr size_t kOffCrc         = 40;

bool ReadExact(int file_fd, void* dest, size_t byte_count) {
  auto buf = std::span<uint8_t>(static_cast<uint8_t*>(dest), byte_count);
  while (!buf.empty()) {
    const ssize_t bytes_read = ::read(file_fd, buf.data(), buf.size());
    if (bytes_read <= 0) {
      return false;
    }
    buf = buf.subspan(static_cast<size_t>(bytes_read));
  }
  return true;
}

// Прочитать скалярное поле из заголовочного буфера по байтовому смещению.
template <typename FieldT>
FieldT ReadField(const std::array<uint8_t, kHeaderSize>& header_buf, size_t offset) {
  FieldT field_val{};
  const auto field_span =
      std::span<const uint8_t>(header_buf).subspan(offset, sizeof(FieldT));
  std::memcpy(&field_val, field_span.data(), sizeof(FieldT));
  return field_val;
}

// Вычислить CRC32c над заголовком с обнулённым полем crc (offset 40).
uint32_t ComputeHeaderCrc(const std::array<uint8_t, kHeaderSize>& header_buf) {
  std::array<uint8_t, kHeaderSize> scratch = header_buf;
  const uint32_t zero = 0U;
  std::memcpy(&scratch[kOffCrc], &zero, sizeof(zero));
  return Crc32c(scratch.data(), kHeaderSize);
}

}  // namespace

std::optional<CheckpointReader::SnapshotHeader> CheckpointReader::ReadHeader(
    const std::string& path) {
  const int file_fd = ::open(path.c_str(), O_RDONLY);
  if (file_fd < 0) {
    return std::nullopt;
  }

  std::array<uint8_t, kHeaderSize> raw{};
  if (!ReadExact(file_fd, raw.data(), kHeaderSize)) {
    ::close(file_fd);
    return std::nullopt;
  }
  ::close(file_fd);

  const auto magic = ReadField<uint32_t>(raw, kOffMagic);
  if (magic != kMagic) {
    return std::nullopt;
  }

  // Проверка CRC: восстанавливаем исходный CRC из хранимого masked значения,
  // сравниваем с вычисленным по заголовку (поле crc обнулено перед вычислением).
  const auto stored_masked = ReadField<uint32_t>(raw, kOffCrc);
  const uint32_t stored_crc    = Crc32cUnmask(stored_masked);
  const uint32_t computed_crc  = ComputeHeaderCrc(raw);
  if (computed_crc != stored_crc) {
    return std::nullopt;
  }

  SnapshotHeader hdr;
  hdr.core_id      = ReadField<uint32_t>(raw, kOffCoreId);
  hdr.num_cores    = ReadField<uint32_t>(raw, kOffNumCores);
  hdr.layout_epoch = ReadField<uint64_t>(raw, kOffLayoutEpoch);
  hdr.wal_lsn      = ReadField<uint64_t>(raw, kOffWalLsn);
  hdr.entry_count  = ReadField<uint64_t>(raw, kOffEntryCount);
  return hdr;
}

CheckpointReader::SnapshotHeader CheckpointReader::Load(const std::string& path,
                                                        StorageEngine& storage) {
  const auto maybe_hdr = ReadHeader(path);
  if (!maybe_hdr.has_value()) {
    throw std::runtime_error("CheckpointReader::Load: некорректный snapshot: " + path);
  }
  const SnapshotHeader hdr = *maybe_hdr;

  const int file_fd = ::open(path.c_str(), O_RDONLY);
  if (file_fd < 0) {
    throw std::runtime_error(std::string("open failed: ") + std::strerror(errno));
  }

  if (::lseek(file_fd, static_cast<off_t>(kHeaderSize), SEEK_SET) == -1) {
    ::close(file_fd);
    throw std::runtime_error(std::string("lseek failed: ") + std::strerror(errno));
  }

  storage.Clear();

  for (uint64_t idx = 0; idx < hdr.entry_count; ++idx) {
    uint32_t key_size = 0;
    if (!ReadExact(file_fd, &key_size, sizeof(key_size))) {
      ::close(file_fd);
      throw std::runtime_error("unexpected EOF reading key_size");
    }

    std::string key(key_size, '\0');
    if (key_size > 0U && !ReadExact(file_fd, key.data(), key_size)) {
      ::close(file_fd);
      throw std::runtime_error("unexpected EOF reading key");
    }

    uint32_t val_size = 0;
    if (!ReadExact(file_fd, &val_size, sizeof(val_size))) {
      ::close(file_fd);
      throw std::runtime_error("unexpected EOF reading val_size");
    }

    BinaryValue value(val_size);
    if (val_size > 0U && !ReadExact(file_fd, value.data(), val_size)) {
      ::close(file_fd);
      throw std::runtime_error("unexpected EOF reading value");
    }

    uint64_t commit_ts = 0;
    if (!ReadExact(file_fd, &commit_ts, sizeof(commit_ts))) {
      ::close(file_fd);
      throw std::runtime_error("unexpected EOF reading commit_ts");
    }

    uint8_t is_deleted_byte = 0;
    if (!ReadExact(file_fd, &is_deleted_byte, sizeof(is_deleted_byte))) {
      ::close(file_fd);
      throw std::runtime_error("unexpected EOF reading is_deleted");
    }

    storage.RestoreCommitted(key, std::move(value), commit_ts,
                             is_deleted_byte != 0U);
  }

  ::close(file_fd);
  return hdr;
}

}  // namespace db
