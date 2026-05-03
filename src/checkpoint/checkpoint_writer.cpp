#include "checkpoint/checkpoint_writer.h"

#include <fcntl.h>
#include <libgen.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <span>
#include <stdexcept>
#include <string>

#include "wal/crc32c.h"

namespace db {

namespace {

constexpr uint32_t kMagic   = 0xDB534E50U;
constexpr uint32_t kVersion = 1U;

// Байтовые смещения для pwrite патчинга заголовка (entry_count и crc)
constexpr off_t  kOffEntryCount = 32;
constexpr off_t  kOffCrc        = 40;
constexpr size_t kHeaderSize    = 44;

void WriteAll(int file_fd, const void* src_buf, size_t byte_count) {
  auto view = std::span<const uint8_t>(
      static_cast<const uint8_t*>(src_buf), byte_count);
  while (!view.empty()) {
    const ssize_t written = ::write(file_fd, view.data(), view.size());
    if (written < 0) {
      throw std::runtime_error(std::string("write failed: ") + std::strerror(errno));
    }
    view = view.subspan(static_cast<size_t>(written));
  }
}

void WriteU32(int file_fd, uint32_t val) { WriteAll(file_fd, &val, sizeof(val)); }
void WriteU64(int file_fd, uint64_t val) { WriteAll(file_fd, &val, sizeof(val)); }
void WriteU8(int file_fd, uint8_t val)   { WriteAll(file_fd, &val, sizeof(val)); }

void PWriteU64(int file_fd, uint64_t val, off_t offset) {
  const ssize_t written = ::pwrite(file_fd, &val, sizeof(val), offset);
  if (written != static_cast<ssize_t>(sizeof(val))) {
    throw std::runtime_error(std::string("pwrite failed: ") + std::strerror(errno));
  }
}

void PWriteU32(int file_fd, uint32_t val, off_t offset) {
  const ssize_t written = ::pwrite(file_fd, &val, sizeof(val), offset);
  if (written != static_cast<ssize_t>(sizeof(val))) {
    throw std::runtime_error(std::string("pwrite failed: ") + std::strerror(errno));
  }
}



}  // namespace

void CheckpointWriter::Write(const StorageEngine& storage,
                             const std::string& path,
                             int core_id,
                             const TopologyMeta& topo,
                             uint64_t wal_lsn) {
  const std::string tmp_path = path + ".tmp";

  const int file_fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (file_fd < 0) {
    throw std::runtime_error(std::string("open tmp failed: ") + std::strerror(errno));
  }

  try {
    // 1. Записать заголовок с нулевыми placeholder-полями entry_count и crc.
    WriteU32(file_fd, kMagic);
    WriteU32(file_fd, kVersion);
    WriteU32(file_fd, static_cast<uint32_t>(core_id));
    WriteU32(file_fd, topo.num_cores);
    WriteU64(file_fd, topo.layout_epoch);
    WriteU64(file_fd, wal_lsn);
    WriteU64(file_fd, 0U);
    WriteU32(file_fd, 0U);

    // 2. Записать все committed-версии и посчитать их.
    uint64_t entry_count = 0;
    storage.ForEachLatestCommitted([&](const std::string& key,
                                       const BinaryValue& value,
                                       uint64_t commit_ts,
                                       bool is_deleted) {
      WriteU32(file_fd, static_cast<uint32_t>(key.size()));
      WriteAll(file_fd, key.data(), key.size());

      WriteU32(file_fd, static_cast<uint32_t>(value.size()));
      if (!value.empty()) {
        WriteAll(file_fd, value.data(), value.size());
      }

      WriteU64(file_fd, commit_ts);
      WriteU8(file_fd, static_cast<uint8_t>(is_deleted ? 1U : 0U));

      ++entry_count;
    });

    // 3. Патчим entry_count в заголовке.
    PWriteU64(file_fd, entry_count, kOffEntryCount);

    // 4. Вычисляем CRC32c над полным заголовком (поле crc=0), маскируем и записываем.
    const auto core_id_u32 = static_cast<uint32_t>(core_id);
    std::array<uint8_t, kHeaderSize> hdr_buf{};
    std::memcpy(hdr_buf.data(),      &kMagic,               4);
    std::memcpy(&hdr_buf[4],         &kVersion,             4);
    std::memcpy(&hdr_buf[8],         &core_id_u32,          4);
    std::memcpy(&hdr_buf[12],        &topo.num_cores,       4);
    std::memcpy(&hdr_buf[16],        &topo.layout_epoch,    8);
    std::memcpy(&hdr_buf[24],        &wal_lsn,              8);
    std::memcpy(&hdr_buf[32],        &entry_count,          8);
    const uint32_t masked_crc = Crc32cMask(Crc32c(hdr_buf.data(), kHeaderSize));
    PWriteU32(file_fd, masked_crc, kOffCrc);

    // 5. Сбросить на диск и закрыть.
    if (::fdatasync(file_fd) != 0) {
      throw std::runtime_error(std::string("fdatasync failed: ") + std::strerror(errno));
    }
  } catch (...) {
    ::close(file_fd);
    ::unlink(tmp_path.c_str());
    throw;
  }

  ::close(file_fd);

  // 6. Атомарный rename tmp → final.
  if (::rename(tmp_path.c_str(), path.c_str()) != 0) {
    ::unlink(tmp_path.c_str());
    throw std::runtime_error(std::string("rename failed: ") + std::strerror(errno));
  }

  // 7. fsync родительской директории — закрепить rename.
  std::string path_copy = path;
  const char* dir_cstr = ::dirname(path_copy.data());
  const int dir_fd = ::open(dir_cstr, O_RDONLY | O_DIRECTORY);
  if (dir_fd >= 0) {
    ::fsync(dir_fd);
    ::close(dir_fd);
  }
}

}  // namespace db
