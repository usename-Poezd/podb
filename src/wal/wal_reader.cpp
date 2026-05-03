#include "wal/wal_reader.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <stdexcept>
#include <vector>

namespace db {

WalReader::WalReader(const std::string &path) : path_(path) {
  fd_ = ::open(path.c_str(), O_RDONLY);
  if (fd_ < 0) {
    throw std::runtime_error("WalReader: не удалось открыть файл: " + path);
  }

  struct stat st {};
  if (::fstat(fd_, &st) != 0) {
    ::close(fd_);
    fd_ = -1;
    throw std::runtime_error("WalReader: не удалось получить размер файла: " + path);
  }
  file_size_ = static_cast<size_t>(st.st_size);
}

WalReader::~WalReader() {
  if (fd_ != -1) {
    ::close(fd_);
  }
}

std::optional<WalRecord> WalReader::Next() {
  if (offset_ + kWalHeaderSize > file_size_) {
    return std::nullopt;
  }

  std::vector<std::byte> header(kWalHeaderSize);
  const ssize_t header_read =
      ::pread(fd_, header.data(), kWalHeaderSize, static_cast<off_t>(offset_));
  if (header_read != static_cast<ssize_t>(kWalHeaderSize)) {
    corrupted_tail_ = true;
    return std::nullopt;
  }

  uint32_t record_size = 0;
  std::memcpy(&record_size, header.data() + 4, 4);

  if (record_size < kWalHeaderSize || offset_ + record_size > file_size_) {
    corrupted_tail_ = true;
    return std::nullopt;
  }

  std::vector<std::byte> buf(record_size);
  const ssize_t total_read =
      ::pread(fd_, buf.data(), record_size, static_cast<off_t>(offset_));
  if (total_read != static_cast<ssize_t>(record_size)) {
    corrupted_tail_ = true;
    return std::nullopt;
  }

  auto result = WalRecord::Deserialize(buf.data(), record_size);
  if (!result.has_value()) {
    corrupted_tail_ = true;
    return std::nullopt;
  }

  offset_ += record_size;
  return result;
}

std::vector<WalRecord> WalReader::ReadAll(uint64_t min_lsn) {
  std::vector<WalRecord> records;
  while (true) {
    auto rec = Next();
    if (!rec.has_value()) {
      break;
    }
    if (rec->lsn > min_lsn) {
      records.push_back(std::move(*rec));
    }
  }
  return records;
}

bool WalReader::HasCorruptedTail() const noexcept { return corrupted_tail_; }

}  // namespace db
