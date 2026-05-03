#include "wal/wal_writer.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <stdexcept>

#include "wal/wal_reader.h"

namespace db {

WalWriter::WalWriter(const std::string &path) : path_(path) {
  fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd_ < 0) {
    throw std::runtime_error("WalWriter: не удалось открыть файл: " + path);
  }

  struct stat st {};
  if (::fstat(fd_, &st) == 0 && st.st_size > 0) {
    WalReader reader(path);
    const auto records = reader.ReadAll(0);
    if (!records.empty()) {
      uint64_t max_lsn = 0;
      for (const auto &r : records) {
        if (r.lsn > max_lsn) {
          max_lsn = r.lsn;
        }
      }
      current_lsn_ = max_lsn + 1;
    }
  }
}

WalWriter::~WalWriter() {
  if (fd_ != -1) {
    ::close(fd_);
  }
}

void WalWriter::Append(WalRecord record) {
  record.lsn = current_lsn_++;
  const auto bytes = record.Serialize();
  const ssize_t written = ::write(fd_, bytes.data(), bytes.size());
  if (written < 0 || static_cast<size_t>(written) != bytes.size()) {
    throw std::runtime_error("WalWriter: ошибка записи в WAL файл");
  }
}

void WalWriter::Sync() {
  if (::fdatasync(fd_) != 0) {
    throw std::runtime_error("WalWriter: ошибка fdatasync");
  }
}

uint64_t WalWriter::CurrentLsn() const noexcept { return current_lsn_; }

const std::string &WalWriter::Path() const noexcept { return path_; }

}  // namespace db
