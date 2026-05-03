#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "wal/wal_record.h"

namespace db {

class WalReader {
public:
  explicit WalReader(const std::string &path);
  ~WalReader();
  WalReader(const WalReader &) = delete;
  WalReader &operator=(const WalReader &) = delete;

  /// Прочитать следующую запись. nullopt = конец файла или corrupted tail.
  [[nodiscard]] std::optional<WalRecord> Next();

  /// Прочитать все записи с LSN > min_lsn.
  [[nodiscard]] std::vector<WalRecord> ReadAll(uint64_t min_lsn = 0);

  /// Был ли обнаружен corrupted tail при чтении.
  [[nodiscard]] bool HasCorruptedTail() const noexcept;

private:
  std::string path_;
  int fd_{-1};
  size_t file_size_{0};
  size_t offset_{0};
  bool corrupted_tail_{false};
};

}  // namespace db
