#pragma once

#include <cstdint>
#include <string>

#include "wal/wal_record.h"

namespace db {

class WalWriter {
public:
  /// Открыть/создать WAL файл. Продолжить с последнего LSN если файл не пуст.
  explicit WalWriter(const std::string &path);
  ~WalWriter();
  WalWriter(const WalWriter &) = delete;
  WalWriter &operator=(const WalWriter &) = delete;

  /// Добавить запись в WAL. Присваивает LSN автоматически.
  void Append(WalRecord record);

  /// fdatasync — делает все записанные данные durable.
  void Sync();

  /// Текущий LSN (следующий LSN для записи).
  [[nodiscard]] uint64_t CurrentLsn() const noexcept;

  /// Путь к WAL файлу.
  [[nodiscard]] const std::string &Path() const noexcept;

private:
  std::string path_;
  int fd_{-1};
  uint64_t current_lsn_{1};
};

}  // namespace db
