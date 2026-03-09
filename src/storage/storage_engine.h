#pragma once

#include <optional>
#include <string>
#include <unordered_map>

namespace db {

class StorageEngine {
public:
  void Set(const std::string &key, const std::string &value) { data_[key] = value; }

  [[nodiscard]] std::optional<std::string> Get(const std::string &key) const {
    if (auto it = data_.find(key); it != data_.end()) {
      return it->second;
    }
    return std::nullopt;
  }

  void Delete(const std::string &key) { data_.erase(key); }

  [[nodiscard]] std::size_t Size() const noexcept { return data_.size(); }

private:
  std::unordered_map<std::string, std::string> data_;
};

} // namespace db
