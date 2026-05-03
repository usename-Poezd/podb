#pragma once

#include <algorithm>
#include <functional>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "core/types.h"
#include "storage/versioned_value.h"

namespace db {

class SnapshotTs {
public:
  SnapshotTs(uint64_t value) noexcept : value_(value) {}

  [[nodiscard]] uint64_t Value() const noexcept { return value_; }

private:
  uint64_t value_;
};

class TxId {
public:
  TxId(uint64_t value) noexcept : value_(value) {}

  [[nodiscard]] uint64_t Value() const noexcept { return value_; }

private:
  uint64_t value_;
};

class CommitTs {
public:
  CommitTs(uint64_t value) noexcept : value_(value) {}

  [[nodiscard]] uint64_t Value() const noexcept { return value_; }

private:
  uint64_t value_;
};

class StorageEngine {
public:
  void Set(const std::string &key, BinaryValue value) { data_[key] = std::move(value); }

  [[nodiscard]] std::optional<BinaryValue> Get(const std::string &key) const {
    if (auto data_it = data_.find(key); data_it != data_.end()) {
      return data_it->second;
    }
    return std::nullopt;
  }

  void Delete(const std::string &key) { data_.erase(key); }

  [[nodiscard]] std::size_t Size() const noexcept { return data_.size(); }

  /// MVCC чтение: сначала собственный intent, затем latest committed <= snapshot_ts.
  [[nodiscard]] MvccReadResult MvccGet(const std::string &key,
                                       SnapshotTs snapshot_ts,
                                       TxId tx_id) const {
    const auto versions_it = versions_.find(key);
    if (versions_it == versions_.end()) {
      return {};
    }

    const auto &chain = versions_it->second;
    for (const auto &version : std::views::reverse(chain)) {
      if (version.is_intent) {
        if (version.tx_id == tx_id.Value()) {
          return {.found = true, .value = version.value, .is_deleted = version.is_deleted};
        }
        continue;
      }

      if (version.commit_ts <= snapshot_ts.Value()) {
        tx_read_set_[tx_id.Value()].insert(key);
        return {.found = true, .value = version.value, .is_deleted = version.is_deleted};
      }
    }

    return {};
  }

  /// Запись intent. Если чужой intent уже на вершине цепочки — конфликт записи.
  WriteIntentResult WriteIntent(const std::string &key, BinaryValue value, uint64_t tx_id) {
    auto &chain = versions_[key];
    if (!chain.empty() && chain.back().is_intent) {
      auto &last = chain.back();
      if (last.tx_id != tx_id) {
        return WriteIntentResult::WRITE_CONFLICT;
      }

      last.value = std::move(value);
      last.is_deleted = false;
      tx_intents_[tx_id].insert(key);
      return WriteIntentResult::OK;
    }

    chain.push_back({.commit_ts = 0,
                     .value = std::move(value),
                     .tx_id = tx_id,
                     .is_intent = true,
                     .is_deleted = false});
    tx_intents_[tx_id].insert(key);
    return WriteIntentResult::OK;
  }

  /// Финализация commit: все intents транзакции становятся committed версиями.
  void CommitTransaction(TxId tx_id, CommitTs commit_ts) {
    const auto tx_it = tx_intents_.find(tx_id.Value());
    if (tx_it == tx_intents_.end()) {
      return;
    }

    for (const auto &key : tx_it->second) {
      auto versions_it = versions_.find(key);
      if (versions_it == versions_.end()) {
        continue;
      }

      auto &chain = versions_it->second;
      for (auto &version : std::views::reverse(chain)) {
        if (version.is_intent && version.tx_id == tx_id.Value()) {
          version.is_intent = false;
          version.commit_ts = commit_ts.Value();
          version.tx_id = 0;
          break;
        }
      }
    }

    tx_intents_.erase(tx_it);
    tx_read_set_.erase(tx_id.Value());
  }

  /// Финализация abort: удалить все intents транзакции из version chains.
  void AbortTransaction(uint64_t tx_id) {
    const auto tx_it = tx_intents_.find(tx_id);
    if (tx_it == tx_intents_.end()) {
      return;
    }

    for (const auto &key : tx_it->second) {
      auto versions_it = versions_.find(key);
      if (versions_it == versions_.end()) {
        continue;
      }

      auto &chain = versions_it->second;
      std::erase_if(chain, [tx_id](const VersionedValue &version) {
        return version.is_intent && version.tx_id == tx_id;
      });

      if (chain.empty()) {
        versions_.erase(versions_it);
      }
    }

    tx_intents_.erase(tx_it);
    tx_read_set_.erase(tx_id);
  }

  [[nodiscard]] const std::unordered_set<std::string> *GetReadSet(uint64_t tx_id) const {
    auto it = tx_read_set_.find(tx_id);
    return it != tx_read_set_.end() ? &it->second : nullptr;
  }

  void ForEachLatestCommitted(
      std::function<void(const std::string &key, const BinaryValue &value,
                         uint64_t commit_ts, bool is_deleted)> callback) const {
    for (const auto &[key, chain] : versions_) {
      for (auto it = chain.rbegin(); it != chain.rend(); ++it) {
        if (!it->is_intent) {
          callback(key, it->value, it->commit_ts, it->is_deleted);
          break;
        }
      }
    }
  }

  void RestoreCommitted(const std::string &key, BinaryValue value,
                        uint64_t commit_ts, bool is_deleted) {
    versions_[key].push_back({
        .commit_ts = commit_ts,
        .value = std::move(value),
        .tx_id = 0,
        .is_intent = false,
        .is_deleted = is_deleted,
    });
  }

  void Clear() {
    data_.clear();
    versions_.clear();
    tx_intents_.clear();
    tx_read_set_.clear();
  }

  [[nodiscard]] PrepareResult ValidatePrepare(uint64_t tx_id) const {
    auto it = tx_intents_.find(tx_id);
    if (it == tx_intents_.end() || it->second.empty()) {
      return {false, "no_intents"};
    }
    for (const auto &key : it->second) {
      auto vit = versions_.find(key);
      if (vit == versions_.end() || vit->second.empty()) {
        return {false, "intent_missing:" + key};
      }
      const auto &top = vit->second.back();
      if (!top.is_intent || top.tx_id != tx_id) {
        return {false, "intent_replaced:" + key};
      }
    }
    return {true, {}};
  }

  /// Garbage collect: удалить committed версии с commit_ts < watermark.
  /// Всегда сохраняет как минимум latest committed version per key.
  /// Удаляет tombstones когда они единственная версия и commit_ts < watermark.
  /// Возвращает количество удалённых версий.
  size_t GarbageCollect(uint64_t watermark) {
    if (watermark == 0) return 0;
    size_t removed = 0;

    auto it = versions_.begin();
    while (it != versions_.end()) {
      auto &chain = it->second;

      // Найти latest committed version (skip intents at top)
      int latest_committed_idx = -1;
      for (int i = static_cast<int>(chain.size()) - 1; i >= 0; --i) {
        if (!chain[i].is_intent) {
          latest_committed_idx = i;
          break;
        }
      }

      if (latest_committed_idx < 0) {
        ++it;
        continue;  // Только intents — не трогать
      }

      // Удалить committed versions ниже latest_committed с commit_ts < watermark
      if (latest_committed_idx > 0) {
        auto begin = chain.begin();
        auto end = begin + latest_committed_idx;
        auto erase_end = std::remove_if(begin, end,
          [watermark, &removed](const VersionedValue &v) {
            if (!v.is_intent && v.commit_ts < watermark) {
              ++removed;
              return true;
            }
            return false;
          });
        chain.erase(erase_end, end);
      }

      // Если остался только один tombstone с commit_ts < watermark и нет intents — удалить весь key
      if (chain.size() == 1 && !chain[0].is_intent &&
          chain[0].is_deleted && chain[0].commit_ts < watermark) {
        it = versions_.erase(it);
        ++removed;
        continue;
      }

      if (chain.empty()) {
        it = versions_.erase(it);
      } else {
        ++it;
      }
    }

    return removed;
  }

private:
  std::unordered_map<std::string, BinaryValue> data_;
  // MVCC: версионированное хранилище (per-key цепочки версий).
  std::unordered_map<std::string, std::vector<VersionedValue>> versions_;
  // Индекс: tx_id -> набор ключей с intents для быстрого commit/abort.
  std::unordered_map<uint64_t, std::unordered_set<std::string>> tx_intents_;
  mutable std::unordered_map<uint64_t, std::unordered_set<std::string>> tx_read_set_;
};

}  // namespace db
