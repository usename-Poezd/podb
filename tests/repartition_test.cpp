#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "checkpoint/checkpoint_reader.h"
#include "checkpoint/checkpoint_writer.h"
#include "recovery/recovery_manager.h"
#include "storage/storage_engine.h"
#include "wal/wal_reader.h"
#include "wal/wal_record.h"
#include "wal/wal_writer.h"

namespace db {
namespace {

class RepartitionTest : public ::testing::Test {
protected:
  void SetUp() override {
    const auto* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    test_dir_ = std::filesystem::temp_directory_path() /
                (std::string("podb_repart_test_") + std::to_string(::getpid()) + "_" +
                 test_info->test_suite_name() + "_" + test_info->name());
    std::filesystem::create_directories(test_dir_);
    data_dir_ = test_dir_.string();
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  static BinaryValue MakeValue(const std::string& s) {
    BinaryValue v(s.size());
    if (!s.empty()) {
      std::memcpy(v.data(), s.data(), s.size());
    }
    return v;
  }

  static std::string ValueToStr(const BinaryValue& v) {
    return {reinterpret_cast<const char*>(v.data()), v.size()};
  }

  static std::vector<std::pair<std::string, std::string>> SamplePairs() {
    return {{"alpha", "v1"},   {"bravo", "v2"},   {"charlie", "v3"},
            {"delta", "v4"},   {"echo", "v5"},    {"foxtrot", "v6"},
            {"golf", "v7"},    {"hotel", "v8"},   {"india", "v9"},
            {"juliet", "v10"}};
  }

  static size_t CountCommitted(const StorageEngine& storage) {
    size_t count = 0;
    storage.ForEachLatestCommitted(
        [&](const std::string&, const BinaryValue&, uint64_t, bool) { ++count; });
    return count;
  }

  void WriteWalRecords(int core_id, const std::vector<WalRecord>& records) const {
    WalWriter writer(RecoveryManager::WalPath(data_dir_, core_id));
    for (const auto& record : records) {
      writer.Append(record);
    }
    writer.Sync();
  }

  void SetupOldTopology(uint32_t old_cores,
                        const std::vector<std::pair<std::string, std::string>>& kv_pairs,
                        uint64_t layout_epoch = 1) {
    std::vector<std::unique_ptr<StorageEngine>> old_storages(old_cores);
    for (uint32_t i = 0; i < old_cores; ++i) {
      old_storages[i] = std::make_unique<StorageEngine>();
    }

    uint64_t tx_id = 1;
    for (const auto& [key, value] : kv_pairs) {
      const uint32_t owner = std::hash<std::string>{}(key) % old_cores;
      old_storages[owner]->WriteIntent(key, MakeValue(value), tx_id);
      old_storages[owner]->CommitTransaction(tx_id, tx_id * 10);
      ++tx_id;
    }

    for (uint32_t i = 0; i < old_cores; ++i) {
      CheckpointWriter::Write(*old_storages[i], RecoveryManager::SnapPath(data_dir_, i),
                              static_cast<int>(i), {old_cores, layout_epoch}, 0);
    }

    RecoveryManager::WriteTopologyMeta(data_dir_, {old_cores, layout_epoch});
  }

  void ExpectKeyOnNewOwner(
      const std::vector<std::unique_ptr<StorageEngine>>& storages,
      uint32_t new_cores,
      const std::string& key,
      const std::string& expected_value) const {
    const uint32_t owner = std::hash<std::string>{}(key) % new_cores;
    for (uint32_t i = 0; i < storages.size(); ++i) {
      const auto result = storages[i]->MvccGet(key, std::numeric_limits<uint64_t>::max(), 1);
      if (i == owner) {
        ASSERT_TRUE(result.found) << "key=" << key << " owner=" << owner;
        EXPECT_FALSE(result.is_deleted);
        EXPECT_EQ(ValueToStr(result.value), expected_value);
      } else {
        EXPECT_FALSE(result.found) << "key=" << key << " leaked to core " << i;
      }
    }
  }

  void ExpectKeyAbsent(const std::vector<std::unique_ptr<StorageEngine>>& storages,
                       const std::string& key) const {
    for (const auto& storage : storages) {
      EXPECT_FALSE(storage->MvccGet(key, std::numeric_limits<uint64_t>::max(), 1).found);
    }
  }

  std::filesystem::path test_dir_;
  std::string data_dir_;
};

TEST_F(RepartitionTest, Repartition_4to6_KeysRehashed) {
  const auto kv_pairs = SamplePairs();
  SetupOldTopology(4, kv_pairs);

  std::vector<std::unique_ptr<StorageEngine>> new_storages;
  RecoveryManager::Repartition(data_dir_, 4, 6, new_storages);

  ASSERT_EQ(new_storages.size(), 6U);
  size_t total = 0;
  for (const auto& storage : new_storages) {
    total += CountCommitted(*storage);
  }
  EXPECT_EQ(total, kv_pairs.size());

  for (const auto& [key, value] : kv_pairs) {
    ExpectKeyOnNewOwner(new_storages, 6, key, value);
  }
}

TEST_F(RepartitionTest, Repartition_6to4_KeysRehashed) {
  const auto kv_pairs = SamplePairs();
  SetupOldTopology(6, kv_pairs);

  std::vector<std::unique_ptr<StorageEngine>> new_storages;
  RecoveryManager::Repartition(data_dir_, 6, 4, new_storages);

  ASSERT_EQ(new_storages.size(), 4U);
  size_t total = 0;
  for (const auto& storage : new_storages) {
    total += CountCommitted(*storage);
  }
  EXPECT_EQ(total, kv_pairs.size());

  for (const auto& [key, value] : kv_pairs) {
    ExpectKeyOnNewOwner(new_storages, 4, key, value);
  }
}

TEST_F(RepartitionTest, Repartition_ResolvesInFlightTxs) {
  StorageEngine old_storage;
  old_storage.WriteIntent("committed", MakeValue("alive"), 1);
  old_storage.CommitTransaction(1, 10);
  CheckpointWriter::Write(old_storage, RecoveryManager::SnapPath(data_dir_, 0), 0,
                          {.num_cores = 1, .layout_epoch = 1}, 0);
  RecoveryManager::WriteTopologyMeta(data_dir_, {.num_cores = 1, .layout_epoch = 1});

  WriteWalRecords(0, {{.type = WalRecordType::TX_BEGIN, .tx_id = 100, .snapshot_ts = 77},
                      {.type = WalRecordType::INTENT,
                       .tx_id = 100,
                       .key = "uncommitted",
                       .value = MakeValue("ghost")}});

  std::vector<std::unique_ptr<StorageEngine>> new_storages;
  RecoveryManager::Repartition(data_dir_, 1, 2, new_storages);

  ExpectKeyOnNewOwner(new_storages, 2, "committed", "alive");
  ExpectKeyAbsent(new_storages, "uncommitted");
}

TEST_F(RepartitionTest, Repartition_FreshSnapshots) {
  SetupOldTopology(4, SamplePairs());

  std::vector<std::unique_ptr<StorageEngine>> new_storages;
  RecoveryManager::Repartition(data_dir_, 4, 6, new_storages);

  for (uint32_t core_id = 0; core_id < 6; ++core_id) {
    const auto path = RecoveryManager::SnapPath(data_dir_, static_cast<int>(core_id));
    EXPECT_TRUE(std::filesystem::exists(path));
    const auto header = CheckpointReader::ReadHeader(path);
    ASSERT_TRUE(header.has_value());
    EXPECT_EQ(header->core_id, core_id);
    EXPECT_EQ(header->num_cores, 6U);
    EXPECT_EQ(header->layout_epoch, 2U);
    EXPECT_EQ(header->wal_lsn, 0U);
  }
}

TEST_F(RepartitionTest, Repartition_OldWalDeleted) {
  SetupOldTopology(4, SamplePairs());
  WriteWalRecords(0, {{.type = WalRecordType::TX_BEGIN, .tx_id = 10, .snapshot_ts = 10}});
  WriteWalRecords(1, {{.type = WalRecordType::ABORT_DECISION, .tx_id = 10}});
  WriteWalRecords(2, {{.type = WalRecordType::CHECKPOINT, .snapshot_lsn = 5}});
  WriteWalRecords(3, {{.type = WalRecordType::COMMIT_DECISION, .tx_id = 11, .commit_ts = 99}});

  std::vector<std::unique_ptr<StorageEngine>> new_storages;
  RecoveryManager::Repartition(data_dir_, 4, 6, new_storages);

  for (uint32_t core_id = 0; core_id < 4; ++core_id) {
    EXPECT_FALSE(
        std::filesystem::exists(RecoveryManager::WalPath(data_dir_, static_cast<int>(core_id))));
  }
}

TEST_F(RepartitionTest, Repartition_OrphanedSnapshotsDeleted) {
  SetupOldTopology(6, SamplePairs());

  std::vector<std::unique_ptr<StorageEngine>> new_storages;
  RecoveryManager::Repartition(data_dir_, 6, 4, new_storages);

  EXPECT_FALSE(std::filesystem::exists(RecoveryManager::SnapPath(data_dir_, 4)));
  EXPECT_FALSE(std::filesystem::exists(RecoveryManager::SnapPath(data_dir_, 5)));
  for (uint32_t core_id = 0; core_id < 4; ++core_id) {
    EXPECT_TRUE(std::filesystem::exists(
        RecoveryManager::SnapPath(data_dir_, static_cast<int>(core_id))));
  }
}

TEST_F(RepartitionTest, Repartition_EmptyDatabase) {
  SetupOldTopology(4, {});

  std::vector<std::unique_ptr<StorageEngine>> new_storages;
  RecoveryManager::Repartition(data_dir_, 4, 2, new_storages);

  ASSERT_EQ(new_storages.size(), 2U);
  for (uint32_t core_id = 0; core_id < 2; ++core_id) {
    EXPECT_EQ(CountCommitted(*new_storages[core_id]), 0U);
    const auto header =
        CheckpointReader::ReadHeader(RecoveryManager::SnapPath(data_dir_, static_cast<int>(core_id)));
    ASSERT_TRUE(header.has_value());
    EXPECT_EQ(header->entry_count, 0U);
    EXPECT_EQ(header->num_cores, 2U);
  }
}

TEST_F(RepartitionTest, Repartition_TombstonesNotCarried) {
  std::vector<std::unique_ptr<StorageEngine>> old_storages(2);
  for (auto& storage : old_storages) {
    storage = std::make_unique<StorageEngine>();
  }

  const std::string key = "ghost";
  const uint32_t owner = std::hash<std::string>{}(key) % 2;
  old_storages[owner]->RestoreCommitted(key, MakeValue("gone"), 55, true);

  for (uint32_t core_id = 0; core_id < 2; ++core_id) {
    CheckpointWriter::Write(*old_storages[core_id], RecoveryManager::SnapPath(data_dir_, core_id),
                            static_cast<int>(core_id), {.num_cores = 2, .layout_epoch = 1}, 0);
  }
  RecoveryManager::WriteTopologyMeta(data_dir_, {.num_cores = 2, .layout_epoch = 1});

  std::vector<std::unique_ptr<StorageEngine>> new_storages;
  RecoveryManager::Repartition(data_dir_, 2, 3, new_storages);

  ExpectKeyAbsent(new_storages, key);
  size_t total = 0;
  for (const auto& storage : new_storages) {
    total += CountCommitted(*storage);
  }
  EXPECT_EQ(total, 0U);
}

TEST_F(RepartitionTest, Repartition_LayoutEpochIncremented) {
  SetupOldTopology(4, SamplePairs(), 1);

  std::vector<std::unique_ptr<StorageEngine>> new_storages;
  RecoveryManager::Repartition(data_dir_, 4, 6, new_storages);

  const auto meta = RecoveryManager::ReadTopologyMeta(data_dir_);
  ASSERT_TRUE(meta.has_value());
  EXPECT_EQ(meta->num_cores, 6U);
  EXPECT_EQ(meta->layout_epoch, 2U);
}

}  // namespace
}  // namespace db
