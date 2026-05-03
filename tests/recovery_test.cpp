#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

#include "checkpoint/checkpoint_writer.h"
#include "recovery/recovery_manager.h"
#include "storage/storage_engine.h"
#include "wal/wal_record.h"
#include "wal/wal_writer.h"

namespace db {
namespace {

class RecoveryTest : public ::testing::Test {
protected:
  void SetUp() override {
    const auto* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    test_dir_ = std::filesystem::temp_directory_path() /
                (std::string("podb_recovery_test_") + std::to_string(::getpid()) + "_" +
                 test_info->test_suite_name() + "_" + test_info->name());
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  void WriteWalRecords(int core_id, const std::vector<WalRecord>& records) {
    WalWriter writer(RecoveryManager::WalPath(test_dir_.string(), core_id));
    for (const auto& record : records) {
      writer.Append(record);
    }
    writer.Sync();
  }

  void WriteSnapshot(int core_id, StorageEngine& storage, uint64_t wal_lsn) {
    CheckpointWriter::Write(storage, RecoveryManager::SnapPath(test_dir_.string(), core_id),
                            core_id, {.num_cores = 4, .layout_epoch = 1}, wal_lsn);
  }

  static BinaryValue MakeValue(const std::string& str) {
    BinaryValue value(str.size());
    std::memcpy(value.data(), str.data(), str.size());
    return value;
  }

  static std::string ValueToStr(const BinaryValue& value) {
    return {reinterpret_cast<const char*>(value.data()), value.size()};
  }

  std::filesystem::path test_dir_;
};

TEST_F(RecoveryTest, ValidateTopology_NoMetaFile_OK) {
  EXPECT_EQ(RecoveryManager::ValidateTopology(test_dir_.string(), 4), "");
}

TEST_F(RecoveryTest, ValidateTopology_Match_OK) {
  RecoveryManager::WriteTopologyMeta(test_dir_.string(), {.num_cores = 4, .layout_epoch = 3});

  EXPECT_EQ(RecoveryManager::ValidateTopology(test_dir_.string(), 4), "");
}

TEST_F(RecoveryTest, ValidateTopology_Mismatch_Error) {
  RecoveryManager::WriteTopologyMeta(test_dir_.string(), {.num_cores = 4, .layout_epoch = 3});

  EXPECT_FALSE(RecoveryManager::ValidateTopology(test_dir_.string(), 6).empty());
}

TEST_F(RecoveryTest, TopologyMeta_WriteRead_Roundtrip) {
  RecoveryManager::WriteTopologyMeta(test_dir_.string(), {.num_cores = 4, .layout_epoch = 7});

  const auto meta = RecoveryManager::ReadTopologyMeta(test_dir_.string());

  ASSERT_TRUE(meta.has_value());
  EXPECT_EQ(meta->num_cores, 4U);
  EXPECT_EQ(meta->layout_epoch, 7U);
}

TEST_F(RecoveryTest, RecoverCore_NoSnapshotNoWal_EmptyStorage) {
  StorageEngine storage;

  EXPECT_EQ(RecoveryManager::RecoverCore(0, test_dir_.string(), storage), 0U);
  EXPECT_FALSE(storage.MvccGet("missing", 100, 1).found);
}

TEST_F(RecoveryTest, RecoverCore_SnapshotOnly_LoadsData) {
  StorageEngine source;
  source.WriteIntent("a", MakeValue("va"), 1);
  source.WriteIntent("b", MakeValue("vb"), 1);
  source.WriteIntent("c", MakeValue("vc"), 1);
  source.CommitTransaction(1, 50);
  WriteSnapshot(0, source, 0);

  StorageEngine recovered;
  const uint64_t max_lsn = RecoveryManager::RecoverCore(0, test_dir_.string(), recovered);

  EXPECT_EQ(max_lsn, 0U);
  ASSERT_TRUE(recovered.MvccGet("a", 100, 99).found);
  ASSERT_TRUE(recovered.MvccGet("b", 100, 99).found);
  ASSERT_TRUE(recovered.MvccGet("c", 100, 99).found);
}

TEST_F(RecoveryTest, RecoverCore_WalOnly_ReplaysIntents) {
  WriteWalRecords(0, {{.type = WalRecordType::INTENT,
                       .tx_id = 1,
                       .key = "k",
                       .value = MakeValue("value")},
                      {.type = WalRecordType::COMMIT_FINALIZE, .tx_id = 1, .commit_ts = 100}});

  StorageEngine recovered;
  const uint64_t max_lsn = RecoveryManager::RecoverCore(0, test_dir_.string(), recovered);

  const auto result = recovered.MvccGet("k", 100, 2);
  ASSERT_TRUE(result.found);
  EXPECT_EQ(ValueToStr(result.value), "value");
  EXPECT_EQ(max_lsn, 2U);
}

TEST_F(RecoveryTest, RecoverCore_SnapshotPlusWalTail) {
  StorageEngine source;
  source.WriteIntent("a", MakeValue("va"), 1);
  source.CommitTransaction(1, 50);
  WriteSnapshot(0, source, 0);

  WriteWalRecords(0, {{.type = WalRecordType::INTENT,
                       .tx_id = 2,
                       .key = "b",
                       .value = MakeValue("vb")},
                      {.type = WalRecordType::COMMIT_FINALIZE, .tx_id = 2, .commit_ts = 60}});

  StorageEngine recovered;
  const uint64_t max_lsn = RecoveryManager::RecoverCore(0, test_dir_.string(), recovered);

  const auto a_result = recovered.MvccGet("a", 100, 99);
  const auto b_result = recovered.MvccGet("b", 100, 99);
  ASSERT_TRUE(a_result.found);
  ASSERT_TRUE(b_result.found);
  EXPECT_EQ(ValueToStr(a_result.value), "va");
  EXPECT_EQ(ValueToStr(b_result.value), "vb");
  EXPECT_EQ(max_lsn, 2U);
}

TEST_F(RecoveryTest, RecoverCore_WalAbortCleansIntents) {
  WriteWalRecords(0, {{.type = WalRecordType::INTENT,
                       .tx_id = 1,
                       .key = "k",
                       .value = MakeValue("value")},
                      {.type = WalRecordType::ABORT_FINALIZE, .tx_id = 1}});

  StorageEngine recovered;
  const uint64_t max_lsn = RecoveryManager::RecoverCore(0, test_dir_.string(), recovered);

  EXPECT_FALSE(recovered.MvccGet("k", 100, 2).found);
  EXPECT_EQ(max_lsn, 2U);
}

TEST_F(RecoveryTest, RecoverCore_SkipsRecordsBelowSnapshotLsn) {
  StorageEngine source;
  source.WriteIntent("base", MakeValue("snap"), 1);
  source.CommitTransaction(1, 50);
  WriteSnapshot(0, source, 5);

  WriteWalRecords(0, {{.type = WalRecordType::INTENT,
                       .tx_id = 10,
                       .key = "old",
                       .value = MakeValue("old")},
                      {.type = WalRecordType::COMMIT_FINALIZE, .tx_id = 10, .commit_ts = 60},
                      {.type = WalRecordType::TX_BEGIN, .tx_id = 11, .snapshot_ts = 70},
                      {.type = WalRecordType::PREPARE, .tx_id = 11, .vote_yes = true},
                      {.type = WalRecordType::CHECKPOINT, .tx_id = 0, .snapshot_lsn = 5},
                      {.type = WalRecordType::INTENT,
                       .tx_id = 12,
                       .key = "new",
                       .value = MakeValue("new")},
                      {.type = WalRecordType::COMMIT_FINALIZE, .tx_id = 12, .commit_ts = 80}});

  StorageEngine recovered;
  const uint64_t max_lsn = RecoveryManager::RecoverCore(0, test_dir_.string(), recovered);

  EXPECT_TRUE(recovered.MvccGet("base", 100, 99).found);
  EXPECT_FALSE(recovered.MvccGet("old", 100, 99).found);
  const auto new_result = recovered.MvccGet("new", 100, 99);
  ASSERT_TRUE(new_result.found);
  EXPECT_EQ(ValueToStr(new_result.value), "new");
  EXPECT_EQ(max_lsn, 7U);
}

TEST_F(RecoveryTest, RecoverCoordinator_EmptyWal_EmptyState) {
  const auto state = RecoveryManager::RecoverCoordinator(test_dir_.string());

  EXPECT_TRUE(state.tx_table.empty());
  EXPECT_EQ(state.max_tx_id, 0U);
  EXPECT_EQ(state.max_snapshot_ts, 0U);
}

TEST_F(RecoveryTest, RecoverCoordinator_BeginAndCommit) {
  WriteWalRecords(0, {{.type = WalRecordType::TX_BEGIN, .tx_id = 1, .snapshot_ts = 10},
                      {.type = WalRecordType::COMMIT_DECISION, .tx_id = 1, .commit_ts = 100}});

  const auto state = RecoveryManager::RecoverCoordinator(test_dir_.string());

  ASSERT_TRUE(state.tx_table.contains(1));
  EXPECT_EQ(state.tx_table.at(1).state, TxState::COMMITTED);
  EXPECT_EQ(state.tx_table.at(1).snapshot_ts, 10U);
  EXPECT_EQ(state.tx_table.at(1).commit_ts, 100U);
}

TEST_F(RecoveryTest, RecoverCoordinator_BeginAndAbort) {
  WriteWalRecords(0, {{.type = WalRecordType::TX_BEGIN, .tx_id = 1, .snapshot_ts = 10},
                      {.type = WalRecordType::ABORT_DECISION, .tx_id = 1}});

  const auto state = RecoveryManager::RecoverCoordinator(test_dir_.string());

  ASSERT_TRUE(state.tx_table.contains(1));
  EXPECT_EQ(state.tx_table.at(1).state, TxState::ABORTED);
}

TEST_F(RecoveryTest, RecoverCoordinator_BeginNoDecision_DefaultsAbort) {
  WriteWalRecords(0, {{.type = WalRecordType::TX_BEGIN, .tx_id = 1, .snapshot_ts = 10}});

  const auto state = RecoveryManager::RecoverCoordinator(test_dir_.string());

  ASSERT_TRUE(state.tx_table.contains(1));
  EXPECT_EQ(state.tx_table.at(1).state, TxState::ABORTED);
}

TEST_F(RecoveryTest, RecoverCoordinator_MultipleTransactions) {
  WriteWalRecords(0, {{.type = WalRecordType::TX_BEGIN, .tx_id = 1, .snapshot_ts = 10},
                      {.type = WalRecordType::COMMIT_DECISION, .tx_id = 1, .commit_ts = 100},
                      {.type = WalRecordType::TX_BEGIN, .tx_id = 2, .snapshot_ts = 11},
                      {.type = WalRecordType::ABORT_DECISION, .tx_id = 2},
                      {.type = WalRecordType::TX_BEGIN, .tx_id = 3, .snapshot_ts = 12}});

  const auto state = RecoveryManager::RecoverCoordinator(test_dir_.string());

  ASSERT_TRUE(state.tx_table.contains(1));
  ASSERT_TRUE(state.tx_table.contains(2));
  ASSERT_TRUE(state.tx_table.contains(3));
  EXPECT_EQ(state.tx_table.at(1).state, TxState::COMMITTED);
  EXPECT_EQ(state.tx_table.at(2).state, TxState::ABORTED);
  EXPECT_EQ(state.tx_table.at(3).state, TxState::ABORTED);
}

TEST_F(RecoveryTest, RecoverCoordinator_MaxTxId_Correct) {
  WriteWalRecords(0, {{.type = WalRecordType::TX_BEGIN, .tx_id = 5, .snapshot_ts = 10},
                      {.type = WalRecordType::TX_BEGIN, .tx_id = 3, .snapshot_ts = 20}});

  const auto state = RecoveryManager::RecoverCoordinator(test_dir_.string());

  EXPECT_EQ(state.max_tx_id, 5U);
}

TEST_F(RecoveryTest, RecoverCoordinator_MaxSnapshotTs_Correct) {
  WriteWalRecords(0, {{.type = WalRecordType::TX_BEGIN, .tx_id = 1, .snapshot_ts = 10},
                      {.type = WalRecordType::COMMIT_DECISION, .tx_id = 1, .commit_ts = 20}});

  const auto state = RecoveryManager::RecoverCoordinator(test_dir_.string());

  EXPECT_GE(state.max_snapshot_ts, 20U);
}

}  // namespace
}  // namespace db
