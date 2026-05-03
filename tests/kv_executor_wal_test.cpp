#include <gtest/gtest.h>
#include <filesystem>
#include <cstring>
#include <unistd.h>

#include "core/task.h"
#include "execution/kv_executor.h"
#include "storage/storage_engine.h"
#include "wal/wal_writer.h"
#include "wal/wal_reader.h"
#include "wal/wal_record.h"

namespace db {
namespace {

class KvExecutorWalTest : public ::testing::Test {
protected:
  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() /
                ("podb_exec_wal_test_" + std::to_string(::getpid()));
    std::filesystem::create_directories(test_dir_);
    wal_path_ = (test_dir_ / "test.wal").string();
    wal_writer_ = std::make_unique<WalWriter>(wal_path_);
    executor_ = std::make_unique<KvExecutor>(storage_, 0, wal_writer_.get());
  }

  void TearDown() override {
    wal_writer_.reset();
    executor_.reset();
    std::filesystem::remove_all(test_dir_);
  }

  std::vector<WalRecord> ReadWalRecords() {
    wal_writer_.reset();
    WalReader reader(wal_path_);
    return reader.ReadAll();
  }

  static BinaryValue MakeValue(const std::string &s) {
    BinaryValue v(s.size());
    std::memcpy(v.data(), s.data(), s.size());
    return v;
  }

  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  std::filesystem::path test_dir_;
  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  std::string wal_path_;
  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  StorageEngine storage_;
  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  std::unique_ptr<WalWriter> wal_writer_;
  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  std::unique_ptr<KvExecutor> executor_;
};

TEST_F(KvExecutorWalTest, TxSet_WritesIntentRecord) {
  Task req;
  req.type = TaskType::TX_EXECUTE_SET_REQUEST;
  req.key = "foo";
  req.value = MakeValue("bar");
  req.tx_id = 42;
  executor_->Execute(req);

  const auto records = ReadWalRecords();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].type, WalRecordType::INTENT);
  EXPECT_EQ(records[0].tx_id, 42u);
  EXPECT_EQ(records[0].key, "foo");
  EXPECT_EQ(records[0].value, MakeValue("bar"));
  EXPECT_FALSE(records[0].is_deleted);
}

TEST_F(KvExecutorWalTest, Prepare_YesVote_WritesPrepareRecord) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("v"), 10), WriteIntentResult::OK);

  Task req;
  req.type = TaskType::TX_PREPARE_REQUEST;
  req.tx_id = 10;
  const auto resp = executor_->Execute(req);
  ASSERT_TRUE(resp.success);

  const auto records = ReadWalRecords();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].type, WalRecordType::PREPARE);
  EXPECT_EQ(records[0].tx_id, 10u);
  EXPECT_TRUE(records[0].vote_yes);
}

TEST_F(KvExecutorWalTest, Prepare_YesVote_Syncs) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("v"), 20), WriteIntentResult::OK);

  Task req;
  req.type = TaskType::TX_PREPARE_REQUEST;
  req.tx_id = 20;
  const auto resp = executor_->Execute(req);
  ASSERT_TRUE(resp.success);

  const auto records = ReadWalRecords();
  ASSERT_FALSE(records.empty());
  EXPECT_EQ(records.back().type, WalRecordType::PREPARE);
  EXPECT_EQ(records.back().tx_id, 20u);
}

TEST_F(KvExecutorWalTest, Prepare_NoVote_NoPrepareRecord) {
  Task req;
  req.type = TaskType::TX_PREPARE_REQUEST;
  req.tx_id = 99;
  const auto resp = executor_->Execute(req);
  EXPECT_FALSE(resp.success);

  const auto records = ReadWalRecords();
  EXPECT_TRUE(records.empty());
}

TEST_F(KvExecutorWalTest, FinalizeCommit_WritesRecord) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("v"), 100), WriteIntentResult::OK);

  Task req;
  req.type = TaskType::TX_FINALIZE_COMMIT_REQUEST;
  req.tx_id = 100;
  req.commit_ts = 500;
  const auto resp = executor_->Execute(req);
  ASSERT_TRUE(resp.success);

  const auto records = ReadWalRecords();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].type, WalRecordType::COMMIT_FINALIZE);
  EXPECT_EQ(records[0].tx_id, 100u);
  EXPECT_EQ(records[0].commit_ts, 500u);
}

TEST_F(KvExecutorWalTest, FinalizeAbort_WritesRecord) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("v"), 200), WriteIntentResult::OK);

  Task req;
  req.type = TaskType::TX_FINALIZE_ABORT_REQUEST;
  req.tx_id = 200;
  const auto resp = executor_->Execute(req);
  ASSERT_TRUE(resp.success);

  const auto records = ReadWalRecords();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].type, WalRecordType::ABORT_FINALIZE);
  EXPECT_EQ(records[0].tx_id, 200u);
}

TEST_F(KvExecutorWalTest, NullWal_StillWorks) {
  KvExecutor null_executor(storage_, 0);

  Task set_req;
  set_req.type = TaskType::TX_EXECUTE_SET_REQUEST;
  set_req.key = "key";
  set_req.value = MakeValue("val");
  set_req.tx_id = 1;
  const auto set_resp = null_executor.Execute(set_req);
  EXPECT_TRUE(set_resp.success);

  Task prep_req;
  prep_req.type = TaskType::TX_PREPARE_REQUEST;
  prep_req.tx_id = 1;
  const auto prep_resp = null_executor.Execute(prep_req);
  EXPECT_TRUE(prep_resp.success);

  Task fin_req;
  fin_req.type = TaskType::TX_FINALIZE_COMMIT_REQUEST;
  fin_req.tx_id = 1;
  fin_req.commit_ts = 100;
  const auto fin_resp = null_executor.Execute(fin_req);
  EXPECT_TRUE(fin_resp.success);
}

TEST_F(KvExecutorWalTest, MultipleOps_CorrectWalOrder) {
  Task set1;
  set1.type = TaskType::TX_EXECUTE_SET_REQUEST;
  set1.key = "key1";
  set1.value = MakeValue("val1");
  set1.tx_id = 7;
  executor_->Execute(set1);

  Task set2;
  set2.type = TaskType::TX_EXECUTE_SET_REQUEST;
  set2.key = "key2";
  set2.value = MakeValue("val2");
  set2.tx_id = 7;
  executor_->Execute(set2);

  Task prep;
  prep.type = TaskType::TX_PREPARE_REQUEST;
  prep.tx_id = 7;
  const auto prep_resp = executor_->Execute(prep);
  ASSERT_TRUE(prep_resp.success);

  const auto records = ReadWalRecords();
  ASSERT_EQ(records.size(), 3u);
  EXPECT_EQ(records[0].type, WalRecordType::INTENT);
  EXPECT_EQ(records[0].key, "key1");
  EXPECT_EQ(records[1].type, WalRecordType::INTENT);
  EXPECT_EQ(records[1].key, "key2");
  EXPECT_EQ(records[2].type, WalRecordType::PREPARE);
  EXPECT_EQ(records[2].tx_id, 7u);
  EXPECT_TRUE(records[0].lsn < records[1].lsn);
  EXPECT_TRUE(records[1].lsn < records[2].lsn);
}

}  // namespace
}  // namespace db
