#include <gtest/gtest.h>

#include <cstddef>
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#define private public
#include "transaction/tx_coordinator.h"
#undef private
#include "wal/wal_reader.h"
#include "wal/wal_record.h"
#include "wal/wal_writer.h"

namespace db {
namespace {

class TxCoordinatorWalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() /
                ("podb_txwal_test_" + std::to_string(::getpid()));
    std::filesystem::create_directories(test_dir_);
    wal_path_ = (test_dir_ / "core_0.wal").string();
    wal_writer_ = std::make_unique<WalWriter>(wal_path_);

    router_ = std::make_unique<Router>(
        0, std::vector<Worker*>{nullptr}, [this](Task task) {
          forwarded_tasks_.push_back(std::move(task));
        });

    coordinator_ = std::make_unique<TxCoordinator>(
        *router_,
        [this](uint64_t request_id, Task response) {
          last_response_ = std::move(response);
          last_response_request_id_ = request_id;
          response_count_++;
        },
        wal_writer_.get());
  }

  void TearDown() override {
    coordinator_.reset();
    wal_writer_.reset();
    router_.reset();
    std::filesystem::remove_all(test_dir_);
  }

  std::vector<WalRecord> ReadWalRecords() {
    wal_writer_.reset();
    WalReader reader(wal_path_);
    return reader.ReadAll();
  }

  uint64_t DoBegin() {
    Task task;
    task.type = TaskType::TX_BEGIN_REQUEST;
    task.request_id = next_req_id_++;
    coordinator_->HandleControl(std::move(task));
    return last_response_.tx_id;
  }

  void SendCommit(uint64_t tx_id) {
    Task task;
    task.type = TaskType::TX_COMMIT_REQUEST;
    task.request_id = next_req_id_++;
    task.tx_id = tx_id;
    coordinator_->HandleControl(std::move(task));
  }

  void SendRollback(uint64_t tx_id) {
    Task task;
    task.type = TaskType::TX_ROLLBACK_REQUEST;
    task.request_id = next_req_id_++;
    task.tx_id = tx_id;
    coordinator_->HandleControl(std::move(task));
  }

  void HandlePrepareResponse(uint64_t tx_id, bool success,
                              const std::string& error = "") {
    Task task;
    task.type = TaskType::TX_PREPARE_RESPONSE;
    task.tx_id = tx_id;
    task.success = success;
    task.error_message = error;
    coordinator_->HandlePrepareResponse(std::move(task));
  }

  std::filesystem::path test_dir_;
  std::string wal_path_;
  std::unique_ptr<WalWriter> wal_writer_;
  std::unique_ptr<Router> router_;
  std::unique_ptr<TxCoordinator> coordinator_;
  Task last_response_;                               // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
  uint64_t last_response_request_id_{0};             // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
  int response_count_{0};                            // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
  uint64_t next_req_id_{100};                        // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
  std::vector<Task> forwarded_tasks_;                // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(TxCoordinatorWalTest, Begin_WritesTxBeginRecord) {
  const uint64_t tx_id = DoBegin();

  const auto records = ReadWalRecords();

  ASSERT_EQ(records.size(), 1U);
  EXPECT_EQ(records[0].type, WalRecordType::TX_BEGIN);
  EXPECT_EQ(records[0].tx_id, tx_id);
  EXPECT_GT(records[0].snapshot_ts, 0U);
}

TEST_F(TxCoordinatorWalTest, CommitDecision_WritesRecord_Syncs) {
  const uint64_t tx_id = DoBegin();
  coordinator_->tx_table_[tx_id].participant_cores.insert(0);
  SendCommit(tx_id);

  HandlePrepareResponse(tx_id, true);

  const auto records = ReadWalRecords();

  const auto it = std::find_if(records.begin(), records.end(), [](const WalRecord& r) {
    return r.type == WalRecordType::COMMIT_DECISION;
  });
  ASSERT_NE(it, records.end());
  EXPECT_EQ(it->tx_id, tx_id);
  EXPECT_GT(it->commit_ts, 0U);
}

TEST_F(TxCoordinatorWalTest, AbortDecision_WritesRecord) {
  const uint64_t tx_id = DoBegin();
  coordinator_->tx_table_[tx_id].participant_cores.insert(0);
  SendCommit(tx_id);

  HandlePrepareResponse(tx_id, false, "write_conflict");

  const auto records = ReadWalRecords();

  const auto it = std::find_if(records.begin(), records.end(), [](const WalRecord& r) {
    return r.type == WalRecordType::ABORT_DECISION;
  });
  ASSERT_NE(it, records.end());
  EXPECT_EQ(it->tx_id, tx_id);
}

TEST_F(TxCoordinatorWalTest, Rollback_WritesAbortDecision) {
  const uint64_t tx_id = DoBegin();
  coordinator_->tx_table_[tx_id].participant_cores.insert(0);

  SendRollback(tx_id);

  const auto records = ReadWalRecords();

  const auto it = std::find_if(records.begin(), records.end(), [](const WalRecord& r) {
    return r.type == WalRecordType::ABORT_DECISION;
  });
  ASSERT_NE(it, records.end());
  EXPECT_EQ(it->tx_id, tx_id);
}

TEST_F(TxCoordinatorWalTest, NullWal_StillWorks) {
  auto null_router = std::make_unique<Router>(
      0, std::vector<Worker*>{nullptr}, [](Task) {});
  TxCoordinator null_coord(
      *null_router,
      [](uint64_t, Task) {},
      nullptr);

  Task task;
  task.type = TaskType::TX_BEGIN_REQUEST;
  task.request_id = 1;
  EXPECT_NO_THROW(null_coord.HandleControl(std::move(task)));
}

TEST_F(TxCoordinatorWalTest, CommitDecision_SyncBeforeFinalize) {
  const uint64_t tx_id = DoBegin();
  coordinator_->tx_table_[tx_id].participant_cores.insert(0);
  SendCommit(tx_id);

  HandlePrepareResponse(tx_id, true);

  const auto records = ReadWalRecords();

  bool found_commit_decision = false;
  for (const auto& rec : records) {
    if (rec.type == WalRecordType::COMMIT_DECISION && rec.tx_id == tx_id) {
      found_commit_decision = true;
      EXPECT_GT(rec.commit_ts, 0U);
    }
  }
  EXPECT_TRUE(found_commit_decision);
}

TEST_F(TxCoordinatorWalTest, LoadRecoveredState_PopulatesTxTable) {
  std::unordered_map<uint64_t, TxRecord> recovered;
  recovered[10] = TxRecord{
      .tx_id = 10,
      .snapshot_ts = 5,
      .state = TxState::COMMITTED,
      .participant_cores = {0},
  };
  recovered[11] = TxRecord{
      .tx_id = 11,
      .snapshot_ts = 6,
      .state = TxState::ABORTED,
      .participant_cores = {},
  };

  coordinator_->LoadRecoveredState(std::move(recovered), 50, 100);

  ASSERT_TRUE(coordinator_->tx_table_.contains(10));
  ASSERT_TRUE(coordinator_->tx_table_.contains(11));
  EXPECT_EQ(coordinator_->tx_table_[10].state, TxState::COMMITTED);
  EXPECT_EQ(coordinator_->tx_table_[11].state, TxState::ABORTED);
  EXPECT_EQ(coordinator_->next_tx_id_, 50U);
  EXPECT_EQ(coordinator_->next_snapshot_ts_, 100U);
}

TEST_F(TxCoordinatorWalTest, ResolveInDoubt_CommittedTx_SendsFinalize) {
  std::unordered_map<uint64_t, TxRecord> recovered;
  recovered[20] = TxRecord{
      .tx_id = 20,
      .snapshot_ts = 42,
      .state = TxState::COMMITTED,
      .participant_cores = {0},
  };

  coordinator_->LoadRecoveredState(std::move(recovered), 100, 200);
  coordinator_->ResolveInDoubt();

  ASSERT_EQ(forwarded_tasks_.size(), 1U);
  EXPECT_EQ(forwarded_tasks_[0].type, TaskType::TX_FINALIZE_COMMIT_REQUEST);
  EXPECT_EQ(forwarded_tasks_[0].tx_id, 20U);
  EXPECT_EQ(forwarded_tasks_[0].reply_to_core, 0);
}

}  // namespace
}  // namespace db
