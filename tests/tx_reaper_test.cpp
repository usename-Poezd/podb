#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#define private public
#include "transaction/tx_coordinator.h"
#undef private

namespace db {
namespace {

using namespace std::chrono_literals;

class TxReaperTest : public ::testing::Test {
 protected:
  void SetUp() override {
    clock_ = std::make_unique<ManualClock>();
    router_ = std::make_unique<Router>(
        0, std::vector<Worker*>{nullptr},
        [this](Task task) { forwarded_.push_back(std::move(task)); });
    coordinator_ = std::make_unique<TxCoordinator>(
        *router_,
        [this](uint64_t rid, Task resp) {
          last_response_ = std::move(resp);
          last_rid_ = rid;
          response_count_++;
        },
        nullptr, clock_.get(), 5s, 3s);
  }

  uint64_t DoBegin() {
    Task task;
    task.type = TaskType::TX_BEGIN_REQUEST;
    task.request_id = next_rid_++;
    coordinator_->HandleControl(std::move(task));
    return last_response_.tx_id;
  }

  void SendHeartbeat(uint64_t tx_id) {
    Task task;
    task.type = TaskType::TX_HEARTBEAT_REQUEST;
    task.request_id = next_rid_++;
    task.tx_id = tx_id;
    coordinator_->HandleControl(std::move(task));
  }

  void SendExecute(TaskType type, uint64_t tx_id) {
    Task task;
    task.type = type;
    task.request_id = next_rid_++;
    task.tx_id = tx_id;
    task.key = "alpha";
    task.value = {static_cast<std::byte>('v')};
    task.reply_to_core = 0;
    coordinator_->HandleExecute(std::move(task));
  }

  void AddParticipant(uint64_t tx_id, int core_id = 0) {
    coordinator_->tx_table_[tx_id].participant_cores.insert(core_id);
  }

  void AckFinalizeAbort(uint64_t tx_id) {
    Task task;
    task.type = TaskType::TX_FINALIZE_ABORT_RESPONSE;
    task.tx_id = tx_id;
    coordinator_->HandleFinalizeResponse(std::move(task));
  }

  std::unique_ptr<ManualClock> clock_;
  std::unique_ptr<Router> router_;
  std::unique_ptr<TxCoordinator> coordinator_;
  Task last_response_;
  uint64_t last_rid_{0};
  int response_count_{0};
  uint64_t next_rid_{100};
  std::vector<Task> forwarded_;
};

TEST_F(TxReaperTest, ManualClock_Advance) {
  const auto start = clock_->Now();

  clock_->Advance(5s);

  EXPECT_EQ(clock_->Now() - start, 5s);
}

TEST_F(TxReaperTest, ManualClock_SetNow) {
  const Clock::TimePoint target = Clock::TimePoint{} + 42s;

  clock_->SetNow(target);

  EXPECT_EQ(clock_->Now(), target);
}

TEST_F(TxReaperTest, Begin_SetsWallClockTimes) {
  const Clock::TimePoint start = Clock::TimePoint{} + 10s;
  clock_->SetNow(start);
  const uint64_t tx_id = DoBegin();

  ASSERT_TRUE(coordinator_->tx_table_.contains(tx_id));
  EXPECT_EQ(coordinator_->tx_table_[tx_id].created_time, start);
  EXPECT_EQ(coordinator_->tx_table_[tx_id].last_heartbeat_time, start);
}

TEST_F(TxReaperTest, Heartbeat_ExtendsLease) {
  const uint64_t tx_id = DoBegin();
  const auto initial = coordinator_->tx_table_[tx_id].last_heartbeat_time;

  clock_->Advance(2s);
  SendHeartbeat(tx_id);

  EXPECT_GT(coordinator_->tx_table_[tx_id].last_heartbeat_time, initial);
  EXPECT_EQ(coordinator_->tx_table_[tx_id].last_heartbeat_time, clock_->Now());
}

TEST_F(TxReaperTest, Heartbeat_NoLongerIncrementsSnapshotTs) {
  const uint64_t tx_id = DoBegin();
  const uint64_t before = coordinator_->next_snapshot_ts_;

  SendHeartbeat(tx_id);

  EXPECT_EQ(coordinator_->next_snapshot_ts_, before);
}

TEST_F(TxReaperTest, Execute_ExtendsLease) {
  const uint64_t tx_id = DoBegin();
  const auto initial = coordinator_->tx_table_[tx_id].last_heartbeat_time;

  clock_->Advance(2s);
  SendExecute(TaskType::TX_EXECUTE_GET_REQUEST, tx_id);

  EXPECT_GT(coordinator_->tx_table_[tx_id].last_heartbeat_time, initial);
  EXPECT_EQ(coordinator_->tx_table_[tx_id].last_heartbeat_time, clock_->Now());
}

TEST_F(TxReaperTest, ReapActive_Expired_Aborts) {
  const uint64_t tx_id = DoBegin();

  clock_->Advance(6s);
  coordinator_->ReapStaleTransactions();

  ASSERT_TRUE(coordinator_->tx_table_.contains(tx_id));
  EXPECT_EQ(coordinator_->tx_table_[tx_id].state, TxState::ABORTED);
}

TEST_F(TxReaperTest, ReapActive_NotExpired_Survives) {
  const uint64_t tx_id = DoBegin();

  clock_->Advance(4s);
  coordinator_->ReapStaleTransactions();

  ASSERT_TRUE(coordinator_->tx_table_.contains(tx_id));
  EXPECT_EQ(coordinator_->tx_table_[tx_id].state, TxState::ACTIVE);
}

TEST_F(TxReaperTest, ReapActive_WithParticipants_SendsFinalize) {
  const uint64_t tx_id = DoBegin();
  AddParticipant(tx_id);

  clock_->Advance(6s);
  coordinator_->ReapStaleTransactions();

  ASSERT_EQ(forwarded_.size(), 1U);
  EXPECT_EQ(forwarded_.back().type, TaskType::TX_FINALIZE_ABORT_REQUEST);
  EXPECT_EQ(forwarded_.back().tx_id, tx_id);
  ASSERT_TRUE(coordinator_->pending_finalizes_.contains(tx_id));
  EXPECT_EQ(coordinator_->pending_finalizes_[tx_id].client_request_id,
            UINT64_MAX);
}

TEST_F(TxReaperTest, ReapSentinel_NoClientResponse) {
  const uint64_t tx_id = DoBegin();
  AddParticipant(tx_id);
  const int baseline_response_count = response_count_;

  clock_->Advance(6s);
  coordinator_->ReapStaleTransactions();
  AckFinalizeAbort(tx_id);

  EXPECT_EQ(response_count_, baseline_response_count);
  EXPECT_FALSE(coordinator_->pending_finalizes_.contains(tx_id));
}

TEST_F(TxReaperTest, GetMinActiveSnapshot_NoActive) {
  EXPECT_EQ(coordinator_->GetMinActiveSnapshot(), UINT64_MAX);
}

TEST_F(TxReaperTest, GetMinActiveSnapshot_MultipleActive) {
  const uint64_t tx1 = DoBegin();
  const uint64_t tx2 = DoBegin();
  const uint64_t tx3 = DoBegin();

  const uint64_t min_snapshot = coordinator_->tx_table_[tx1].snapshot_ts;

  ASSERT_TRUE(coordinator_->tx_table_.contains(tx2));
  ASSERT_TRUE(coordinator_->tx_table_.contains(tx3));
  EXPECT_EQ(coordinator_->GetMinActiveSnapshot(), min_snapshot);
}

}  // namespace
}  // namespace db
