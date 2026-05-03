#include <gtest/gtest.h>

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#define private public
#include "transaction/tx_coordinator.h"
#undef private

namespace db {
namespace {

// Тестовый фикстура для TxCoordinator.
// Router нужен для конструктора, но в unit-тестах Execute тесты
// не проверяют реальную маршрутизацию — проверяем только валидацию.
class TxCoordinatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Создаём Router с минимальными параметрами для теста.
    router_ = std::make_unique<Router>(0, std::vector<Worker*>{}, nullptr);

    coordinator_ = std::make_unique<TxCoordinator>(
        *router_, [this](uint64_t request_id, Task response) {
          last_response_ = std::move(response);
          last_response_request_id_ = request_id;
          response_count_++;
        });
  }

  [[nodiscard]] Task& LastResponse() { return last_response_; }
  [[nodiscard]] const Task& LastResponse() const { return last_response_; }
  [[nodiscard]] uint64_t LastResponseRequestId() const {
    return last_response_request_id_;
  }
  [[nodiscard]] int ResponseCount() const { return response_count_; }
  [[nodiscard]] TxCoordinator& Coordinator() { return *coordinator_; }

  // Хелпер: отправить Begin и вернуть tx_id из ответа.
  uint64_t DoBegin() {
    SendBeginWithRequestId(next_req_id_++);
    return last_response_.tx_id;
  }

  void SendBeginWithRequestId(uint64_t request_id) {
    Task task;
    task.type = TaskType::TX_BEGIN_REQUEST;
    task.request_id = request_id;
    coordinator_->HandleControl(std::move(task));
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

  void SendHeartbeat(uint64_t tx_id) {
    Task task;
    task.type = TaskType::TX_HEARTBEAT_REQUEST;
    task.request_id = next_req_id_++;
    task.tx_id = tx_id;
    coordinator_->HandleControl(std::move(task));
  }

  void SendExecute(TaskType type, uint64_t tx_id) {
    Task task;
    task.type = type;
    task.request_id = next_req_id_++;
    task.tx_id = tx_id;
    task.key = "alpha";
    task.value = {static_cast<std::byte>('v'), static_cast<std::byte>('1')};
    task.reply_to_core = 0;
    coordinator_->HandleExecute(std::move(task));
  }

 private:
  std::unique_ptr<Router> router_;  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
  std::unique_ptr<TxCoordinator> coordinator_;  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
  Task last_response_;  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
  uint64_t last_response_request_id_{0};  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
  int response_count_{0};  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
  uint64_t next_req_id_{100};  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
};

// Фикстура для проверки успешного forward через локальный callback Router.
class TxCoordinatorRoutingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    router_ = std::make_unique<Router>(
        0, std::vector<Worker*>{nullptr}, [this](Task task) {
          forwarded_task_ = std::move(task);
          forwarded_count_++;
        });

    coordinator_ = std::make_unique<TxCoordinator>(
        *router_, [this](uint64_t request_id, Task response) {
          last_response_ = std::move(response);
          last_response_request_id_ = request_id;
          response_count_++;
        });
  }

  [[nodiscard]] int ResponseCount() const { return response_count_; }
  [[nodiscard]] int ForwardedCount() const { return forwarded_count_; }
  [[nodiscard]] const Task& ForwardedTask() const { return forwarded_task_; }
  [[nodiscard]] const Task& LastResponse() const { return last_response_; }
  [[nodiscard]] TxCoordinator& Coordinator() { return *coordinator_; }
  [[nodiscard]] uint64_t LastResponseRequestId() const {
    return last_response_request_id_;
  }
  [[nodiscard]] uint64_t LastBeginSnapshotTs() const {
    return last_response_.snapshot_ts;
  }

  uint64_t DoBegin() {
    Task task;
    task.type = TaskType::TX_BEGIN_REQUEST;
    task.request_id = next_req_id_++;
    coordinator_->HandleControl(std::move(task));
    return last_response_.tx_id;
  }

  uint64_t SendCommit(uint64_t tx_id) {
    Task task;
    task.type = TaskType::TX_COMMIT_REQUEST;
    task.request_id = next_req_id_++;
    task.tx_id = tx_id;
    const uint64_t request_id = task.request_id;
    coordinator_->HandleControl(std::move(task));
    return request_id;
  }

  uint64_t SendRollback(uint64_t tx_id) {
    Task task;
    task.type = TaskType::TX_ROLLBACK_REQUEST;
    task.request_id = next_req_id_++;
    task.tx_id = tx_id;
    const uint64_t request_id = task.request_id;
    coordinator_->HandleControl(std::move(task));
    return request_id;
  }

  void SendExecuteSetToCoreSeven(uint64_t tx_id) {
    Task task;
    task.type = TaskType::TX_EXECUTE_SET_REQUEST;
    task.request_id = next_req_id_++;
    task.tx_id = tx_id;
    task.key = "alpha";
    task.value = {static_cast<std::byte>('v'), static_cast<std::byte>('1')};
    task.reply_to_core = 7;
    coordinator_->HandleExecute(std::move(task));
  }

  void HandleFinalizeResponse(TaskType type, uint64_t tx_id) {
    Task task;
    task.type = type;
    task.tx_id = tx_id;
    coordinator_->HandleFinalizeResponse(std::move(task));
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

 private:
  std::unique_ptr<Router> router_;  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
  std::unique_ptr<TxCoordinator> coordinator_;  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
  Task last_response_;  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
  Task forwarded_task_;  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
  uint64_t last_response_request_id_{0};  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
  int response_count_{0};  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
  int forwarded_count_{0};  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
  uint64_t next_req_id_{100};  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(TxCoordinatorTest, BeginTx_ReturnsUniqueTxId) {
  const uint64_t first_tx_id = DoBegin();
  const uint64_t second_tx_id = DoBegin();

  EXPECT_GT(first_tx_id, 0U);
  EXPECT_GT(second_tx_id, 0U);
  EXPECT_NE(first_tx_id, second_tx_id);
}

TEST_F(TxCoordinatorTest, BeginTx_StateIsActive) {
  SendBeginWithRequestId(100);

  EXPECT_EQ(LastResponseRequestId(), 100U);
  EXPECT_EQ(LastResponse().type, TaskType::TX_BEGIN_RESPONSE);
  EXPECT_TRUE(LastResponse().success);
  EXPECT_GT(LastResponse().tx_id, 0U);
  EXPECT_GT(LastResponse().snapshot_ts, 0U);
  EXPECT_TRUE(LastResponse().error_message.empty());
}

TEST_F(TxCoordinatorTest, CommitActiveTx_Succeeds) {
  const uint64_t tx_id = DoBegin();

  SendCommit(tx_id);

  EXPECT_EQ(LastResponse().type, TaskType::TX_COMMIT_RESPONSE);
  EXPECT_TRUE(LastResponse().success);
  EXPECT_EQ(LastResponse().tx_id, tx_id);
  EXPECT_TRUE(LastResponse().error_message.empty());
}

TEST_F(TxCoordinatorTest, RollbackActiveTx_Succeeds) {
  const uint64_t tx_id = DoBegin();

  SendRollback(tx_id);

  EXPECT_EQ(LastResponse().type, TaskType::TX_ROLLBACK_RESPONSE);
  EXPECT_TRUE(LastResponse().success);
  EXPECT_EQ(LastResponse().tx_id, tx_id);
  EXPECT_TRUE(LastResponse().error_message.empty());
}

TEST_F(TxCoordinatorTest, CommitCommittedTx_Fails) {
  const uint64_t tx_id = DoBegin();
  SendCommit(tx_id);

  SendCommit(tx_id);

  EXPECT_EQ(LastResponse().type, TaskType::TX_COMMIT_RESPONSE);
  EXPECT_FALSE(LastResponse().success);
  EXPECT_FALSE(LastResponse().error_message.empty());
  EXPECT_NE(LastResponse().error_message.find("not_active"), std::string::npos);
}

TEST_F(TxCoordinatorTest, RollbackCommittedTx_Fails) {
  const uint64_t tx_id = DoBegin();
  SendCommit(tx_id);

  SendRollback(tx_id);

  EXPECT_EQ(LastResponse().type, TaskType::TX_ROLLBACK_RESPONSE);
  EXPECT_FALSE(LastResponse().success);
  EXPECT_NE(LastResponse().error_message.find("not_active"), std::string::npos);
}

TEST_F(TxCoordinatorTest, CommitAbortedTx_Fails) {
  const uint64_t tx_id = DoBegin();
  SendRollback(tx_id);

  SendCommit(tx_id);

  EXPECT_EQ(LastResponse().type, TaskType::TX_COMMIT_RESPONSE);
  EXPECT_FALSE(LastResponse().success);
  EXPECT_NE(LastResponse().error_message.find("not_active"), std::string::npos);
}

TEST_F(TxCoordinatorTest, CommitUnknownTx_Fails) {
  SendCommit(999);

  EXPECT_EQ(LastResponse().type, TaskType::TX_COMMIT_RESPONSE);
  EXPECT_FALSE(LastResponse().success);
  EXPECT_NE(LastResponse().error_message.find("not_found"), std::string::npos);
}

TEST_F(TxCoordinatorRoutingTest, ExecuteOnActiveTx_Forwards) {
  const uint64_t tx_id = DoBegin();
  const uint64_t snapshot_ts = LastBeginSnapshotTs();
  const int baseline_response_count = ResponseCount();

  SendExecuteSetToCoreSeven(tx_id);

  EXPECT_EQ(ResponseCount(), baseline_response_count);
  EXPECT_EQ(ForwardedCount(), 1);
  EXPECT_EQ(ForwardedTask().type, TaskType::TX_EXECUTE_SET_REQUEST);
  EXPECT_EQ(ForwardedTask().request_id, 101U);
  EXPECT_EQ(ForwardedTask().tx_id, tx_id);
  EXPECT_EQ(ForwardedTask().snapshot_ts, snapshot_ts);
  EXPECT_EQ(ForwardedTask().key, "alpha");
  EXPECT_EQ(ForwardedTask().value,
            BinaryValue({static_cast<std::byte>('v'),
                         static_cast<std::byte>('1')}));
  EXPECT_EQ(ForwardedTask().reply_to_core, 7);
}

TEST_F(TxCoordinatorTest, CommitNoParticipants_ImmediateResponse) {
  const uint64_t tx_id = DoBegin();
  const int baseline_response_count = ResponseCount();

  SendCommit(tx_id);

  EXPECT_EQ(ResponseCount(), baseline_response_count + 1);
  EXPECT_EQ(LastResponse().type, TaskType::TX_COMMIT_RESPONSE);
  EXPECT_TRUE(LastResponse().success);
  EXPECT_EQ(LastResponse().tx_id, tx_id);
}

TEST_F(TxCoordinatorRoutingTest,
       CommitWithParticipants_SendsPrepareThenFinalizeAndResponds) {
  const uint64_t tx_id = DoBegin();
  SendExecuteSetToCoreSeven(tx_id);
  const int baseline_response_count = ResponseCount();

  const uint64_t commit_request_id = SendCommit(tx_id);

  EXPECT_EQ(ResponseCount(), baseline_response_count);
  EXPECT_EQ(ForwardedCount(), 2);
  EXPECT_EQ(ForwardedTask().type, TaskType::TX_PREPARE_REQUEST);
  EXPECT_EQ(ForwardedTask().tx_id, tx_id);
  EXPECT_EQ(ForwardedTask().snapshot_ts, LastBeginSnapshotTs());
  EXPECT_EQ(ForwardedTask().reply_to_core, 0);

  HandlePrepareResponse(tx_id, true);

  EXPECT_EQ(ForwardedCount(), 3);
  EXPECT_EQ(ForwardedTask().type, TaskType::TX_FINALIZE_COMMIT_REQUEST);
  EXPECT_EQ(ForwardedTask().tx_id, tx_id);
  EXPECT_GT(ForwardedTask().commit_ts, 0U);
  EXPECT_EQ(ForwardedTask().reply_to_core, 0);

  HandleFinalizeResponse(TaskType::TX_FINALIZE_COMMIT_RESPONSE, tx_id);

  EXPECT_EQ(ResponseCount(), baseline_response_count + 1);
  EXPECT_EQ(LastResponse().type, TaskType::TX_COMMIT_RESPONSE);
  EXPECT_TRUE(LastResponse().success);
  EXPECT_EQ(LastResponse().tx_id, tx_id);
  EXPECT_EQ(LastResponseRequestId(), commit_request_id);
}

TEST_F(TxCoordinatorRoutingTest, RollbackWithParticipants_SendsAbortFinalize) {
  const uint64_t tx_id = DoBegin();
  SendExecuteSetToCoreSeven(tx_id);
  const int baseline_response_count = ResponseCount();

  const uint64_t rollback_request_id = SendRollback(tx_id);

  EXPECT_EQ(ResponseCount(), baseline_response_count);
  EXPECT_EQ(ForwardedCount(), 2);
  EXPECT_EQ(ForwardedTask().type, TaskType::TX_FINALIZE_ABORT_REQUEST);
  EXPECT_EQ(ForwardedTask().tx_id, tx_id);
  EXPECT_EQ(ForwardedTask().reply_to_core, 0);

  HandleFinalizeResponse(TaskType::TX_EXECUTE_RESPONSE, tx_id);

  EXPECT_EQ(ResponseCount(), baseline_response_count + 1);
  EXPECT_EQ(LastResponse().type, TaskType::TX_ROLLBACK_RESPONSE);
  EXPECT_TRUE(LastResponse().success);
  EXPECT_EQ(LastResponse().tx_id, tx_id);
  EXPECT_EQ(LastResponseRequestId(), rollback_request_id);
}

TEST_F(TxCoordinatorRoutingTest, CommitWithParticipants_SendsPrepare) {
  const uint64_t tx_id = DoBegin();
  SendExecuteSetToCoreSeven(tx_id);
  const int baseline_response_count = ResponseCount();

  SendCommit(tx_id);

  EXPECT_EQ(ResponseCount(), baseline_response_count);
  EXPECT_EQ(ForwardedCount(), 2);
  EXPECT_EQ(ForwardedTask().type, TaskType::TX_PREPARE_REQUEST);
  EXPECT_EQ(ForwardedTask().tx_id, tx_id);
  EXPECT_EQ(ForwardedTask().snapshot_ts, LastBeginSnapshotTs());
  EXPECT_EQ(ForwardedTask().reply_to_core, 0);
  EXPECT_TRUE(Coordinator().pending_prepares_.contains(tx_id));
  EXPECT_EQ(Coordinator().tx_table_[tx_id].state, TxState::PREPARING);
}

TEST_F(TxCoordinatorRoutingTest, PrepareAllYes_SendsFinalizeCommit) {
  const uint64_t tx_id = DoBegin();
  SendExecuteSetToCoreSeven(tx_id);
  SendCommit(tx_id);

  HandlePrepareResponse(tx_id, true);

  EXPECT_EQ(ForwardedCount(), 3);
  EXPECT_EQ(ForwardedTask().type, TaskType::TX_FINALIZE_COMMIT_REQUEST);
  EXPECT_EQ(ForwardedTask().tx_id, tx_id);
  EXPECT_GT(ForwardedTask().commit_ts, 0U);
  EXPECT_EQ(ForwardedTask().reply_to_core, 0);
  EXPECT_FALSE(Coordinator().pending_prepares_.contains(tx_id));
  EXPECT_TRUE(Coordinator().pending_finalizes_.contains(tx_id));
  EXPECT_EQ(Coordinator().tx_table_[tx_id].state, TxState::COMMITTED);
}

TEST_F(TxCoordinatorRoutingTest, PrepareAnyNo_SendsFinalizeAbort) {
  const uint64_t tx_id = DoBegin();
  SendExecuteSetToCoreSeven(tx_id);
  SendCommit(tx_id);

  HandlePrepareResponse(tx_id, false, "write_conflict");

  EXPECT_EQ(ForwardedCount(), 3);
  EXPECT_EQ(ForwardedTask().type, TaskType::TX_FINALIZE_ABORT_REQUEST);
  EXPECT_EQ(ForwardedTask().tx_id, tx_id);
  EXPECT_EQ(ForwardedTask().reply_to_core, 0);
  ASSERT_TRUE(Coordinator().pending_finalizes_.contains(tx_id));
  EXPECT_EQ(Coordinator().pending_finalizes_[tx_id].client_response_type,
            TaskType::TX_COMMIT_RESPONSE);
  EXPECT_EQ(Coordinator().pending_finalizes_[tx_id].error_message,
            "write_conflict");
  EXPECT_EQ(Coordinator().tx_table_[tx_id].state, TxState::ABORTED);
}

TEST_F(TxCoordinatorRoutingTest,
       PrepareAllYes_ThenFinalize_CommitResponseSucceeds) {
  const uint64_t tx_id = DoBegin();
  SendExecuteSetToCoreSeven(tx_id);
  const int baseline_response_count = ResponseCount();
  const uint64_t commit_request_id = SendCommit(tx_id);

  HandlePrepareResponse(tx_id, true);
  HandleFinalizeResponse(TaskType::TX_FINALIZE_COMMIT_RESPONSE, tx_id);

  EXPECT_EQ(ResponseCount(), baseline_response_count + 1);
  EXPECT_EQ(LastResponseRequestId(), commit_request_id);
  EXPECT_EQ(LastResponse().type, TaskType::TX_COMMIT_RESPONSE);
  EXPECT_TRUE(LastResponse().success);
  EXPECT_TRUE(LastResponse().error_message.empty());
}

TEST_F(TxCoordinatorRoutingTest,
       PrepareAnyNo_ThenFinalize_CommitResponseFails) {
  const uint64_t tx_id = DoBegin();
  SendExecuteSetToCoreSeven(tx_id);
  const int baseline_response_count = ResponseCount();
  const uint64_t commit_request_id = SendCommit(tx_id);

  HandlePrepareResponse(tx_id, false, "prepare_rejected:write_conflict");
  HandleFinalizeResponse(TaskType::TX_FINALIZE_ABORT_RESPONSE, tx_id);

  EXPECT_EQ(ResponseCount(), baseline_response_count + 1);
  EXPECT_EQ(LastResponseRequestId(), commit_request_id);
  EXPECT_EQ(LastResponse().type, TaskType::TX_COMMIT_RESPONSE);
  EXPECT_FALSE(LastResponse().success);
  EXPECT_EQ(LastResponse().error_message, "prepare_rejected:write_conflict");
}

TEST_F(TxCoordinatorTest, PrepareResponseForUnknownTx_Ignored) {
  const int baseline_response_count = ResponseCount();

  Task task;
  task.type = TaskType::TX_PREPARE_RESPONSE;
  task.tx_id = 404;
  task.success = false;
  task.error_message = "write_conflict";
  Coordinator().HandlePrepareResponse(std::move(task));

  EXPECT_EQ(ResponseCount(), baseline_response_count);
  EXPECT_TRUE(Coordinator().pending_prepares_.empty());
}

TEST_F(TxCoordinatorTest, ExecuteDuringPreparing_Fails) {
  const uint64_t tx_id = DoBegin();
  Coordinator().tx_table_[tx_id].state = TxState::PREPARING;

  SendExecute(TaskType::TX_EXECUTE_SET_REQUEST, tx_id);

  EXPECT_EQ(LastResponse().type, TaskType::TX_EXECUTE_RESPONSE);
  EXPECT_FALSE(LastResponse().success);
  EXPECT_NE(LastResponse().error_message.find("not_active"), std::string::npos);
}

TEST_F(TxCoordinatorTest, CommitDuringPreparing_Fails) {
  const uint64_t tx_id = DoBegin();
  Coordinator().tx_table_[tx_id].state = TxState::PREPARING;

  SendCommit(tx_id);

  EXPECT_EQ(LastResponse().type, TaskType::TX_COMMIT_RESPONSE);
  EXPECT_FALSE(LastResponse().success);
  EXPECT_NE(LastResponse().error_message.find("not_active"), std::string::npos);
}

TEST_F(TxCoordinatorTest, RollbackDuringPreparing_Fails) {
  const uint64_t tx_id = DoBegin();
  Coordinator().tx_table_[tx_id].state = TxState::PREPARING;

  SendRollback(tx_id);

  EXPECT_EQ(LastResponse().type, TaskType::TX_ROLLBACK_RESPONSE);
  EXPECT_FALSE(LastResponse().success);
  EXPECT_NE(LastResponse().error_message.find("not_active"), std::string::npos);
}

TEST_F(TxCoordinatorTest, HandleFinalizeResponse_CompletesCommit) {
  Coordinator().pending_finalizes_[77] = TxCoordinator::PendingFinalize{
      .client_request_id = 555,
      .tx_id = 77,
      .remaining = 2,
      .is_commit = true,
      .client_response_type = TaskType::TX_COMMIT_RESPONSE,
  };

  const int baseline_response_count = ResponseCount();

  Task first_ack;
  first_ack.type = TaskType::TX_FINALIZE_COMMIT_RESPONSE;
  first_ack.tx_id = 77;
  Coordinator().HandleFinalizeResponse(std::move(first_ack));

  EXPECT_EQ(ResponseCount(), baseline_response_count);
  ASSERT_TRUE(Coordinator().pending_finalizes_.contains(77));
  EXPECT_EQ(Coordinator().pending_finalizes_[77].remaining, 1);

  Task second_ack;
  second_ack.type = TaskType::TX_FINALIZE_COMMIT_RESPONSE;
  second_ack.tx_id = 77;
  Coordinator().HandleFinalizeResponse(std::move(second_ack));

  EXPECT_EQ(ResponseCount(), baseline_response_count + 1);
  EXPECT_EQ(LastResponseRequestId(), 555U);
  EXPECT_EQ(LastResponse().type, TaskType::TX_COMMIT_RESPONSE);
  EXPECT_TRUE(LastResponse().success);
  EXPECT_EQ(LastResponse().tx_id, 77U);
  EXPECT_FALSE(Coordinator().pending_finalizes_.contains(77));
}

TEST_F(TxCoordinatorTest,
       HandleFinalizeResponse_FailedCommitReturnsCommitError) {
  Coordinator().pending_finalizes_[88] = TxCoordinator::PendingFinalize{
      .client_request_id = 777,
      .tx_id = 88,
      .remaining = 1,
      .is_commit = false,
      .client_response_type = TaskType::TX_COMMIT_RESPONSE,
      .error_message = "prepare_rejected:constraint_violation",
  };

  Task ack;
  ack.type = TaskType::TX_FINALIZE_ABORT_RESPONSE;
  ack.tx_id = 88;
  Coordinator().HandleFinalizeResponse(std::move(ack));

  EXPECT_EQ(LastResponseRequestId(), 777U);
  EXPECT_EQ(LastResponse().type, TaskType::TX_COMMIT_RESPONSE);
  EXPECT_FALSE(LastResponse().success);
  EXPECT_EQ(LastResponse().error_message,
            "prepare_rejected:constraint_violation");
  EXPECT_FALSE(Coordinator().pending_finalizes_.contains(88));
}

TEST_F(TxCoordinatorTest, ExecuteOnCommittedTx_Fails) {
  const uint64_t tx_id = DoBegin();
  SendCommit(tx_id);

  SendExecute(TaskType::TX_EXECUTE_SET_REQUEST, tx_id);

  EXPECT_EQ(LastResponse().type, TaskType::TX_EXECUTE_RESPONSE);
  EXPECT_FALSE(LastResponse().success);
  EXPECT_NE(LastResponse().error_message.find("not_active"), std::string::npos);
}

TEST_F(TxCoordinatorTest, HeartbeatUpdatesTimestamp) {
  const uint64_t tx_id = DoBegin();

  SendHeartbeat(tx_id);

  EXPECT_EQ(LastResponse().type, TaskType::TX_HEARTBEAT_RESPONSE);
  EXPECT_TRUE(LastResponse().success);
  EXPECT_EQ(LastResponse().tx_id, tx_id);
  EXPECT_TRUE(LastResponse().error_message.empty());
}

TEST_F(TxCoordinatorTest, SnapshotTsMonotonicallyIncreases) {
  DoBegin();
  const uint64_t snapshot_ts_1 = LastResponse().snapshot_ts;

  DoBegin();
  const uint64_t snapshot_ts_2 = LastResponse().snapshot_ts;

  DoBegin();
  const uint64_t snapshot_ts_3 = LastResponse().snapshot_ts;

  EXPECT_LT(snapshot_ts_1, snapshot_ts_2);
  EXPECT_LT(snapshot_ts_2, snapshot_ts_3);
}

TEST_F(TxCoordinatorTest, RollbackUnknownTx_Fails) {
  SendRollback(404);

  EXPECT_EQ(LastResponse().type, TaskType::TX_ROLLBACK_RESPONSE);
  EXPECT_FALSE(LastResponse().success);
  EXPECT_NE(LastResponse().error_message.find("not_found"), std::string::npos);
}

TEST_F(TxCoordinatorTest, HeartbeatUnknownTx_Fails) {
  SendHeartbeat(404);

  EXPECT_EQ(LastResponse().type, TaskType::TX_HEARTBEAT_RESPONSE);
  EXPECT_FALSE(LastResponse().success);
  EXPECT_NE(LastResponse().error_message.find("not_found"), std::string::npos);
}

}  // namespace
}  // namespace db
