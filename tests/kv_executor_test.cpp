#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <string>

#include "core/backoff.h"
#include "core/task.h"
#include "execution/kv_executor.h"
#include "storage/storage_engine.h"

namespace db {
namespace {

class KvExecutorTest : public ::testing::Test {
protected:
  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  StorageEngine storage_;
  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  KvExecutor executor_{storage_, 0};

  static BinaryValue MakeValue(const std::string &s) {
    BinaryValue v(s.size());
    std::memcpy(v.data(), s.data(), s.size());
    return v;
  }

  static std::string ValueToStr(const BinaryValue &v) {
    return {reinterpret_cast<const char *>(v.data()), v.size()};
  }
};

TEST_F(KvExecutorTest, NonTxGetSet_StillWorks) {
  Task set_req;
  set_req.type = TaskType::SET_REQUEST;
  set_req.key = "k";
  set_req.value = MakeValue("hello");
  set_req.reply_to_core = 0;

  const auto set_resp = executor_.Execute(std::move(set_req));

  EXPECT_EQ(set_resp.type, TaskType::SET_RESPONSE);
  EXPECT_TRUE(set_resp.success);

  Task get_req;
  get_req.type = TaskType::GET_REQUEST;
  get_req.key = "k";
  get_req.reply_to_core = 0;

  const auto get_resp = executor_.Execute(std::move(get_req));

  EXPECT_EQ(get_resp.type, TaskType::GET_RESPONSE);
  EXPECT_TRUE(get_resp.found);
  EXPECT_EQ(ValueToStr(get_resp.value), "hello");
}

TEST_F(KvExecutorTest, TxExecuteSet_CreatesIntent) {
  Task req;
  req.type = TaskType::TX_EXECUTE_SET_REQUEST;
  req.key = "k";
  req.value = MakeValue("intent_val");
  req.tx_id = 1;
  req.reply_to_core = 0;

  const auto resp = executor_.Execute(std::move(req));

  EXPECT_EQ(resp.type, TaskType::TX_EXECUTE_RESPONSE);
  EXPECT_TRUE(resp.success);

  const auto mvcc_result = storage_.MvccGet("k", 100, 1);
  ASSERT_TRUE(mvcc_result.found);
  EXPECT_EQ(ValueToStr(mvcc_result.value), "intent_val");
}

TEST_F(KvExecutorTest, TxExecuteGet_ReadsIntent) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("from_intent"), 2), WriteIntentResult::OK);

  Task req;
  req.type = TaskType::TX_EXECUTE_GET_REQUEST;
  req.key = "k";
  req.tx_id = 2;
  req.snapshot_ts = 100;
  req.reply_to_core = 0;

  const auto resp = executor_.Execute(std::move(req));

  EXPECT_EQ(resp.type, TaskType::TX_EXECUTE_RESPONSE);
  EXPECT_TRUE(resp.found);
  EXPECT_TRUE(resp.success);
  EXPECT_EQ(ValueToStr(resp.value), "from_intent");
}

TEST_F(KvExecutorTest, TxExecuteGet_SnapshotIsolation) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("v"), 1), WriteIntentResult::OK);
  storage_.CommitTransaction(1, 50);

  Task req;
  req.type = TaskType::TX_EXECUTE_GET_REQUEST;
  req.key = "k";
  req.tx_id = 2;
  req.snapshot_ts = 30;
  req.reply_to_core = 0;

  const auto resp = executor_.Execute(std::move(req));

  EXPECT_EQ(resp.type, TaskType::TX_EXECUTE_RESPONSE);
  EXPECT_FALSE(resp.found);
  EXPECT_TRUE(resp.success);
}

TEST_F(KvExecutorTest, TxExecuteSet_Conflict) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("tx1"), 1), WriteIntentResult::OK);

  Task req;
  req.type = TaskType::TX_EXECUTE_SET_REQUEST;
  req.key = "k";
  req.value = MakeValue("tx2");
  req.tx_id = 2;
  req.priority = kTxPriorityNormal;
  req.reply_to_core = 0;

  const auto resp = executor_.Execute(std::move(req));

  EXPECT_EQ(resp.type, TaskType::TX_EXECUTE_RESPONSE);
  EXPECT_FALSE(resp.success);
  EXPECT_EQ(resp.error_message, "write_write_conflict");
  EXPECT_GT(resp.retry_after_ms, 0U);
}

TEST_F(KvExecutorTest, TxExecuteSet_Success_RetryAfterMsIsZero) {
  Task req;
  req.type = TaskType::TX_EXECUTE_SET_REQUEST;
  req.key = "k";
  req.value = MakeValue("v");
  req.tx_id = 1;
  req.priority = kTxPriorityNormal;
  req.reply_to_core = 0;

  const auto resp = executor_.Execute(std::move(req));

  EXPECT_TRUE(resp.success);
  EXPECT_EQ(resp.retry_after_ms, 0U);
}

TEST_F(KvExecutorTest, TxExecuteSet_RepeatedConflicts_BackoffUpperBoundGrows) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("holder"), 1), WriteIntentResult::OK);

  auto conflict_backoff = [this]() {
    Task req;
    req.type = TaskType::TX_EXECUTE_SET_REQUEST;
    req.key = "k";
    req.value = MakeValue("tx2");
    req.tx_id = 2;
    req.priority = kTxPriorityNormal;
    req.reply_to_core = 0;
    return executor_.Execute(std::move(req)).retry_after_ms;
  };

  // Первый конфликт (streak=1) задаёт верхнюю границу диапазона backoff.
  const uint32_t first_backoff = conflict_backoff();

  // Каждый следующий конфликт для той же tx увеличивает streak, а значит и
  // диапазон backoff — минимум диапазона streak=2 уже равен максимуму
  // диапазона streak=1, поэтому неравенство ниже гарантировано, а не
  // вероятностно.
  uint32_t max_later_backoff = 0;
  for (int i = 0; i < 10; ++i) {
    max_later_backoff = std::max(max_later_backoff, conflict_backoff());
  }

  EXPECT_GE(max_later_backoff, first_backoff);
}

TEST_F(KvExecutorTest, TxExecuteSet_HighPriority_GetsSmallerOrEqualBackoff) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("holder"), 1), WriteIntentResult::OK);

  auto max_backoff_for = [this](uint64_t tx_id, uint32_t priority, int attempts) {
    uint32_t max_seen = 0;
    for (int i = 0; i < attempts; ++i) {
      Task req;
      req.type = TaskType::TX_EXECUTE_SET_REQUEST;
      req.key = "k";
      req.value = MakeValue("v");
      req.tx_id = tx_id;
      req.priority = priority;
      req.reply_to_core = 0;
      const auto resp = executor_.Execute(std::move(req));
      max_seen = std::max(max_seen, resp.retry_after_ms);
    }
    return max_seen;
  };

  const uint32_t normal_max = max_backoff_for(2, kTxPriorityNormal, 300);
  const uint32_t high_max = max_backoff_for(3, kTxPriorityHigh, 300);

  EXPECT_LE(high_max, normal_max);
}

TEST_F(KvExecutorTest, FinalizeCommit_PromotesIntents) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("committed_val"), 5), WriteIntentResult::OK);

  Task req;
  req.type = TaskType::TX_FINALIZE_COMMIT_REQUEST;
  req.tx_id = 5;
  req.commit_ts = 100;
  req.reply_to_core = 0;

  const auto resp = executor_.Execute(std::move(req));

  EXPECT_EQ(resp.type, TaskType::TX_FINALIZE_COMMIT_RESPONSE);
  EXPECT_TRUE(resp.success);

  const auto result = storage_.MvccGet("k", 100, 99);
  ASSERT_TRUE(result.found);
  EXPECT_EQ(ValueToStr(result.value), "committed_val");
}

TEST_F(KvExecutorTest, FinalizeAbort_RemovesIntents) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("will_be_removed"), 7), WriteIntentResult::OK);

  Task req;
  req.type = TaskType::TX_FINALIZE_ABORT_REQUEST;
  req.tx_id = 7;
  req.reply_to_core = 0;

  const auto resp = executor_.Execute(std::move(req));

  EXPECT_TRUE(resp.success);

  EXPECT_FALSE(storage_.MvccGet("k", 100, 99).found);
}

TEST_F(KvExecutorTest, Prepare_IntentsAlive_VotesYes) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("val"), 5), WriteIntentResult::OK);

  Task req;
  req.type = TaskType::TX_PREPARE_REQUEST;
  req.tx_id = 5;
  req.reply_to_core = 0;

  const auto resp = executor_.Execute(std::move(req));

  EXPECT_EQ(resp.type, TaskType::TX_PREPARE_RESPONSE);
  EXPECT_TRUE(resp.success);
  EXPECT_TRUE(resp.error_message.empty());
}

TEST_F(KvExecutorTest, Prepare_NoIntents_VotesNo) {
  Task req;
  req.type = TaskType::TX_PREPARE_REQUEST;
  req.tx_id = 5;
  req.reply_to_core = 0;

  const auto resp = executor_.Execute(std::move(req));

  EXPECT_EQ(resp.type, TaskType::TX_PREPARE_RESPONSE);
  EXPECT_FALSE(resp.success);
  EXPECT_NE(resp.error_message.find("no_intents"), std::string::npos);
}

TEST_F(KvExecutorTest, FinalizeAbort_ReturnsProperResponseType) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("val"), 7), WriteIntentResult::OK);

  Task req;
  req.type = TaskType::TX_FINALIZE_ABORT_REQUEST;
  req.tx_id = 7;
  req.reply_to_core = 0;

  const auto resp = executor_.Execute(std::move(req));

  EXPECT_EQ(resp.type, TaskType::TX_FINALIZE_ABORT_RESPONSE);
  EXPECT_TRUE(resp.success);
}

}  // namespace
}  // namespace db
