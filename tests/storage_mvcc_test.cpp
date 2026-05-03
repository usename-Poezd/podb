#include <gtest/gtest.h>

#include <cstring>
#include <string>

#include "storage/storage_engine.h"

namespace db {
namespace {

class StorageMvccTest : public ::testing::Test {
protected:
  static BinaryValue MakeValue(const std::string &input) {
    BinaryValue value(input.size());
    std::memcpy(value.data(), input.data(), input.size());
    return value;
  }

  static std::string ValueToStr(const BinaryValue &value) {
    return {reinterpret_cast<const char *>(value.data()), value.size()};
  }

  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  StorageEngine storage_;
};

TEST_F(StorageMvccTest, NonTxGetSet_StillWorks) {
  storage_.Set("k", MakeValue("value"));

  const auto result = storage_.Get("k");

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(ValueToStr(*result), "value");
}

TEST_F(StorageMvccTest, MvccGet_EmptyStorage_NotFound) {
  const auto result = storage_.MvccGet("missing", 10, 1);

  EXPECT_FALSE(result.found);
}

TEST_F(StorageMvccTest, WriteIntent_CreatesIntent) {
  EXPECT_EQ(storage_.WriteIntent("k", MakeValue("intent"), 1), WriteIntentResult::OK);

  const auto result = storage_.MvccGet("k", 10, 1);

  ASSERT_TRUE(result.found);
  EXPECT_EQ(ValueToStr(result.value), "intent");
}

TEST_F(StorageMvccTest, MvccGet_OwnIntent_Visible) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("mine"), 1), WriteIntentResult::OK);

  const auto result = storage_.MvccGet("k", 10, 1);

  ASSERT_TRUE(result.found);
  EXPECT_EQ(ValueToStr(result.value), "mine");
}

TEST_F(StorageMvccTest, MvccGet_ForeignIntent_NotVisible) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("hidden"), 1), WriteIntentResult::OK);

  const auto result = storage_.MvccGet("k", 10, 2);

  EXPECT_FALSE(result.found);
}

TEST_F(StorageMvccTest, WriteIntent_Conflict_ForeignIntent) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("tx1"), 1), WriteIntentResult::OK);

  EXPECT_EQ(storage_.WriteIntent("k", MakeValue("tx2"), 2),
            WriteIntentResult::WRITE_CONFLICT);
}

TEST_F(StorageMvccTest, WriteIntent_UpdateOwnIntent) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("first"), 1), WriteIntentResult::OK);

  EXPECT_EQ(storage_.WriteIntent("k", MakeValue("second"), 1), WriteIntentResult::OK);

  const auto result = storage_.MvccGet("k", 10, 1);
  ASSERT_TRUE(result.found);
  EXPECT_EQ(ValueToStr(result.value), "second");
}

TEST_F(StorageMvccTest, CommitTransaction_PromotesToCommitted) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("committed"), 1), WriteIntentResult::OK);

  storage_.CommitTransaction(1, 100);

  const auto result = storage_.MvccGet("k", 100, 2);
  ASSERT_TRUE(result.found);
  EXPECT_EQ(ValueToStr(result.value), "committed");
}

TEST_F(StorageMvccTest, CommitTransaction_VisibleBySnapshot) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("value"), 1), WriteIntentResult::OK);

  storage_.CommitTransaction(1, 100);

  EXPECT_FALSE(storage_.MvccGet("k", 99, 2).found);

  const auto visible = storage_.MvccGet("k", 100, 2);
  ASSERT_TRUE(visible.found);
  EXPECT_EQ(ValueToStr(visible.value), "value");
}

TEST_F(StorageMvccTest, AbortTransaction_RemovesIntents) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("value"), 1), WriteIntentResult::OK);

  storage_.AbortTransaction(1);

  EXPECT_FALSE(storage_.MvccGet("k", 100, 2).found);
}

TEST_F(StorageMvccTest, MvccGet_LatestCommittedVersion) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("v50"), 1), WriteIntentResult::OK);
  storage_.CommitTransaction(1, 50);
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("v100"), 2), WriteIntentResult::OK);
  storage_.CommitTransaction(2, 100);

  const auto at_75 = storage_.MvccGet("k", 75, 3);
  ASSERT_TRUE(at_75.found);
  EXPECT_EQ(ValueToStr(at_75.value), "v50");

  const auto at_150 = storage_.MvccGet("k", 150, 3);
  ASSERT_TRUE(at_150.found);
  EXPECT_EQ(ValueToStr(at_150.value), "v100");
}

TEST_F(StorageMvccTest, ReadYourOwnWrites) {
  ASSERT_EQ(storage_.WriteIntent("a", MakeValue("tx1"), 1), WriteIntentResult::OK);

  const auto result = storage_.MvccGet("a", 1, 1);

  ASSERT_TRUE(result.found);
  EXPECT_EQ(ValueToStr(result.value), "tx1");
}

TEST_F(StorageMvccTest, MvccGet_CommittedPlusOwnIntent) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("base"), 1), WriteIntentResult::OK);
  storage_.CommitTransaction(1, 50);
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("intent"), 2), WriteIntentResult::OK);

  const auto result = storage_.MvccGet("k", 50, 2);

  ASSERT_TRUE(result.found);
  EXPECT_EQ(ValueToStr(result.value), "intent");
}

TEST_F(StorageMvccTest, CommitTransaction_MultipleKeys) {
  ASSERT_EQ(storage_.WriteIntent("a", MakeValue("va"), 1), WriteIntentResult::OK);
  ASSERT_EQ(storage_.WriteIntent("b", MakeValue("vb"), 1), WriteIntentResult::OK);

  storage_.CommitTransaction(1, 100);

  const auto a_result = storage_.MvccGet("a", 100, 2);
  const auto b_result = storage_.MvccGet("b", 100, 2);

  ASSERT_TRUE(a_result.found);
  ASSERT_TRUE(b_result.found);
  EXPECT_EQ(ValueToStr(a_result.value), "va");
  EXPECT_EQ(ValueToStr(b_result.value), "vb");
}

TEST_F(StorageMvccTest, AbortTransaction_MultipleKeys) {
  ASSERT_EQ(storage_.WriteIntent("a", MakeValue("va"), 1), WriteIntentResult::OK);
  ASSERT_EQ(storage_.WriteIntent("b", MakeValue("vb"), 1), WriteIntentResult::OK);

  storage_.AbortTransaction(1);

  EXPECT_FALSE(storage_.MvccGet("a", 100, 2).found);
  EXPECT_FALSE(storage_.MvccGet("b", 100, 2).found);
}

TEST_F(StorageMvccTest, ValidatePrepare_IntentsExist_Success) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("val"), 1), WriteIntentResult::OK);

  const auto result = storage_.ValidatePrepare(1);

  EXPECT_TRUE(result.can_commit);
  EXPECT_TRUE(result.reason.empty());
}

TEST_F(StorageMvccTest, ValidatePrepare_MultipleKeys_Success) {
  ASSERT_EQ(storage_.WriteIntent("a", MakeValue("va"), 1), WriteIntentResult::OK);
  ASSERT_EQ(storage_.WriteIntent("b", MakeValue("vb"), 1), WriteIntentResult::OK);

  const auto result = storage_.ValidatePrepare(1);

  EXPECT_TRUE(result.can_commit);
  EXPECT_TRUE(result.reason.empty());
}

TEST_F(StorageMvccTest, ValidatePrepare_NoIntents_Fails) {
  const auto result = storage_.ValidatePrepare(99);

  EXPECT_FALSE(result.can_commit);
  EXPECT_NE(result.reason.find("no_intents"), std::string::npos);
}

TEST_F(StorageMvccTest, ValidatePrepare_AfterAbort_Fails) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("val"), 1), WriteIntentResult::OK);
  storage_.AbortTransaction(1);

  const auto result = storage_.ValidatePrepare(1);

  EXPECT_FALSE(result.can_commit);
  EXPECT_NE(result.reason.find("no_intents"), std::string::npos);
}

TEST_F(StorageMvccTest, ReadSetTracking_CommittedReadTracksKey) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("v"), 1), WriteIntentResult::OK);
  storage_.CommitTransaction(1, 50);

  storage_.MvccGet("k", 100, 2);

  const auto *rs = storage_.GetReadSet(2);
  ASSERT_NE(rs, nullptr);
  EXPECT_TRUE(rs->contains("k"));
}

TEST_F(StorageMvccTest, ReadSetTracking_OwnIntentNotTracked) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("v"), 1), WriteIntentResult::OK);

  storage_.MvccGet("k", 100, 1);

  const auto *rs = storage_.GetReadSet(1);
  EXPECT_TRUE(rs == nullptr || !rs->contains("k"));
}

TEST_F(StorageMvccTest, ReadSetTracking_MissNotTracked) {
  storage_.MvccGet("missing", 100, 1);

  const auto *rs = storage_.GetReadSet(1);
  EXPECT_TRUE(rs == nullptr || rs->empty());
}

TEST_F(StorageMvccTest, ReadSetTracking_CommitCleansUp) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("v"), 1), WriteIntentResult::OK);
  storage_.CommitTransaction(1, 50);
  storage_.MvccGet("k", 100, 2);
  ASSERT_NE(storage_.GetReadSet(2), nullptr);

  ASSERT_EQ(storage_.WriteIntent("x", MakeValue("x"), 2), WriteIntentResult::OK);
  storage_.CommitTransaction(2, 200);

  EXPECT_EQ(storage_.GetReadSet(2), nullptr);
}

TEST_F(StorageMvccTest, ReadSetTracking_AbortCleansUp) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("v"), 1), WriteIntentResult::OK);
  storage_.CommitTransaction(1, 50);
  storage_.MvccGet("k", 100, 2);
  ASSERT_NE(storage_.GetReadSet(2), nullptr);

  ASSERT_EQ(storage_.WriteIntent("x", MakeValue("x"), 2), WriteIntentResult::OK);
  storage_.AbortTransaction(2);

  EXPECT_EQ(storage_.GetReadSet(2), nullptr);
}

TEST_F(StorageMvccTest, ValidatePrepare_AfterCommit_Fails) {
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("val"), 1), WriteIntentResult::OK);
  storage_.CommitTransaction(1, 100);

  const auto result = storage_.ValidatePrepare(1);

  EXPECT_FALSE(result.can_commit);
  EXPECT_NE(result.reason.find("no_intents"), std::string::npos);
}

}  // namespace
}  // namespace db
