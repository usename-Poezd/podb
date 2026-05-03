#include <gtest/gtest.h>

#include <cstring>
#include <string>

#include "storage/storage_engine.h"

namespace db {
namespace {

class StorageGcTest : public ::testing::Test {
protected:
  static BinaryValue MakeValue(const std::string &s) {
    BinaryValue v(s.size());
    std::memcpy(v.data(), s.data(), s.size());
    return v;
  }

  static std::string ValueToStr(const BinaryValue &v) {
    return {reinterpret_cast<const char *>(v.data()), v.size()};
  }

  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  StorageEngine storage_;
};

TEST_F(StorageGcTest, GC_RemovesOldVersions) {
  storage_.RestoreCommitted("k", MakeValue("v10"), 10, false);
  storage_.RestoreCommitted("k", MakeValue("v20"), 20, false);
  storage_.RestoreCommitted("k", MakeValue("v30"), 30, false);

  const size_t removed = storage_.GarbageCollect(25);

  EXPECT_EQ(removed, 2U);
  const auto result = storage_.MvccGet("k", 30, 0);
  ASSERT_TRUE(result.found);
  EXPECT_EQ(ValueToStr(result.value), "v30");
}

TEST_F(StorageGcTest, GC_KeepsLatestVersion) {
  storage_.RestoreCommitted("k", MakeValue("only"), 5, false);

  const size_t removed = storage_.GarbageCollect(100);

  EXPECT_EQ(removed, 0U);
  const auto result = storage_.MvccGet("k", 100, 0);
  ASSERT_TRUE(result.found);
  EXPECT_EQ(ValueToStr(result.value), "only");
}

TEST_F(StorageGcTest, GC_PreservesIntents) {
  storage_.RestoreCommitted("k", MakeValue("base"), 10, false);
  ASSERT_EQ(storage_.WriteIntent("k", MakeValue("intent_val"), 1), WriteIntentResult::OK);

  storage_.GarbageCollect(100);

  const auto result = storage_.MvccGet("k", 10, 1);
  ASSERT_TRUE(result.found);
  EXPECT_EQ(ValueToStr(result.value), "intent_val");
}

TEST_F(StorageGcTest, GC_RemovesTombstones) {
  storage_.RestoreCommitted("k", MakeValue(""), 5, true);

  const size_t removed = storage_.GarbageCollect(10);

  EXPECT_EQ(removed, 1U);
  EXPECT_FALSE(storage_.MvccGet("k", 100, 0).found);
}

TEST_F(StorageGcTest, GC_KeepsRecentTombstones) {
  storage_.RestoreCommitted("k", MakeValue(""), 15, true);

  const size_t removed = storage_.GarbageCollect(10);

  EXPECT_EQ(removed, 0U);
  const auto result = storage_.MvccGet("k", 100, 0);
  ASSERT_TRUE(result.found);
  EXPECT_TRUE(result.is_deleted);
}

TEST_F(StorageGcTest, GC_WatermarkZero_NoOp) {
  storage_.RestoreCommitted("k", MakeValue("val"), 5, false);

  const size_t removed = storage_.GarbageCollect(0);

  EXPECT_EQ(removed, 0U);
  ASSERT_TRUE(storage_.MvccGet("k", 100, 0).found);
}

TEST_F(StorageGcTest, GC_MultipleKeys) {
  storage_.RestoreCommitted("a", MakeValue("a5"), 5, false);
  storage_.RestoreCommitted("a", MakeValue("a15"), 15, false);
  storage_.RestoreCommitted("b", MakeValue("b20"), 20, false);

  const size_t removed = storage_.GarbageCollect(10);

  EXPECT_EQ(removed, 1U);

  const auto a_result = storage_.MvccGet("a", 100, 0);
  ASSERT_TRUE(a_result.found);
  EXPECT_EQ(ValueToStr(a_result.value), "a15");

  const auto b_result = storage_.MvccGet("b", 100, 0);
  ASSERT_TRUE(b_result.found);
  EXPECT_EQ(ValueToStr(b_result.value), "b20");
}

TEST_F(StorageGcTest, GC_ReturnsCorrectCount) {
  storage_.RestoreCommitted("x", MakeValue("v1"), 10, false);
  storage_.RestoreCommitted("x", MakeValue("v2"), 20, false);
  storage_.RestoreCommitted("x", MakeValue("v3"), 30, false);
  storage_.RestoreCommitted("y", MakeValue("u1"), 5, false);
  storage_.RestoreCommitted("y", MakeValue("u2"), 15, false);

  const size_t removed = storage_.GarbageCollect(25);

  EXPECT_EQ(removed, 3U);
}

}  // namespace
}  // namespace db
