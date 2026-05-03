#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "wal/crc32c.h"
#include "wal/wal_record.h"

namespace db {
namespace {

TEST(WalRecord, Crc32c_KnownVector) {
  EXPECT_EQ(Crc32c("123456789", 9), 0xE3069283u);
}

TEST(WalRecord, Crc32c_MaskUnmask_Roundtrip) {
  const uint32_t original = 0xDEADBEEFu;
  EXPECT_EQ(Crc32cUnmask(Crc32cMask(original)), original);
}

TEST(WalRecord, Serialize_TxBegin_Roundtrip) {
  WalRecord rec;
  rec.type = WalRecordType::TX_BEGIN;
  rec.tx_id = 42;
  rec.lsn = 100;
  rec.snapshot_ts = 999;

  const auto bytes = rec.Serialize();
  const auto result = WalRecord::Deserialize(bytes.data(), bytes.size());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->type, WalRecordType::TX_BEGIN);
  EXPECT_EQ(result->tx_id, 42u);
  EXPECT_EQ(result->lsn, 100u);
  EXPECT_EQ(result->snapshot_ts, 999u);
}

TEST(WalRecord, Serialize_Intent_Roundtrip) {
  WalRecord rec;
  rec.type = WalRecordType::INTENT;
  rec.tx_id = 7;
  rec.lsn = 200;
  rec.key = "some_key";
  rec.value = {std::byte{0x01}, std::byte{0x02}, std::byte{0x03}};
  rec.is_deleted = true;

  const auto bytes = rec.Serialize();
  const auto result = WalRecord::Deserialize(bytes.data(), bytes.size());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->type, WalRecordType::INTENT);
  EXPECT_EQ(result->tx_id, 7u);
  EXPECT_EQ(result->lsn, 200u);
  EXPECT_EQ(result->key, "some_key");
  EXPECT_EQ(result->value, rec.value);
  EXPECT_TRUE(result->is_deleted);
}

TEST(WalRecord, Serialize_CommitDecision_Roundtrip) {
  WalRecord rec;
  rec.type = WalRecordType::COMMIT_DECISION;
  rec.tx_id = 55;
  rec.lsn = 300;
  rec.commit_ts = 1234567890;

  const auto bytes = rec.Serialize();
  const auto result = WalRecord::Deserialize(bytes.data(), bytes.size());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->type, WalRecordType::COMMIT_DECISION);
  EXPECT_EQ(result->commit_ts, 1234567890u);
}

TEST(WalRecord, Serialize_AbortDecision_Roundtrip) {
  WalRecord rec;
  rec.type = WalRecordType::ABORT_DECISION;
  rec.tx_id = 11;
  rec.lsn = 400;

  const auto bytes = rec.Serialize();
  EXPECT_EQ(bytes.size(), kWalHeaderSize);

  const auto result = WalRecord::Deserialize(bytes.data(), bytes.size());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->type, WalRecordType::ABORT_DECISION);
  EXPECT_EQ(result->tx_id, 11u);
}

TEST(WalRecord, Serialize_Prepare_Roundtrip) {
  WalRecord rec;
  rec.type = WalRecordType::PREPARE;
  rec.tx_id = 33;
  rec.lsn = 500;
  rec.vote_yes = true;

  const auto bytes = rec.Serialize();
  const auto result = WalRecord::Deserialize(bytes.data(), bytes.size());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->type, WalRecordType::PREPARE);
  EXPECT_TRUE(result->vote_yes);
}

TEST(WalRecord, Serialize_Checkpoint_Roundtrip) {
  WalRecord rec;
  rec.type = WalRecordType::CHECKPOINT;
  rec.tx_id = 0;
  rec.lsn = 600;
  rec.snapshot_lsn = 595;

  const auto bytes = rec.Serialize();
  const auto result = WalRecord::Deserialize(bytes.data(), bytes.size());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->type, WalRecordType::CHECKPOINT);
  EXPECT_EQ(result->snapshot_lsn, 595u);
}

TEST(WalRecord, Deserialize_CorruptedCrc_Nullopt) {
  WalRecord rec;
  rec.type = WalRecordType::TX_BEGIN;
  rec.tx_id = 1;
  rec.lsn = 1;
  rec.snapshot_ts = 100;

  auto bytes = rec.Serialize();
  bytes[kWalHeaderSize] = ~bytes[kWalHeaderSize];

  const auto result = WalRecord::Deserialize(bytes.data(), bytes.size());
  EXPECT_FALSE(result.has_value());
}

TEST(WalRecord, Deserialize_BadMagic_Nullopt) {
  WalRecord rec;
  rec.type = WalRecordType::TX_BEGIN;
  rec.tx_id = 2;
  rec.lsn = 2;
  rec.snapshot_ts = 200;

  auto bytes = rec.Serialize();
  bytes[0] = std::byte{0};
  bytes[1] = std::byte{0};
  bytes[2] = std::byte{0};
  bytes[3] = std::byte{0};

  const auto result = WalRecord::Deserialize(bytes.data(), bytes.size());
  EXPECT_FALSE(result.has_value());
}

TEST(WalRecord, Serialize_CommitFinalize_Roundtrip) {
  WalRecord rec;
  rec.type = WalRecordType::COMMIT_FINALIZE;
  rec.tx_id = 77;
  rec.lsn = 700;
  rec.commit_ts = 9876543210ULL;

  const auto bytes = rec.Serialize();
  const auto result = WalRecord::Deserialize(bytes.data(), bytes.size());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->type, WalRecordType::COMMIT_FINALIZE);
  EXPECT_EQ(result->commit_ts, 9876543210ULL);
}

}  // namespace
}  // namespace db
