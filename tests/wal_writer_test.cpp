#include <gtest/gtest.h>

#include <filesystem>
#include <sys/stat.h>
#include <unistd.h>

#include "wal/wal_reader.h"
#include "wal/wal_record.h"
#include "wal/wal_writer.h"

namespace db {

class WalWriterTest : public ::testing::Test {
protected:
  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() /
                ("podb_wal_test_" + std::to_string(::getpid()));
    std::filesystem::create_directories(test_dir_);
    wal_path_ = (test_dir_ / "test.wal").string();
  }
  void TearDown() override { std::filesystem::remove_all(test_dir_); }
  std::filesystem::path test_dir_;
  std::string wal_path_;
};

TEST_F(WalWriterTest, WriteOneRecord_ReadBack) {
  {
    WalWriter writer(wal_path_);
    WalRecord r;
    r.type = WalRecordType::TX_BEGIN;
    r.tx_id = 42;
    r.snapshot_ts = 999;
    writer.Append(r);
  }

  WalReader reader(wal_path_);
  const auto rec = reader.Next();
  ASSERT_TRUE(rec.has_value());
  EXPECT_EQ(rec->type, WalRecordType::TX_BEGIN);
  EXPECT_EQ(rec->tx_id, 42u);
  EXPECT_EQ(rec->snapshot_ts, 999u);
  EXPECT_EQ(rec->lsn, 1u);
}

TEST_F(WalWriterTest, WriteMultiple_ReadAllInOrder) {
  {
    WalWriter writer(wal_path_);
    for (int i = 0; i < 5; ++i) {
      WalRecord r;
      r.type = WalRecordType::TX_BEGIN;
      r.tx_id = static_cast<uint64_t>(i + 1);
      r.snapshot_ts = static_cast<uint64_t>(i * 10);
      writer.Append(r);
    }
  }

  WalReader reader(wal_path_);
  const auto records = reader.ReadAll();
  ASSERT_EQ(records.size(), 5u);
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(records[i].lsn, i + 1);
    EXPECT_EQ(records[i].tx_id, i + 1);
  }
}

TEST_F(WalWriterTest, LsnMonotonicallyIncreases) {
  WalWriter writer(wal_path_);

  const uint64_t lsn1 = writer.CurrentLsn();
  WalRecord r;
  r.type = WalRecordType::TX_BEGIN;
  writer.Append(r);

  const uint64_t lsn2 = writer.CurrentLsn();
  writer.Append(r);

  const uint64_t lsn3 = writer.CurrentLsn();
  writer.Append(r);

  EXPECT_EQ(lsn1, 1u);
  EXPECT_EQ(lsn2, 2u);
  EXPECT_EQ(lsn3, 3u);
  EXPECT_EQ(writer.CurrentLsn(), 4u);
}

TEST_F(WalWriterTest, EmptyWal_ReaderReturnsNullopt) {
  { WalWriter writer(wal_path_); }

  WalReader reader(wal_path_);
  const auto rec = reader.Next();
  EXPECT_FALSE(rec.has_value());
  EXPECT_FALSE(reader.HasCorruptedTail());
}

TEST_F(WalWriterTest, CorruptedTail_StopsAtLastValid) {
  {
    WalWriter writer(wal_path_);
    for (int i = 0; i < 4; ++i) {
      WalRecord r;
      r.type = WalRecordType::TX_BEGIN;
      r.tx_id = static_cast<uint64_t>(i + 1);
      r.snapshot_ts = static_cast<uint64_t>(i * 100);
      writer.Append(r);
    }
  }

  struct stat st {};
  ::stat(wal_path_.c_str(), &st);
  const off_t truncated = st.st_size - 8;
  ASSERT_EQ(::truncate(wal_path_.c_str(), truncated), 0);

  WalReader reader(wal_path_);
  const auto records = reader.ReadAll();
  EXPECT_EQ(records.size(), 3u);
  EXPECT_TRUE(reader.HasCorruptedTail());
}

TEST_F(WalWriterTest, SyncFlushesToDisk) {
  {
    WalWriter writer(wal_path_);
    WalRecord r;
    r.type = WalRecordType::TX_BEGIN;
    r.tx_id = 7;
    r.snapshot_ts = 42;
    writer.Append(r);
    writer.Sync();
  }

  WalReader reader(wal_path_);
  const auto records = reader.ReadAll();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].tx_id, 7u);
  EXPECT_EQ(records[0].snapshot_ts, 42u);
}

TEST_F(WalWriterTest, ReopenExistingWal_ContinuesLsn) {
  {
    WalWriter writer(wal_path_);
    for (int i = 0; i < 3; ++i) {
      WalRecord r;
      r.type = WalRecordType::TX_BEGIN;
      r.tx_id = static_cast<uint64_t>(i + 1);
      writer.Append(r);
    }
  }

  {
    WalWriter writer(wal_path_);
    EXPECT_EQ(writer.CurrentLsn(), 4u);
    WalRecord r;
    r.type = WalRecordType::TX_BEGIN;
    r.tx_id = 4;
    writer.Append(r);
  }

  WalReader reader(wal_path_);
  const auto records = reader.ReadAll();
  ASSERT_EQ(records.size(), 4u);
  for (size_t i = 0; i < 4; ++i) {
    EXPECT_EQ(records[i].lsn, i + 1);
  }
}

TEST_F(WalWriterTest, ReopenExistingWal_TruncatesCorruptedTailBeforeAppend) {
  {
    WalWriter writer(wal_path_);
    for (int i = 0; i < 3; ++i) {
      WalRecord r;
      r.type = WalRecordType::TX_BEGIN;
      r.tx_id = static_cast<uint64_t>(i + 1);
      writer.Append(r);
    }
  }

  struct stat st {};
  ASSERT_EQ(::stat(wal_path_.c_str(), &st), 0);
  ASSERT_EQ(::truncate(wal_path_.c_str(), st.st_size - 8), 0);

  {
    WalWriter writer(wal_path_);
    EXPECT_EQ(writer.CurrentLsn(), 3u);

    WalRecord r;
    r.type = WalRecordType::TX_BEGIN;
    r.tx_id = 99;
    writer.Append(r);
  }

  WalReader reader(wal_path_);
  const auto records = reader.ReadAll();
  ASSERT_EQ(records.size(), 3u);
  EXPECT_FALSE(reader.HasCorruptedTail());
  EXPECT_EQ(records[0].tx_id, 1u);
  EXPECT_EQ(records[1].tx_id, 2u);
  EXPECT_EQ(records[2].tx_id, 99u);
  EXPECT_EQ(records[0].lsn, 1u);
  EXPECT_EQ(records[1].lsn, 2u);
  EXPECT_EQ(records[2].lsn, 3u);
}

TEST_F(WalWriterTest, LargeIntentPayload) {
  WalRecord r;
  r.type = WalRecordType::INTENT;
  r.tx_id = 42;
  r.key = std::string(10240, 'k');
  r.value = BinaryValue(10240, std::byte{0xAB});
  r.is_deleted = false;

  {
    WalWriter writer(wal_path_);
    writer.Append(r);
  }

  WalReader reader(wal_path_);
  const auto records = reader.ReadAll();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].type, WalRecordType::INTENT);
  EXPECT_EQ(records[0].tx_id, 42u);
  EXPECT_EQ(records[0].key, r.key);
  EXPECT_EQ(records[0].value, r.value);
  EXPECT_FALSE(records[0].is_deleted);
}

TEST_F(WalWriterTest, AllRecordTypes_Roundtrip) {
  {
    WalWriter writer(wal_path_);

    WalRecord r1;
    r1.type = WalRecordType::TX_BEGIN;
    r1.tx_id = 1;
    r1.snapshot_ts = 100;
    writer.Append(r1);

    WalRecord r2;
    r2.type = WalRecordType::INTENT;
    r2.tx_id = 1;
    r2.key = "mykey";
    r2.value = {std::byte{0x01}, std::byte{0x02}};
    writer.Append(r2);

    WalRecord r3;
    r3.type = WalRecordType::PREPARE;
    r3.tx_id = 1;
    r3.vote_yes = true;
    writer.Append(r3);

    WalRecord r4;
    r4.type = WalRecordType::COMMIT_DECISION;
    r4.tx_id = 1;
    r4.commit_ts = 200;
    writer.Append(r4);

    WalRecord r5;
    r5.type = WalRecordType::ABORT_DECISION;
    r5.tx_id = 2;
    writer.Append(r5);

    WalRecord r6;
    r6.type = WalRecordType::COMMIT_FINALIZE;
    r6.tx_id = 1;
    r6.commit_ts = 200;
    writer.Append(r6);

    WalRecord r7;
    r7.type = WalRecordType::ABORT_FINALIZE;
    r7.tx_id = 2;
    writer.Append(r7);

    WalRecord r8;
    r8.type = WalRecordType::CHECKPOINT;
    r8.tx_id = 0;
    r8.snapshot_lsn = 7;
    writer.Append(r8);
  }

  WalReader reader(wal_path_);
  const auto records = reader.ReadAll();
  ASSERT_EQ(records.size(), 8u);

  EXPECT_EQ(records[0].type, WalRecordType::TX_BEGIN);
  EXPECT_EQ(records[0].snapshot_ts, 100u);

  EXPECT_EQ(records[1].type, WalRecordType::INTENT);
  EXPECT_EQ(records[1].key, "mykey");

  EXPECT_EQ(records[2].type, WalRecordType::PREPARE);
  EXPECT_TRUE(records[2].vote_yes);

  EXPECT_EQ(records[3].type, WalRecordType::COMMIT_DECISION);
  EXPECT_EQ(records[3].commit_ts, 200u);

  EXPECT_EQ(records[4].type, WalRecordType::ABORT_DECISION);

  EXPECT_EQ(records[5].type, WalRecordType::COMMIT_FINALIZE);
  EXPECT_EQ(records[5].commit_ts, 200u);

  EXPECT_EQ(records[6].type, WalRecordType::ABORT_FINALIZE);

  EXPECT_EQ(records[7].type, WalRecordType::CHECKPOINT);
  EXPECT_EQ(records[7].snapshot_lsn, 7u);
}

}  // namespace db
