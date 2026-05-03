#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <unistd.h>

#include "checkpoint/checkpoint_reader.h"
#include "checkpoint/checkpoint_writer.h"
#include "storage/storage_engine.h"

namespace db {

class CheckpointTest : public ::testing::Test {
protected:
  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() /
                ("podb_ckpt_test_" + std::to_string(::getpid()));
    std::filesystem::create_directories(test_dir_);
    snap_path_ = (test_dir_ / "core_0.snap").string();
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  static BinaryValue MakeValue(const std::string& str) {
    BinaryValue bval(str.size());
    std::memcpy(bval.data(), str.data(), str.size());
    return bval;
  }

  static std::string ValueToStr(const BinaryValue& bval) {
    return {reinterpret_cast<const char*>(bval.data()), bval.size()};
  }

  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  std::filesystem::path test_dir_;
  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  std::string snap_path_;
};

// T1: пустое хранилище — запись и загрузка.
TEST_F(CheckpointTest, EmptyStorage_WriteRead) {
  StorageEngine src;
  CheckpointWriter::TopologyMeta topo{.num_cores = 2, .layout_epoch = 1};
  CheckpointWriter::Write(src, snap_path_, 0, topo, 0);

  StorageEngine dst;
  const auto hdr = CheckpointReader::Load(snap_path_, dst);

  EXPECT_EQ(hdr.entry_count, 0U);

  int count = 0;
  dst.ForEachLatestCommitted(
      [&](const std::string&, const BinaryValue&, uint64_t, bool) { ++count; });
  EXPECT_EQ(count, 0);
}

// T2: три committed ключа — roundtrip через файл.
TEST_F(CheckpointTest, CommittedData_Roundtrip) {
  StorageEngine src;
  src.WriteIntent("a", MakeValue("va"), 1);
  src.WriteIntent("b", MakeValue("vb"), 1);
  src.WriteIntent("c", MakeValue("vc"), 1);
  src.CommitTransaction(1, 50);

  CheckpointWriter::TopologyMeta topo{.num_cores = 1, .layout_epoch = 0};
  CheckpointWriter::Write(src, snap_path_, 0, topo, 0);

  StorageEngine dst;
  const auto hdr = CheckpointReader::Load(snap_path_, dst);
  EXPECT_EQ(hdr.entry_count, 3U);

  const auto res_a = dst.MvccGet("a", 100, 99);
  const auto res_b = dst.MvccGet("b", 100, 99);
  const auto res_c = dst.MvccGet("c", 100, 99);

  ASSERT_TRUE(res_a.found);
  ASSERT_TRUE(res_b.found);
  ASSERT_TRUE(res_c.found);
  EXPECT_EQ(ValueToStr(res_a.value), "va");
  EXPECT_EQ(ValueToStr(res_b.value), "vb");
  EXPECT_EQ(ValueToStr(res_c.value), "vc");
}

// T3: uncommitted intent не попадает в snapshot.
TEST_F(CheckpointTest, IntentsNotInSnapshot) {
  StorageEngine src;
  src.WriteIntent("committed", MakeValue("yes"), 1);
  src.CommitTransaction(1, 10);

  src.WriteIntent("pending", MakeValue("no"), 2);

  CheckpointWriter::TopologyMeta topo{.num_cores = 1, .layout_epoch = 0};
  CheckpointWriter::Write(src, snap_path_, 0, topo, 0);

  StorageEngine dst;
  const auto hdr = CheckpointReader::Load(snap_path_, dst);
  EXPECT_EQ(hdr.entry_count, 1U);

  EXPECT_TRUE(dst.MvccGet("committed", 100, 99).found);
  EXPECT_FALSE(dst.MvccGet("pending", 100, 99).found);
}

// T4: topology metadata сохраняется корректно.
TEST_F(CheckpointTest, TopologyMeta_Preserved) {
  StorageEngine src;
  CheckpointWriter::TopologyMeta topo{.num_cores = 4, .layout_epoch = 7};
  CheckpointWriter::Write(src, snap_path_, 0, topo, 0);

  const auto hdr = CheckpointReader::ReadHeader(snap_path_);
  ASSERT_TRUE(hdr.has_value());
  EXPECT_EQ(hdr->num_cores, 4U);
  EXPECT_EQ(hdr->layout_epoch, 7U);
}

// T5: wal_lsn сохраняется в заголовке.
TEST_F(CheckpointTest, WalLsn_Preserved) {
  StorageEngine src;
  CheckpointWriter::TopologyMeta topo{.num_cores = 1, .layout_epoch = 0};
  CheckpointWriter::Write(src, snap_path_, 0, topo, 42);

  const auto hdr = CheckpointReader::ReadHeader(snap_path_);
  ASSERT_TRUE(hdr.has_value());
  EXPECT_EQ(hdr->wal_lsn, 42U);
}

// T6: после Write() временный .tmp файл удалён.
TEST_F(CheckpointTest, AtomicWrite_TmpCleaned) {
  StorageEngine src;
  CheckpointWriter::TopologyMeta topo{.num_cores = 1, .layout_epoch = 0};
  CheckpointWriter::Write(src, snap_path_, 0, topo, 0);

  EXPECT_FALSE(std::filesystem::exists(snap_path_ + ".tmp"));
  EXPECT_TRUE(std::filesystem::exists(snap_path_));
}

// T7: один повреждённый байт в заголовке → ReadHeader возвращает nullopt.
TEST_F(CheckpointTest, CorruptedHeader_ReadHeaderReturnsNullopt) {
  StorageEngine src;
  CheckpointWriter::TopologyMeta topo{.num_cores = 1, .layout_epoch = 0};
  CheckpointWriter::Write(src, snap_path_, 0, topo, 0);

  {
    std::fstream file(snap_path_, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(file.is_open());
    file.seekp(10);
    const char bad = '\xAB';
    file.write(&bad, 1);
  }

  const auto hdr = CheckpointReader::ReadHeader(snap_path_);
  EXPECT_FALSE(hdr.has_value());
}

// T8: несколько версий одного ключа — в snapshot только последняя committed.
TEST_F(CheckpointTest, MultipleVersions_OnlyLatestCommitted) {
  StorageEngine src;
  src.WriteIntent("k", MakeValue("old"), 1);
  src.CommitTransaction(1, 50);

  src.WriteIntent("k", MakeValue("new"), 2);
  src.CommitTransaction(2, 100);

  CheckpointWriter::TopologyMeta topo{.num_cores = 1, .layout_epoch = 0};
  CheckpointWriter::Write(src, snap_path_, 0, topo, 0);

  StorageEngine dst;
  CheckpointReader::Load(snap_path_, dst);

  const auto result = dst.MvccGet("k", 200, 99);
  ASSERT_TRUE(result.found);
  EXPECT_EQ(ValueToStr(result.value), "new");
}

}  // namespace db
