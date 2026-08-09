#include <gtest/gtest.h>

#include <algorithm>

#include "core/backoff.h"

namespace db {
namespace {

TEST(BackoffTest, ZeroStreak_NormalPriority_WithinBaseRange) {
  for (int i = 0; i < 200; ++i) {
    const uint32_t backoff = ComputeBackoffMs(0, kTxPriorityNormal);
    EXPECT_GE(backoff, kBackoffBaseMs / 2);
    EXPECT_LE(backoff, kBackoffBaseMs);
  }
}

TEST(BackoffTest, IncreasingStreak_IncreasesUpperBound) {
  // Верхняя граница диапазона backoff должна расти с числом подряд идущих
  // конфликтов (пока не упрётся в kBackoffMaxMs).
  uint32_t previous_max = 0;
  for (uint32_t streak = 0; streak <= 6; ++streak) {
    uint32_t max_seen = 0;
    for (int i = 0; i < 200; ++i) {
      max_seen = std::max(max_seen, ComputeBackoffMs(streak, kTxPriorityNormal));
    }
    EXPECT_GE(max_seen, previous_max);
    previous_max = max_seen;
  }
}

TEST(BackoffTest, StreakBeyondCap_DoesNotExceedMax) {
  for (int i = 0; i < 200; ++i) {
    const uint32_t backoff = ComputeBackoffMs(kBackoffMaxStreak + 50, kTxPriorityMin);
    EXPECT_LE(backoff, kBackoffMaxMs);
  }
}

TEST(BackoffTest, HigherPriority_ProducesSmallerOrEqualBackoff) {
  // Приоритет масштабирует backoff обратно пропорционально: high-priority tx
  // должна в среднем получать не больший backoff, чем normal-priority при
  // том же streak, чтобы иметь больше шансов на успех в единицу времени.
  constexpr uint32_t kStreak = 4;
  uint32_t max_high = 0;
  uint32_t max_normal = 0;
  for (int i = 0; i < 500; ++i) {
    max_high = std::max(max_high, ComputeBackoffMs(kStreak, kTxPriorityHigh));
    max_normal = std::max(max_normal, ComputeBackoffMs(kStreak, kTxPriorityNormal));
  }
  EXPECT_LE(max_high, max_normal);
}

TEST(BackoffTest, LowerPriority_ProducesLargerOrEqualBackoff) {
  constexpr uint32_t kStreak = 4;
  uint32_t max_low = 0;
  uint32_t max_normal = 0;
  for (int i = 0; i < 500; ++i) {
    max_low = std::max(max_low, ComputeBackoffMs(kStreak, kTxPriorityLow));
    max_normal = std::max(max_normal, ComputeBackoffMs(kStreak, kTxPriorityNormal));
  }
  EXPECT_GE(max_low, max_normal);
}

TEST(BackoffTest, PriorityOutOfRange_IsClamped) {
  // priority=0 (не задан явно) не должен приводить к переполнению/UB —
  // трактуется как kTxPriorityMin.
  for (int i = 0; i < 200; ++i) {
    const uint32_t backoff_zero = ComputeBackoffMs(2, 0);
    const uint32_t backoff_min = ComputeBackoffMs(2, kTxPriorityMin);
    EXPECT_LE(backoff_zero, kBackoffMaxMs);
    EXPECT_LE(backoff_min, kBackoffMaxMs);
  }

  for (int i = 0; i < 200; ++i) {
    const uint32_t backoff = ComputeBackoffMs(2, kTxPriorityMax * 100);
    EXPECT_LE(backoff, kBackoffMaxMs);
    EXPECT_GT(backoff, 0u);
  }
}

}  // namespace
}  // namespace db
