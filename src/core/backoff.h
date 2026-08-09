#pragma once

#include <algorithm>
#include <cstdint>
#include <random>

namespace db {

/// Приоритет транзакции: чем выше значение, тем важнее транзакция.
/// Используется ТОЛЬКО для масштабирования retry-backoff при write-write
/// Сам конфликт по-прежнему разрешается по правилу no-wait "первый intent
/// побеждает" — приоритет не даёт права вытеснить чужой intent (это был бы
/// wound-wait/push, отдельный, более тяжёлый механизм).
constexpr uint32_t kTxPriorityMin = 1;
constexpr uint32_t kTxPriorityLow = 100;
constexpr uint32_t kTxPriorityNormal = 500;
constexpr uint32_t kTxPriorityHigh = 900;
constexpr uint32_t kTxPriorityMax = 1000;

constexpr uint32_t kBackoffBaseMs = 5;
constexpr uint32_t kBackoffMaxMs = 2000;
constexpr uint32_t kBackoffMaxStreak = 8; // ограничивает рост экспоненты

/// Экспоненциальный backoff с jitter для write-write конфликта.
///
/// backoff растёт экспоненциально с числом подряд идущих конфликтов этой tx
/// на её текущем write-пути (conflict_streak) и масштабируется обратно
/// пропорционально priority: высокоприоритетная (например, долгая
/// аналитическая) tx получает меньший backoff и в среднем чаще успевает
/// повторно захватить intent раньше потока низкоприоритетных
/// короткоживущих писателей.
///
/// Это НЕ гарантия отсутствия голодания (в отличие от wound-wait/push) —
/// только снижение вероятности систематического livelock за счёт desync
/// повторов (jitter) и статистического сдвига шансов в пользу
/// приоритетной tx.
inline uint32_t ComputeBackoffMs(uint32_t conflict_streak, uint32_t priority) {
  const uint32_t capped_streak = std::min(conflict_streak, kBackoffMaxStreak);
  const uint64_t exp_backoff_ms = static_cast<uint64_t>(kBackoffBaseMs) << capped_streak;

  const uint32_t safe_priority = std::clamp(priority, kTxPriorityMin, kTxPriorityMax);
  uint64_t scaled_ms = exp_backoff_ms * kTxPriorityNormal / safe_priority;
  scaled_ms = std::min<uint64_t>(scaled_ms, kBackoffMaxMs);

  thread_local std::mt19937_64 rng{std::random_device{}()};
  std::uniform_int_distribution<uint64_t> jitter(scaled_ms / 2, scaled_ms);
  return static_cast<uint32_t>(jitter(rng));
}

} // namespace db
