#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

namespace db {

namespace detail {

/// Генерация lookup-таблицы CRC32c (полином Кастаньоли 0x82F63B78) во время компиляции.
inline constexpr std::array<uint32_t, 256> MakeCrc32cTable() {
  std::array<uint32_t, 256> table{};
  for (uint32_t i = 0; i < 256; ++i) {
    uint32_t crc = i;
    for (int j = 0; j < 8; ++j) {
      crc = (crc & 1u) ? ((crc >> 1u) ^ 0x82F63B78u) : (crc >> 1u);
    }
    table[i] = crc;
  }
  return table;
}

}  // namespace detail

/// CRC32c (Castagnoli) с аппаратным ускорением SSE4.2 и программным fallback.
/// Стандартный тест-вектор: Crc32c("123456789", 9) == 0xE3069283.
inline uint32_t Crc32c(const void *data, size_t size) {
  const auto *p = static_cast<const uint8_t *>(data);

#ifdef __SSE4_2__
  uint32_t crc = 0xFFFFFFFFu;

#if defined(__x86_64__) || defined(_M_X64)
  // Обрабатываем по 8 байт через _mm_crc32_u64 (только на 64-бит платформах)
  while (size >= 8) {
    uint64_t val;
    std::memcpy(&val, p, 8);
    crc = static_cast<uint32_t>(_mm_crc32_u64(static_cast<uint64_t>(crc), val));
    p += 8;
    size -= 8;
  }
  // Обрабатываем 4 байта через _mm_crc32_u32
  if (size >= 4) {
    uint32_t val;
    std::memcpy(&val, p, 4);
    crc = _mm_crc32_u32(crc, val);
    p += 4;
    size -= 4;
  }
#endif

  // Оставшиеся байты через _mm_crc32_u8
  for (size_t i = 0; i < size; ++i) {
    crc = _mm_crc32_u8(crc, p[i]);
  }

  return crc ^ 0xFFFFFFFFu;

#else
  // Программный fallback: lookup-таблица с Castagnoli-полиномом 0x82F63B78
  static constexpr auto kTable = detail::MakeCrc32cTable();
  uint32_t crc = 0xFFFFFFFFu;
  for (size_t i = 0; i < size; ++i) {
    crc = (crc >> 8u) ^ kTable[(crc ^ p[i]) & 0xFFu];
  }
  return crc ^ 0xFFFFFFFFu;
#endif
}

/// Маскирование CRC (паттерн LevelDB) — предотвращает ложные совпадения в all-zero блоках.
inline uint32_t Crc32cMask(uint32_t crc) {
  return ((crc >> 15u) | (crc << 17u)) + 0xa282ead8u;
}

/// Обратное маскирование CRC.
inline uint32_t Crc32cUnmask(uint32_t masked) {
  uint32_t rot = masked - 0xa282ead8u;
  return (rot >> 17u) | (rot << 15u);
}

}  // namespace db
