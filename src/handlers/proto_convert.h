#pragma once

#include <string>

#include "core/types.h"

namespace db {

/// Конвертация protobuf bytes (std::string) → BinaryValue.
inline BinaryValue FromProtoBytes(const std::string &bytes) {
  auto ptr = reinterpret_cast<const std::byte *>(bytes.data());
  return BinaryValue(ptr, ptr + bytes.size());
}

/// Конвертация BinaryValue → protobuf bytes (std::string).
inline std::string ToProtoBytes(const BinaryValue &value) {
  auto ptr = reinterpret_cast<const char *>(value.data());
  return std::string(ptr, ptr + value.size());
}

}  // namespace db
