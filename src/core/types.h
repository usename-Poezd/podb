#pragma once

#include <cstddef>
#include <vector>

namespace db {

/// Бинарное значение — opaque последовательность байт.
using BinaryValue = std::vector<std::byte>;

}  // namespace db
