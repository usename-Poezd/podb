#pragma once

#include <cstdint>

#include <string>

namespace db {

enum class TaskType : uint8_t {
  SET_REQUEST = 0,
  GET_REQUEST = 1,
  SET_RESPONSE = 2,
  GET_RESPONSE = 3,
};

inline const char *TaskTypeName(TaskType t) noexcept {
  switch (t) {
  case TaskType::GET_REQUEST:
    return "GET ";
  case TaskType::SET_REQUEST:
    return "SET ";
  case TaskType::GET_RESPONSE:
    return "GET←";
  case TaskType::SET_RESPONSE:
    return "SET←";
  }
  return "???";
}

struct Task {
  TaskType type{TaskType::GET_REQUEST};

  std::string key;
  std::string value;

  bool found{false};
  bool success{true};

  uint64_t request_id{0};
  int reply_to_core{-1};
};

} // namespace db
