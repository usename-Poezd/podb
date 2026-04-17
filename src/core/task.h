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

  // --- Request payload ---
  std::string key;
  std::string value;

  // --- Response payload ---
  bool found{false};
  bool success{true};

  // --- Routing/correlation ---
  uint64_t request_id{0};
  int reply_to_core{-1};

  // Helper methods
  bool IsRequest() const noexcept {
    return type == TaskType::GET_REQUEST || type == TaskType::SET_REQUEST;
  }

  bool IsResponse() const noexcept {
    return type == TaskType::GET_RESPONSE || type == TaskType::SET_RESPONSE;
  }

  const char* OpName() const noexcept {
    return (type == TaskType::GET_REQUEST || type == TaskType::GET_RESPONSE) ? "GET" : "SET";
  }
};

} // namespace db
