#pragma once

#include <chrono>

namespace db {

/// Абстракция часов для инъекции в компоненты с временными зависимостями.
class Clock {
public:
  using TimePoint = std::chrono::steady_clock::time_point;
  using Duration = std::chrono::steady_clock::duration;
  virtual ~Clock() = default;
  virtual TimePoint Now() const = 0;
};

/// Продакшн-реализация: делегирует std::chrono::steady_clock.
class SteadyClock : public Clock {
public:
  TimePoint Now() const override { return std::chrono::steady_clock::now(); }
};

/// Тестовая реализация: управляемое время для детерминированных тестов.
class ManualClock : public Clock {
public:
  TimePoint Now() const override { return now_; }
  void Advance(Duration d) { now_ += d; }
  void SetNow(TimePoint tp) { now_ = tp; }

private:
  TimePoint now_{std::chrono::steady_clock::now()};
};

}  // namespace db
