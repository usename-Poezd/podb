# Core-Clock — Абстракция времени

## Что это

Файл `src/core/clock.h` определяет абстрактный интерфейс для получения текущего времени с двумя реализациями:

- **`SteadyClock`** — production-реализация на основе `std::chrono::steady_clock`;
- **`ManualClock`** — тестовая реализация с ручным управлением временем.

## Зачем нужно

Транзакционный координатор (`TxCoordinator`) зависит от времени для:

- назначения `snapshot_ts` и `commit_ts`;
- отслеживания `lease_timeout` (30с) и `stuck_timeout` (10с);
- определения момента `last_heartbeat_time`;
- purge терминальных транзакций (60с).

Без абстракции времени тесты были бы недетерминированными — зависели бы от реального хода часов. `Clock` позволяет инжектировать `ManualClock` в тестах и точно контролировать временные переходы.

### Почему `steady_clock`, а не `system_clock`

`steady_clock` — монотонные часы, которые:

- **не прыгают назад** при синхронизации NTP;
- **не зависят от системных настроек** (изменение timezone, ручная коррекция);
- гарантируют, что `Now()` всегда возвращает значение ≥ предыдущего.

Для внутренних таймаутов и lease-логики это критично.

## Как работает

### Иерархия классов

```
Clock (abstract)
├── SteadyClock   — production: steady_clock::now()
└── ManualClock   — tests: ручное управление
```

### Паттерн dependency injection

```cpp
// Production (main.cpp)
SteadyClock clock;
TxCoordinator coordinator(router, resume_fn, wal, &clock);

// Tests
ManualClock clock;
TxCoordinator coordinator(router, resume_fn, nullptr, &clock);
clock.Advance(std::chrono::seconds(31));  // симулируем протухание lease
coordinator.ReapStaleTransactions();       // транзакция будет aborted
```

## Публичный API

### Базовый класс `Clock`

```cpp
class Clock {
public:
    using TimePoint = std::chrono::steady_clock::time_point;
    using Duration = std::chrono::steady_clock::duration;

    virtual ~Clock() = default;
    virtual TimePoint Now() const = 0;
};
```

### `SteadyClock` — production-реализация

```cpp
class SteadyClock : public Clock {
public:
    TimePoint Now() const override;
    // Возвращает std::chrono::steady_clock::now()
};
```

### `ManualClock` — тестовая реализация

```cpp
class ManualClock : public Clock {
public:
    TimePoint Now() const override;
    // Возвращает внутреннее значение now_

    void Advance(Duration d);
    // Сдвигает now_ вперёд на d

    void SetNow(TimePoint tp);
    // Устанавливает now_ в конкретную точку

private:
    TimePoint now_{std::chrono::steady_clock::now()};
    // Инициализируется реальным временем при создании
};
```

## Связи с другими модулями

| Модуль | Как используется |
|--------|-----------------|
| [Transaction-TxCoordinator](Transaction-TxCoordinator) | Получает `Clock*` в конструкторе для `snapshot_ts`, lease tracking, reaper |
| Tests (`tx_coordinator_test.cpp`, `tx_reaper_test.cpp`) | Инжектируют `ManualClock` для детерминированного тестирования таймаутов |

## См. также

- [Transaction-TxCoordinator](Transaction-TxCoordinator) — основной потребитель `Clock`
- [Core-Task](Core-Task) — поля `snapshot_ts`, `commit_ts`, значения которых определяются через `Clock`
