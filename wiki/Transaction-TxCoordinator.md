# Transaction-TxCoordinator — Координатор транзакций

## Что это

`TxCoordinator` (`src/transaction/tx_coordinator.h`, `src/transaction/tx_coordinator.cpp`) — координатор Two-Phase Commit (2PC) на Core 0. Управляет полным жизненным циклом транзакций: Begin → Execute → Prepare → Commit/Abort, плюс stale transaction cleanup.

## Зачем нужно

В multi-key транзакции ключи могут принадлежать разным ядрам:

```
key A → Core 1
key B → Core 3
key C → Core 2
```

Нужен единый компонент, который:
- создаёт `tx_id` и `snapshot_ts`;
- отслеживает участников (participant cores);
- координирует 2PC: собирает голоса, принимает решение, рассылает finalize;
- управляет stale транзакциями (lease expiration, stuck detection).

Core 0 — естественный кандидат, так как он уже единственная ingress-точка.

## Как работает

### State machine транзакции

```mermaid
stateDiagram-v2
    [*] --> ACTIVE: HandleBegin()
    ACTIVE --> PREPARING: HandleCommit() (есть participants)
    ACTIVE --> COMMITTED: HandleCommit() (нет participants)
    ACTIVE --> ABORTED: HandleRollback()
    ACTIVE --> ABORTED: ReapStaleTransactions() (lease expired)

    PREPARING --> COMMITTED: Все participants → YES
    PREPARING --> ABORTED: Хотя бы один → NO
    PREPARING --> ABORTED: ReapStaleTransactions() (stuck)

    COMMITTED --> [*]: Purge через 60с
    ABORTED --> [*]: Purge через 60с
```

### Структуры данных

```cpp
enum class TxState : uint8_t {
    ACTIVE,      // Транзакция выполняется
    PREPARING,   // Фаза PREPARE: ожидание голосов
    COMMITTED,   // Решение COMMIT принято
    ABORTED,     // Решение ABORT принято
};

struct TxRecord {
    uint64_t tx_id;
    uint64_t snapshot_ts;
    uint64_t commit_ts;
    TxState state;
    std::unordered_set<int> participant_cores;  // Ядра с intent'ами
    Clock::TimePoint created_time;
    Clock::TimePoint last_heartbeat_time;
};

struct PendingPrepare {
    uint64_t client_request_id;   // Для ответа клиенту
    uint64_t tx_id;
    int remaining;                // Осталось голосов
    bool any_no;                  // Есть ли отказ
    Clock::TimePoint created_time;
};

struct PendingFinalize {
    uint64_t client_request_id;
    uint64_t tx_id;
    int remaining;                // Осталось ACK
    bool is_commit;               // commit или abort
    Clock::TimePoint created_time;
};
```

### 2PC message flow

```mermaid
sequenceDiagram
    participant C as Client
    participant TC as TxCoordinator<br/>(Core 0)
    participant P1 as Participant<br/>(Core 1)
    participant P2 as Participant<br/>(Core 3)

    C->>TC: Commit(tx_id)
    Note over TC: state → PREPARING

    par Prepare phase
        TC->>P1: TX_PREPARE_REQUEST
        TC->>P2: TX_PREPARE_REQUEST
    end

    P1->>P1: ValidatePrepare()
    P1->>P1: WAL: PREPARE + Sync()
    P1-->>TC: TX_PREPARE_RESPONSE (YES)

    P2->>P2: ValidatePrepare()
    P2->>P2: WAL: PREPARE + Sync()
    P2-->>TC: TX_PREPARE_RESPONSE (YES)

    Note over TC: Все YES → COMMIT
    TC->>TC: WAL: COMMIT_DECISION + Sync()
    Note over TC: state → COMMITTED

    par Finalize phase
        TC->>P1: TX_FINALIZE_COMMIT_REQUEST
        TC->>P2: TX_FINALIZE_COMMIT_REQUEST
    end

    P1->>P1: WAL: COMMIT_FINALIZE
    P1->>P1: CommitTransaction()
    P1-->>TC: TX_FINALIZE_COMMIT_RESPONSE

    P2->>P2: WAL: COMMIT_FINALIZE
    P2->>P2: CommitTransaction()
    P2-->>TC: TX_FINALIZE_COMMIT_RESPONSE

    TC-->>C: CommitResponse(success=true)
```

### Reaper — очистка stale транзакций

Запускается каждую секунду таймером на Core 0:

```mermaid
flowchart TD
    START["ReapStaleTransactions()"] --> SCAN["Сканировать tx_table_"]

    SCAN --> ACT{"state == ACTIVE &&<br/>now - last_heartbeat > 30s?"}
    ACT -->|Да| ABORT_ACTIVE["state → ABORTED<br/>WAL: ABORT_DECISION<br/>Отправить FINALIZE_ABORT"]
    ACT -->|Нет| PREP{"state == PREPARING &&<br/>now - created > 10s?"}

    PREP -->|Да| ABORT_PREP["Удалить pending_prepare<br/>state → ABORTED<br/>WAL: ABORT_DECISION<br/>Отправить FINALIZE_ABORT"]
    PREP -->|Нет| FIN{"pending_finalize &&<br/>now - created > 10s?"}

    FIN -->|Да| RESEND["Переотправить FINALIZE<br/>Reset created_time"]
    FIN -->|Нет| PURGE{"Terminal state &&<br/>!pending_finalize &&<br/>now - created > 60s?"}

    PURGE -->|Да| REMOVE["Удалить из tx_table_"]
    PURGE -->|Нет| DONE["Готово"]
```

**Таймауты:**

| Параметр | Значение | Описание |
|----------|----------|----------|
| `lease_timeout` | 30с | ACTIVE транзакция без heartbeat |
| `stuck_timeout` | 10с | PREPARING или FINALIZE застряли |
| Purge timeout | 60с | Terminal (COMMITTED/ABORTED) очищается из памяти |

**Sentinel**: `kReaperSentinel = UINT64_MAX` — используется как `client_request_id` для finalize, инициированных reaper'ом. Когда finalize с sentinel завершается, ответ клиенту не отправляется.

### GC watermark

```cpp
uint64_t GetMinActiveSnapshot() const;
// Возвращает минимальный snapshot_ts среди ACTIVE транзакций.
// Используется для MVCC garbage collection:
// версии с commit_ts < watermark можно удалить.
```

### Recovery

```cpp
void LoadRecoveredState(
    std::unordered_map<uint64_t, TxRecord> recovered_tx_table,
    uint64_t next_tx_id,
    uint64_t next_snapshot_ts);
// Загружает состояние координатора из WAL replay.

void ResolveInDoubt(int num_cores);
// Разрешает in-doubt транзакции после crash:
// COMMITTED → отправляет FINALIZE_COMMIT на все ядра
// ACTIVE/PREPARING/ABORTED → отправляет FINALIZE_ABORT на все ядра
```

## Публичный API

```cpp
class TxCoordinator {
public:
    TxCoordinator(Router& router,
                  std::function<void(uint64_t, Task)> resume_fn,
                  WalWriter* wal = nullptr,
                  Clock* clock = nullptr,
                  std::chrono::milliseconds lease_timeout = 30s,
                  std::chrono::milliseconds stuck_timeout = 10s);

    void HandleControl(Task task);
    // Dispatch: BEGIN/COMMIT/ROLLBACK/HEARTBEAT

    void HandleExecute(Task task);
    // TX_EXECUTE_GET/SET: validate tx → add snapshot_ts → route

    void HandlePrepareResponse(Task task);
    // Собирает YES/NO голоса → решение COMMIT/ABORT

    void HandleFinalizeResponse(Task task);
    // Собирает ACK → ответ клиенту

    void ReapStaleTransactions();
    // Очистка stale транзакций (1с таймер)

    [[nodiscard]] uint64_t GetMinActiveSnapshot() const;
    // Watermark для MVCC GC

    void LoadRecoveredState(...);
    void ResolveInDoubt(int num_cores);
};
```

## Связи с другими модулями

| Модуль | Взаимодействие |
|--------|---------------|
| [Core-CoreDispatcher](Core-CoreDispatcher) | Вызывает `HandleControl`, `HandleExecute`, `HandlePrepareResponse`, `HandleFinalizeResponse` |
| [Router](Router) | `RouteTask()` для TX_EXECUTE; `SendToCore()` для PREPARE/FINALIZE |
| [Handlers-GrpcHandler](Handlers-GrpcHandler) | `resume_fn_` возвращает ответ через `ResumeCoroutine()` |
| [WAL](WAL) | `Append()` для TX_BEGIN, COMMIT/ABORT_DECISION |
| [Core-Clock](Core-Clock) | `Now()` для timestamps и lease tracking |
| [Recovery](Recovery) | `RecoverCoordinator()` → `LoadRecoveredState()` → `ResolveInDoubt()` |

## См. также

- [Transaction-Flow](Transaction-Flow) — полный end-to-end flow транзакции
- [Storage-StorageEngine](Storage-StorageEngine) — MVCC store, на котором работают participant cores
- [Execution-KvExecutor](Execution-KvExecutor) — выполнение TX операций на owner core
- [Recovery](Recovery) — восстановление координатора после crash
- [Transaction-Flow](Transaction-Flow) — полный end-to-end flow транзакции
