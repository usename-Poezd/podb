# Transaction-Flow — Путь транзакции

## Что это

Полный end-to-end flow транзакции через 2PC: Begin → Execute → Commit (Prepare → Vote → Finalize).

## Полный sequence diagram

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant GH as GrpcHandler
    participant TC as TxCoordinator
    participant R as Router
    participant KV1 as KvExecutor<br/>(Core 1)
    participant SE1 as StorageEngine<br/>(Core 1)
    participant KV3 as KvExecutor<br/>(Core 3)
    participant SE3 as StorageEngine<br/>(Core 3)

    Note over C,SE3: === Фаза BEGIN ===
    C->>GH: BeginTransaction("SNAPSHOT")
    GH->>TC: TX_BEGIN_REQUEST
    TC->>TC: tx_id=42, snapshot_ts=1000
    TC->>TC: WAL: TX_BEGIN
    TC-->>GH: TX_BEGIN_RESPONSE
    GH-->>C: tx_id=42, snapshot_ts=1000

    Note over C,SE3: === Фаза EXECUTE ===
    C->>GH: Execute(tx=42, GET, "keyA")
    GH->>TC: TX_EXECUTE_GET_REQUEST
    TC->>R: RouteTask (hash("keyA") → Core 1)
    R->>KV1: PushTask → Execute
    KV1->>SE1: MvccGet("keyA", snap=1000, tx=42)
    SE1-->>KV1: found=true, value
    KV1-->>TC: TX_EXECUTE_RESPONSE
    TC-->>GH: response
    GH-->>C: found=true, value

    C->>GH: Execute(tx=42, SET, "keyB", newValue)
    GH->>TC: TX_EXECUTE_SET_REQUEST
    TC->>R: RouteTask (hash("keyB") → Core 3)
    R->>KV3: PushTask → Execute
    KV3->>SE3: WriteIntent("keyB", newValue, tx=42)
    KV3->>KV3: WAL: INTENT
    SE3-->>KV3: OK
    KV3-->>TC: TX_EXECUTE_RESPONSE (success)
    TC->>TC: participant_cores += {3}
    TC-->>GH: response
    GH-->>C: success=true

    Note over C,SE3: === Фаза COMMIT (2PC) ===
    C->>GH: Commit(tx=42)
    GH->>TC: TX_COMMIT_REQUEST
    TC->>TC: state → PREPARING

    Note over TC,SE3: --- Prepare ---
    par
        TC->>KV3: TX_PREPARE_REQUEST (direct-to-core)
    end
    KV3->>SE3: ValidatePrepare(tx=42)
    SE3-->>KV3: can_commit=true
    KV3->>KV3: WAL: PREPARE + Sync()
    KV3-->>TC: TX_PREPARE_RESPONSE (YES)

    Note over TC,SE3: --- Decision ---
    TC->>TC: Все YES → commit_ts=1001
    TC->>TC: WAL: COMMIT_DECISION + Sync()
    TC->>TC: state → COMMITTED

    Note over TC,SE3: --- Finalize ---
    par
        TC->>KV3: TX_FINALIZE_COMMIT_REQUEST(commit_ts=1001)
    end
    KV3->>KV3: WAL: COMMIT_FINALIZE
    KV3->>SE3: CommitTransaction(tx=42, ts=1001)
    Note over SE3: intent → committed version
    KV3-->>TC: TX_FINALIZE_COMMIT_RESPONSE

    TC-->>GH: TX_COMMIT_RESPONSE (success)
    GH-->>C: success=true
```

## Пошаговое описание

### 1. Begin

Клиент вызывает `BeginTransaction`. [TxCoordinator](Transaction-TxCoordinator) на Core 0:
- назначает `tx_id` (автоинкремент);
- назначает `snapshot_ts` (автоинкремент);
- создаёт `TxRecord` со state=ACTIVE;
- пишет `TX_BEGIN` в WAL Core 0;
- возвращает `tx_id` и `snapshot_ts` клиенту.

### 2. Execute

Для каждой операции внутри транзакции:

**GET**: маршрутизируется на owner core → `MvccGet(key, snapshot_ts, tx_id)`:
- Видит собственные intent'ы (read-your-own-writes);
- Видит committed версии ≤ snapshot_ts;
- Не видит чужие intent'ы.

**SET**: маршрутизируется на owner core → `WriteIntent(key, value, tx_id)`:
- Создаёт uncommitted intent;
- Если другая транзакция уже имеет intent → `WRITE_CONFLICT`;
- INTENT записывается в WAL;
- TxCoordinator добавляет ядро в `participant_cores`.

### 3. Commit — Prepare phase

TxCoordinator отправляет `TX_PREPARE_REQUEST` на каждый participant core.

Каждый participant:
1. `StorageEngine.ValidatePrepare(tx_id)` — проверяет, что все intent'ы на месте;
2. WAL: запись PREPARE;
3. `WAL.Sync()` — **fdatasync** обеспечивает durability перед голосованием;
4. Возвращает YES или NO.

### 4. Commit — Decision phase

TxCoordinator собирает голоса:
- **Все YES** → назначает `commit_ts`, пишет `COMMIT_DECISION` в WAL, `Sync()`;
- **Хотя бы один NO** → пишет `ABORT_DECISION` в WAL.

### 5. Commit — Finalize phase

TxCoordinator рассылает `TX_FINALIZE_COMMIT_REQUEST` или `TX_FINALIZE_ABORT_REQUEST`.

**На COMMIT**: participant вызывает `CommitTransaction(tx_id, commit_ts)` — intent'ы промоутятся в committed версии.

**На ABORT**: participant вызывает `AbortTransaction(tx_id)` — intent'ы удаляются.

### 6. Response

Когда все participant'ы подтвердили finalize → ответ возвращается клиенту.

## Rollback

```mermaid
sequenceDiagram
    participant C as Client
    participant TC as TxCoordinator
    participant P as Participants

    C->>TC: Rollback(tx_id)
    TC->>TC: state → ABORTED
    TC->>TC: WAL: ABORT_DECISION
    TC->>P: TX_FINALIZE_ABORT_REQUEST
    P->>P: AbortTransaction(tx_id)
    P-->>TC: TX_FINALIZE_ABORT_RESPONSE
    TC-->>C: success=true
```

## Конфликтные сценарии

### Write-Write конфликт

```
Tx1: WriteIntent("key", "A", tx_id=1) → OK
Tx2: WriteIntent("key", "B", tx_id=2) → WRITE_CONFLICT
     → Execute response: success=false, error="write_write_conflict"
```

Обнаруживается **сразу** при Execute, не при Commit.

### Stale transaction

```
Tx начата → клиент исчез → нет heartbeat → 30с timeout
→ ReapStaleTransactions() → state=ABORTED → FINALIZE_ABORT
```

## WAL-записи в контексте транзакции

| Фаза | Компонент | WAL-запись | Sync? |
|------|-----------|-----------|-------|
| Begin | TxCoordinator (Core 0) | TX_BEGIN | Нет |
| Execute SET | KvExecutor (owner core) | INTENT | Нет |
| Prepare YES | KvExecutor (owner core) | PREPARE | **Да** |
| Decision | TxCoordinator (Core 0) | COMMIT/ABORT_DECISION | **Да** |
| Finalize | KvExecutor (owner core) | COMMIT/ABORT_FINALIZE | Нет |

## См. также

- [Request-Flow](Request-Flow) — flow нетранзакционных запросов
- [Transaction-TxCoordinator](Transaction-TxCoordinator) — детали координатора
- [Storage-StorageEngine](Storage-StorageEngine) — MVCC-механика
- [gRPC-API](gRPC-API) — API транзакционных методов
- [Recovery](Recovery) — восстановление in-doubt транзакций
