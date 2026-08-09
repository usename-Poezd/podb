# Execution-KvExecutor — Исполнитель операций

## Что это

`KvExecutor` (`src/execution/kv_executor.h`) — слой локального выполнения операций на owner core. Принимает Task, dispatch'ит по типу, вызывает StorageEngine и WAL, формирует response Task.

## Зачем нужно

Разделение между Router (куда отправить) и KvExecutor (что выполнить) делает каждый слой простым:
- Router не знает логику GET/SET/TX;
- KvExecutor не знает о маршрутизации и transport;
- StorageEngine не знает о Task'ах и WAL.

KvExecutor — это тонкий dispatch-слой, связывающий Task, StorageEngine и WAL.

## Как работает

### Dispatch по TaskType

```mermaid
flowchart TD
    START["Execute(request)"] --> TYPE{"task.type?"}

    TYPE -->|GET_REQUEST| GET["storage_.Get(key)<br/>→ GET_RESPONSE"]
    TYPE -->|SET_REQUEST| SET["storage_.Set(key, value)<br/>→ SET_RESPONSE"]

    TYPE -->|TX_EXECUTE_GET| MVCC_GET["storage_.MvccGet(key, snapshot_ts, tx_id)<br/>→ TX_EXECUTE_RESPONSE"]
    TYPE -->|TX_EXECUTE_SET| MVCC_SET["storage_.WriteIntent(key, value, tx_id)"]
    MVCC_SET --> NOTE["storage_.NoteWriteIntentOutcome(tx_id, result)<br/>→ conflict_streak"]
    NOTE --> WI_OK{"WriteIntent<br/>result?"}
    WI_OK -->|OK| WAL_INTENT["wal_.Append(INTENT)<br/>→ TX_EXECUTE_RESPONSE success"]
    WI_OK -->|WRITE_CONFLICT| CONFLICT["ComputeBackoffMs(streak, request.priority)<br/>→ TX_EXECUTE_RESPONSE<br/>error='write_write_conflict'<br/>retry_after_ms=..."]

    TYPE -->|TX_PREPARE_REQUEST| PREPARE["storage_.ValidatePrepare(tx_id)"]
    PREPARE --> PREP_OK{"can_commit?"}
    PREP_OK -->|Да| WAL_PREP["wal_.Append(PREPARE)<br/>wal_.Sync()<br/>→ TX_PREPARE_RESPONSE yes"]
    PREP_OK -->|Нет| PREP_NO["→ TX_PREPARE_RESPONSE no"]

    TYPE -->|TX_FINALIZE_COMMIT| FIN_COMMIT["wal_.Append(COMMIT_FINALIZE)<br/>storage_.CommitTransaction(tx_id, commit_ts)<br/>→ TX_FINALIZE_COMMIT_RESPONSE"]
    TYPE -->|TX_FINALIZE_ABORT| FIN_ABORT["wal_.Append(ABORT_FINALIZE)<br/>storage_.AbortTransaction(tx_id)<br/>→ TX_FINALIZE_ABORT_RESPONSE"]

    TYPE -->|GC_REQUEST| GC["storage_.GarbageCollect(watermark)<br/>→ GC_RESPONSE"]
```

### Write-ahead guarantee

Для каждой мутации WAL-запись создаётся **до** изменения StorageEngine:

| Операция | WAL-запись | Sync? | Затем StorageEngine |
|----------|-----------|-------|---------------------|
| TX_EXECUTE_SET | INTENT | Нет | WriteIntent() |
| TX_PREPARE (YES) | PREPARE | **Да** (`fdatasync`) | — |
| TX_FINALIZE_COMMIT | COMMIT_FINALIZE | Нет | CommitTransaction() |
| TX_FINALIZE_ABORT | ABORT_FINALIZE | Нет | AbortTransaction() |

`Sync()` вызывается **только при PREPARE** — это гарантирует, что все INTENT записи и сам PREPARE durably на диске перед отправкой YES-vote.

### Формирование response

Каждый response Task наследует из request:
- `request_id` — для корреляции с ожидающим coroutine;
- `reply_to_core` — куда отправить ответ (всегда Core 0);
- `key` — echo обратно;
- `tx_id` — для транзакционных операций.

### Retry-backoff при write-write конфликте

При `WRITE_CONFLICT` executor не просто возвращает ошибку — он подсказывает клиенту, сколько подождать перед повтором:

1. `storage_.NoteWriteIntentOutcome(request.tx_id, result)` — инкрементирует per-tx счётчик подряд идущих конфликтов (`conflict_streak`) на этом owner-core;
2. `ComputeBackoffMs(conflict_streak, request.priority)` (`core/backoff.h`) — экспоненциальный backoff с jitter, обратно пропорциональный `priority` транзакции (см. [[Design-MVCC-Transactions]], раздел «Priority + retry-backoff»);
3. Результат кладётся в `response.retry_after_ms` → доходит до клиента через `ExecuteResponse.retry_after_ms`.

`request.priority` заполняется координатором в `TxCoordinator::HandleExecute` из `TxRecord.priority` перед маршрутизацией — сам KvExecutor приоритет не назначает, только использует.

Это **не меняет** исход самого конфликта — `WriteIntent` остаётся no-wait. Backoff — это только подсказка для клиента, снижающая вероятность систематического livelock, а не гарантия прогресса.

## Публичный API

```cpp
class KvExecutor {
public:
    KvExecutor(StorageEngine& storage, int core_id, WalWriter* wal = nullptr);
    // storage: ссылка на per-core StorageEngine (non-owning)
    // core_id: ID ядра (для логирования)
    // wal: опциональный WAL writer (nullptr = без durability)

    Task Execute(Task request);
    // Dispatch по request.type → вызов StorageEngine/WAL → response Task
};
```

### Логирование

```
[Core 2] EXEC SET "user:1" size=64 → OK reply→Core 0
[Core 1] EXEC GET "user:1" → FOUND reply→Core 0
[Core 3] EXEC TX_SET "key" tx=5 → OK reply→Core 0
[Core 3] EXEC TX_SET "key" tx=7 → CONFLICT reply→Core 0  (retry_after_ms в response, не в логе)
[Core 1] EXEC PREPARE tx=5 → YES
[Core 1] EXEC FIN_COMMIT tx=5 commit_ts=150
[Core 2] GC watermark=100 removed=3
```

Эти строки печатаются через `PODB_VLOG(...)` (`src/core/verbose_log.h`), а не голый `std::printf` — по умолчанию
выключены (флаг `--verbose` у `db_engine`, см. [Build-Deploy § Логирование](Build-Deploy#логирование-verbose)).
Безусловный `printf` на каждый Execute был реальным hot-path overhead'ом (~3% Core 0 CPU по `perf`-профилю), не
просто отладочным шумом.

## Связи с другими модулями

| Модуль | Взаимодействие |
|--------|---------------|
| [Storage-StorageEngine](Storage-StorageEngine) | Вызывает все CRUD и MVCC-методы |
| [WAL](WAL) | `Append()` для INTENT, PREPARE, FINALIZE; `Sync()` при PREPARE |
| [Router](Router) | `local_execute` callback вызывает `Execute()` |
| [Core-Worker](Core-Worker) | Response отправляется через `PushTask()` обратно на Core 0 |

## См. также

- [Storage-StorageEngine](Storage-StorageEngine) — MVCC-хранилище, вызываемое KvExecutor
- [WAL](WAL) — write-ahead log для durability
- [Router](Router) — маршрутизация задач к KvExecutor
- [Core-Task](Core-Task) — все TaskType, обрабатываемые в Execute()
