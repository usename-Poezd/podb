# podb Wiki

> Thread-per-core in-memory KV engine на C++20 с MVCC, 2PC транзакциями, WAL durability и crash recovery.

## Что такое podb

`podb` — key-value движок, где каждое ядро CPU владеет своей партицией данных. Внешний клиент общается по gRPC, а внутри процесса запросы маршрутизируются по `hash(key) % num_cores` через lock-free очереди.

**Ключевые возможности:**
- Нетранзакционные `Get` / `Set` с hash-based routing;
- MVCC с Snapshot Isolation для транзакционных операций;
- Two-Phase Commit (2PC) для multi-key транзакций;
- Per-core WAL для durability;
- Snapshot checkpointing для быстрого recovery;
- Crash recovery с поддержкой repartitioning при смене числа ядер;
- MVCC garbage collection с watermark.

## Быстрый старт

```bash
make build
./build/db_engine --cores 4 --port 9906 --data-dir ./data
```

Подробнее: [Build-Deploy](Build-Deploy)

## Навигация

### Архитектура
- [Architecture-Overview](Architecture-Overview) — обзор системы, слои, принципы
- [Request-Flow](Request-Flow) — путь нетранзакционного запроса
- [Transaction-Flow](Transaction-Flow) — путь транзакции через 2PC

### Модули: Core
- [Core-Task](Core-Task) — внутренний envelope сообщений (22 типа)
- [Core-Worker](Core-Worker) — runtime ядра (event loop, transport, CPU affinity)
- [Core-CoreDispatcher](Core-CoreDispatcher) — центральный диспетчер Core 0
- [Core-SlabAllocator](Core-SlabAllocator) — generation-based аллокатор слотов
- [Core-Clock](Core-Clock) — абстракция времени
- [Core-Types](Core-Types) — BinaryValue

### Модули: Данные и выполнение
- [Storage-StorageEngine](Storage-StorageEngine) — MVCC-хранилище
- [Execution-KvExecutor](Execution-KvExecutor) — исполнитель операций
- [Router](Router) — маршрутизация по ключам

### Модули: Протокол и async
- [Handlers-GrpcHandler](Handlers-GrpcHandler) — gRPC обработчики
- [Async-RequestTracker](Async-RequestTracker) — корреляция запросов

### Модули: Транзакции
- [Transaction-TxCoordinator](Transaction-TxCoordinator) — 2PC координатор

### Модули: Durability
- [WAL](WAL) — write-ahead log
- [Checkpoint](Checkpoint) — snapshots
- [Recovery](Recovery) — crash recovery

### Справочники
- [gRPC-API](gRPC-API) — полный API reference
- [Build-Deploy](Build-Deploy) — сборка, запуск, Docker

### Дизайн-решения
- [Design-MVCC-Transactions](Design-MVCC-Transactions) — ACID, MVCC, 2PC, OCC, isolation levels
- [Design-Binary-Values](Design-Binary-Values) — почему `vector<byte>`, а не `string`

## Статистика проекта

| Метрика | Значение |
|---------|----------|
| LOC | ~12 600 |
| Исходных файлов | 42 |
| CMake-модулей | 12 |
| gRPC RPC | 7 |
| TaskType вариантов | 22 |
| GTest исполняемых | 13 |
| Язык | C++20 |
