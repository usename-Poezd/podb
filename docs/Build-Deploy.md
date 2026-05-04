# Build-Deploy — Сборка и запуск

## Зависимости

Проект использует `vcpkg` для управления зависимостями. Если `VCPKG_ROOT` не выставлен, `Makefile` использует `/opt/vcpkg`.

### Основные зависимости (`vcpkg.json`)

| Зависимость | Для чего |
|-------------|----------|
| `gRPC` | Внешний протокол (gRPC-сервер на Core 0) |
| `Protobuf` | Сериализация сообщений |
| `asio-grpc` | Интеграция gRPC с boost::asio awaitable coroutines |
| `concurrentqueue` | Lock-free очереди между ядрами (`moodycamel::ConcurrentQueue`) |
| `Boost` | `asio` (event loop), `program_options` (CLI) |
| `jemalloc` | Аллокатор памяти |
| `gtest` | Тестовый фреймворк (Google Test) |

## Make-таргеты

```bash
make configure          # CMake + vcpkg, экспорт compile_commands.json
make build              # Параллельная сборка (-j nproc), auto-configure если нужно
make test               # Запуск всех GTest-тестов через ctest
make proto              # Регенерация protobuf/gRPC из proto/service.proto
make clean              # rm -rf build/
make docker-build       # Сборка Docker-образа (Ubuntu 24.04 + vcpkg)
make docker-run         # Запуск с --privileged (для CPU affinity)
```

## CLI-аргументы

```bash
./build/db_engine [OPTIONS]
```

| Аргумент | Short | Тип | По умолчанию | Описание |
|----------|-------|-----|--------------|----------|
| `--cores` | `-c` | int | `min(2, hardware_concurrency)` | Количество worker-потоков/ядер |
| `--port` | `-p` | int | `9906` | TCP-порт для gRPC-сервера |
| `--data-dir` | `-d` | string | `./data` | Директория для WAL-файлов и snapshot'ов |
| `--repartition-on-recovery` | — | bool | `false` | Разрешить offline repartitioning при изменении `--cores` |
| `--help` | `-h` | flag | — | Показать справку |

### Примеры запуска

```bash
# Минимальный запуск (2 ядра, порт 9906, data в ./data)
./build/db_engine

# Продакшн-конфигурация
./build/db_engine --cores 8 --port 9906 --data-dir /var/lib/podb/data

# С разрешением repartitioning при смене числа ядер
./build/db_engine --cores 6 --data-dir ./data --repartition-on-recovery
```

## Структура CMake-модулей

| Библиотека | Содержимое | Зависимости |
|-----------|-----------|-------------|
| `db_api` | Generated `.pb.cc` / `.grpc.pb.cc` | gRPC++, protobuf |
| `db_core` | Worker, Task, SlabAllocator, CoreDispatcher, Clock, Types | asio-grpc, concurrentqueue, jemalloc |
| `db_async` | RequestTracker | db_core |
| `db_router` | Router | db_core, db_storage |
| `db_execution` | KvExecutor | db_storage, db_core, db_wal |
| `db_storage` | StorageEngine, VersionedValue | — |
| `db_handlers` | GrpcHandler, ProtoConvert | db_router, db_api, asio-grpc |
| `db_transaction` | TxCoordinator | db_core, db_wal |
| `db_wal` | WalWriter, WalReader, WalRecord, CRC32c | db_core |
| `db_checkpoint` | CheckpointWriter, CheckpointReader | db_wal, db_storage |
| `db_recovery` | RecoveryManager | db_wal, db_checkpoint, db_storage |
| `db_engine` | main.cpp (executable) | все вышеперечисленные + Boost::program_options |

## Тестирование

Проект содержит 13 GTest-исполняемых файлов:

| Тест | Что проверяет |
|------|--------------|
| `storage_mvcc_test` | MVCC: version chains, snapshot isolation, write intents |
| `storage_gc_test` | Garbage collection: watermark, tombstone cleanup |
| `kv_executor_test` | KvExecutor: dispatch GET/SET/TX операций |
| `kv_executor_wal_test` | KvExecutor: интеграция с WAL |
| `wal_record_test` | WalRecord: serialize/deserialize, CRC32c |
| `wal_writer_test` | WalWriter/Reader: append, sync, recovery |
| `checkpoint_test` | Checkpoint: write/read, atomic pattern |
| `recovery_test` | Recovery: WAL replay, snapshot loading |
| `repartition_test` | Repartition: topology change, key redistribution |
| `tx_coordinator_test` | TxCoordinator: 2PC lifecycle |
| `tx_coordinator_wal_test` | TxCoordinator: WAL integration |
| `tx_reaper_test` | Reaper: stale tx cleanup, lease expiration |
| `smoke_test` | End-to-end smoke test |

```bash
make test  # Запуск всех тестов
```

## Docker

```bash
# Сборка образа
make docker-build
# Базовый образ: Ubuntu 24.04, vcpkg at /opt/vcpkg

# Запуск
make docker-run
# --privileged обязателен для pthread_setaffinity_np (CPU pinning)
```

### Почему `--privileged`

`podb` использует `pthread_setaffinity_np()` для привязки каждого worker-потока к конкретному CPU. Без `--privileged` (или `CAP_SYS_NICE`) эта операция завершится с ошибкой.

## Файлы проекта

| Файл | Описание |
|------|----------|
| `CMakeLists.txt` | Главный CMake (C++20, -O3, vcpkg toolchain) |
| `Makefile` | Обёртка над CMake с удобными таргетами |
| `vcpkg.json` | Декларация зависимостей |
| `Dockerfile` | Сборочный образ |
| `.clang-format` | LLVM style, 100-col limit |
| `.clang-tidy` | Статический анализ (clang-analyzer, bugprone, modernize, ...) |
| `.clangd` | Конфигурация LSP (`UnusedIncludes: Strict`) |
| `compile_commands.json` | Симлинк на `build/compile_commands.json` (gitignored) |

## Данные на диске

```
data/
├── core_0.wal          # WAL для Core 0 (координатор + данные)
├── core_1.wal          # WAL для Core 1
├── ...
├── core_N.wal          # WAL для Core N
├── core_0.snap         # Checkpoint для Core 0
├── core_1.snap         # Checkpoint для Core 1
├── ...
└── topology.meta       # Метаданные топологии (num_cores, layout_epoch)
```

## См. также

- [Architecture-Overview](Architecture-Overview) — общая архитектура системы
- [WAL](WAL) — формат WAL-файлов
- [Checkpoint](Checkpoint) — формат snapshot-файлов
- [Recovery](Recovery) — процесс восстановления после crash
