# Design-Binary-Values — Дизайн бинарных значений

> Этот документ фиксирует архитектурные решения по поддержке бинарных значений в `podb` без смены индекса `std::unordered_map`.

## Итоговый выбор

```
key   = std::string                    (строковый, без изменений)
value = std::vector<std::byte>         (явный бинарный тип)
index = std::unordered_map             (пока без изменений)
wire  = protobuf bytes                 (бинарный wire-формат)
```

## Почему `std::string` для value не подходит

Хотя технически `std::string` может хранить `\0` и произвольные байты, в кодовой базе он создаёт проблемы:

### Семантическая проблема

`std::string value` визуально выглядит как текст. Разработчик неизбежно начинает писать:

```cpp
value.c_str()
printf("%s", value.c_str())
```

Для бинарного payload это неверная mental model.

### Проблема логов

Логирование `value.c_str()` для binary значений:
- некорректно по смыслу;
- обрезает вывод на первом `\0`;
- ломает терминал мусорными байтами.

### Проблема доменной модели

```
"на wire это bytes, внутри это string, но считай что это bytes"
```

Скрытое правило вместо явного типа. Нежелательный компромисс.

## Почему ключи остаются строковыми

Routing строится на `std::hash<std::string>{}(key) % num_cores`. Смена типа ключа потащит за собой:
- router hashing semantics;
- protobuf schema;
- клиентский API;
- совместимость.

Задача касается **значений**, а не key space. Минимальный дифф и нулевой архитектурный риск.

## Выбор типа: `std::vector<std::byte>`

```cpp
using BinaryValue = std::vector<std::byte>;
```

**Плюсы:**
- бинарная семантика очевидна на уровне типа;
- contiguous storage;
- стандартный контейнер с полной поддержкой STL;
- совместим с будущими arena/blob-handle redesign;
- не требует сложной ownership-системы.

### Почему не custom container

Позже может понадобиться own wrapper, blob handle, arena-backed storage, small-buffer optimization. Но сейчас это преждевременное усложнение. Цель этапа — **типобезопасность**, не перепроектирование хранилища.

## Protobuf / wire boundary

Protobuf `bytes` в generated C++ всё равно представлен как `std::string` — это **transport detail**, а не доменная модель:

```
protobuf/generated layer → std::string (transport)
domain model → BinaryValue (explicit binary type)
```

### Конвертация — только на boundary

```cpp
// src/handlers/proto_convert.h
inline BinaryValue FromProtoBytes(const std::string& bytes);
inline std::string ToProtoBytes(const BinaryValue& value);
```

Нельзя размазывать конвертацию по StorageEngine, KvExecutor, Task, Router. Она живёт **только** в GrpcHandler / protocol boundary.

## Hot-path: минимизация копий

### Write path

```
protobuf request bytes
  → GrpcHandler: конвертация один раз → BinaryValue
  → move into Task
  → move into StorageEngine
```

### Read path

```
StorageEngine returns BinaryValue
  → move into response Task
  → GrpcHandler: serialize один раз → protobuf bytes
```

Фокус: **minimize copies, not obsess over container name.** Главные риски — лишние временные объекты и повторные конвертации, а не сам тип контейнера.

## Правила логирования

Для binary values:
- логировать **размер**, не содержимое;
- опционально: hex preview первых N байт;
- **никогда** не печатать raw string.

```
SET key="blob:1" size=128
GET key="blob:1" found=yes size=128
```

## Инварианты

1. **BinaryValue is opaque** — никакой код вне протокольного слоя не трактует value как UTF-8 / C-string;
2. **BinaryValue may contain any byte sequence**, включая `0x00`;
3. **Logging must not print value as text** by default;
4. **Key routing remains string-based** and unchanged;
5. **Wire format uses protobuf `bytes`** for value fields.

## Почему `std::unordered_map` остаётся

Текущая задача — явно поддержать binary values, не перепроектируя storage engine:
- текущая простота кода;
- минимальный diff;
- понятное поведение Get/Set;
- нет необходимости сейчас проектировать arena/blob-handle/index redesign.

### Чего осознанно не делаем

- flat hash map;
- custom allocator for long-lived blobs;
- segmented blob arena;
- inline-small-value optimization;
- zero-copy binary handles;
- small-vector / rope / chunked value storage.

Всё это — другой этап оптимизационной эволюции.

## Ограничения подхода

### Что даёт
- типовая чистота;
- явная бинарная семантика;
- минимальный архитектурный риск;
- хорошая совместимость с текущим кодом.

### Что не решает
- zero-alloc hot path;
- arena-backed storage;
- cheap MVCC version sharing;
- минимизация аллокаций под большие blobs.

Это **корректный и чистый baseline**, а не финальная high-performance blob architecture.

## Совместимость с MVCC / WAL

Выбранный тип совместим с транзакционной моделью:

```cpp
struct VersionedValue {
  uint64_t commit_ts;
  BinaryValue value;
  uint64_t tx_id;
  bool is_intent;
  bool is_deleted;
};
```

WAL сериализует binary payload как raw bytes + length. Checkpoint serialization проектировалась под arbitrary bytes с самого начала.

## Риски и митигации

| Риск | Митигация |
|------|-----------|
| Больше аллокаций, чем со string (нет SSO) | Осознанная цена за явную модель; оптимизировать позже по profiling |
| Конвертации размажутся по коду | Держать конвертацию строго в protocol boundary helpers |
| Потеря observability без логов payload | Логировать размер и controlled hex preview |

## Итоговая формула

```
Было:    unordered_map<string, string>  — text-biased
Сейчас:  unordered_map<string, BinaryValue>  — binary-first
Будущее: flat index + blob handles + arena — production-grade

Принцип: correct binary semantics first, performance redesign later if needed
```

## См. также

- [Core-Types](Core-Types) — определение `BinaryValue`
- [Handlers-GrpcHandler](Handlers-GrpcHandler) — протокольная граница (конвертация)
- [Storage-StorageEngine](Storage-StorageEngine) — хранение `BinaryValue` в MVCC-версиях
