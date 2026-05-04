# Core-Types — Типы данных

## Что это

Файл `src/core/types.h` определяет центральный тип данных для хранения значений во всей системе:

```cpp
using BinaryValue = std::vector<std::byte>;
```

Это единственный тип в файле, но он пронизывает всю архитектуру — от gRPC-протокола до per-core storage engine.

## Зачем нужно

Исходно `podb` хранил значения как `std::string`. Это создавало семантическую двусмысленность:

- `std::string` визуально выглядит как текст, провоцируя `c_str()`, `printf("%s", ...)` и другие текстовые assumptions;
- значения в KV-движке — это произвольные байтовые последовательности, включая `\0`;
- логирование бинарного payload как строки ломает терминал и искажает вывод.

`BinaryValue = std::vector<std::byte>` решает это:

- **явная бинарная семантика** — тип не выглядит как текст;
- **contiguous storage** — данные лежат в непрерывном блоке памяти;
- **стандартный контейнер** — полная поддержка STL (итераторы, `size()`, `data()`);
- **совместимость с будущим** — легко адаптируется под arena/blob-handle redesign.

## Как работает

### Определение

```cpp
// src/core/types.h
#pragma once
#include <cstddef>
#include <vector>

namespace db {
using BinaryValue = std::vector<std::byte>;
}
```

### Инварианты

1. **BinaryValue is opaque** — никакой код вне протокольного слоя не должен трактовать value как UTF-8 / C-string.
2. **BinaryValue может содержать любую байтовую последовательность**, включая `0x00`.
3. **Логирование не должно печатать value как текст** — только размер и опционально hex-preview.
4. **Маршрутизация ключей остаётся строковой** и не затрагивается.
5. **Wire-формат использует protobuf `bytes`** для value-полей.

### Конвертация на границе протокола

Единственное место конвертации между protobuf `bytes` (который в C++ представлен как `std::string`) и `BinaryValue` — обработчик gRPC:

```cpp
// src/handlers/proto_convert.h
inline BinaryValue FromProtoBytes(const std::string& bytes);
inline std::string ToProtoBytes(const BinaryValue& value);
```

## Публичный API

```cpp
namespace db {
    using BinaryValue = std::vector<std::byte>;
}
```

`BinaryValue` — это type alias, у него нет собственных методов сверх стандартного `std::vector<std::byte>`.

## Связи с другими модулями

| Модуль | Где используется |
|--------|-----------------|
| [Core-Task](Core-Task) | `Task::value` — payload запроса/ответа |
| [Storage-StorageEngine](Storage-StorageEngine) | `VersionedValue::value`, `Set()`, `Get()`, `MvccGet()` |
| [Execution-KvExecutor](Execution-KvExecutor) | Передаёт `BinaryValue` между Task и StorageEngine |
| [Handlers-GrpcHandler](Handlers-GrpcHandler) | Конвертация protobuf `bytes` ↔ `BinaryValue` |
| [WAL](WAL) | Сериализация в WAL-записи (INTENT record payload) |
| [Checkpoint](Checkpoint) | Сериализация в snapshot-файлы |

## См. также

- [Core-Task](Core-Task) — внутренний envelope сообщения, где хранится `BinaryValue`
- [Handlers-GrpcHandler](Handlers-GrpcHandler) — конвертация на границе протокола
- [Storage-StorageEngine](Storage-StorageEngine) — хранение `BinaryValue` в MVCC-версиях
- [Design-Binary-Values](Design-Binary-Values) — подробное обоснование выбора типа

