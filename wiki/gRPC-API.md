# gRPC API — Справочник по API

## Что это

Файл `proto/service.proto` определяет единственный gRPC-сервис `Database` с 7 RPC-методами и 14 типами сообщений. Это полная внешняя API-поверхность `podb`.

## Сервис `Database`

```protobuf
service Database {
    rpc Set (SetRequest) returns (SetResponse);
    rpc Get (GetRequest) returns (GetResponse);
    rpc BeginTransaction (BeginTxRequest) returns (BeginTxResponse);
    rpc Execute (ExecuteRequest) returns (ExecuteResponse);
    rpc Commit (CommitRequest) returns (CommitResponse);
    rpc Rollback (RollbackRequest) returns (RollbackResponse);
    rpc Heartbeat (HeartbeatRequest) returns (HeartbeatResponse);
}
```

## Нетранзакционные операции

### `Set` — Запись ключа

Безусловная запись значения по ключу. Не требует транзакции.

**Запрос:**

| Поле | Тип | Описание |
|------|-----|----------|
| `key` | `string` | Ключ (непустая строка) |
| `value` | `bytes` | Значение (произвольные бинарные данные, может быть пустым) |

**Ответ:**

| Поле | Тип | Описание |
|------|-----|----------|
| `success` | `bool` | `true` — запись прошла успешно |
| `error_message` | `string` | Детали ошибки (только если `success = false`) |

**Семантика:**
- Безусловная запись (без CAS, без версионирования на уровне API);
- Дождётся ответа после выполнения на owner core;
- Запись в WAL обеспечивает durability;
- Нетранзакционная — сразу видна другим клиентам.

---

### `Get` — Чтение ключа

Точечное чтение последней committed-версии.

**Запрос:**

| Поле | Тип | Описание |
|------|-----|----------|
| `key` | `string` | Ключ для чтения |

**Ответ:**

| Поле | Тип | Описание |
|------|-----|----------|
| `found` | `bool` | `true` — ключ существует |
| `value` | `bytes` | Значение (пусто если `found = false`) |

**Семантика:**
- Чтение на текущий момент (point-in-time);
- Возвращает последнюю committed-версию;
- Нетранзакционное — без snapshot isolation;
- Быстрый путь: не пишет в WAL (read-only).

---

## Транзакционные операции

### `BeginTransaction` — Начало транзакции

Создаёт новую транзакцию с выделенным snapshot timestamp.

**Запрос:**

| Поле | Тип | Описание |
|------|-----|----------|
| `isolation_level` | `string` | `"SNAPSHOT"` (по умолчанию). В будущем: `"SERIALIZABLE"` |

**Ответ:**

| Поле | Тип | Описание |
|------|-----|----------|
| `tx_id` | `uint64` | Уникальный ID транзакции |
| `snapshot_ts` | `uint64` | Snapshot timestamp для Snapshot Isolation |
| `success` | `bool` | `true` — транзакция начата |
| `error` | `string` | Детали ошибки (если `success = false`) |

**Семантика:**
- Snapshot Isolation: все чтения внутри транзакции видят консистентный snapshot на `snapshot_ts`;
- Координатор (Core 0) назначает `tx_id` и `snapshot_ts`;
- Транзакция живёт 30 секунд без heartbeat (lease timeout);
- `Heartbeat` продлевает lease.

---

### `Execute` — Выполнение операции внутри транзакции

Выполняет GET или SET в контексте активной транзакции.

**Запрос:**

| Поле | Тип | Описание |
|------|-----|----------|
| `tx_id` | `uint64` | ID транзакции (из `BeginTransaction`) |
| `operation` | `string` | `"GET"` или `"SET"` |
| `key` | `string` | Ключ |
| `value` | `bytes` | Значение (только для SET, пусто для GET) |

**Ответ:**

| Поле | Тип | Описание |
|------|-----|----------|
| `tx_id` | `uint64` | Echo transaction ID |
| `found` | `bool` | Для GET: найден ли ключ |
| `value` | `bytes` | Для GET: значение |
| `success` | `bool` | `true` — операция прошла |
| `error` | `string` | Детали ошибки (если `success = false`) |

**Семантика:**
- **GET**: читает из snapshot на `snapshot_ts`, видит собственные write intents (read-your-own-writes);
- **SET**: создаёт write intent (ещё не committed);
- Множественные Execute накапливают write intents;
- Write-write конфликт обнаруживается сразу (если другая транзакция уже имеет intent на тот же ключ) → `success = false`, `error = "write_write_conflict"`.

---

### `Commit` — Фиксация транзакции

Инициирует 2PC-протокол для атомарной фиксации всех write intents.

**Запрос:**

| Поле | Тип | Описание |
|------|-----|----------|
| `tx_id` | `uint64` | ID транзакции |

**Ответ:**

| Поле | Тип | Описание |
|------|-----|----------|
| `tx_id` | `uint64` | Echo transaction ID |
| `success` | `bool` | `true` — commit успешен |
| `error` | `string` | Причина abort (если `success = false`) |

**Семантика:**
- Two-Phase Commit (2PC):
  1. **Prepare** — координатор отправляет `TX_PREPARE_REQUEST` всем participant cores;
  2. **Validation** — каждый core проверяет свои write intents (OCC);
  3. **Vote** — YES (можно commit) или NO (конфликт);
  4. **Finalize** — если все YES → `TX_FINALIZE_COMMIT_REQUEST`; если хоть один NO → `TX_FINALIZE_ABORT_REQUEST`;
- Атомарность: всё или ничего на всех ядрах;
- Durability: commit decision записывается в WAL до ответа.

---

### `Rollback` — Откат транзакции

Немедленно откатывает все write intents.

**Запрос:**

| Поле | Тип | Описание |
|------|-----|----------|
| `tx_id` | `uint64` | ID транзакции |

**Ответ:**

| Поле | Тип | Описание |
|------|-----|----------|
| `tx_id` | `uint64` | Echo transaction ID |
| `success` | `bool` | `true` — rollback выполнен |

**Семантика:**
- Немедленный abort: все write intents удаляются;
- Идемпотентный: повторный rollback несуществующей транзакции — success.

---

### `Heartbeat` — Продление lease

Продлевает время жизни активной транзакции.

**Запрос:**

| Поле | Тип | Описание |
|------|-----|----------|
| `tx_id` | `uint64` | ID транзакции |

**Ответ:**

| Поле | Тип | Описание |
|------|-----|----------|
| `tx_id` | `uint64` | Echo transaction ID |
| `alive` | `bool` | `true` — транзакция ещё активна; `false` — уже expired/aborted |

**Семантика:**
- Продлевает lease на 30 секунд;
- Без heartbeat reaper на Core 0 автоматически абортит транзакцию после таймаута;
- Полезно для долгих клиентских операций.

---

## Пример использования (псевдокод)

```python
# Простые операции
db.Set(key="user:1", value=b"Alice")
resp = db.Get(key="user:1")
# resp.found = true, resp.value = b"Alice"

# Транзакционный перевод
tx = db.BeginTransaction(isolation_level="SNAPSHOT")
# tx.tx_id = 42, tx.snapshot_ts = 1000

a = db.Execute(tx_id=42, operation="GET", key="account:A")
b = db.Execute(tx_id=42, operation="GET", key="account:B")

db.Execute(tx_id=42, operation="SET", key="account:A", value=encode(decode(a.value) - 100))
db.Execute(tx_id=42, operation="SET", key="account:B", value=encode(decode(b.value) + 100))

result = db.Commit(tx_id=42)
if not result.success:
    # Конфликт → retry с новой транзакцией
    db.Rollback(tx_id=42)
```

## Обработка ошибок

| Ситуация | RPC | Поведение |
|----------|-----|-----------|
| Ключ не найден | Get | `found = false`, `success = true` (не ошибка) |
| Write-write конфликт | Execute (SET) | `success = false`, `error = "write_write_conflict"` |
| Транзакция не найдена | Execute/Commit/Rollback | `success = false`, `error = "tx_not_found"` |
| Транзакция не активна | Execute/Commit | `success = false`, `error = "tx_not_active"` |
| OCC конфликт при prepare | Commit | `success = false`, `error` содержит причину |
| Lease expired | Heartbeat | `alive = false` |

## Типы protobuf

Все value-поля используют `bytes` (не `string`), что означает:
- произвольные бинарные данные;
- могут содержать `\0`;
- на стороне C++ protobuf представлены как `std::string`, но конвертируются в `BinaryValue` на границе протокола.

## См. также

- [Handlers-GrpcHandler](Handlers-GrpcHandler) — обработка RPC-вызовов на стороне сервера
- [Transaction-Flow](Transaction-Flow) — полный flow транзакции через 2PC
- [Request-Flow](Request-Flow) — полный flow нетранзакционного запроса
- [Core-Types](Core-Types) — тип `BinaryValue` для значений
