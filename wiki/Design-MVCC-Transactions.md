# Design-MVCC-Transactions — Дизайн транзакционной модели

> Этот документ фиксирует архитектурные решения и обоснования для MVCC, 2PC, OCC и ACID-модели `podb`.

## Целевая формула

```
MVCC for snapshot reads
+ write intents in shard-local storage
+ optimistic validation at PREPARE
+ 2PC coordinated by Core 0
+ WAL for durability
+ snapshot + replay for recovery
```

## Словарь терминов

| Термин | Определение |
|--------|------------|
| **Atomicity** | Либо применяются все операции транзакции, либо ни одна |
| **Consistency** | После COMMIT база в валидном логическом состоянии |
| **Isolation** | Параллельные транзакции не портят друг другу наблюдаемую картину |
| **Durability** | После успешного COMMIT данные переживают crash |
| **Coordinator** | Компонент управления lifecycle distributed transaction (Core 0) |
| **Participant** | Owner-core конкретных ключей, хранит интенты, участвует в PREPARE/COMMIT/ABORT |
| **MVCC** | Multi-Version Concurrency Control — для одного ключа хранится несколько committed-версий, чтение выбирает видимую по `snapshot_ts` |
| **Intent** | Uncommitted версия значения, созданная транзакцией; видна только своей транзакции |
| **OCC** | Optimistic Concurrency Control — валидация конфликтов при PREPARE, не при каждом READ |
| **2PC** | Two-Phase Commit: PREPARE (все проверяют) → COMMIT/ABORT (coordinator решает) |

## Почему именно эта комбинация

- **MVCC** решает snapshot reads и repeatable reads — читатели не блокируют писателей;
- **Intents** позволяют клиенту видеть own writes до COMMIT (read-your-own-writes);
- **OCC** позволяет не превращать каждый READ в жёсткий lock-step — оптимистичное выполнение, проверка только при COMMIT;
- **2PC** нужен для atomicity across cores — multi-key транзакция затрагивает несколько owner-core;
- **WAL** даёт durability;
- **Snapshot + replay** делает recovery быстрым и практичным.

### Различие OCC и 2PC

Они решают разные задачи:

```
OCC: "можно ли этой транзакции закоммититься сейчас?"
     → локальная валидация на каждом participant

2PC: "как добиться, чтобы все участники приняли одно решение?"
     → протокол координации между cores
```

OCC не заменяет 2PC, а 2PC не заменяет validation. Они дополняют друг друга.

## Storage-модель: intents vs write-set overlay

Сделан осознанный выбор: **хранение staged writes прямо в shard-local MVCC storage как intents** (а не отдельный write-set overlay в TxContext).

```
key A:
  [commit_ts=90,  value=10, status=COMMITTED]
  [commit_ts=105, value=12, status=COMMITTED]
  [tx_id=42,      value=15, status=INTENT]
```

**Почему:**
- participant-core видит полный локальный state по ключу;
- проще локальная conflict detection на owner-core;
- легче моделировать recovery и in-doubt transaction;
- близко к Percolator/TiKV-подобной модели.

**Компромиссы:**
- сложнее visibility rules;
- нужен explicit transaction status table (tx_intents_, tx_read_set_);
- cleanup aborted/stale intents — отдельная задача (reaper + GC).

## Правила видимости

### Read-your-own-writes

```
READ(key, tx):
  1. если есть own intent(tx, key) → вернуть его
  2. иначе найти latest committed version ≤ snapshot_ts
```

### Видимость чужих intents

Чужие intents **не видны** обычному чтению:

```
A:
  committed v90 = 10
  intent tx42   = 15

read(A, tx42) → 15      (свой intent)
read(A, tx50) → 10      (чужой intent не виден)
```

Это базовый инвариант всей модели.

## Уровни изоляции

### Read Uncommitted
**Не поддерживается.** Если в storage лежат чужие intents, Read Uncommitted ломает чистую модель видимости. Для transactional KV engine это плохая сделка.

### Read Committed
Каждый statement читает latest committed version **на момент начала statement**, плюс собственные intents. Повторное чтение может вернуть новое значение, если кто-то закоммитил между вызовами.

### Snapshot Isolation (реализовано)
На BEGIN выдаётся один `snapshot_ts`, и **вся транзакция** читает только committed-версии `≤ snapshot_ts`, плюс собственные intents. Повторное чтение всегда возвращает одно и то же.

```
BEGIN tx42 snapshot=100

A:
  v90 = 10, v105 = 20

tx42 reads A → 10
tx42 reads A again → still 10
```

Это естественный основной режим для выбранной архитектуры.

### Serializable (будущее)
Serializable не появляется автоматически из MVCC. Дополнительно нужно ловить `rw`-конфликты:
- read set tracking (уже реализовано: `tx_read_set_`);
- validation на PREPARE (ready для расширения);
- в будущем: predicate/range tracking или SSI-подобная логика.

```
MVCC + intents + OCC + 2PC → Snapshot Isolation
+ rw validation / extra conflict logic → путь к Serializable
```

## Почему координатор на Core 0

Каждый key имеет ровно одного owner-core — это хорошо для single-key операций. Но multi-key транзакция затрагивает несколько cores:

```
key A → Core 1
key B → Core 3
key C → Core 2
```

Кто гарантирует, что все три изменения commit'нутся как одно целое? Нужен coordinator. Core 0 — естественный кандидат: он уже единственная ingress-точка и принимает все внешние RPC.

**Обязанности coordinator:**
- создаёт `tx_id` и `snapshot_ts`;
- отслеживает список participants;
- собирает YES/NO на PREPARE;
- принимает final decision COMMIT/ABORT;
- хранит transaction table;
- выполняет recovery и reaping.

## Разрешение конфликтов

### Главный принцип

Конфликт по конкретному key **всегда решает owner-core** этого key. Coordinator только собирает решения участников:

```
per-key conflict detection = local on owner-core
global tx outcome          = coordinator on Core 0
```

### Write-Write conflict: no-wait policy

Если на key уже есть active intent другой транзакции — немедленный конфликт:

```
Tx42 writes A → intent created
Tx50 writes A → sees foreign intent → immediate WRITE_CONFLICT
```

**Плюсы no-wait:**
- очень просто;
- нет deadlock queueing;
- хорошо подходит проекту.

**Минус:** выше шанс abort under contention.

### Read-Write conflict

Под Snapshot Isolation: чтение идёт по snapshot и игнорирует чужие intents, поэтому достаточно `ww`-conflict detection.

Под Serializable: дополнительно нужно ловить `rw`-конфликты через read set validation на PREPARE.

## Что валидируется при OCC (PREPARE)

На PREPARE каждый participant проверяет:
1. Не конфликтуют ли write intents с другими транзакциями;
2. Все ли локальные intents транзакции на месте (не были вытеснены);
3. Можно ли durably записать PREPARE.

При YES — запись PREPARE в WAL + `fdatasync()` до отправки голоса.

## Клиентский API: почему не batch

Если клиенту нужны промежуточные результаты (read → compute → write), интерфейс должен быть stateful. `ExecuteBatchAtomic` не позволяет видеть результаты промежуточных операций. Поэтому выбран transaction handle:

```python
tx = db.begin_transaction(isolation="SNAPSHOT")
a = db.get(tx.id, "account:A")
db.set(tx.id, "account:A", a - 10)
b = db.get(tx.id, "account:B")
db.set(tx.id, "account:B", b + 10)
db.commit(tx.id)
```

Клиент должен быть готов к retry при optimistic conflict:

```python
while True:
    tx = db.begin_transaction(isolation="SNAPSHOT")
    try:
        # ... operations ...
        db.commit(tx.id)
        break
    except TransactionConflict:
        db.rollback(tx.id)
        continue
```

## Stale-транзакции: типы и обработка

| Тип | Описание | Правило |
|-----|----------|---------|
| ACTIVE expired | Клиент начал tx, но исчез | `lease_timeout` (30с) → coordinator aborts |
| PREPARED stuck | Participant сказал YES, но final decision не дошло | Нельзя просто удалить — resolve through coordinator |
| IN-DOUBT | После crash есть intent/PREPARE, но outcome неясен | Resolve по coordinator decision из WAL |

**Ключевое правило для intents:**

```
cleanup only when tx outcome is known:
  ABORTED   → delete intent
  COMMITTED → promote/finalize
  ACTIVE expired → ask coordinator to abort
  PREPARED → resolve via coordinator / WAL
```

Participant **не должен** удалять intent просто по возрасту — только когда outcome известен.

## Дизайн WAL: почему per-core

Per-core WAL (`core_0.wal`, `core_1.wal`, ...) лучше всего подходит thread-per-core модели:
- один writer на shard — нет contention;
- нет shared mutable log state;
- recovery раскладывается по core;
- один write path: `append → fsync → apply`.

**Что фиксируется:**
- BEGIN tx / transaction metadata;
- INTENT (local operation / intent creation);
- PREPARE;
- coordinator decision COMMIT/ABORT;
- participant finalize COMMIT/ABORT;
- checkpoint metadata.

## Дизайн recovery при смене числа ядер

### Почему обычный recovery ломается

Старые snapshot/WAL записаны при `owner_old = hash(key) % old_num_cores`. При рестарте с новым числом ядер routing меняется — часть ключей должна переехать.

### Поведение по умолчанию: fail-fast

```
stored_num_cores != configured_num_cores
  → refuse normal recovery
  → require --repartition-on-recovery
```

Safest default: иначе можно поднять базу в неверном layout и получить silent corruption.

### Repartition recovery pipeline

```
1. Recover old topology to last consistent state
2. Resolve in-doubt transactions under OLD topology rules
3. Materialize committed state (latest committed per key)
4. Rehash keys: owner_new = hash(key) % new_num_cores
5. Build fresh snapshots/WAL baseline for new topology
6. Start serving traffic
```

**Инварианты:**
1. **Нет live-трафика** до завершения;
2. **Old topology first** — все PREPARE/COMMIT/ABORT разбираются под старым num_cores;
3. **Только committed state** переезжает — не intents;
4. **Fresh durability boundary** — новые snapshot/WAL, не продолжение старых;
5. **Topology metadata** — часть durability contract.

**MVCC history при repartition:** переносится только latest committed version per key. После crash нет живых reader snapshots, history не нужна. Для time-travel/CDC в будущем: `--repartition-preserve-history=true`.

## Roadmap реализации (статус)

| Этап | Описание | Статус |
|------|----------|--------|
| 1 | Transactional API skeleton (proto, tx_id, coordinator) | ✅ Реализовано |
| 2 | MVCC + intents (storage versioning, visibility) | ✅ Реализовано |
| 3 | OCC validation + 2PC (prepare, vote, finalize) | ✅ Реализовано |
| 4 | WAL + recovery (per-core WAL, checkpoint, replay) | ✅ Реализовано |
| 5 | Stale tx management (heartbeat, lease, reaper) | ✅ Реализовано |
| 5.5 | Topology change recovery (repartition) | ✅ Реализовано |
| 6 | Stronger isolation (rw validation → Serializable) | 🔜 Будущее |

## Итоговая модель

```
Client
  → BeginTransaction() → tx_id, snapshot_ts
  → Execute(tx_id, op)
       → Core 0 coordinator
       → owner core participant
       → read committed version or own intent
       → write intent for writes
  → Commit(tx_id)
       → PREPARE on all participants
       → OCC validation
       → all YES ⇒ COMMIT
       → any NO  ⇒ ABORT
       → participants finalize intents
       → WAL guarantees durability
  → Crash?
       → snapshot load
       → WAL replay
       → resolve in-doubt tx
```

## См. также

- [Transaction-TxCoordinator](Transaction-TxCoordinator) — реализация координатора
- [Storage-StorageEngine](Storage-StorageEngine) — реализация MVCC-хранилища
- [Transaction-Flow](Transaction-Flow) — полный flow транзакции
- [Recovery](Recovery) — реализация crash recovery
- [WAL](WAL) — реализация write-ahead log
