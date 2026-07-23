# Broker position feed v2 — shadow runbook

**Статус:** shadow-only; не заменяет текущий `run_position_sync.py`  
**Сервис:** `broker_position_feed`  
**Entrypoint:** `apps/run_position_feed_v2.py`  
**Store:** `data_root/position_feed/broker_positions.sqlite3`

## 1. Назначение

Новый position feed читает полный сырой snapshot позиций заданного IB account и публикует его в собственную SQLite-БД.

Он намеренно:

- не отправляет ордера;
- не фильтрует позиции по `trading_enabled`;
- не создаёт synthetic `FLAT` для инструментов робота;
- не определяет active/non-active futures contract;
- не импортирует signal, decision или execution packages;
- не пишет в legacy `trade.sqlite3` и `positions_latest`;
- не влияет на текущий trader/execution.

Пустой подтверждённый `COMPLETE` snapshot означает, что IB вернул отсутствие ненулевых позиций по account. Ошибка запроса не создаёт пустой snapshot и не обновляет freshness последнего успешного.

## 2. Deployment model

Каждый account запускает собственную копию сервиса с отдельными:

- `IB_HOST`, `IB_PORT`, `IB_CLIENT_ID`, `IB_ACCOUNT_ID`;
- `IBMD_DEPLOYMENT_ID`;
- `IBMD_DATA_ROOT`;
- health и lock files.

По умолчанию position feed использует `IB_CLIENT_ID + 60`. Offset можно изменить параметром `--client-id-offset`.

## 3. Обязательные environment values

```text
IBMD_DEPLOYMENT_ID=paper-mnq-01
IBMD_DATA_ROOT=C:/path/to/ibmd-data
IBMD_APPLICATION_VERSION=0.1.0-dev

IB_HOST=127.0.0.1
IB_PORT=7497
IB_CLIENT_ID=200
IB_ACCOUNT_ID=U1234567
```

`IBMD_DATA_ROOT` должен быть отдельным для каждого deployment.

## 4. Миграция store

Runtime startup не создаёт и не перестраивает schema.

Сначала выполнить explicit migration при остановленном target position-feed writer:

```bash
python scripts/run_target_migrations.py \
  --manifest migrations/position_feed.v1.json \
  --database <IBMD_DATA_ROOT>/position_feed/broker_positions.sqlite3 \
  --application-version 0.1.0-dev \
  --apply
```

Без `--apply` команда выполняет только dry-run и не создаёт БД.

## 5. Offline validation

Проверка schema и public health/lock без подключения к IB:

```bash
python apps/run_position_feed_v2.py --validate-store-only
```

Успешный результат:

```text
position-feed store is compatible: .../broker_positions.sqlite3
```

## 6. Один shadow snapshot

```bash
python apps/run_position_feed_v2.py --once
```

Команда:

1. подключается к заданному TWS/Gateway;
2. проверяет наличие точного `IB_ACCOUNT_ID` в managed accounts;
3. выполняет `reqPositionsAsync` с timeout;
4. фильтрует только строки выбранного account, сохраняя все его contracts;
5. публикует один атомарный `COMPLETE` snapshot;
6. завершает работу без broker-changing calls.

## 7. Непрерывный shadow mode

```bash
python apps/run_position_feed_v2.py
```

Defaults:

```text
poll interval          = 2 seconds
application poll timeout = 20 seconds
IB position timeout    = 15 seconds
freshness policy       = 10 seconds
client id offset       = 60
```

Параметры доступны через `--help`.

## 8. Public data product

Public read-only views:

```text
public_broker_position_snapshots_v1
public_broker_position_rows_v1
public_broker_position_latest_v1
```

`public_broker_position_latest_v1` указывает только на последний `COMPLETE` snapshot.

Internal `FAILED` poll attempts сохраняются для диагностики, но не сдвигают latest-complete pointer.

Каждая position row содержит:

```text
con_id
local_symbol
symbol
sec_type
exchange
currency
signed_quantity
average_cost
```

Отсутствие row в полном snapshot означает нулевую позицию по contract. Нулевая quantity отдельной строкой не хранится.

## 9. Health и singleton lock

Health:

```text
<IBMD_DATA_ROOT>/runtime/health/broker_position_feed.json
```

Lock:

```text
<IBMD_DATA_ROOT>/runtime/locks/broker_position_feed.lock
```

Readiness semantics:

- `READY` — последний poll успешен;
- `DEGRADED` — текущий poll ошибочен, но последний `COMPLETE` snapshot ещё fresh;
- `BLOCKED` — успешного snapshot нет либо он stale;
- ошибка IB не обновляет `last_success_at` и snapshot timestamp.

Прямой второй запуск того же deployment должен fail fast на service-owned lock.

## 10. Shadow comparison

До включения target execution сравнивать:

- account identity;
- набор `con_id/local_symbol`;
- signed quantities;
- average cost;
- timestamp и completeness;
- поведение при TWS disconnect/reconnect;
- отсутствие fake freshness;
- представление позиций, которые не относятся к текущему роботу.

Legacy `positions_latest` является robot-specific projection, поэтому его строки нельзя сравнивать один-к-одному с raw snapshot. Сначала сравниваются broker facts, а projection проверяется отдельным слоем.

## 11. Ограничения текущего этапа

- Сервис не подключён к launcher/supervisor старого робота.
- Execution и decision пока не читают новый store.
- Windows lock path реализован, но должен быть отдельно прогнан на реальном Windows deployment.
- Live cutover не разрешён только на основании успешного shadow position feed.
- На одном account нельзя одновременно включать old и target execution; position-feed shadow к этому запрету не относится, потому что он не отправляет ордера.
