# Целевая компонентная архитектура IBMarketData v1

**Статус:** ACCEPTED для начала реализации foundation и migration seams  
**Исходный runtime:** `5073dbea3f96740b4390e113d06578a514918f00`  
**Инвентаризация:** `docs/current-system-inventory.md`  
**Поведенческий контракт:** `docs/current-system-behavior-contract.md`  
**Архитектурные решения:** `docs/architecture-decisions-v1.md`  
**Рабочая ветка:** `agent/architecture-rewrite`  
**Дата:** 2026-07-23

## 1. Назначение

Этот документ фиксирует целевые:

- границы компонентов;
- направление зависимостей;
- владельцев данных;
- публичные межкомпонентные контракты;
- process topology;
- failure isolation;
- правила SQLite и транзакций;
- migration seams от текущего runtime.

Документ не является детализацией всех таблиц и классов. Он задаёт ограничения, внутри которых можно начинать писать новую реализацию.

## 2. Главный архитектурный выбор

Target v1 — не микросервисная платформа и не один огромный процесс.

Используется:

```text
несколько изолированных long-running services
+ чистые доменные библиотеки
+ reusable IB adapter
+ отдельное storage каждого writer-а
+ versioned read-only data products между процессами
```

Количество обязательных runtime-процессов сохраняется небольшим:

```text
1. market_data
2. broker_position_feed
3. execution
4. signal
5. decision
6. supervisor/launcher
```

Дополнительно допускаются:

```text
notification/artifact worker — optional
administrative/migration CLI — не long-running runtime service
```

Пять доменных процессов оправданы фактическими разными жизненными циклами и failure domains. Дальнейшее дробление каждой функции в отдельный процесс запрещено без отдельного ADR.

## 3. Deployment model

Один deployment обслуживает один ожидаемый IB account и один экземпляр стратегии.

```text
account_id
strategy_id
strategy_version
deployment_id
```

Несколько счетов запускаются как независимые копии одного deployment package с отдельными:

- `.env`/deployment config;
- `deployment_id`;
- IB port/client-id range;
- data directory;
- logs и health files.

В target v1 не строится multi-account control plane, общий execution service или общая БД для нескольких счетов.

## 4. Неподвижные архитектурные правила

### 4.1 Один изменяемый набор данных — один writer

Ни один SQLite-файл не имеет двух runtime writer-процессов.

Разные таблицы в одном файле не считаются достаточной изоляцией: SQLite блокирует запись на уровне файла.

### 4.2 Межсервисное чтение только через public data product

Consumer может читать owner store только через:

- versioned public table/view;
- отдельный read adapter;
- documented schema и freshness semantics.

Consumer не импортирует storage/repository модуль owner service и не выполняет SQL к внутренним таблицам.

### 4.3 Python-import не пересекает service boundary

Разрешены imports:

```text
service → foundation
service → public contracts
service → pure domain library
service → infrastructure adapter, собранный внутри своего entrypoint
```

Запрещены:

```text
market_data → signal application package
signal → decision application package
position_feed → execution package
execution → position_feed application package
decision → execution repositories
supervisor → любые domain/service packages
```

### 4.4 Runtime startup не выполняет migrations

Service startup:

- проверяет schema version;
- отказывается работать при несовместимости;
- не перестраивает таблицы;
- не удаляет legacy state;
- не выполняет historical cleanup.

Все migrations выполняются отдельной командой при остановленных затрагиваемых writer-сервисах.

### 4.5 SQLite transaction не переживает `await`

Запрещено:

```text
BEGIN
→ broker/network await
→ COMMIT
```

Правильный паттерн:

```text
короткая local transaction
→ commit
→ broker/network operation
→ короткая local transaction
→ commit
```

Crash window между broker call и local commit закрывается stable operation/order identity и reconciliation, а не длинной транзакцией.

### 4.6 Конфигурация передаётся явно

Нет import-time `settings_live`.

Entry point:

1. читает environment/deployment files;
2. валидирует typed settings;
3. загружает versioned strategy/catalog/calendar artifacts;
4. создаёт adapters;
5. передаёт зависимости application service.

Библиотечный import не требует production environment variables.

## 5. Слои и направление зависимостей

```text
foundation
    ↑
public_contracts      catalog/domain policies
    ↑                         ↑
application/domain services   │
    ↑                         │
infrastructure adapters ──────┘
    ↑
entrypoints
```

### 5.1 Foundation

Содержит только технические примитивы:

- UTC/time abstractions;
- stable IDs;
- serialization;
- configuration loading helpers;
- SQLite connection/transaction helpers;
- atomic file replace;
- process lock;
- structured logging interfaces;
- retry/backoff utilities без domain policy.

Foundation не знает:

- MNQ;
- signal;
- position episode;
- TP/SL;
- daily halt;
- Interactive Brokers domain objects.

### 5.2 Public contracts

Чистые versioned DTO/schema definitions:

- market bars;
- broker position snapshots;
- signal events;
- strategy commands;
- execution read models;
- health/readiness;
- audit envelopes.

Public contracts не содержат repositories, network clients или business orchestration.

### 5.3 Domain

Чистая логика и state transitions:

- contract/calendar resolution;
- signal calculation;
- decision table;
- risk rules;
- execution state transitions;
- protection policy;
- liquidation state machine;
- daily halt state machine.

Domain functions получают факты аргументами и не открывают SQLite/IB/Telegram самостоятельно.

### 5.4 Application

Application service:

- читает факты через ports;
- вызывает domain logic;
- координирует short transactions;
- вызывает adapters;
- публикует audit/public read models.

### 5.5 Infrastructure

Adapters:

- Interactive Brokers;
- SQLite;
- filesystem health;
- Telegram;
- plot generation;
- Windows process launching.

Infrastructure реализует domain/application ports, но domain не импортирует infrastructure.

## 6. Reusable IB integration capability

### 6.1 Назначение

IB adapter — независимая библиотека, которую можно перенести в другого робота без импортов IBMarketData strategy packages.

Логические capabilities:

```text
IBConnection
IBAccountAccess
IBClockSampler
IBContractQualifier
IBPositionReader
IBOpenOrderReader
IBExecutionReader
IBOrderGateway
IBOrderMonitor
```

### 6.2 Что IB adapter знает

- IB host/port/clientId/account;
- broker contract fields;
- request timeouts;
- API errors/statuses;
- raw positions/orders/fills;
- connection lifecycle;
- serialization/concurrency внутри одного IB client.

### 6.3 Что IB adapter не знает

- `strategy_id` semantics;
- active quarterly contract policy;
- rolling signal;
- daily target;
- target quantity;
- TP/SL distances;
- position episode;
- `trade_intent` tables;
- robot-specific SQLite paths.

### 6.4 Broker state gateway scope

Один IB client имеет один gateway instance, который сериализует concurrent broker refreshes.

Cache принадлежит конкретной API-session и не публикуется как глобальный source of truth.

После reconnect gateway invalidates:

- positions cache;
- open-order cache;
- execution cache;
- account/portfolio cache;
- clock sample.

## 7. Catalog и versioned policy artifacts

Это library/data capability, а не long-running process.

Разделяются четыре продукта:

```text
instrument master
futures contract calendar
session calendar
strategy/instrument policy
```

### 7.1 Instrument master

Минимум:

```text
instrument_id
sec_type
symbol/trading_class
exchange
currency
multiplier
price_tick
price precision
```

### 7.2 Futures contract calendar

Минимум:

```text
calendar_version
instrument_id
con_id
local_symbol
last_trade_date
active_from_utc
active_to_utc
```

### 7.3 Session calendar

Минимум:

```text
calendar_version
market/session_id
timezone
trading intervals
maintenance intervals
holidays
early closes
exceptional closures
```

### 7.4 Strategy policy snapshot

Минимум:

```text
strategy_id
strategy_version
configuration_hash
instrument_id
target_quantity
signal parameters
protective policy
daily-flat policy
daily-PnL policy
```

Policy snapshot, использованный для открытого position episode, immutable и хранится в execution audit.

## 8. Обязательные runtime services

## 8.1 `market_data`

### Responsibility

```text
IB historical/realtime facts
→ canonical complete BID/ASK bars
→ active-contract hot switch
→ public market-data product
```

### Владеет

- history/realtime subscriptions;
- per-instrument price DB;
- market-data ingestion audit;
- own health file;
- active market-data contract state.

### Читает

- instrument master;
- contract calendar;
- session calendar;
- deployment IB settings.

### Не делает

- не рассчитывает signal;
- не знает trading position;
- не создаёт commands;
- не пишет risk/execution state;
- не определяет target quantity.

### Internal components

```text
history_loader
realtime_collector
bar_assembler
bar_writer
contract_switch_coordinator
market_data_health
```

History и realtime пишут через один serialized writer queue внутри процесса.

### Public products

```text
MarketBarV1
MarketDataInstrumentStatusV1
ActiveMarketDataContractV1
```

## 8.2 `broker_position_feed`

### Responsibility

```text
IB account positions
→ complete raw account snapshot
→ public broker-position product
```

Это независимый reusable service.

### Владеет

- отдельной IB session;
- polling positions;
- raw snapshot DB;
- snapshot completeness/freshness;
- own health file.

### Публикует

Все позиции configured account как broker facts:

```text
account_id
snapshot_id
captured_at
con_id
local_symbol
symbol
sec_type
exchange
currency
signed_quantity
average_cost
```

### Не делает

- не фильтрует по `trading_enabled`;
- не создаёт synthetic robot `FLAT` rows;
- не сопоставляет position со strategy instrument;
- не определяет active/non-active;
- не пишет execution/decision DB;
- не решает, является ли multi-contract state ошибкой.

### Snapshot semantics

Snapshot публикуется атомарно:

```text
header status = WRITING
→ insert all rows
→ header status = COMPLETE
→ update latest_complete_snapshot pointer
```

Отсутствие contract row в подтверждённом COMPLETE snapshot означает zero broker position по этому contract.

Ошибка/outage не меняет timestamp последнего COMPLETE snapshot.

## 8.3 `signal`

### Responsibility

```text
canonical market bars
→ deterministic rolling calculation
→ immutable SignalEventV1
```

### Владеет

- signal calculation DB;
- calculation attempts;
- signal events;
- signal audit;
- plot outbox/artifact references;
- own health file.

### Читает

- market-data public product;
- frozen strategy profile;
- session/contract provenance.

### Не делает

- не читает broker positions;
- не создаёт broker commands;
- не знает daily halt;
- не пишет decision/execution DB;
- plot failure не меняет signal result.

### Public products

```text
SignalCalculationV1
SignalEventV1
SignalServiceStatusV1
```

## 8.4 `decision`

### Responsibility

```text
SignalEventV1
+ strategy position/risk public facts
→ immutable StrategyCommandRequestV1 or explicit no-action/rejection audit
```

Decision отвечает только за обычное стратегическое действие.

Forced liquidation не создаётся decision service.

### Владеет

- consumed signal cursors;
- decision audit;
- rejected/no-action reasons;
- command request outbox;
- own health file.

### Читает

- signal public product;
- execution public strategy-position projection;
- execution public command status;
- execution public risk state;
- execution-session clock/readiness;
- strategy policy.

### Создаёт

```text
OPEN target position
REVERSE target position
optional ordinary strategic CLOSE
```

### Не создаёт

- daily-flat close;
- rollover liquidation;
- daily take-profit liquidation;
- missing-STOP emergency close;
- duplicate retry broker order.

Все forced closes сходятся в execution liquidation coordinator.

### Public products

```text
DecisionRecordV1
StrategyCommandRequestV1
```

Command request immutable после публикации.

## 8.5 `execution`

### Responsibility

Execution — единственный runtime owner broker orders этой strategy/deployment.

```text
strategy command requests
+ broker facts
+ risk policies
→ broker order attempts
→ fills/reconciliation
→ position episodes
→ protective orders
→ liquidation operations
→ daily risk state
```

### Владеет

- отдельной IB execution session;
- command ingestion/state;
- broker order attempts;
- fills и commissions;
- strategy position projection;
- position episodes;
- protective order state;
- liquidation operations;
- daily PnL/halt state;
- execution-session clock state;
- execution audit;
- own health file.

### Internal components

```text
command_ingestor
risk_admission
broker_preflight
order_coordinator
execution_ledger
execution_reconciler
strategy_position_projector
position_episode_manager
protection_manager
liquidation_coordinator
daily_risk_manager
execution_public_read_model
```

Это компоненты одного process, а не отдельные microservices.

### Почему forced liquidation здесь

Daily flat, rollover, daily halt, missing STOP и manual emergency требуют:

- одной очереди broker actions;
- проверки live orders;
- terminal cancellation proof;
- fresh broker position;
- idempotent liquidation state;
- reconciliation после crash.

Размещение этих действий в разных процессах создало бы competing order owners.

### Single broker-action coordinator

Все broker-changing operations проходят через один coordinator:

```text
normal strategy command
protective placement/cancel
forced liquidation
manual emergency operation
```

Coordinator сериализует conflicting actions по:

```text
account_id + strategy_id + deployment_id + instrument_id
```

Долгое ожидание broker result не удерживает SQLite transaction.

### Public products

```text
ExecutionCommandStateV1
BrokerOrderAttemptV1
FillFactV1
StrategyPositionV1
PositionEpisodeV1
ProtectionStateV1
LiquidationOperationV1
DailyRiskStateV1
ExecutionReadinessV1
```

## 8.6 `supervisor`

### Responsibility

- запустить процессы в безопасном порядке;
- проверить singleton/process identity;
- читать public health contracts;
- показывать operator status;
- выполнить controlled stop/restart.

### Не делает

- не импортирует service packages;
- не читает domain SQLite;
- не знает таблицы;
- не принимает trading decisions;
- не выполняет migrations;
- не посылает broker orders.

### Health transport v1

Каждый service атомарно пишет собственный JSON:

```text
data/runtime/health/<service>.json
```

Через temp file + atomic replace.

Supervisor читает только JSON schema `ServiceHealthV1` и process lock/PID metadata.

## 8.7 Notification/artifact worker

Опциональный generic worker.

Читает immutable notification/artifact outboxes разных owner stores read-only.

Владеет собственной delivery DB:

```text
notification_event_id
destination
attempt
status
last_error
sent_at
```

Он не обновляет owner domain rows.

Его outage не влияет на broker/reconciliation state.

## 9. Process topology

```text
                    versioned catalog/policies
                         │
        ┌────────────────┼────────────────────┐
        │                │                    │
        ▼                ▼                    ▼
 market_data      broker_position_feed     execution
        │                │                    │
        │ MarketBarV1    │ RawPositionV1      │ execution/risk/position read models
        ▼                │                    │
      signal             │                    │
        │ SignalEventV1  │                    │
        ▼                └──────────────┐     │
     decision  ◄────────────────────────┴─────┘
        │ StrategyCommandRequestV1
        └───────────────────────────────► execution

 supervisor ── reads only ServiceHealthV1
 notifier   ── reads only notification outboxes
```

## 10. Safe startup order

```text
0. offline schema/catalog/calendar validation
1. market_data
2. broker_position_feed
3. execution
4. signal
5. decision
```

### 10.1 Market-data readiness

До READY:

- active contract version resolved;
- required history present;
- fresh complete BID/ASK bars available;
- contract-switch state valid.

### 10.2 Position-feed readiness

До READY:

- account validated;
- первый COMPLETE raw snapshot published;
- snapshot freshness within policy.

### 10.3 Execution readiness

До READY:

- account validated;
- schema compatible;
- execution broker caches refreshed;
- uncertain commands reconciled;
- live robot orders discovered;
- strategy position projection built;
- missing protection recovered or blocking incident raised;
- active liquidation operations resumed;
- daily risk state loaded;
- execution clock sample healthy enough for risk increase.

Execution запускается до signal/decision, чтобы не создавать команды при неготовом consumer-е.

### 10.4 Signal readiness

- market-data public product compatible;
- source bars fresh;
- strategy profile hash loaded.

### 10.5 Decision readiness

- signal product compatible;
- execution position/risk/readiness products compatible and fresh;
- no unresolved migration state.

## 11. Public contract rules

### 11.1 Versioning

Каждый contract имеет:

```text
schema_name
schema_version
producer_version
created_at_utc
```

Breaking change создаёт новый contract version/table/view. Старый не меняется silently.

### 11.2 Stable identity chain

```text
source_bar_id
→ calculation_id
→ signal_id
→ decision_id
→ command_id
→ execution_operation_id
→ order_attempt_id
→ broker_order_id
→ exec_id
→ position_episode_id
→ liquidation_operation_id
```

Каждый audit/log event содержит доступные correlation IDs.

### 11.3 Freshness

Каждый snapshot/read model имеет явные:

```text
observed_at
published_at
source_status
is_complete
fresh_until or max_age policy
```

Consumer не выводит freshness из file mtime или собственного времени чтения.

### 11.4 Idempotency

Producer определяет stable business key до публикации.

Consumer хранит imported event/command id и безопасно игнорирует повторное чтение той же immutable row.

## 12. Публичные data products

## 12.1 `MarketBarV1`

Минимум:

```text
instrument_id
bar_time_utc
bar_duration_seconds
con_id
local_symbol
bid_open/high/low/close
ask_open/high/low/close
volume/average/bar_count when available
source_kind(history/realtime)
complete
published_at
```

`mid_close` вычисляется reader/domain function, не хранится как независимый факт.

## 12.2 `BrokerPositionSnapshotV1`

Header:

```text
snapshot_id
account_id
captured_at
published_at
status COMPLETE/FAILED
row_count
source_session_id
```

Rows:

```text
snapshot_id
con_id
local_symbol
symbol
sec_type
exchange
currency
signed_quantity
average_cost
```

## 12.3 `SignalEventV1`

```text
signal_id
strategy_id
strategy_version
configuration_hash
instrument_id
source_bar_id
signal_time
direction
entry_reference_price
potential metrics
calculation_id
published_at
```

## 12.4 `StrategyCommandRequestV1`

```text
command_id
strategy_id
strategy_version
deployment_id
instrument_id
source_signal_id
desired_target_side
desired_target_quantity
command_kind OPEN/REVERSE/CLOSE
reason
created_at
expires_at
policy_hash
```

Strategic LIMIT fields отсутствуют.

## 12.5 `StrategyPositionV1`

```text
account_id
strategy_id
deployment_id
instrument_id
position_episode_id
side
quantity
broker contracts[]
projection_status
broker_snapshot_id
updated_at
freshness
```

`projection_status` включает минимум:

```text
FLAT
OPEN
UNKNOWN
STALE
MULTI_CONTRACT_INCIDENT
OWNERSHIP_UNPROVEN
```

## 12.6 `ExecutionCommandStateV1`

```text
command_id
state
requested_qty
filled_qty
remaining_qty
latest_attempt_id
blocking_reason
updated_at
terminal_at
```

## 12.7 `ProtectionStateV1`

```text
position_episode_id
stop_state
stop_order_id
stop_quantity
stop_price
tp_state
tp_order_id
tp_quantity
tp_price
protection_status
last_broker_proof_at
```

## 12.8 `LiquidationOperationV1`

```text
liquidation_operation_id
position_episode_id
target_state FLAT
state
trigger_set[]
current_attempt_no
remaining_quantity
blocking_reason
updated_at
```

## 12.9 `DailyRiskStateV1`

```text
account_id
strategy_id
deployment_id
trading_day
status NOT_READY/MONITORING/TRIGGERED/CLOSING/HALTED
realized_pnl
unrealized_pnl
total_pnl
target
cleanup_status
updated_at
```

## 12.10 `ServiceHealthV1`

```text
schema_version
service
instance_id
pid
liveness
readiness
started_at
last_heartbeat_at
last_success_at
source_freshness_seconds
dependency_status
blocking_reason
application_version
configuration_hash
```

## 13. Data ownership matrix

| Data product/store | Единственный writer | Readers |
|---|---|---|
| instrument master artifacts | administrative deployment tool | все services |
| contract calendar artifacts | administrative deployment tool | market_data, execution |
| session calendar artifacts | administrative deployment tool | market_data, signal, execution |
| price DB per instrument | market_data | signal, execution diagnostics/watchdog if needed |
| raw position snapshot DB | broker_position_feed | execution, supervisor через health only |
| signal DB | signal | decision, notifier |
| decision DB/command outbox | decision | execution, notifier |
| execution DB | execution | decision через public read views, notifier |
| per-service audit tables | corresponding owner service | audit/notifier readers |
| health JSON per service | corresponding service | supervisor/operator tools |
| notification delivery DB | notifier | operator tools |

Запрещён общий `state.sqlite3` и общий `trade.sqlite3` для нескольких writers.

## 14. Physical storage layout v1

```text
data/
  catalog/
    instruments.v1.json
    contracts.<version>.json
    sessions.<version>.json
    strategy.IBMarketData.rolling.v1.json

  market_data/
    MNQ.sqlite3
    ...

  position_feed/
    broker_positions.sqlite3

  signal/
    signal.sqlite3

  decision/
    decision.sqlite3

  execution/
    execution.sqlite3

  notification/
    delivery.sqlite3

  runtime/
    health/
      market_data.json
      broker_position_feed.json
      execution.json
      signal.json
      decision.json
    locks/
      <service>.lock
```

Каждый deployment имеет собственный `data` root.

## 15. SQLite rules

### 15.1 Connection ownership

- writer connection не передаётся между processes;
- background tasks одного service не пишут параллельно напрямую;
- один serialized state writer или application-level write lock;
- readers используют read-only connections где возможно;
- WAL допустим, но не заменяет single-writer ownership.

### 15.2 Transactions

- short and explicit;
- no network await;
- no plotting/Telegram inside transaction;
- no schema migration inside transaction normal runtime;
- broker order identity/state commit immediately after receipt;
- append audit в той же transaction, что и state transition owner-а.

### 15.3 Public views

Owner DB разделяет:

```text
internal_* tables
public_*_v1 tables/views
```

Consumer access проверяется tests и documentation. Breaking public schema создаёт `_v2`.

### 15.4 Retention

Runtime cleanup не удаляет execution/risk audit.

Market bars, plots и verbose calculations имеют отдельную retention/archival policy.

## 16. Execution state ownership

## 16.1 Command ingestion

Execution polling decision public outbox:

1. читает immutable command;
2. проверяет schema/version/expiry;
3. insert-or-ignore по `command_id` в execution DB;
4. публикует imported/rejected state;
5. consumer cursor не является единственным dedup mechanism.

## 16.2 Final risk admission

Decision guard недостаточен для broker action.

Перед risk-increasing submission execution повторно проверяет:

- daily risk state;
- session/calendar;
- execution clock;
- active contract;
- fresh live broker position;
- unresolved command/liquidation;
- position ownership/episode;
- pending commission/PnL policy;
- protective cleanup before position change.

## 16.3 Broker submission

До broker call persist-ятся:

```text
execution_operation_id
order_attempt_id
stable order_ref
requested action/quantity/contract
state = SUBMITTING
```

После broker placement receipt короткой transaction persist-ятся:

```text
broker order id
broker accepted identity
state = LIVE/RECONCILING
```

Если process умирает между call и commit, recovery использует stable `order_ref`.

## 16.4 Position episodes

Execution создаёт/завершает episode только после broker proof/fills и reconciliation.

Raw broker position сам по себе не доказывает ownership. Ownership связывается через:

- strategy/deployment command identity;
- fills/order refs;
- prior episode state;
- broker contract/quantity.

Manual/external divergence создаёт operator-visible incident и блокирует new risk.

## 16.5 Protection

Protection manager:

- привязан к `position_episode_id`;
- STOP first;
- adopted existing broker orders before replacement;
- TP failure не уничтожает proven STOP;
- lost/unproven STOP triggers one liquidation operation;
- protection policy snapshot immutable for episode.

## 16.6 Liquidation

Все triggers объединяются в одну operation per position episode:

```text
daily flat
daily take-profit
rollover
missing/rejected STOP
stop breach
manual emergency
```

Trigger добавляется к operation audit, а не создаёт competing close command.

## 17. Failure isolation

## 17.1 Market-data failure

Результат:

- signal stops producing new events;
- decision cannot create new risk from stale data;
- execution remains alive for broker reconciliation, protection и close;
- proven broker STOP remains authoritative;
- critical health state published.

## 17.2 Position-feed failure

Результат:

- raw public snapshot becomes stale;
- decision new risk blocked through stale strategy projection;
- execution may still perform direct broker preflight/reconciliation with its own IB session;
- outage does not manufacture fresh FLAT.

## 17.3 Signal failure

Результат:

- no new strategy signals;
- decision/execution safety loops continue;
- daily flat/rollover/protection do not depend on signal process.

## 17.4 Decision failure

Результат:

- no new normal strategy commands;
- execution safety loops continue;
- existing commands/reconciliation/protection/liquidation continue.

## 17.5 Execution failure

Результат:

- no new broker actions;
- existing broker STOP/TP continue broker-side;
- supervisor raises critical incident;
- restart must reconcile before READY;
- signal/decision may run but decision must see execution NOT_READY and not publish risk-increasing command.

## 17.6 Notification failure

Результат:

- delivery retry/audit only;
- no rollback of domain/broker state;
- no readiness failure of trading core solely due Telegram.

## 17.7 Calendar/config invalidity

Результат:

- affected service BLOCKED/NOT_READY;
- OPEN/REVERSE blocked;
- existing positions remain closeable where broker contract identity is known;
- no fallback to guessed active/session state.

## 18. Reconnect semantics

Каждый IB-connected service reconnects independently.

После reconnect service:

1. validates account;
2. invalidates all local broker cache;
3. refreshes relevant broker facts;
4. updates own clock sample;
5. reconciles owned state;
6. publishes READY only after proof.

`market_data` дополнительно восстанавливает subscriptions и hot-switch phase.

`broker_position_feed` публикует новый COMPLETE snapshot, а не обновляет timestamp старого.

`execution` не отправляет new risk до order/position/execution reconciliation.

## 19. Singleton и process identity

Каждый required service имеет:

```text
service_name
deployment_id
instance_id
process lock
pid metadata
```

Прямой повторный запуск должен fail fast.

Supervisor lock не заменяет service-owned lock.

## 20. Shutdown semantics

Controlled shutdown:

1. decision stops publishing commands;
2. signal stops new calculations;
3. execution stops accepting new normal commands;
4. execution commits current local state and marks STOPPING;
5. broker live orders не отменяются автоматически только из-за service shutdown;
6. market-data/position-feed stop subscriptions after dependents stopped;
7. health files mark STOPPED.

Forced shutdown recovery покрывается startup reconciliation.

## 21. Proposed repository structure

Runtime implementation создаётся side-by-side со старой системой:

```text
src/ibmd/
  foundation/
  public_contracts/
  catalog/
  ib_gateway/

  market_data/
    domain/
    application/
    adapters/

  position_feed/
    domain/
    application/
    adapters/

  signal/
    domain/
    application/
    adapters/

  decision/
    domain/
    application/
    adapters/

  execution/
    domain/
    application/
    adapters/

  operations/
    health/
    notification/
    supervisor/
    migrations/

apps/
  run_market_data_v2.py
  run_position_feed_v2.py
  run_execution_v2.py
  run_signal_v2.py
  run_decision_v2.py
  run_supervisor_v2.py
```

Названия `_v2` допустимы только на migration period. После cutover старый runtime удаляется, а target entrypoints получают окончательные имена.

Внутри каждого service разрешены только собственные adapters/repositories.

## 22. Forbidden dependency checks

CI должен запрещать минимум:

```text
src/ibmd/ib_gateway imports src/ibmd/execution|signal|decision|market_data
src/ibmd/position_feed imports execution/decision/signal
src/ibmd/signal imports decision/execution adapters
src/ibmd/decision imports execution repositories/adapters
src/ibmd/execution imports position_feed application/repositories
src/ibmd/operations/supervisor imports domain/service packages
```

Также запрещаются:

- `from config import settings_live` в target tree;
- direct import старого `contracts.py`;
- direct path to another service internal DB tables;
- network/SQLite access в domain modules;
- runtime schema mutation.

## 23. Migration strategy

Используется strangler migration. Старый runtime остаётся рабочим до доказанного паритета.

### Phase 0 — baseline

Завершено:

- rollback branch;
- inventory;
- behavior contract;
- characterization suites;
- DEC-001–DEC-010;
- component architecture.

### Phase 1 — foundation and contracts

Создать без production activation:

- typed config;
- IDs/time;
- public DTO schemas;
- SQLite primitives;
- atomic health files;
- import-boundary CI checks.

Gate:

- target imports без `.env`;
- no domain-to-infrastructure dependency;
- contract serialization tests.

### Phase 2 — catalog/calendar extraction

Извлечь из старого `contracts.py`:

- instrument master;
- contract calendar;
- strategy policy;
- session calendar.

Gate:

- exact current MNQ values preserved;
- calendar gap/overlap validation;
- active resolution parity on historical timestamps;
- no target runtime import of old `contracts.py`.

### Phase 3 — raw position feed shadow

Запустить новый `broker_position_feed` параллельно старому position-sync, но он пишет только собственную DB.

Сравнивать:

- account;
- raw contracts;
- signed quantities;
- completeness/freshness;
- outage behavior.

Он не влияет на old trader/execution.

Gate:

- stable paper/live observation period;
- no fake freshness;
- all broker positions represented;
- reusable package has no robot imports.

### Phase 4 — market-data shadow/read parity

Новый market-data сначала пишет shadow DB.

Сравнивать old/new:

- history coverage;
- realtime BID/ASK bars;
- complete-bar semantics;
- contract identity;
- reconnect;
- hot-switch behavior.

Gate:

- deterministic bar parity where sources match;
- no loss at history/realtime seam;
- switch failure blocks readiness.

### Phase 5 — signal shadow

New signal читает target market-data product и пишет shadow signal DB.

Broker commands не создаются.

Сравнивать:

- due calculations;
- candidates;
- potential;
- direction;
- dedup;
- profile hash.

Gate:

- frozen dataset exact parity;
- controlled live shadow parity;
- understood differences only.

### Phase 6 — decision shadow

New decision читает target signals и target execution/position fixtures, пишет command outbox, но execution consumer disabled.

Сравнивать intended OPEN/REVERSE/no-action/rejections с old trader.

Gate:

- risk guards parity/approved changes;
- no duplicate command IDs;
- forced liquidation absent from decision service.

### Phase 7 — execution on paper account

New execution включается только на отдельном paper deployment.

Запрещено одновременное old и new execution на одном account/deployment.

Проверяются:

- order identity/recovery;
- fills/commissions;
- position episodes;
- STOP/TP;
- liquidation operation;
- daily halt;
- reconnect/hard-kill.

Gate:

- all KEEP characterization tests;
- target change tests;
- no duplicate orders;
- no unprotected position;
- successful forced failure drills.

### Phase 8 — full target paper run

Все target services работают вместе.

Минимум:

- multiple full sessions;
- rollover simulation;
- daily-flat simulation;
- midnight daily-halt recovery;
- TWS restart;
- process hard-kills;
- notification outage;
- stale market-data incident.

### Phase 9 — controlled cutover

На выбранном deployment:

1. stop old services;
2. reconcile broker positions/orders;
3. run explicit migration/import tools;
4. start target in safe order;
5. verify READY and FLAT or approved owned position state;
6. enable decision command publication;
7. observe.

### Phase 10 — legacy removal

Только после:

- production observation;
- rollback plan validation;
- no target dependency on old stores/packages;
- audit export complete.

Удаляются:

- old runtime entrypoints/packages;
- old multi-writer schemas;
- transition adapters;
- legacy cleanup code.

## 24. Migration seams

### 24.1 Catalog seam

Reader interface сначала может читать old `contracts.py`, затем versioned artifact. Domain consumers не меняются.

Этот transition adapter временный и запрещён после Phase 2 gate.

### 24.2 Market-data seam

Signal reader port поддерживает:

```text
LegacyPriceReader
TargetMarketDataReader
```

Shadow comparator читает оба.

### 24.3 Position seam

Projection tests получают raw position DTO. Legacy robot-specific snapshot может временно преобразовываться в DTO только для comparison, но target feed никогда не пишет old `positions_latest`.

### 24.4 Signal seam

Decision reader port поддерживает legacy signal view и target `SignalEventV1` during shadow phase.

### 24.5 Command seam

Target execution принимает только `StrategyCommandRequestV1`.

Legacy `trade_intents` импортируются migration tool в read-only historical audit; target execution не polls old command table в production.

### 24.6 Execution audit seam

Historical old executions импортируются с:

```text
legacy_source_id
legacy_trade_intent_id
migration_batch_id
```

Они не создают live commands.

## 25. Rollback rules

Rollback после target broker activity не является `git checkout + old DB restore`.

Порядок:

1. stop command producers;
2. stop target execution after local commit;
3. capture target DB/logs/health;
4. refresh broker positions/orders/executions;
5. reconcile ownership and live protection;
6. decide whether legacy execution can safely start;
7. import/record broker changes made by target or keep legacy disabled;
8. only then switch code.

Blind restore старой trade DB после новых fills запрещён.

## 26. Test architecture

### 26.1 Pure domain tests

- state transitions;
- decision tables;
- calendars;
- quantity maths;
- idempotency keys;
- protection/liquidation policy.

### 26.2 Adapter contract tests

- IB raw DTO mapping;
- SQLite public schemas;
- atomic snapshot publication;
- health JSON;
- config loading.

### 26.3 Service integration tests

- owner DB + application + fake broker;
- crash at every persistence seam;
- duplicate polling;
- reconnect cache invalidation;
- stale source behavior.

### 26.4 Cross-service contract tests

Consumer tests run against producer public schema fixtures, not producer repository modules.

### 26.5 Paper drills

- order submitted then process kill;
- fill before identity commit;
- entry fill then kill before STOP;
- STOP missing/rejected;
- partial fill;
- daily halt through midnight;
- rollover preparation failure;
- duplicate liquidation triggers.

## 27. Architecture acceptance criteria

Реализация target architecture не считается готовой, пока:

1. отсутствуют cyclic imports;
2. IB reusable layer не импортирует robot packages;
3. каждый DB имеет одного runtime writer;
4. нет transaction across `await`;
5. migrations отделены от startup;
6. supervisor не читает domain SQL;
7. raw position feed не содержит robot projection;
8. decision не владеет forced liquidation;
9. execution является единственным broker order owner;
10. liquidation idempotent per position episode;
11. all public contracts versioned;
12. all nonterminal states имеют recovery/operator path;
13. health/readiness public и narrow;
14. characterization/parity gates пройдены;
15. paper hard-kill/reconnect drills пройдены;
16. rollback после broker activity проверен.

## 28. Первый implementation slice

Следующий commit не переносит market-data или execution logic.

Первый вертикальный срез:

```text
foundation
+ typed deployment config
+ public contract envelopes
+ atomic ServiceHealthV1
+ service-owned process lock
+ import-boundary verifier
+ isolated SQLite migration runner skeleton
```

Этот срез:

- не подключается к IB;
- не отправляет orders;
- не меняет old runtime;
- создаёт каркас, на котором следующие services смогут строиться без возврата к global config и shared state.

После него второй implementation slice — versioned catalog/calendar extraction.
