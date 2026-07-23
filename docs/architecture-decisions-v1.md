# Архитектурные решения IBMarketData v1

**Статус:** ACCEPTED для проектирования target architecture v1  
**Исходный runtime:** `5073dbea3f96740b4390e113d06578a514918f00`  
**Поведенческий контракт:** `docs/current-system-behavior-contract.md`  
**Рабочая ветка:** `agent/architecture-rewrite`  
**Дата:** 2026-07-23

## 1. Назначение

Этот документ закрывает решения `DEC-001`–`DEC-010`, которые были оставлены открытыми после инвентаризации и characterization текущей системы.

Решения ниже определяют **требуемое поведение и ограничения target architecture**, но ещё не задают имена каталогов, классов, таблиц или transport между компонентами.

Приоритеты решений:

1. безопасность позиции и отсутствие duplicate broker actions;
2. воспроизводимость текущей стратегии без скрытого изменения торговой идеи;
3. явное владение данными и состояниями;
4. переносимость IB-интеграционных компонентов;
5. минимизация неподтверждённой функциональности.

Изменение любого принятого решения требует отдельного ADR с причиной, влиянием на behavior contract, миграцией и тестами. Нельзя незаметно менять эти правила внутри implementation commit.

## 2. Краткая сводка

| ID | Принятое решение |
|---|---|
| `DEC-001` | Daily PnL и daily halt относятся к одной явно идентифицированной стратегии/роботу, а не ко всему IB account |
| `DEC-002` | Стратегические LIMIT-входы удаляются из v1; OPEN/REVERSE/CLOSE выполняются MARKET, protective TP остаётся LIMIT |
| `DEC-003` | Размер позиции — configurable fixed positive integer, default `1`; execution обязан корректно моделировать multi-contract partial fills |
| `DEC-004` | Daily flat сохраняется для включённых futures; normal CME window `14:59:50–16:00 CT`, календарь сессий обязателен |
| `DEC-005` | В v1 используется явный versioned futures contract calendar; открытие только active contract, закрытие — фактически удерживаемого |
| `DEC-006` | Доказанный STOP обязателен, TP опционален; stale local feed не ликвидирует позицию при доказанном live STOP |
| `DEC-007` | Rolling signal algorithm и текущие числовые параметры замораживаются как versioned strategy profile v1 |
| `DEC-008` | Structured logs, immutable domain audit и public health/readiness обязательны; Telegram и plots — заменяемые side-effect sinks |
| `DEC-009` | Emergency liquidation идемпотентна по position episode; повторные broker attempts живут внутри одной liquidation operation |
| `DEC-010` | Active futures contract переключается без restart через двухфазный hot switch; failure блокирует новый risk |

## 3. Общие определения

### 3.1 Strategy identity

Каждый торговый deployment получает устойчивые поля:

```text
strategy_id
strategy_version
deployment_id
account_id
```

Для первого target-профиля:

```text
strategy_id      = IBMarketData.rolling
strategy_version = 1
```

`deployment_id` отличает независимые экземпляры одной стратегии. `account_id` остаётся broker identity и не заменяет `strategy_id`.

### 3.2 Position episode

`position_episode_id` — устойчивый идентификатор непрерывного периода владения позицией конкретной стратегии по инструменту:

```text
FLAT → LONG/SHORT     создаёт новый episode
LONG/SHORT → FLAT     завершает episode
LONG ↔ SHORT          завершает старый и создаёт новый episode после подтверждённого reverse fill
```

Broker position остаётся авторитетным фактом. Position episode — локальная причинная identity, необходимая для commands, protection, PnL и recovery.

### 3.3 Risk-increasing action

Risk-increasing считаются:

- OPEN;
- REVERSE;
- увеличение абсолютного количества позиции.

CLOSE и уменьшение абсолютного количества не являются risk-increasing, но всё равно требуют broker preflight, idempotency и reconciliation.

---

## DEC-001 — Scope daily PnL и daily halt

### Решение

Daily PnL target и daily halt относятся к **одной стратегии в одном deployment**, а не ко всему broker account.

Ключ scope:

```text
account_id + strategy_id + deployment_id + trading_day
```

Для текущего профиля сохраняются:

```text
day timezone = Europe/Moscow
daily target = 500 USD
enabled = true
```

### Что входит в PnL

Realized PnL рассчитывается по executions, принадлежащим strategy/deployment.

Unrealized PnL рассчитывается по broker positions, которые доказанно связаны с незавершёнными position episodes этой strategy/deployment.

В расчёт не включаются автоматически:

- ручные позиции;
- позиции другого робота;
- позиции другого deployment;
- посторонние broker orders без ownership identity.

### Поведение при неоднозначности

Если нельзя доказать полноту execution statistics, ownership позиции или соответствие portfolio item:

```text
PnL state = NOT_READY
OPEN/REVERSE = blocked
existing daily halt = remains blocking
```

Нельзя трактовать ошибку расчёта как нулевой PnL.

### Cleanup

Daily-halt cleanup отменяет и закрывает только strategy-owned orders и positions.

Account-wide risk guard может быть добавлен отдельным компонентом и отдельным ADR. Он не должен скрытно появиться внутри strategy daily take-profit.

### Причина

Текущий код приблизительно реализует scope одного робота. Переход на account-wide semantics без явного решения способен закрыть ручную позицию или позицию другого робота.

### Обязательные тесты

- ниже target → MONITORING;
- target достигнут → persisted TRIGGERED до cleanup;
- посторонняя позиция не входит в strategy PnL и не закрывается;
- incomplete commission/PnL делает monitor NOT_READY;
- prior-day CLOSING остаётся блокирующим после midnight.

---

## DEC-002 — Поддержка LIMIT

### Решение

Target architecture v1 **не поддерживает стратегические LIMIT intents** для OPEN, REVERSE и обычного CLOSE.

Стратегические commands используют:

```text
OPEN    → MARKET
REVERSE → MARKET
CLOSE   → MARKET
```

Исключения относятся к другому домену:

```text
protective TAKE_PROFIT → LIMIT
protective STOP_LOSS   → STOP/STP
```

### Что удаляется из v1 command contract

- `limit_price` для стратегического intent;
- `limit_offset_points`;
- strategic LIMIT TTL;
- strategic LIMIT expiry/cancel loop;
- futures LIMIT cutoff logic;
- legacy terminal mapping LIMIT partial fill → EXECUTED.

Legacy-поля могут быть прочитаны миграционным инструментом для истории, но не являются частью нового runtime API.

### Partial fills

Удаление LIMIT не отменяет partial-fill model. MARKET, STOP и LIMIT TP также могут иметь несколько fills.

Execution model обязан хранить:

```text
requested_qty
filled_qty
remaining_qty
broker_order_status
fill set keyed by execId
commission completeness
```

Terminal outcome определяется broker facts, а не типом order.

### Причина

Текущий rolling trader создаёт только MARKET intents. Сохранять неиспользуемый стратегический LIMIT path с противоречивой partial-fill semantics — архитектурный мусор.

Добавление стратегических LIMIT в будущем требует отдельного ADR и полноценной state machine.

### Обязательные тесты

- strategy command schema не принимает LIMIT;
- legacy LIMIT rows мигрируются в read-only audit;
- partial MARKET fill не теряется и имеет bounded resolution;
- protective TP LIMIT остаётся независимым от strategic command type.

---

## DEC-003 — Position sizing

### Решение

Размер target position задаётся как **configurable fixed positive integer** для пары:

```text
strategy_version + instrument_id
```

Для текущего профиля:

```text
MNQ target_quantity = 1
```

Default для нового инструмента не назначается автоматически. Инструмент без явного target quantity не допускается к trading.

### Не входит в v1

- динамический sizing по equity;
- volatility sizing;
- scaling-in/pyramiding;
- portfolio allocator;
- изменение target quantity внутри открытого position episode.

### Multi-contract semantics

Несмотря на default `1`, execution и recovery моделируют произвольное положительное целое количество.

Правила:

- same-direction signal не увеличивает позицию;
- OPEN из FLAT отправляет target quantity;
- REVERSE delta равна `abs(current_signed_qty) + target_qty`;
- CLOSE использует фактическое broker quantity;
- fractional quantity для текущих futures запрещена;
- partial fill обновляет фактический remaining quantity и не скрывается под полным EXECUTED.

### Причина

Жёстко зашитый один контракт слишком узок для reusable execution, но dynamic sizing сейчас не является проверенной логикой стратегии.

### Обязательные тесты

- default profile открывает один MNQ;
- quantity `0`, negative или fractional отклоняется до broker call;
- reverse `LONG 2 → SHORT 1` отправляет SELL 3;
- partial fill quantity 2/3 остаётся видимым и reconciled;
- same-direction signal не создаёт add-position command.

---

## DEC-004 — Futures daily flat

### Решение

Daily flat включается явно в instrument trading policy.

Для текущего CME equity futures profile сохраняется normal-session policy:

```text
liquidation starts = 14:59:50 CT
no new risk from  = 15:00:00 CT
risk remains blocked through clearing/maintenance window until 16:00:00 CT
```

Daily flat применяется только к инструментам с:

```text
sec_type = FUT
daily_flat_enabled = true
```

### Session calendar

Hardcoded weekday/hour недостаточно.

Target system использует versioned session calendar, который содержит:

- normal sessions;
- holidays;
- early close;
- exceptional closures;
- maintenance windows;
- timezone и DST rules.

Для normal day deadline выводится из session policy. Для early close deadline сдвигается относительно фактического session close.

Если календарь отсутствует, противоречив или устарел:

```text
OPEN/REVERSE = blocked
existing position = operator-visible risk incident
service does not invent a normal session
```

### Liquidation semantics

- service close не зависит от нового signal;
- существующий unresolved close не дублируется;
- protective exits terminally cancel/reconcile до MARKET close;
- `HALTED/FLAT` объявляется только после свежего broker proof;
- failure остаётся blocking и повторяется через ту же liquidation operation.

### Причина

Текущая граница сохраняет стратегическое поведение, но holidays и early close нельзя решать грубой проверкой часа.

### Обязательные тесты

- normal boundary `14:59:49 / 14:59:50 / 15:59:59 / 16:00:00 CT`;
- DST transition;
- holiday closed day;
- early close;
- unresolved daily close suppresses duplicate;
- close failure keeps risk blocked.

---

## DEC-005 — Futures rollover lifecycle

### Решение

Target v1 использует **явный versioned contract calendar** как production source of truth.

Broker discovery может существовать как проверочный/administrative tool, но не меняет production calendar автоматически.

Каждая запись контракта содержит минимум:

```text
instrument_id
con_id
local_symbol
last_trade_date
active_from_utc
active_to_utc
calendar_version
```

Intervals не должны иметь gap или overlap для trading-enabled периода.

### Trading rules

- OPEN/REVERSE разрешены только в active contract;
- active contract вычисляется на момент command, не кэшируется навсегда при startup;
- CLOSE всегда использует фактически удерживаемый `conId/localSymbol`;
- non-active position закрывается до нового входа;
- old contract остаётся resolvable для close, reconciliation и audit;
- market-data history хранит broker contract identity в каждом bar.

### Две позиции в разных contracts

Если у одной strategy одновременно открыты позиции одного logical instrument в двух contracts:

```text
new risk = blocked
state = MULTI_CONTRACT_INCIDENT
positions are not aggregated
```

Liquidation coordinator обрабатывает каждый broker contract отдельно. Нельзя посылать один net close по active contract.

### Calendar update

Новая calendar version проходит:

1. schema validation;
2. отсутствие gap/overlap;
3. broker qualification всех будущих contracts;
4. dry-run active resolution на контрольных timestamps;
5. explicit deployment.

### Причина

Автоматическое broker-derived переключение меняет торговое поведение и создаёт риск ошибочного contract selection. Явный календарь уже является накопленным operational knowledge и должен стать отдельным versioned data product.

### Обязательные тесты

- boundary active interval;
- gap/overlap rejection;
- close старого contract;
- reverse старого contract запрещён;
- two-contract incident блокирует risk и сохраняет обе позиции отдельно.

---

## DEC-006 — Protective policy

### Решение

### STOP

Доказанный broker STOP обязателен для каждого OPEN/REVERSE position episode.

Позиция считается защищённой только когда:

- broker order identity сохранена;
- status входит в утверждённый working set;
- action и quantity соответствуют позиции;
- contract соответствует удерживаемому contract;
- STOP не held until RTH;
- order принадлежит текущему strategy/deployment.

Code `399`, неизвестный status, missing open-order snapshot или timeout не считаются доказанной защитой.

### TP

TAKE_PROFIT опционален на уровне instrument policy.

Для текущего MNQ profile он включён:

```text
stop_loss_points  = 150
take_profit_points = 75
price_tick = 0.25
```

STOP отправляется и подтверждается раньше TP.

Failure TP при доказанном STOP:

```text
position remains protected
TP state = FAILED
alert = required
no automatic emergency close solely because TP is absent
```

### TIF и outside RTH

Target model поддерживает policy per instrument. Текущий profile сохраняет:

```text
protective TIF = DAY
futures STOP outsideRth = true
TP outsideRth = false
```

Изменение TIF на GTC не выполняется без отдельного paper-tested ADR.

### Stale local price feed

Это сознательное изменение текущей optional fail-safe policy.

Если broker STOP доказанно live и корректен:

```text
stale/missing local price feed
→ block new risk
→ critical alert
→ no automatic MARKET liquidation solely because local feed is stale
```

Emergency close требуется, когда:

- STOP отсутствует, rejected, cancelled, held или не может быть доказан;
- stop breach доказан market data, а broker STOP не исполнился;
- protection recovery не может безопасно adopted/replace STOP;
- partially placed protective set невозможно terminally clean up.

### Price distances

Distances и tick normalization берутся из immutable strategy/instrument policy snapshot, сохранённого при создании position episode. Последующее изменение config не пересчитывает silently защиту уже открытой позиции.

### Причина

STOP — safety requirement. TP — profit policy. Смешивать их terminal semantics нельзя. Ликвидировать защищённую позицию только из-за падения локального price feed — лишний broker risk и ложное действие.

### Обязательные тесты

- STOP before TP;
- code 399 rejected as protection;
- TP failure with live STOP does not close position;
- missing STOP → UNPROTECTED → emergency liquidation;
- stale feed + proven STOP blocks risk but does not close;
- stale feed + unproven STOP triggers liquidation path;
- config change does not mutate protection of open episode.

---

## DEC-007 — Signal contract

### Решение

Target v1 воспроизводит текущую rolling strategy как versioned deterministic profile. Архитектурное переписывание не используется для незаметной оптимизации стратегии.

### Frozen profile v1

```text
source bar size                         = 5 seconds
complete BID/ASK required               = true
max complete bar lag                    = 60 seconds
decision pipeline max age               = 30 seconds
rolling step                            = 60 seconds
pattern lookback                        = 90 minutes
potential horizon                       = 30 minutes
historical lookback                     = 365 days
Pearson minimum                         = 0.7
min/max hard-filter maximum ratio       = 1.5
score Pearson weight                    = 1.0
score end-delta weight                  = 1.0
score min/max weight                    = 1.0
potential candidate count               = 7..9
minimum absolute potential end delta    = 30 points
```

Candidate-hour policy v1:

- FUT: no signal calculation at CT hours `15` and `16`; otherwise historical candidates may come from all CT hours;
- CASH: current four broad CT-hour groups are preserved;
- CRYPTO: all hours comparable with all hours.

### Output contract

Signal event contains минимум:

```text
strategy_id
strategy_version
instrument_id
signal_time
source_bar_id
direction
entry_reference_price
potential metrics
configuration_hash
calculation_id
```

Deduplication key:

```text
strategy_id + strategy_version + instrument_id + source_bar_id
```

### Plot

Plot является asynchronous observability artifact:

- не участвует в signal decision;
- failure не отменяет signal;
- generation можно отключить;
- plot связывается с `calculation_id`.

### Retention

Signal event, который породил command, хранится не меньше соответствующего execution audit.

Непоторговые calculation artifacts и plots могут архивироваться по deployment policy; default plot retention — `7` дней.

### Причина

Сначала нужна эквивалентная архитектура. Изменение thresholds одновременно с переписыванием сделает невозможным понять, сломалась архитектура или изменилась стратегия.

### Обязательные тесты

- deterministic result на frozen dataset;
- incomplete/stale bar не создаёт signal;
- duplicate source bar не создаёт второй event;
- config hash меняется при изменении profile;
- plot failure не влияет на signal event.

---

## DEC-008 — Observability

### Решение

Observability разделяется на четыре независимых продукта.

### 1. Structured operational logs

Каждая запись содержит минимум:

```text
timestamp_utc
service
instance_id
strategy_id/deployment_id when applicable
account_id when applicable
severity
event_code
correlation_id
human_message
structured fields
```

### 2. Immutable domain audit

Durable audit обязателен для:

- signal decisions;
- trade commands;
- broker submissions;
- order/fill/commission facts;
- position episodes;
- protective orders;
- liquidation operations;
- daily risk state;
- operator overrides.

Runtime service не удаляет execution/risk audit автоматически. Архивация — отдельная administrative operation.

### 3. Public health/readiness

Каждый long-running service публикует narrow contract:

```text
liveness
readiness
last_success_at
source freshness
dependency status
blocking reason
version/config hash
```

Launcher/orchestrator не читает внутренние domain tables и не импортирует service packages.

Transport health contract определяется на этапе component architecture; semantics уже фиксированы здесь.

### 4. Notification/artifact sinks

Telegram, deal messages и plots являются заменяемыми adapters.

Правила:

- notification failure не откатывает broker/DB state;
- критическое событие сначала persist-ится в audit, потом отправляется;
- repeated alerts deduplicate/throttle по event identity;
- секреты не попадают в logs/audit;
- deal notification отправляется только по committed execution fact.

### Минимальная operational policy v1

```text
critical/error log retention = deployment-controlled, minimum 30 days
execution/risk audit          = no automatic deletion
plot retention                = 7 days default
health freshness              = explicit per service
```

### Причина

Текущий Telegram и launcher слишком близко связаны с runtime internals. Observability должна сообщать о системе, а не управлять её бизнес-транзакциями.

### Обязательные тесты

- Telegram outage не ломает reconciliation;
- launcher определяет readiness без SQL;
- secret redaction;
- duplicate alert throttling;
- audit event существует до notification attempt.

---

## DEC-009 — Emergency close idempotency

### Решение

Emergency liquidation — отдельная persisted operation, а не новый timestamp-based trade intent на каждый watchdog tick.

Stable operation key:

```text
account_id
+ strategy_id
+ deployment_id
+ instrument_id
+ position_episode_id
+ target_state(FLAT)
```

Reason не входит в uniqueness key. Несколько triggers прикрепляются к одной operation:

```text
MISSING_STOP
STOP_REJECTED
STOP_BREACHED
DAILY_HALT
ROLLOVER
MANUAL_EMERGENCY
```

### State machine

```text
REQUESTED
→ PREPARING
→ CANCELING_EXITS
→ SUBMITTING
→ LIVE
→ RECONCILING
→ SUCCEEDED

terminal alternatives:
FAILED_RETRYABLE
FAILED_OPERATOR_REQUIRED
CANCELLED_AS_ALREADY_FLAT
```

Все nonterminal states блокируют создание второй liquidation operation для того же position episode.

### Attempts

Повторный broker order допускается только как новый `attempt_no` внутри той же operation, когда одновременно доказано:

1. предыдущий broker order terminal no-fill/failed/cancelled;
2. нет другого live/unknown order этой operation;
3. свежая broker position всё ещё открыта;
4. exits снова reconciled/cancelled;
5. remaining quantity рассчитана из broker fact.

Unknown outcome не разрешает retry.

Partial fill остаётся в той же operation. Следующая попытка закрывает только fresh broker remaining quantity.

### Already flat

Если fresh broker snapshot показывает FLAT:

```text
operation → CANCELLED_AS_ALREADY_FLAT or SUCCEEDED
no broker order is sent
```

### Crash recovery

Operation и attempt identity persist-ятся до broker wait. Broker `order_ref` включает operation id и attempt number.

Recovery ищет order/fills по:

- broker order id;
- stable order ref;
- execution ids.

### Причина

Characterization доказал, что текущий timestamp входит в intent uniqueness и позволяет создать два uncertain emergency close. Это способно открыть обратную позицию.

### Обязательные тесты

- repeated trigger создаёт одну operation;
- unknown first attempt suppresses second submission;
- terminal no-fill разрешает attempt 2;
- partial fill retry использует remaining quantity;
- restart восстанавливает operation по order ref;
- already FLAT не отправляет close;
- concurrent daily-halt и missing-stop triggers объединяются в одну operation.

---

## DEC-010 — Active contract hot switch

### Решение

Market-data process обязан менять active futures contract без restart.

Execution также не может полагаться на contract, выбранный при process startup.

### Двухфазный switch

#### Phase A — prepare

До `active_from_utc` нового contract:

1. calendar resolver публикует upcoming contract;
2. broker contract квалифицируется;
3. недостающая history загружается;
4. realtime subscription нового contract запускается параллельно;
5. freshness и schema readiness доказываются.

#### Phase B — activate

В effective timestamp:

1. resolver atomically меняет active contract version;
2. signal input переключается только на complete fresh bars нового contract;
3. OPEN/REVERSE используют только новый active contract;
4. old subscription может жить grace period для reconciliation/diagnostics;
5. old contract остаётся доступен для CLOSE фактической позиции.

### Failure

Если новый contract не готов:

```text
new risk = blocked
service readiness = DEGRADED/BLOCKED
old contract is not silently treated as active
existing old-contract position remains closeable
critical alert = required
```

### Data continuity

Logical price series не скрывает contract identity. Каждый bar хранит `con_id/local_symbol`.

Signal policy на boundary должна явно определить, разрешено ли rolling window смешивать два contracts. Для v1:

```text
live signal window may not cross contract boundary
```

После switch signal ждёт достаточное новое rolling window нового contract. Исторический candidate search может использовать старые contracts как нормализованную logical history, потому что каждый row сохраняет provenance.

### Причина

Restart как rollover mechanism создаёт operational gap и скрытую зависимость deployment от календаря. Hot switch нужен для корректного самостоятельного сервиса market data.

### Обязательные тесты

- prepare next contract;
- atomic activation at boundary;
- no live window across boundary;
- failed preparation blocks risk;
- old position closes по old contract после switch;
- repeated switch event idempotent;
- process restart до/после boundary восстанавливает ту же active version.

---

## 4. Frozen strategy profile v1

Следующие значения являются отправной точкой parity implementation, а не универсальными defaults для всех роботов.

```text
strategy_id: IBMarketData.rolling
strategy_version: 1
trading instrument: MNQ
strategic order type: MARKET
fixed target quantity: 1
daily PnL target: 500 USD
trading day timezone: Europe/Moscow
normal futures daily flat: 14:59:50–16:00 CT
MNQ price tick: 0.25
MNQ take profit: 75 points
MNQ stop loss: 150 points
protective TIF: DAY
futures STOP outside RTH: true
```

Signal profile указан в `DEC-007`.

Другие инструменты из старого `contracts.py` не становятся trading-enabled автоматически. Их contract metadata и price history могут быть мигрированы, но включение trading требует отдельной instrument policy и characterization.

## 5. Явные изменения относительно current runtime

Следующее намеренно не воспроизводится один в один:

1. strategic LIMIT path удаляется;
2. emergency close получает stable operation identity;
3. stale local price feed не закрывает позицию при доказанном live STOP;
4. market-data rollover не требует restart;
5. session/calendar uncertainty блокирует risk вместо использования грубого normal-day assumption;
6. signal/trade/risk audit не удаляется вместе с короткоживущими plots;
7. daily PnL scope получает явную strategy/deployment identity;
8. partial fills получают формальную quantity model.

Эти изменения должны иметь target tests и migration notes. Они не могут быть объявлены «рефакторингом без изменения поведения».

## 6. Следствия для component architecture

Target architecture обязана предоставить отдельные capabilities:

- instrument master;
- versioned futures contract calendar;
- session calendar;
- broker connectivity/gateway;
- raw broker position feed;
- strategy-owned position projection и position episodes;
- canonical market-data writer;
- signal calculation;
- decision/risk admission;
- command store;
- execution/reconciliation;
- protective-order management;
- liquidation operations;
- strategy daily PnL/halt;
- public health/readiness;
- audit and notification adapters;
- explicit migration tooling.

Это список responsibilities, а не требование создать отдельный process на каждый пункт.

## 7. Критерий закрытия DEC-этапа

`DEC-001`–`DEC-010` считаются закрытыми для target architecture v1.

Следующий документ должен определить:

```text
component boundaries
direction of dependencies
single-writer data ownership
public contracts
process topology
failure isolation
migration seams from current runtime
```

До этого runtime packages не создаются и production code не переносится.
