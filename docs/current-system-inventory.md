# Инвентаризация текущей системы IBMarketData

**Статус:** зафиксированное состояние исходной системы, не целевая архитектура  
**Исходный commit:** `5073dbea3f96740b4390e113d06578a514918f00`  
**Рабочая ветка:** `agent/architecture-rewrite`  
**Дата аудита:** 2026-07-22

## 1. Назначение документа

Этот документ фиксирует фактическое устройство IBMarketData перед полным архитектурным переписыванием.

Он отвечает на вопросы:

- какие процессы реально существуют;
- кто и зачем подключается к Interactive Brokers;
- какие данные производит и потребляет каждый процесс;
- какие SQLite-файлы и таблицы используются как межпроцессные контракты;
- где пересекаются зоны ответственности;
- какие торговые и защитные механизмы уже реализованы;
- какие свойства необходимо сохранить;
- какие свойства являются случайными дефектами текущей реализации и переноситься не должны.

Этот документ **не проектирует новую систему**. Любая целевая схема, не выведенная из фактического поведения и подтверждённых требований, на этом этапе была бы фантазией.

## 2. Ограничения аудита

Аудит выполнен статически по исходному коду commit `5073dbe`.

В рамках этого этапа:

- не изменялся runtime-код;
- не запускались реальные сервисы;
- не выполнялось подключение к TWS / IB Gateway;
- не анализировалось содержимое рабочих `state.sqlite3`, `trade.sqlite3` и price DB;
- не выполнялись paper- или live-сделки;
- не проверялось фактическое поведение конкретной установленной версии TWS.

Следовательно, документ различает:

- **подтверждённое кодом поведение**;
- **архитектурный вывод из кода**;
- **поведение, которое ещё нужно подтвердить characterization-тестом или записью реального сценария**.

## 3. Итоговый диагноз

В системе есть значительный объём полезной и местами аккуратно защищённой торговой логики. Это хороший исходный материал.

Но текущая архитектура — не набор независимых сервисов. Это **распределённый монолит**, связанный через:

- глобальные Python-импорты;
- общий `contracts.py`;
- импортируемый при старте глобальный `settings_live`;
- два общих SQLite-файла;
- прямое знание таблиц и путей к БД;
- циклические зависимости между пакетами;
- прямые запросы к одному и тому же broker state из разных подсистем.

Файлы разделены по каталогам и процессы разделены по окнам, но архитектурные границы не совпадают с этими каталогами и процессами.

Текущий код нельзя считать безусловной спецификацией новой системы. В нём одновременно находятся:

1. обязательная бизнес-логика;
2. защитные торговые инварианты;
3. технические обходные решения;
4. последствия прежних рефакторингов;
5. подтверждённые дефекты.

Новая реализация должна воспроизводить пункты 1 и 2. Пункты 3–5 необходимо сначала распознать, а не копировать.

## 4. Точки запуска и процессы

В production-контуре существуют пять долгоживущих процессов и один Windows-лаунчер.

| Точка запуска | IB-соединение | Основная фактическая роль |
|---|---:|---|
| `run_market_data.py` | да, базовый `clientId` | история, realtime BID/ASK, active contract на старте, price DB, readiness инструмента |
| `run_position_sync.py` | да, `clientId + 60` | периодический запрос позиций, проекция на логические инструменты робота, запись `positions_latest` |
| `run_execution.py` | да, `clientId + 40` | исполнение intents, broker preflight, TP/SL, reconciliation, daily take-profit, аварийное закрытие |
| `run_signal.py` | нет | чтение price DB, rolling-анализ, запись `signal_events` |
| `run_trader.py` | нет | преобразование signal events и служебных условий в trade intents |
| `run_wt.py` | нет | запуск и проверка готовности остальных процессов в Windows Terminal |

`run_wt.py` не является владельцем доменной логики, но фактически знает:

- порядок запуска сервисов;
- пути к SQLite;
- имена таблиц;
- поля таблиц;
- допустимый возраст position snapshot;
- формат clock health;
- формат daily guard;
- признаки готовности market-data.

Поэтому launcher сейчас связан не с публичными health-контрактами сервисов, а с их внутренней реализацией.

## 5. Порядок запуска и фактический поток данных

### 5.1 Порядок безопасного запуска

Launcher использует порядок:

```text
market_data
    ↓ readiness price/history
position_sync
    ↓ readiness positions
execution
    ↓ readiness broker/clock/daily monitor
signal
    ↓ process stable
trader
```

Этот порядок разумен как startup policy: execution запускается до появления новых сигналов и intents.

### 5.2 Причинный торговый поток

Причинный поток торгового решения другой:

```text
Interactive Brokers
        ↓ historical/realtime BID/ASK
market_data
        ↓
price DB каждого инструмента
        ↓
signal
        ↓ signal_events
state.sqlite3
        ↓
trader
        ↓ trade_intents
trade.sqlite3
        ↓
execution
        ↓ orders / cancellations / reconciliation
Interactive Brokers
```

Позиции образуют отдельную ветку:

```text
Interactive Brokers
        ↓ reqPositions
position_sync
        ↓ robot-specific positions_latest
trade.sqlite3
        ├────────→ trader decision
        └────────→ launcher readiness
```

При этом execution не доверяет только сохранённому snapshot и перед отправкой ордера снова запрашивает broker positions:

```text
execution
    ↓ fresh broker position preflight
Interactive Brokers
```

Само повторное чтение broker state оправдано безопасностью. Ошибка в том, что execution вызывает функцию `sync_broker_positions_once()`, которая совмещает чтение, robot-specific проекцию и запись в `trade.sqlite3`.

### 5.3 Сквозные защитные ветки

Дополнительно существуют независимые от основного сигнального потока ветки:

```text
IB server time → ib_clock_health → trader clock guard

trade_intents + IB portfolio + positions
    → daily PnL
    → daily_trading_guard
    → блокировка торговли / отмена заявок / закрытие позиций

broker orders + local protective_orders
    → reconciliation / watchdog / emergency close
```

## 6. Инвентаризация сервисов

## 6.1 Market data

### Подтверждённые обязанности

`run_market_data.py`:

- загружает глобальные settings и реестр `Instrument`;
- сбрасывает readiness торгуемых инструментов перед стартом;
- подключается к IB;
- получает server time;
- выбирает active futures contract из ручных UTC-интервалов в `contracts.py`;
- инициализирует price DB;
- последовательно загружает history по включённым инструментам;
- после успешной history запускает realtime по инструменту;
- пишет readiness/error в `state.sqlite3`;
- поддерживает monitor, heartbeat и Telegram status.

### Данные

Пишет:

- `data/prices/<instrument>.sqlite3`;
- таблицу `instrument_state` в `data/state.sqlite3`;
- clock sample в `data/state.sqlite3` через общий IB connector.

Читает:

- `config.py`;
- `contracts.py`;
- IB server time;
- IB historical/realtime market data.

### Существенные свойства

- История и realtime хранятся в одной канонической BID/ASK-таблице.
- `mid_close` физически не хранится и вычисляется при чтении.
- Realtime ASK и BID могут приходить отдельно и UPSERT-ятся без взаимного затирания.
- Для сигнала требуются заполненные BID и ASK.
- Выбор active futures contract выполняется по временному реестру.

### Архитектурные замечания

- Техническая загрузка market data смешана с lifecycle/readiness конкретного торгового робота.
- Active contract строится в startup context и передаётся в realtime как зафиксированное значение. Автоматическое внутрипроцессное переключение на следующую серию в текущем аудите не доказано.
- Универсальный IB market-data слой зависит от robot-specific `contracts.py` и глобального settings-объекта.

## 6.2 Position sync

### Подтверждённые обязанности

`run_position_sync.py` и пакет `ib_position_sync`:

- создают отдельное IB-соединение;
- проверяют ожидаемый account;
- раз в две секунды запрашивают позиции;
- ставят timeout на зависший `reqPositions`;
- не выдают старый cached TWS state за свежий при недоступном backend;
- фильтруют позиции по account;
- сопоставляют broker contracts логическим инструментам робота;
- определяют active/non-active futures contract;
- обнаруживают несколько открытых quarterly contracts одного инструмента;
- создают искусственный `FLAT` для торгуемого инструмента без позиции;
- записывают результат в `positions_latest`.

### Фактические зависимости

`ib_position_sync.position_store` импортирует:

- `config.settings_live`;
- `contracts.Instrument`;
- `ib_execution.broker_state_service`;
- `ib_execution.contract_resolver`;
- `ib_trader.trade_schema`.

Следовательно, это не независимый broker position feed. Это адаптер позиции именно текущего робота.

### Данные

Пишет:

- `positions_latest` в `data/trade.sqlite3`;
- clock sample в `data/state.sqlite3` через общий IB connector.

Читает:

- IB positions;
- account configuration;
- реестр инструментов и квартальных контрактов;
- active contract logic из execution-пакета.

### Архитектурное замечание

Сервис слишком рано превращает факты брокера в решение конкретного робота. Получение всех broker positions, сопоставление с robot instruments, определение rollover и сохранение trading projection должны рассматриваться как разные ответственности.

## 6.3 Signal

### Подтверждённые обязанности

Signal-процесс:

- выбирает инструменты, у которых одновременно включены history, realtime и trading;
- ждёт свежий полный BID/ASK bar;
- работает только в rolling-режиме;
- строит текущее окно;
- ищет исторические candidate windows;
- строит pattern matrix;
- рассчитывает Pearson;
- применяет min/max hard filter;
- ранжирует candidates;
- рассчитывает weighted potential;
- строит diagnostic plot;
- при прохождении порога создаёт LONG/SHORT `signal_event`;
- очищает старые signal events по retention policy.

### Зафиксированные параметры текущей стратегии

- шаг расчёта: 60 секунд;
- pattern back window: 90 минут;
- future trade window: 30 минут;
- минимальный Pearson: 0.7;
- основной history lookback: 365 дней;
- candidate potential: 7–9 кандидатов;
- минимальный абсолютный end delta: 30 points;
- signal retention: 7 дней.

Эти значения являются текущими настройками, но ещё не классифицированы как неизменяемые бизнес-требования.

### Данные

Читает:

- per-instrument price DB;
- `contracts.py` через price/session helpers;
- статическую `DEFAULT_SIGNAL_CONFIG`.

Пишет:

- `signal_events` в `data/state.sqlite3`;
- diagnostic plots;
- логи.

### Архитектурные замечания

- Signal events смешаны в одном физическом файле с readiness, IB clock и daily risk state.
- Стратегический алгоритм, storage и plotting вызываются из одного calculation flow.
- Trader импортирует signal config, поэтому граница signal → trader не является чистым контрактом данных.

## 6.4 Trader

### Подтверждённые обязанности

Trader-процесс раз в секунду:

- читает последние свежие signal events;
- читает сохранённую позицию;
- проверяет freshness позиции;
- не создаёт второй unresolved intent по инструменту;
- не создаёт повторный intent для уже обработанного сигнала;
- строит OPEN при `FLAT`;
- строит REVERSE при противоположной позиции;
- не действует при сигнале в направлении текущей позиции;
- блокирует risk-increasing intent при незавершённой PnL/commission reconciliation;
- блокирует risk-increasing intent при плохом IB clock health;
- учитывает persisted daily trading halt;
- создаёт daily flat intent для futures перед no-trade hour;
- создаёт rollover close intent для позиции на non-active contract;
- пишет новый trade intent и audit event.

### Данные

Читает:

- `signal_events` из `state.sqlite3`;
- `ib_clock_health` из `state.sqlite3`;
- `daily_trading_guard` из `state.sqlite3`;
- `positions_latest` из `trade.sqlite3`;
- текущие/незавершённые `trade_intents`;
- `contracts.py`.

Пишет:

- новые строки и события в `trade_intents`.

### Архитектурное замечание

Название `trader` скрывает несколько разных политик:

- реакция на стратегический сигнал;
- scheduled daily flat;
- rollover liquidation;
- risk admission;
- deduplication;
- формирование execution command.

Это не обязательно должны быть разные процессы, но это разные доменные ответственности и состояния.

## 6.5 Execution

### Подтверждённые обязанности

Execution-процесс:

- создаёт отдельное IB-соединение;
- проверяет account;
- отменяет legacy auxiliary exit orders на старте;
- читает новые trade intents;
- переводит intent по состояниям исполнения;
- перед отправкой ордера получает свежую broker position;
- вычисляет фактическое действие и количество;
- поддерживает MARKET и LIMIT execution paths;
- записывает order submission до ожидания финального результата;
- обрабатывает partial/uncertain outcomes;
- ставит обычные TP/SL;
- reconciles protective orders;
- reconciles uncertain executions;
- дозаполняет commission/realized PnL;
- запускает protective price watchdog;
- при потере защиты может выполнить emergency close;
- рассчитывает daily realized/unrealized PnL;
- persisted daily take-profit halt;
- отменяет pending robot orders после halt;
- закрывает открытые позиции после halt;
- поддерживает recovery после reconnect/restart.

### Данные

Читает и пишет:

- `trade_intents`;
- `protective_orders`;
- `positions_latest` косвенно через `sync_broker_positions_once()`;
- `daily_trading_guard`;
- `ib_clock_health` через общий connector;
- price DB для executable-price watchdog и анализа protective levels.

Читает непосредственно из IB:

- positions;
- portfolio/account updates;
- open orders;
- executions/fills/commission reports;
- order statuses.

Пишет непосредственно в IB:

- entry/reverse/close orders;
- TP/SL;
- cancellations;
- emergency close orders.

### Архитектурное замечание

Execution содержит слишком много координируемых подсистем в одном loop. Особенно перегружен `daily_take_profit.py`: он одновременно рассчитывает PnL, читает позиции, обновляет risk state, блокирует intents, отменяет orders и ликвидирует positions.

## 6.6 Launcher

### Подтверждённые обязанности

`run_wt.py` / `wt_run`:

- создают отдельные вкладки Windows Terminal;
- ведут JSON-status каждого процесса;
- избегают повторного запуска уже managed-процесса;
- запускают сервисы в безопасном порядке;
- напрямую проверяют readiness по SQLite;
- проверяют PID и состояние процесса.

### Архитектурное замечание

Launcher выступает внешним orchestration-слоем, но импортирует внутренние Python-модули домена и знает SQL-схемы. Это не внешний оркестратор, а ещё один участник монолита.

## 7. Хранилища и владельцы записи

## 7.1 Price DB

Физически используется отдельная SQLite-БД на логический инструмент:

```text
data/prices/MNQ.sqlite3
data/prices/MES.sqlite3
...
```

Каноническая таблица содержит:

- UTC/CT/MSK timestamps;
- broker contract;
- BID OHLC;
- ASK OHLC;
- volume/average/bar count.

| Участник | Доступ |
|---|---|
| market-data history | запись |
| market-data realtime | запись |
| signal | чтение |
| execution protective watchdog | чтение |
| repair/cleanup scripts | административная запись |

На уровне основных runtime-сервисов price DB ближе всего к правилу single writer: оба runtime writer-path находятся внутри market-data процесса. Административные scripts всё равно требуют остановленного или контролируемого контура.

## 7.2 `state.sqlite3`

В одном файле находятся несвязанные домены:

| Таблица | Основной writer | Readers |
|---|---|---|
| `instrument_state` | market_data | launcher, readiness helpers |
| `signal_events` | signal | trader |
| `ib_clock_health` | market_data, position_sync и execution через общий connector/heartbeat | trader, launcher |
| `daily_trading_guard` | execution | execution, trader, launcher |

Даже когда разные процессы пишут разные таблицы, SQLite блокирует запись на уровне файла. Поэтому разделение по таблицам не создаёт реального single-writer ownership.

Особенно слабое место — `ib_clock_health`:

- таблица singleton;
- все IB-connected процессы вызывают общий clock sampler;
- heartbeat записывает sample без устойчивой identity владельца;
- последнее успешно записавшее соединение становится источником истины;
- launcher при этом частично ожидает sample от execution clientId.

Это last-writer-wins контракт без явно назначенного владельца.

## 7.3 `trade.sqlite3`

В одном файле находятся:

| Таблица | Writers | Readers |
|---|---|---|
| `positions_latest` | position_sync; execution через writeful position sync | trader, launcher, execution-related code |
| `trade_intents` | trader; execution; daily take-profit/reconciliation | trader, execution, launcher/scripts |
| `protective_orders` | execution | execution/reconciliation/scripts |

Здесь нарушено не только владение файлом, но и владение отдельными сущностями.

Подтверждённый симптом — последний baseline commit исправляет SQLite writer lock, возникавший при параллельной работе execution и position-sync путей.

## 7.4 Runtime JSON

`data/runtime/wt_run/*.json` используется launcher/host для process status.

Этот механизм изолирован лучше SQLite-контрактов, но:

- защищает главным образом managed-запуск через `run_wt.py`;
- не является общей гарантией singleton для всех сервисов;
- отдельный `service_instance_lock` явно используется только execution-процессом.

## 8. Текущий граф пакетных зависимостей

Упрощённая фактическая картина:

```text
config.py ───────────────────────────────┐
contracts.py ────────────────────────────┤
                                        ↓
core ───────────────→ market_data / signal / trader / execution / position_sync
  │
  ├─ содержит IB integration
  ├─ содержит SQLite state
  ├─ содержит market-data pipeline
  ├─ содержит risk guard
  └─ зависит от robot instrument registry

ib_signal ──────────→ core
    ↑                    ↓
    └──────── ib_trader ─┘
                 ↓
          trade.sqlite3

ib_position_sync ──→ ib_execution
        │                ↑
        ├───────────────→│
        └──────────────→ ib_trader

ib_execution ──────→ ib_position_sync
ib_execution ──────→ ib_trader
ib_execution ──────→ ib_signal
```

Критический цикл:

```text
ib_position_sync
    → ib_execution.broker_state_service
    → ib_execution.contract_resolver

ib_execution
    → ib_position_sync.position_store
    → ib_position_sync.position_models
```

Дополнительно оба пакета зависят от `ib_trader.trade_schema`.

Это означает, что названия каталогов не задают направление архитектуры. Нижний broker adapter импортирует верхнюю execution-логику, а execution импортирует position-sync application service.

## 9. Пересекающиеся зоны ответственности

## 9.1 Broker positions

Позиции читают или интерпретируют:

- position_sync;
- execution preflight;
- daily take-profit;
- protective reconciliation;
- emergency close;
- manual scripts.

Разные чтения допустимы, когда цели различаются. Недопустимо текущее смешение:

```text
broker read
+ robot instrument mapping
+ rollover interpretation
+ persistence in trade DB
```

в одной функции `sync_broker_positions_once()`.

## 9.2 Active contract

Active contract используется в:

- market-data startup;
- position projection;
- execution contract resolution;
- rollover policy;
- protective logic.

Источник один — `contracts.py`, но правила доступа и смысл результата распределены по `core`, `ib_execution`, `ib_position_sync` и `ib_trader`.

## 9.3 Risk admission и liquidation

Risk logic распределена между:

- trader: clock guard, stale position, pending stats, unresolved intent, daily halt, daily flat, rollover close;
- execution: повторная daily TP проверка перед risk-increasing order;
- execution daily monitor: trigger halt;
- execution cleanup: cancel/close;
- protective reconciliation: emergency actions.

Некоторые проверки обязаны существовать на нескольких рубежах. Но сейчас между ними нет формального risk contract и списка инвариантов.

## 9.4 Schema lifecycle

Система одновременно утверждает, что runtime migrations не поддерживаются, и выполняет ad hoc schema rewrite при первом обращении:

- `signal_events` перестраивается при несовпадении колонок;
- `trade_intents` перестраивается при несовпадении колонок;
- execution удаляет legacy table/state;
- startup проверяет отсутствие некоторых legacy tables.

Schema upgrade, normal startup и legacy cleanup смешаны.

## 9.5 Health и readiness

Health определяется несколькими способами:

- in-memory `ib_health`;
- JSON process status;
- `instrument_state`;
- свежесть price bar;
- свежесть positions;
- `ib_clock_health`;
- наличие daily guard update;
- простой факт, что процесс прожил несколько секунд.

Единого health/readiness contract нет.

## 10. Защитные механизмы, которые уже существуют

Следующие механизмы нельзя потерять при переписывании:

1. Fail-closed проверка ожидаемого IB account.
2. Разные IB clientId для независимых API-сессий.
3. Требование свежего полного BID/ASK bar перед сигналом.
4. Read-time вычисление `mid_close` без второго физического источника истины.
5. Deduplication signal event по instrument/bar.
6. Deduplication trade intent по source/action context.
7. Не более одного unresolved intent на инструмент.
8. Stale position snapshot блокирует risk-increasing decision.
9. Нездоровый IB clock блокирует OPEN/REVERSE.
10. Незавершённая PnL/commission reconciliation блокирует увеличение риска.
11. Live broker-position preflight непосредственно перед order submission.
12. CLOSE использует фактически удерживаемый broker contract, включая старый futures contract.
13. Reverse блокируется для позиции на non-active futures contract.
14. Daily flat futures перед no-trade/clearing interval.
15. Rollover close для старого quarterly contract.
16. Persisted daily take-profit halt по московскому дню.
17. Prior-day незавершённая liquidation остаётся блокирующей после полуночи.
18. Order submission фиксируется до длительного ожидания terminal result.
19. Неопределённый результат отправленного order переводится в reconciliation, а не объявляется failed вслепую.
20. TP/SL tracked в локальном protective store.
21. Reconciliation local protective state с broker state.
22. Protective price watchdog.
23. Emergency close при недоказуемой или потерянной защите.
24. Обработка partial fill и позднего commission report.
25. Startup cleanup legacy `_EXT_TP` / `_EXT_SL` orders с fail-closed поведением.
26. При outage последний подтверждённый position snapshot не получает ложный свежий timestamp.
27. Paper-first deployment и осторожный rollback при наличии новых broker executions.

Этот список — начало behavior contract, но ещё не полный набор сценариев и состояний.

## 11. Архитектурные дефекты и риски

## ARC-001 — несколько writers одного `trade.sqlite3`

**Приоритет:** P0  
**Статус:** подтверждено кодом и regression fix

Position sync, trader и execution пишут в один SQLite-файл. Execution дополнительно пишет `positions_latest` через вызов position-sync функции.

Последствия:

- writer contention;
- зависимость несвязанных подсистем от длительности чужой транзакции;
- сложный restart/recovery;
- невозможность однозначно назвать владельца сущности;
- уже случившаяся ошибка `database is locked`.

## ARC-002 — writeful broker read в execution preflight

**Приоритет:** P0

Execution должен перепроверять позицию перед ордером. Но используемый API одновременно:

- читает IB;
- применяет business projection;
- открывает trade DB;
- обновляет `positions_latest`.

Безопасное чтение и побочный эффект неразделимы.

## ARC-003 — циклические зависимости доменных пакетов

**Приоритет:** P0

`ib_execution` и `ib_position_sync` импортируют друг друга. Оба зависят от `ib_trader` storage. Execution также импортирует signal configuration/logic.

Это блокирует независимое тестирование, повторное использование и замену подсистем.

## ARC-004 — schema mutation во время normal startup

**Приоритет:** P0

Обычный runtime-вызов может перестроить таблицу, отфильтровать legacy rows, удалить legacy state или аварийно завершиться из-за схемы.

Upgrade procedure не отделена от service startup. Ошибка в середине такого процесса способна превратить запуск сервиса в миграционный инцидент.

## ARC-005 — `state.sqlite3` как общий multi-domain multi-writer файл

**Приоритет:** P1

В одном файле смешаны readiness, signals, clock health и daily risk state. Пишут несколько процессов.

Это скрытая междоменная связанность даже при разных таблицах.

## ARC-006 — `contracts.py` перегружен разными видами конфигурации

**Приоритет:** P1

Один mutable dictionary содержит:

- broker contract identity;
- ручной futures calendar;
- market-data flags;
- trading flags;
- storage filename;
- session model;
- price precision/tick;
- multiplier;
- TP/SL policy.

Это одновременно instrument master, deployment config, strategy config и risk config.

## ARC-007 — `core` не является нижним стабильным слоем

**Приоритет:** P1

В `core` находятся:

- IB connector;
- account/clock/health;
- SQLite state;
- market-data pipeline;
- sessions;
- daily trading guard;
- Telegram/logging;
- instrument-dependent helpers.

Некоторые core-модули зависят от `contracts.py`. Это склад общего кода, а не архитектурное ядро.

## ARC-008 — execution loop и daily take-profit перегружены

**Приоритет:** P1

Один execution process координирует слишком много независимых state machines. `daily_take_profit.py` совмещает calculation, policy, persistence, intent quarantine, order cleanup и liquidation.

Ошибка или зависание одной обязанности влияет на весь execution lifecycle.

## ARC-009 — launcher связан с внутренними таблицами

**Приоритет:** P1

Launcher импортирует domain constants и выполняет SQL напрямую. Любая смена storage требует изменения orchestration.

Readiness сейчас является знанием launcher, а не контрактом сервиса.

## ARC-010 — глобальная import-time конфигурация

**Приоритет:** P1

`config.py` читает обязательные env vars при импорте и создаёт singleton `settings_live`.

Последствия:

- библиотечный импорт зависит от окружения production-приложения;
- зависимости скрыты;
- компоненты трудно переносить между роботами;
- тестам приходится подготавливать env до import;
- несколько конфигураций в одном процессе практически невозможны.

## ARC-011 — непоследовательная защита от duplicate processes

**Приоритет:** P1

WT launcher избегает duplicate managed launch, но явный `service_instance_lock` применяется только execution. Прямой запуск других scripts может создать конкурирующие IB clients/writers.

## ARC-012 — clock singleton использует last-writer-wins

**Приоритет:** P1

Несколько IB-сессий обновляют один `ib_clock_health`. Владелец и приоритет источника не определены.

Это делает clock guard зависимым от порядка heartbeat разных процессов.

## ARC-013 — документация смешивает startup order и causal data flow

**Приоритет:** P2

Строка `market_data -> position_sync -> execution -> signal -> trader` корректна как порядок безопасного запуска, но некорректна как поток торговой команды. Реальный command flow проходит signal → trader → execution.

## ARC-014 — текущие тесты недостаточны как спецификация переписывания

**Приоритет:** P1

CI проверяет rolling verifier, три unittest-модуля, compile, изменённые Python-файлы и imports.

Покрыты отдельные схемы, rolling smoke, price pipeline и SQLite-lock regression. Не покрыта полная матрица:

- broker order lifecycle;
- crash windows между IB и DB;
- restart recovery;
- reconnect;
- partial fills;
- protective reconciliation;
- daily halt cleanup;
- rollover;
- stale/out-of-order data;
- одновременные процессы;
- end-to-end behavior parity.

## ARC-015 — active-contract lifecycle требует отдельной проверки

**Приоритет:** P2 / investigation

Startup выбирает active contract по ручному календарю и передаёт symbol в realtime task. Нужно доказать реальное поведение процесса при наступлении rollover без рестарта.

Пока это не классифицировано как дефект: production procedure может предполагать restart. Но предположение должно стать явным контрактом.

## 12. Что нельзя переносить в новую систему автоматически

Следующие свойства текущего кода не являются требованиями сами по себе:

- имена текущих каталогов;
- наличие `core` как общего склада;
- один `state.sqlite3`;
- один `trade.sqlite3`;
- прямой SQL между сервисами;
- расположение `BrokerStateService` в execution-пакете;
- функция `sync_broker_positions_once()` в текущем виде;
- глобальный `settings_live`;
- mutable dictionary `Instrument`;
- runtime schema rewrite;
- Windows Terminal как обязательная архитектурная часть;
- Telegram side effects в entrypoints;
- конкретные интервалы polling как вечные константы;
- текущие имена таблиц и колонок;
- случайное расположение business rules по файлам.

## 13. Что следует сохранить как исходный капитал

Новая система должна использовать, а не выбрасывать:

- фактические алгоритмы history/realtime загрузки;
- правила получения полного BID/ASK bar;
- rolling signal pipeline;
- contract calendar и накопленные conId/localSymbol;
- signal/trade deduplication;
- intent lifecycle;
- broker-safe order submission;
- reconciliation logic;
- protective-order logic;
- daily PnL и halt semantics;
- daily flat и rollover semantics;
- fail-closed guards;
- диагностические сообщения и operational knowledge;
- migration/rollback уроки из прежних изменений;
- существующие тестовые fixtures и сценарии.

Сохранять нужно поведение и инварианты, а не текущую связанность.

## 14. Что ещё не доказано

До проектирования target architecture требуется отдельно восстановить и проверить:

1. Полный state machine `trade_intent` со всеми переходами и владельцем каждого перехода.
2. Полный state machine protective order.
3. Crash windows:
   - order ушёл в IB, DB write не завершился;
   - DB отмечена, broker response потерян;
   - partial fill во время cancellation;
   - fill получен, commission report задержан;
   - process умер между entry и TP/SL.
4. Поведение после restart каждого отдельного сервиса.
5. Поведение после TWS/Gateway reconnect.
6. Семантика daily halt при ошибках расчёта PnL и после полуночи.
7. Поведение rollover при старой позиции и при смене active contract во время работы.
8. Правила trading hours для каждого secType.
9. Источник истины для broker account PnL и commissions.
10. Требуемая точность, задержка и retention каждого data product.
11. Какие Telegram/plot/log outputs являются обязательными для эксплуатации.
12. Какие scripts являются частью production operations, а какие историческими инструментами.

## 15. Следующий этап

Следующий этап — не создание новых packages и не перенос кода.

Нужно сформировать `current-system-behavior-contract.md`, содержащий:

1. end-to-end сценарии от market bar до broker fill;
2. торговые инварианты;
3. state machines intent/protective/daily halt;
4. failure matrix;
5. restart/reconnect semantics;
6. список characterization-тестов;
7. классификацию каждого поведения:
   - сохранить;
   - изменить осознанно;
   - удалить как legacy/дефект;
   - требует решения владельца стратегии.

Только после этого можно проектировать границы target components и data ownership.

## 16. Критерий завершения текущего этапа

Этап инвентаризации считается завершённым, потому что:

- перечислены runtime-процессы;
- восстановлен фактический data flow;
- перечислены основные data stores и writers;
- выявлен package dependency cycle;
- зафиксированы пересечения ответственности;
- собран первичный список защитных механизмов;
- зарегистрированы архитектурные дефекты `ARC-001`–`ARC-015`;
- определён следующий проверяемый результат.

Runtime-код на этом этапе не изменён.