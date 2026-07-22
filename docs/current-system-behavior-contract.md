# Поведенческий контракт текущей системы IBMarketData

**Статус:** baseline behavior contract v1; основан на коде, часть сценариев требует characterization-тестов  
**Исходный runtime commit:** `5073dbea3f96740b4390e113d06578a514918f00`  
**Предыдущий этап:** `07e8e0a53898ba3735ec10ba016811ea2f56d309` — инвентаризация текущей архитектуры  
**Рабочая ветка:** `agent/architecture-rewrite`  
**Дата:** 2026-07-23

## 1. Назначение

Этот документ фиксирует не структуру файлов, а наблюдаемое и выводимое из кода поведение робота, которое необходимо разобрать до проектирования новой архитектуры.

Он нужен для четырёх задач:

1. отделить торговые требования и safety-инварианты от случайных особенностей реализации;
2. восстановить state machine команд, защитных ордеров и дневной остановки;
3. перечислить crash/restart/reconnect-сценарии;
4. определить characterization-тесты, по которым старая и новая реализации можно будет сравнивать.

Документ не утверждает, что каждое текущее поведение правильно. Для каждого существенного свойства используется одна из классификаций:

- **KEEP** — сохранить как обязательный контракт;
- **CHANGE** — изменить осознанно, не воспроизводить текущую реализацию;
- **REMOVE** — удалить как legacy или дефект после безопасной миграции;
- **DECIDE** — требуется явное решение владельца стратегии/эксплуатации;
- **TEST** — поведение выведено из кода, но должно быть подтверждено тестом или paper-сценарием.

## 2. Границы достоверности

Контракт восстановлен статически по коду baseline commit.

Не выполнялись:

- подключения к реальной TWS / IB Gateway;
- чтение рабочих SQLite-файлов;
- paper- или live-исполнения;
- искусственные disconnect/reconnect;
- hard-kill процессов в критических точках;
- проверка конкретной версии IB API/TWS.

Поэтому факты в документе делятся на:

- **CODE** — прямо следует из текущего кода;
- **INFERENCE** — логический вывод из нескольких участков кода;
- **UNPROVEN** — требует characterization-теста.

## 3. Авторитетные источники фактов в текущей системе

| Факт | Текущий источник истины | Замечание |
|---|---|---|
| фактическая позиция | Interactive Brokers | `positions_latest` является производной проекцией и cache |
| живые/терминальные ордера | Interactive Brokers | локальный store нужен для ownership, audit и recovery |
| fills и commission reports | Interactive Brokers | локальные поля могут некоторое время быть `NULL` |
| instrument/contract identity | `contracts.py` | перегруженный ручной реестр; реализация должна измениться |
| active futures contract | ручные UTC-интервалы в `contracts.py` | market-data фиксирует active contract на старте; execution определяет при вызове |
| канонические price bars | SQLite price DB каждого инструмента | BID/ASK OHLC; `mid_close` вычисляется при чтении |
| signal event | `signal_events` | уникальность по instrument + signal bar |
| торговая команда | `trade_intents` | одновременно command, state machine и audit row |
| локальное состояние TP/SL | `protective_orders` | сверяется с broker state |
| дневная блокировка | `daily_trading_guard` | persisted по account + московскому дню |
| clock guard | singleton `ib_clock_health` | текущий owner не определён; last-writer-wins |

**KEEP:** брокер остаётся авторитетом для positions, orders и executions.  
**CHANGE:** локальные storage и ownership не должны быть смешаны между независимыми процессами.

## 4. Сквозной happy path: price bar → broker fill

### 4.1 Market data

**CODE**

1. Market-data процесс подключается к IB.
2. Получает IB server time.
3. По ручному futures calendar выбирает active contract.
4. Загружает недостающую историю.
5. Запускает realtime BID и ASK.
6. Пишет стороны бара независимо, не затирая ещё не пришедшую сторону.
7. Downstream считает бар пригодным только при наличии полного BID/ASK.
8. Готовность инструмента записывается в `instrument_state`.

Каноническая price row содержит:

- `bar_time_ts` и текстовые UTC/CT/MSK timestamps;
- broker contract;
- BID OHLC;
- ASK OHLC;
- volume/average/bar count.

`mid_close` не хранится отдельным столбцом и вычисляется как округлённое среднее `bid_close` и `ask_close`.

**KEEP**

- сигнал не строится по неполному BID/ASK bar;
- `mid_close` не становится вторым физическим источником истины;
- history и realtime должны формировать один канонический формат price bar.

**DECIDE**

- точная bar duration;
- допустимая задержка;
- retention;
- обязательность хранения трёх текстовых временных зон;
- должен ли active contract переключаться без рестарта market-data.

### 4.2 Signal calculation

**CODE**

Signal-процесс:

1. работает только по инструментам с одновременно включёнными history, realtime и trading;
2. ждёт полный свежий BID/ASK bar;
3. считает price bar устаревшим при lag более `60` секунд;
4. использует rolling step `60` секунд;
5. строит pattern window назад на `90` минут;
6. использует future/potential horizon `30` минут;
7. ищет исторические candidate windows;
8. рассчитывает centered Pearson;
9. пропускает candidates с Pearson ниже `0.7`;
10. применяет min/max hard filter с ratio `1.5`;
11. ранжирует candidates по Pearson, end delta и min/max features;
12. строит potential по `7–9` кандидатам;
13. создаёт LONG/SHORT event только при доступном potential и абсолютном end delta больше `30` points;
14. сохраняет plot независимо от того, создан торговый signal event или нет;
15. пишет не более одного `signal_event` на instrument + signal bar.

Текущая candidate-hour policy:

- FUT: в CT 15:00 и 16:00 candidates запрещены; в остальные часы поиск фактически разрешён по всем часам;
- CASH: используются четыре широкие группы часов;
- CRYPTO: все часы сопоставимы со всеми.

**KEEP**

- deterministic deduplication signal event;
- отсутствие повторного расчёта одной due-точки каждую секунду;
- сигнал создаётся только после полного вычислительного pipeline.

**DECIDE**

- все числовые thresholds и веса;
- hour grouping;
- обязательность plot;
- retention `7` дней;
- пропуск текущей due-точки при старте signal-процесса.

### 4.3 Trader decision

**CODE**

Trader раз в секунду читает последний signal event каждого инструмента, если `signal_bar_ts` не старше `30` секунд.

Перед созданием обычного сигнального intent он применяет следующие проверки:

1. нет persisted daily halt;
2. для signal ещё не существует intent;
3. нет unresolved intent по инструменту;
4. position snapshot существует и не старше `10` секунд;
5. для risk-increasing действия нет pending execution PnL/commission текущего московского дня;
6. IB clock sample здоров: абсолютный offset не больше `3` секунд, sample age не больше `180` секунд;
7. position не находится на non-active futures contract.

Правило решения:

| Broker projection | Signal | Результат |
|---|---|---|
| `FLAT` | LONG | `OPEN_POSITION`, target `LONG/1` |
| `FLAT` | SHORT | `OPEN_POSITION`, target `SHORT/1` |
| LONG | LONG | intent не создаётся |
| SHORT | SHORT | intent не создаётся |
| LONG | SHORT | `REVERSE_POSITION`, target `SHORT/max(1,current_qty)` |
| SHORT | LONG | `REVERSE_POSITION`, target `LONG/max(1,current_qty)` |
| UNKNOWN | любой | intent не создаётся |
| non-active futures position | любой | сигнальный intent не создаётся; применяется rollover policy |

Текущий signal trader создаёт только `MARKET` intents.

Перед обычными сигналами trader также создаёт служебные close intents:

- rollover close для позиции на non-active futures contract;
- daily flat для futures с `14:59:50 CT` до `16:00:00 CT`.

**KEEP**

- same-direction signal не наращивает позицию;
- risk-increasing intent запрещён при stale/unknown broker projection;
- не более одного unresolved intent на инструмент;
- rollover close имеет приоритет над новым входом;
- service liquidation не должна зависеть от появления нового signal event.

**DECIDE**

- всегда ли target quantity равен одному контракту;
- точное окно daily flat;
- должна ли стратегия поддерживать position scaling;
- остаётся ли MARKET единственным типом стратегического входа.

### 4.4 Trade intent persistence

**CODE**

При создании intent:

1. row вставляется со статусом `NEW`;
2. действует уникальность `(instrument_code, source_signal_id, signal_bar_ts, action)`;
3. после получения autoincrement id формируется `order_ref` вида `IBMD_INTENT_<id>_<instrument>`;
4. создание intent и назначение `order_ref` коммитятся одной внешней транзакцией caller-а.

Intent содержит одновременно:

- источник и причину решения;
- before/target position;
- order command;
- execution state;
- broker order identity;
- fill/PnL statistics;
- cancellation request;
- audit timestamps.

**KEEP:** idempotency command creation и устойчивый broker-visible `order_ref`.  
**CHANGE:** command, execution state и audit могут быть разделены, но потеря информации недопустима.

### 4.5 Execution preflight и order submission

**CODE**

Execution:

1. читает свежий `NEW` intent;
2. переводит его в `SENDING` и коммитит это до broker call;
3. перед изменением позиции reconciles и отменяет активные protective orders;
4. принудительно перечитывает broker position;
5. строит реальный order action/quantity/contract по broker state в момент отправки;
6. отправляет order в IB с устойчивым `order_ref`;
7. сразу после получения broker `order_id` сохраняет identity и коммитит её;
8. только затем ожидает terminal result;
9. пишет terminal или reconciling result;
10. после подтверждённого OPEN/REVERSE ставит TP/SL.

Preflight rules:

- OPEN требует broker `FLAT`;
- CLOSE требует фактически открытую broker position;
- CLOSE использует точный удерживаемый `conId/localSymbol`, включая старый quarterly contract;
- REVERSE требует открытую позицию противоположной стороны;
- REVERSE запрещён, если позиция находится на non-active futures contract;
- quantity должна быть положительной и целой;
- reverse order delta равна current quantity + target quantity.

**KEEP**

- live broker preflight непосредственно перед risk-changing order;
- CLOSE старого фьючерса выполняется по фактически удерживаемому контракту;
- order identity фиксируется до длительного ожидания результата;
- неизвестный результат отправленного order не объявляется безопасно failed.

**CHANGE**

Текущий preflight использует `sync_broker_positions_once()`, которая одновременно читает IB, применяет robot projection и пишет `positions_latest`. В новой реализации preflight должен быть side-effect-free относительно position-feed storage.

### 4.6 MARKET result

**CODE**

MARKET order:

1. отправляется без ожидания acceptance;
2. broker identity сохраняется callback-ом;
3. terminal state ожидается до `60` секунд;
4. cancelled/rejected без fill приводит к ошибке;
5. status не `Filled` или fill меньше expected quantity считается неопределённым исполнением;
6. полное исполнение становится `EXECUTED`;
7. commission report ожидается ограниченное время, но отсутствие финальной комиссии не отменяет факт исполнения.

**KEEP:** факт broker fill важнее готовности commission statistics.

### 4.7 LIMIT result

**CODE**

Execution содержит LIMIT path, хотя текущий trader его не создаёт.

LIMIT path:

- сохраняет broker identity;
- ждёт terminal status или persisted `cancel_requested`;
- по timeout запрашивает cancel и требует terminal confirmation;
- full fill → `EXECUTED`;
- partial fill в обычном synchronous path → `EXECUTED` с error text;
- timeout без fill → `EXPIRED`;
- broker cancellation без TTL → `CANCELLED`;
- broker cancellation с TTL → `EXPIRED`;
- reject/inactive → `FAILED`.

**DECIDE:** либо LIMIT становится поддерживаемым продуктовым контрактом с полной partial-fill семантикой, либо path удаляется. Сохранять полумёртвую ветку «на всякий случай» нельзя.

### 4.8 Protective placement

**CODE**

После `EXECUTED` OPEN/REVERSE при известном `avg_fill_price`:

1. рассчитываются TP/SL из instrument settings и tick size;
2. STOP ставится первым;
3. TP ставится вторым;
4. при наличии обоих используется OCA group;
5. futures STOP получает `outsideRth=True`;
6. status `PreSubmitted`/`Submitted` считается рабочим;
7. IB code `399`, означающий held-until-RTH STOP, считается отсутствием текущей защиты;
8. локальная row `protective_orders` создаётся только после broker acceptance;
9. ошибка размещения вызывает cleanup уже поставленных orders и emergency market close.

**KEEP**

- STOP имеет приоритет над TP;
- broker acceptance должна быть доказана;
- held-until-RTH STOP не считается защитой;
- отсутствие доказанной защиты ведёт к fail-safe liquidation, а не к продолжению торговли.

**DECIDE**

- точные TP/SL distances;
- `DAY` или другой TIF;
- обязательность TP при наличии STOP;
- модель OCA.

## 5. State machine `trade_intent`

### 5.1 Состояния

| Status | Смысл текущего кода | Terminal |
|---|---|---:|
| `NEW` | command сохранена, broker submission ещё не начат | нет |
| `SENDING` | execution взял command; broker identity может быть ещё неизвестна | нет |
| `ACCEPTED` | reconciliation доказал, что broker order всё ещё live | нет |
| `RECONCILING` | локальный terminal result недоказан; требуется broker reconciliation | нет |
| `EXECUTED` | fill признан исполненным | да для execution, но может временно блокировать следующее решение до position refresh |
| `CANCELLED` | terminal cancellation доказана или NEW отменён policy | да |
| `EXPIRED` | LIMIT истёк/был отменён по TTL без исполняемого результата | да |
| `FAILED` | order не ушёл либо доказан terminal failure | да |

`ACCEPTED` не является обычным happy-path этапом текущего MARKET исполнения. Он в основном появляется во время reconciliation, когда broker order найден в live status.

### 5.2 Переходы

```text
NEW
 ├─ execution pickup ───────────────→ SENDING
 ├─ stale before pickup ────────────→ FAILED
 └─ daily halt quarantine ──────────→ CANCELLED

SENDING
 ├─ broker identity persisted ──────→ SENDING(order_id known)
 ├─ full fill ──────────────────────→ EXECUTED
 ├─ proven cancel ──────────────────→ CANCELLED
 ├─ TTL/timeout cancel ─────────────→ EXPIRED
 ├─ proven reject before fill ──────→ FAILED
 └─ result uncertain ───────────────→ RECONCILING

SENDING / ACCEPTED / RECONCILING
 ├─ broker order live ──────────────→ ACCEPTED
 ├─ full fill recovered ────────────→ EXECUTED
 ├─ cancel recovered ───────────────→ CANCELLED
 ├─ reject recovered ───────────────→ FAILED
 ├─ partial/unknown result ─────────→ RECONCILING
 └─ order identity unresolved ──────→ RECONCILING

protective broker fill
 └─ synthetic close audit row ──────→ EXECUTED directly
```

### 5.3 Дополнительная unresolved-семантика

`EXECUTED` intent всё ещё блокирует следующее решение по инструменту, пока `positions_latest.updated_at_ts` не станет строго новее event timestamp intent.

Это обеспечивает причинную границу:

```text
broker execution recorded
        ↓
position-sync подтверждает новое broker state
        ↓
следующая decision разрешена
```

**KEEP:** новое решение не должно основываться на position snapshot, созданном до последнего исполнения.

### 5.4 `cancel_requested` — не status

`cancel_requested` является ортогональным persisted flag.

- Для `NEW` daily halt сразу переводит row в `CANCELLED`.
- Для `SENDING/ACCEPTED/RECONCILING` выставляется request flag.
- Активный LIMIT polling читает flag и пытается отменить broker order.
- Наличие request не означает, что broker cancellation уже доказана.

**KEEP:** request и подтверждённый terminal result должны оставаться разными фактами.

### 5.5 Выявленные дефекты state machine

#### BEH-INT-001 — `RECONCILING` без `order_id` не имеет гарантированного terminal timeout

Если молодой `SENDING` row без `order_id` попадает в uncertain reconciliation, он переводится в `RECONCILING`. Stale-active cleanup обрабатывает только `SENDING/ACCEPTED`, поэтому такой row может блокировать инструмент бессрочно.

**Классификация:** CHANGE + TEST.

#### BEH-INT-002 — partial fill в recovery path может оставаться `RECONCILING` после terminal broker cancellation

Uncertain reconciliation сначала видит `filled_qty > 0` и продолжает `RECONCILING`, не доходя до проверки terminal cancelled status.

**Классификация:** CHANGE + TEST.

#### BEH-INT-003 — LIMIT synchronous и recovery paths по-разному трактуют partial fill

Обычный LIMIT path признаёт terminal partial fill как `EXECUTED`, recovery path может удерживать его в `RECONCILING`.

**Классификация:** DECIDE + CHANGE.

## 6. State machine protective order

### 6.1 Состояния

| Status | Смысл | Actionable |
|---|---|---:|
| `ACTIVE` | локально ожидается живой broker TP/SL | да |
| `UNPROTECTED` | STOP не доказан, а broker position всё ещё открыта | да |
| `FILLED` | protective order полностью исполнился | нет |
| `CANCELLED` | cancellation/orphan/OCA terminalized локально | нет |
| `FAILED` | protective order не работает; для TP это не обязательно требует liquidation | нет |

Локальная row появляется сразу как `ACTIVE` только после broker acceptance.

### 6.2 Переходы

```text
broker accepted + local record
        ↓
      ACTIVE
       ├─ full fill ─────────────────────→ FILLED
       ├─ broker cancelled ──────────────→ CANCELLED
       ├─ sibling OCA filled ────────────→ CANCELLED
       ├─ absent + broker FLAT ──────────→ CANCELLED (orphan)
       ├─ TP absent/rejected + pos open ─→ FAILED
       └─ SL absent/rejected + pos open ─→ UNPROTECTED

UNPROTECTED
       ├─ broker order live again ───────→ ACTIVE
       ├─ full fill found ───────────────→ FILLED
       └─ orphan/cleanup ────────────────→ CANCELLED
```

### 6.3 Protective fill

При полном fill:

1. создаётся synthetic `trade_intent` со статусом `EXECUTED` и action `CLOSE_POSITION`;
2. source различает TAKE_PROFIT и STOP_LOSS;
3. synthetic row получает broker order identity и fill statistics;
4. protective row становится `FILLED` и связывается с synthetic intent;
5. ACTIVE OCA sibling становится `CANCELLED`;
6. commission может остаться `NULL` и быть дозаполнена позднее.

Synthetic intent не является новой broker command: он отражает уже состоявшийся protective fill.

**KEEP:** protective fill должен попадать в единый trade/audit/PnL ledger idempotently.

### 6.4 Missing protective recovery после restart

Текущий execution периодически ищет последний `EXECUTED` OPEN/REVERSE без ACTIVE/UNPROTECTED STOP.

Если broker position соответствует target:

1. обновляется open-order cache;
2. matching broker orders ищутся по ожидаемым `order_ref`;
3. набор принимается только при наличии валидного STOP с правильными side/quantity;
4. TP-only остаток отменяется и подтверждается;
5. валидные broker orders adopted в local store;
6. если live STOP не найден, TP/SL размещаются заново;
7. если `avg_fill_price` неизвестна либо cleanup неоднозначен — выполняется emergency close.

Это покрывает два crash-окна:

- entry уже `EXECUTED`, но TP/SL ещё не поставлены;
- broker TP/SL уже принят, но local protective row ещё не записана.

**KEEP:** recovery должен сначала пытаться доказать и adopt существующую защиту, а не слепо создавать дубликат.

### 6.5 Protective watchdog

Для ACTIVE STOP:

- читается executable BID/ASK price path;
- для LONG stop проверяется по BID low;
- для SHORT stop проверяется по ASK high;
- отсутствие свежей цены или lag более `600` секунд при включённом fail-safe вызывает emergency close;
- обнаруженное пересечение stop level при всё ещё активной позиции вызывает emergency close.

**KEEP:** доказанный stop breach при неисполненном broker STOP не должен оставлять позицию открытой.

**DECIDE:** является ли отсутствие market-data в течение 600 секунд безусловным основанием для MARKET close для всех классов инструментов.

### 6.6 Выявленные дефекты protective state

#### BEH-PRO-001 — partial protective fill не имеет полной доменной семантики

Если fill меньше expected quantity, поведение зависит от broker status. При terminal cancellation local row может стать `CANCELLED` без synthetic close для исполненной части.

В текущей стратегии quantity обычно равна одному, но код заявляет поддержку произвольной целой quantity.

**Классификация:** DECIDE + CHANGE + TEST.

#### BEH-PRO-002 — TP failure и STOP failure имеют разную safety-семантику

Missing/rejected STOP требует emergency close. Missing/rejected TP становится `FAILED`, но позиция может продолжить жить только со STOP.

**Классификация:** KEEP mechanism, DECIDE policy.

#### BEH-PRO-003 — placement cleanup временно помечает cancellation до полной broker terminal verification

При частично успешном размещении cleanup вызывает cancel, после чего local row может быть помечена `CANCELLED`. Последующий emergency-close flow повторно читает broker-only exit orders и блокирует close до подтверждения, что частично компенсирует риск.

**Классификация:** CHANGE; terminal local status должен отражать доказанный broker terminal fact.

## 7. State machine daily take-profit halt

### 7.1 Scope текущего PnL

Текущий daily PnL является **PnL робота**, а не безусловно всего broker account:

- realized PnL берётся из локальных `EXECUTED trade_intents` текущего московского дня;
- для чистого OPEN из realized вычитается отдельная commission;
- reverse/close используют IB `realizedPNL` как уже включающий commission;
- unrealized PnL берётся из live IB portfolio только для projected open positions торгуемых инструментов;
- manual positions и orders без robot `order_ref` не входят в robot cleanup и ledger автоматически.

**DECIDE:** должен ли дневной лимит относиться к одному роботу, набору стратегий или всему broker account. Это критическое продуктовое решение.

### 7.2 Состояния

| Status | Смысл | Блокирует новую торговлю |
|---|---|---:|
| `MONITORING` | PnL рассчитан, target ещё не достигнут | нет |
| `TRIGGERED` | target достигнут и halt persisted | да |
| `CLOSING` | идёт quarantine/cancel/liquidation/reconciliation | да |
| `HALTED` | cleanup доказан: robot positions FLAT и robot orders отсутствуют | да до смены московского дня |

### 7.3 Переходы

```text
no row / MONITORING
        ├─ total PnL < target ───────────→ MONITORING
        └─ total PnL >= target ──────────→ TRIGGERED

TRIGGERED
        └─ execution cleanup starts ─────→ CLOSING

CLOSING
        ├─ positions/orders remain ──────→ CLOSING(error_text updated)
        └─ proven FLAT + no robot orders → HALTED(cleanup_completed)
```

### 7.4 Trigger ordering

**CODE**

1. complete PnL snapshot рассчитывается;
2. halt `TRIGGERED` persist-ится;
3. только затем NEW intents quarantine-ятся и активным intents выставляется cancel request;
4. execution loop начинает cleanup.

**KEEP:** блокировка должна быть persisted до потенциально долгой liquidation.

### 7.5 Cleanup

Cleanup:

1. переводит guard в `CLOSING`;
2. повторно quarantine-ит intents;
3. сохраняет unresolved daily-close orders;
4. отменяет прочие non-exit robot orders с terminal confirmation;
5. получает forced broker position snapshot;
6. для FLAT instruments удаляет оставшиеся exit orders;
7. при open position без unresolved daily close вызывает safe market close;
8. повторно читает positions и open orders;
9. переводит state в `HALTED` только если:
   - non-exit cleanup доказан;
   - открытых projected positions нет;
   - живых robot orders нет.

Если cleanup не доказан, guard остаётся `CLOSING`.

### 7.6 Midnight semantics

- Current-day `TRIGGERED`, `CLOSING` и `HALTED` блокируют торговлю.
- Prior-day `TRIGGERED/CLOSING` без `cleanup_completed_at_ts` остаётся блокирующим после полуночи.
- После завершения prior-day cleanup и перехода в `HALTED` старая row перестаёт быть effective halt для нового московского дня.

**KEEP:** смена календарного дня не должна разблокировать счёт с незавершённой liquidation.

### 7.7 Fail-closed monitor readiness

Execution process держит in-memory readiness daily PnL monitor.

- После нового process start readiness по умолчанию `False`.
- Первый успешный complete PnL evaluation делает monitor ready.
- Любая ошибка расчёта делает monitor not ready.
- OPEN/REVERSE не исполняются, пока monitor не ready.

**KEEP:** невозможность вычислить risk limit блокирует увеличение риска.

**CHANGE:** readiness должна быть формализована и наблюдаема, а не спрятана только в process memory.

## 8. Broker position behavior

### 8.1 Periodic projection

Position-sync раз в `2` секунды:

1. проверяет IB socket и backend health;
2. запрашивает positions с timeout `10` секунд;
3. фильтрует по ожидаемому account;
4. для каждого trading-enabled instrument сопоставляет broker contract;
5. возвращает LONG/SHORT/FLAT;
6. определяет active/non-active futures contract;
7. пишет `positions_latest` с timestamp успешного запроса.

При outage последний подтверждённый snapshot остаётся, но timestamp не обновляется.

**KEEP:** отсутствие свежего broker response нельзя маскировать новым timestamp.

### 8.2 Multiple futures contracts

Если на одном account одновременно открыты две ненулевые позиции в разных зарегистрированных quarterly contracts одного логического futures instrument, projection выбрасывает ошибку вместо агрегации или ложного FLAT.

**KEEP:** неоднозначную position нельзя молча сворачивать в одну строку.

**DECIDE:** допустима ли в будущем настоящая multi-contract позиция и какой компонент должен её интерпретировать.

### 8.3 Current defect

Periodic projection, execution preflight, daily PnL, protective reconciliation и emergency close используют одну writeful-функцию. Это смешивает factual read, robot mapping и persistence.

**Классификация:** CHANGE.

## 9. Restart semantics

### 9.1 Market-data restart

**CODE**

- readiness enabled instruments сбрасывается;
- active futures contract определяется заново по server time;
- history coverage восстанавливается из price DB;
- realtime запускается после history success;
- downstream launcher ждёт readiness и fresh bars.

**UNPROVEN**

- гарантированное восстановление всех realtime subscriptions после TWS reconnect без process restart;
- rollover active contract внутри уже работающего процесса.

### 9.2 Position-sync restart

- новый process создаёт отдельный IB client;
- после первого успешного запроса обновляет projection;
- до успеха старый snapshot постепенно становится stale;
- stale snapshot блокирует новые risk-increasing decisions.

### 9.3 Signal restart

При старте после появления свежего bar signal loop принимает текущий latest bar как начальный и не рассчитывает текущую due-точку. Расчёт начинается со следующей due-точки.

Существующие `signal_events` защищены unique constraint.

**DECIDE:** сознательный это anti-duplicate policy или потеря потенциального сигнала после restart.

### 9.4 Trader restart

- читает только последние свежие signals;
- deduplication не позволяет повторить уже обработанный signal;
- unresolved intent и position-causality guard предотвращают новую команду поверх незавершённой.

### 9.5 Execution restart

Startup и periodic recovery выполняют:

1. fail-closed проверку account;
2. cleanup legacy `_EXT_TP/_EXT_SL` broker orders;
3. stale NEW/active intent cleanup;
4. uncertain execution reconciliation по `order_id` или `order_ref`;
5. recovery fills и terminal order statuses;
6. missing commission/PnL backfill;
7. protective reconciliation;
8. adoption/restoration missing protection;
9. continuation daily halt cleanup.

**KEEP:** restart не должен слепо повторно отправлять command, чей broker outcome неизвестен.

## 10. Reconnect semantics

### 10.1 Shared connector

Каждый IB-connected process использует отдельный IB client и собственный monitor.

После socket loss connector:

- сбрасывает health;
- повторяет connect;
- обновляет account/portfolio cache;
- получает новый clock sample;
- сохраняет тот же process lifecycle.

### 10.2 Execution

При недоступном backend execution:

- invalidates broker-state cache;
- не обрабатывает новые orders;
- не выполняет reconciliation/watchdog/forced close;
- возобновляет broker actions после восстановления.

### 10.3 Position sync

При недоступном backend:

- loop остаётся жив;
- не обновляет snapshot timestamp;
- возобновляет polling после восстановления.

### 10.4 Не доказано

- полное восстановление market-data subscriptions;
- отсутствие duplicate callbacks после нескольких reconnect;
- корректность `openTrades/trades/fills` caches сразу после reconnect;
- поведение active LIMIT во время disconnect;
- гарантированная последовательность recovery между несколькими IB clients.

**Классификация:** TEST.

## 11. Crash-window matrix

| ID | Crash window | Текущее восстановление | Классификация |
|---|---|---|---|
| CW-001 | signal рассчитан, event не закоммичен | event отсутствует; следующий due не повторяется в этом process | TEST / DECIDE |
| CW-002 | `NEW` intent закоммичен, execution не взял | execution заберёт пока intent fresh; иначе `FAILED` | KEEP |
| CW-003 | intent `SENDING`, broker call ещё не сделан | reconciliation ищет order по ref; возможен бессрочный no-order-id state | CHANGE |
| CW-004 | broker order создан, local `order_id` не закоммичен | reconciliation ищет по устойчивому `order_ref` | KEEP + TEST |
| CW-005 | local `order_id` сохранён, terminal response потерян | `RECONCILING`, broker status/fills восстанавливаются | KEEP |
| CW-006 | fill произошёл, `EXECUTED` не закоммичен | executions/fills reconciliation | KEEP |
| CW-007 | `EXECUTED` закоммичен, TP/SL не поставлены | missing-protection recovery | KEEP |
| CW-008 | broker STOP/TP accepted, local row не записана | adoption по expected refs при наличии валидного STOP | KEEP |
| CW-009 | protective fill, synthetic close не закоммичен | повторный reconciliation; insert/update в одной DB transaction | KEEP + TEST |
| CW-010 | sibling OCA broker-cancelled, local row ACTIVE | fill reconciliation локально cancel-ит sibling | KEEP |
| CW-011 | fill происходит во время cancel | cancel flow aborts последующее действие и требует re-read | KEEP |
| CW-012 | commission report задержан | execution остаётся `EXECUTED` с `NULL`; stats backfill | KEEP |
| CW-013 | daily halt persisted, cleanup не начат | restart видит `TRIGGERED` и продолжает cleanup | KEEP |
| CW-014 | daily close order отправлен, result неизвестен | ordinary intent reconciliation | KEEP |
| CW-015 | process умер после cancel request, до terminal proof | broker refresh и reconciliation должны доказать status | TEST |
| CW-016 | DB writer lock после broker operation | часть paths переводит command в reconciliation; общий multi-writer остаётся дефектом | CHANGE |
| CW-017 | emergency close отправлен повторно до terminal proof предыдущего | стабильная cross-attempt idempotency не доказана | CHANGE + TEST |

## 12. Failure matrix

| Failure | Текущее ожидаемое действие | Требование |
|---|---|---|
| неправильный/неполученный account | startup failure | KEEP |
| неполный BID/ASK bar | signal не рассчитывается | KEEP |
| stale price bar | signal не создаётся | KEEP |
| отсутствует position snapshot | risk-increasing intent не создаётся | KEEP |
| stale position snapshot | intent rejected, warning | KEEP |
| несколько quarterly positions | projection error; snapshot не обновляется и станет stale | KEEP mechanism / DECIDE multi-contract support |
| IB clock missing/stale/drifted | OPEN/REVERSE блокируются | KEEP |
| pending commission/PnL | увеличение риска блокируется | KEEP |
| daily PnL evaluation error | execution risk-increasing actions блокируются monitor readiness | KEEP |
| IB backend down | broker actions paused; no fake position freshness | KEEP |
| open-order refresh failed | cancellation/cleanup не считается успешным | KEEP |
| order cancel не подтверждён | новая close/change command не отправляется | KEEP |
| order filled during cancellation | действие прерывается, position перечитывается | KEEP |
| entry result uncertain | `RECONCILING`, не blind retry | KEEP |
| protective STOP rejected/missing | `UNPROTECTED` + emergency close | KEEP |
| protective TP rejected/missing | TP `FAILED`; STOP может оставаться единственной защитой | DECIDE |
| STOP price breached, broker order не закрыл | emergency close | KEEP |
| watchdog price data stale/missing | emergency close при включённом fail-safe | DECIDE policy |
| Telegram notification failed | broker/DB reconciliation не откатывается | KEEP |
| schema mismatch на startup | ad hoc migration или crash | CHANGE; migrations отделить |
| SQLite lock | retry не унифицирован; возможен process failure | CHANGE |

## 13. Safety-инварианты для новой реализации

Следующие инварианты считаются baseline **KEEP**, пока владелец явно не отменит их отдельным решением.

### Market data и signal

1. Неполный BID/ASK bar не используется для live signal.
2. `mid_close` является производной величиной, а не независимым фактом.
3. Signal event idempotent по instrument + signal time.
4. Устаревшие market data не порождают новый risk.

### Decision

5. UNKNOWN/stale position не разрешает OPEN/REVERSE.
6. Same-direction signal не увеличивает position.
7. Не более одного unresolved command на instrument.
8. Новое решение не использует position snapshot старше последнего execution.
9. Rollover position закрывается до нового входа.
10. Daily flat/forced close имеет приоритет над обычным signal.
11. Нездоровый broker clock блокирует risk increase.
12. Неполный current-day PnL/commission ledger блокирует risk increase.
13. Persisted daily halt блокирует новые решения.

### Execution

14. Broker position перепроверяется непосредственно перед order submission.
15. CLOSE применяется к реально удерживаемому broker contract.
16. Broker order identity persist-ится до длительного wait.
17. Неизвестный broker outcome не считается failed и не повторяется слепо.
18. Cancellation request не равен terminal cancellation.
19. Partial fill не может быть проигнорирован.
20. После reconnect/restart broker state снова доказывается.

### Protection

21. OPEN/REVERSE position не должна оставаться без доказанного STOP.
22. STOP размещается до менее критичного TP.
23. Held/unaccepted STOP не считается защитой.
24. Existing live protective order сначала adopted/reconciled, а не дублируется.
25. Перед market close существующие exit orders должны быть terminally cancelled либо fill должен быть обработан.
26. Protective fill попадает в единый execution/PnL audit.
27. Lost protection или доказанный unhandled stop breach приводит к fail-safe liquidation.

### Daily halt

28. Halt persist-ится до cleanup.
29. Ошибка liquidation оставляет state блокирующим.
30. Midnight не снимает prior-day unfinished liquidation.
31. `HALTED` устанавливается только после доказанных FLAT positions и отсутствия robot orders.

## 14. Поведения, которые нельзя считать требованиями

Следующее классифицировано как **CHANGE** или **REMOVE**, а не KEEP:

- общий `trade.sqlite3` для position, decision и execution;
- общий multi-domain `state.sqlite3`;
- прямой SQL одного сервиса по storage другого;
- writeful `sync_broker_positions_once()`;
- цикл импортов execution ↔ position_sync;
- broker gateway внутри execution package;
- singleton `settings_live`, создаваемый import-time;
- mutable `Instrument` dictionary как смесь instrument master, strategy и deployment config;
- runtime schema rewrite и legacy cleanup внутри normal startup;
- launcher, читающий внутренние таблицы;
- last-writer-wins clock singleton;
- непоследовательный singleton lock;
- бессрочный `RECONCILING` без broker identity;
- неформализованная partial-fill семантика;
- legacy `_EXT_TP/_EXT_SL` cleanup после завершения переходной миграции;
- имена каталогов и таблиц как самоцель.

## 15. Решения владельца, обязательные до target architecture freeze

### DEC-001 — scope daily PnL

Выбрать одно:

- только этот робот;
- все стратегии данного deployment;
- весь IB account.

Текущий код реализует приблизительно scope «этот робот и его trading-enabled instruments».

### DEC-002 — LIMIT support

- полноценная поддержка с формальной partial-fill/cancel/recovery моделью;
- либо полное удаление LIMIT path.

### DEC-003 — position sizing

- всегда один контракт;
- configurable fixed quantity;
- dynamic sizing;
- multi-contract partial-fill support.

### DEC-004 — daily flat

Подтвердить `14:59:50–16:00 CT`, применимость только к futures и поведение в holidays/early close.

### DEC-005 — rollover lifecycle

Подтвердить:

- ручной calendar или broker-derived contract discovery;
- необходимость process restart;
- поведение при позиции сразу в двух contracts;
- дата/время переключения market-data и execution.

### DEC-006 — protective policy

Подтвердить:

- обязательность STOP;
- обязательность TP;
- TP/SL distances;
- TIF;
- outside-RTH policy;
- действие при stale price feed.

### DEC-007 — signal contract

Подтвердить rolling parameters, candidate-hour groups, thresholds, scoring weights и необходимость plot.

### DEC-008 — observability

Определить обязательные:

- logs;
- Telegram alerts;
- deal messages;
- plots;
- health/readiness metrics;
- audit retention.

### DEC-009 — emergency close idempotency

Определить устойчивый operation key и правило, исключающее повторную liquidation command при неопределённом результате предыдущей.

### DEC-010 — active contract hot switch

Решить, обязан ли market-data process менять futures contract без restart.

## 16. Characterization test backlog

### End-to-end decision

- **CHR-001:** complete BID/ASK bar создаёт ровно одну due signal attempt.
- **CHR-002:** incomplete BID или ASK не создаёт signal.
- **CHR-003:** stale bar не создаёт live signal.
- **CHR-004:** один instrument/bar не создаёт duplicate signal event.
- **CHR-005:** FLAT + LONG/SHORT создаёт OPEN target quantity 1.
- **CHR-006:** same-direction signal не создаёт intent.
- **CHR-007:** opposite signal создаёт REVERSE с корректным delta.
- **CHR-008:** stale/missing position создаёт reject, но не intent.
- **CHR-009:** unhealthy clock блокирует risk increase.
- **CHR-010:** pending current-day stats блокирует risk increase.
- **CHR-011:** rollover close создаётся раньше signal action.
- **CHR-012:** daily flat suppresses simultaneous signal action.

### Intent lifecycle

- **CHR-020:** NEW → SENDING commit происходит до broker call.
- **CHR-021:** broker identity commit происходит сразу после placement receipt.
- **CHR-022:** crash после broker placement, до identity commit восстанавливается по `order_ref`.
- **CHR-023:** full MARKET fill → EXECUTED.
- **CHR-024:** rejected no-fill MARKET → FAILED.
- **CHR-025:** uncertain MARKET result → RECONCILING.
- **CHR-026:** live broker order during recovery → ACCEPTED.
- **CHR-027:** recovered fill → EXECUTED without duplicate order.
- **CHR-028:** stale NEW → FAILED.
- **CHR-029:** EXECUTED блокирует следующее решение до нового position snapshot.
- **CHR-030:** RECONCILING without `order_id` has explicit bounded resolution in target system.
- **CHR-031:** partial fill + broker cancellation terminalizes deterministically.

### Protective lifecycle

- **CHR-040:** STOP отправляется до TP.
- **CHR-041:** code 399/held STOP не принимается как working protection.
- **CHR-042:** accepted STOP/TP сохраняются с правильными refs и OCA.
- **CHR-043:** failure второго protective order безопасно очищает первый.
- **CHR-044:** hard kill после entry, до protection восстанавливает либо закрывает position.
- **CHR-045:** broker STOP accepted, local row missing — order adopted, не duplicated.
- **CHR-046:** TP-only leftover terminally cancelled до новой pair.
- **CHR-047:** protective fill создаёт один synthetic EXECUTED close.
- **CHR-048:** OCA sibling становится CANCELLED.
- **CHR-049:** delayed commission сначала оставляет NULL, затем backfill.
- **CHR-050:** missing STOP + open position вызывает emergency close.
- **CHR-051:** broker FLAT + missing local protective превращает row в orphan/cancelled.
- **CHR-052:** partial protective fill получает формально утверждённый результат.
- **CHR-053:** stop breach watchdog закрывает position только после cancellation existing exits.

### Daily halt

- **CHR-060:** PnL ниже target → MONITORING.
- **CHR-061:** PnL на/выше target persist-ит TRIGGERED до cleanup.
- **CHR-062:** NEW intents quarantine-ятся; active получают cancel request.
- **CHR-063:** cleanup остаётся CLOSING, пока есть position или robot order.
- **CHR-064:** proven FLAT + no orders → HALTED.
- **CHR-065:** prior-day CLOSING остаётся блокирующим после midnight.
- **CHR-066:** prior-day cleanup completion разблокирует новый Moscow day.
- **CHR-067:** PnL calculation failure blocks OPEN/REVERSE.
- **CHR-068:** manual/non-robot position handling соответствует выбранному DEC-001 scope.

### Restart/reconnect

- **CHR-080:** position-sync outage не обновляет freshness.
- **CHR-081:** execution disconnect pauses all broker actions.
- **CHR-082:** reconnect invalidates caches и повторно доказывает order state.
- **CHR-083:** active order survives execution restart without duplicate submission.
- **CHR-084:** active protective order survives restart and is adopted.
- **CHR-085:** market-data subscriptions реально восстанавливаются после reconnect.
- **CHR-086:** signal restart semantics на текущей due-точке зафиксированы.
- **CHR-087:** repeated emergency-close trigger не создаёт duplicate close exposure.
- **CHR-088:** simultaneous SQLite writers больше не являются частью target behavior; regression reproduces old lock only as migration evidence.

### Migration and operations

- **CHR-100:** schema migration выполняется отдельной командой и не запускается implicitly runtime service.
- **CHR-101:** rollback после broker executions требует reconciliation, а не blind DB restore.
- **CHR-102:** каждый service имеет собственный health/readiness contract без знания внутренних SQL tables.
- **CHR-103:** duplicate process start блокируется для каждого single-instance service.

## 17. Минимальный parity gate новой реализации

Новая система не считается функционально эквивалентной, пока не выполнены одновременно условия:

1. пройдены все KEEP characterization tests;
2. все DECIDE-пункты имеют письменное решение;
3. каждый broker command имеет idempotency/recovery identity;
4. все nonterminal states имеют bounded или operator-visible resolution path;
5. position, order и fill facts можно восстановить после process restart;
6. OPEN/REVERSE не возможны при недоказанном risk state;
7. protection recovery проверена hard-kill тестами;
8. daily halt проверен через midnight;
9. paper run подтверждает отсутствие duplicate orders и unprotected positions;
10. migration и rollback отделены от normal runtime.

## 18. Итог текущего этапа

Поведенческий контракт показывает, что ценность текущего проекта находится не в существующей структуре каталогов, а в накопленных safety-механизмах:

- broker preflight;
- command idempotency;
- uncertain execution reconciliation;
- protective recovery;
- cancellation proof;
- stale-data guards;
- daily halt;
- rollover и daily flat;
- fail-closed реакции на неоднозначность.

Одновременно контракт выявил поведение, которое нельзя переносить:

- бессрочные неопределённые состояния;
- неформализованные partial fills;
- writeful reads;
- multi-writer storage;
- runtime migrations;
- last-writer-wins health state.

Следующий этап — не писать target packages. Сначала необходимо:

1. превратить P0/P1 KEEP-инварианты в executable characterization tests;
2. зафиксировать решения `DEC-001`–`DEC-010`;
3. только затем построить target component boundaries, data ownership и migration seams.
