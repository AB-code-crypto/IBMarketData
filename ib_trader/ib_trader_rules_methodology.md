# Методичка по правилам `ib_trader`

## 1. Назначение

`ib_trader` теперь является полноценным слоем принятия торгового решения.

Он не ищет сигнал и не исполняет ордер. Его задача — взять уже найденный сигнал, посмотреть рыночное состояние, позицию, набор правил и решить:

- ничего не делать;
- открыть позицию;
- закрыть позицию;
- уменьшить позицию;
- добавить к позиции;
- сделать реверс.

Итог решения записывается в `trade.sqlite3`.

Текущая цепочка:

```text
ib_signal
    → signal_events

ib_trader
    читает signal_events
    читает regime / ma_zone из job DB
    читает positions_latest из trade DB
    прогоняет правила
    пишет trade_decisions
    если нужно исполнение — пишет trade_intents

ib_execution
    читает trade_intents
    исполняет приказ
```

Удалённый слой:

```text
run_signal_filters.py
ib_signal_filters/
filtered_signal_latest
```

Теперь фильтрация и принятие решения находятся в одном месте — в `ib_trader`.

---

## 2. Главные файлы

### Конфиг правил

```text
ib_trader/trader_rules_config.py
```

Здесь включаются, выключаются и настраиваются правила.

### Rule engine

```text
ib_trader/rule_engine.py
```

Прогоняет активные правила по `TraderRuleContext`.

### Базовые структуры правил

```text
ib_trader/rules/base.py
```

Содержит:

```text
TraderRuleContext
TraderRuleState
```

### Реестр правил

```text
ib_trader/rules/registry.py
```

Связывает строковый `id` правила с Python-классом.

### Текущие правила

```text
ib_trader/rules/order_type_by_session.py
ib_trader/rules/zone_direction_policy.py
ib_trader/rules/regime_signal_strength.py
```

### Основная торговая логика

```text
ib_trader/trade_store.py
```

Читает сигналы, market-features, позицию, вызывает rule engine и пишет решения.

### Execution

```text
ib_execution/execution_logic.py
ib_execution/execution_store.py
ib_execution/execution_runner.py
```

Исполняет `trade_intents`.

---

## 3. Данные, которые использует `ib_trader`

### 3.1. Сигнал из `signal_events`

`ib_trader` читает свежие сигналы из:

```text
state.sqlite3 → signal_events
```

Основные поля:

```text
signal_id
instrument_code
signal_bar_ts
signal_time_utc
signal_time_ct
signal_time_msk

direction
entry_price

best_pearson
candidate_score_best

potential_end_delta_points
potential_max_profit_points
potential_max_drawdown_points
potential_used
```

`direction` может быть:

```text
LONG
SHORT
```

### 3.2. Рыночные признаки из job DB

Для бара сигнала читаются:

```text
regime
ma_zone
```

Из таблицы:

```text
<instrument>_job.sqlite3 → sma_5s
```

`regime`:

```text
 1  режим вверх
 0  флет
-1  режим вниз
```

`ma_zone`:

```text
 4  цена выше дальней границы upper_far
 3  upper_far
 2  upper_middle
 1  upper_near
 0  нет данных / около MA
-1  lower_near
-2  lower_middle
-3  lower_far
-4  цена ниже дальней границы lower_far
```

`±4` — это зона за пределами обычного percentile-range. В текущей логике она используется как простой признак повышенного выноса/волатильности.

### 3.3. Текущая позиция

Из:

```text
trade.sqlite3 → positions_latest
```

Читаются:

```text
instrument_code
side
quantity
```

`side`:

```text
UNKNOWN
FLAT
LONG
SHORT
```

Если строки по инструменту нет, позиция считается:

```text
UNKNOWN
```

Это важно: `UNKNOWN` не равно `FLAT`. При `UNKNOWN` трейдер не должен открывать сделку.

---

## 4. Как работает rule engine

Сначала `ib_trader` собирает контекст:

```text
TraderRuleContext:
    signal
    market_features
    position
```

Потом вызывает:

```python
evaluate_trader_rules(context)
```

Rule engine:

1. создаёт `TraderRuleState`;
2. проверяет наличие `regime` и `ma_zone`;
3. сортирует активные правила по `priority`;
4. последовательно вызывает `rule.apply(...)`;
5. собирает итог:
   - allowed / rejected;
   - signal_strength;
   - order_type;
   - order_policy_reason;
   - limit_offset_points;
   - ttl_seconds;
   - rules_json.

Результат правил используется в `trade_store.py`, где уже принимается итоговое действие:

```text
NO_ACTION
OPEN_POSITION
REVERSE_POSITION
ADD_TO_POSITION
REDUCE_POSITION
CLOSE_POSITION
```

---

## 5. Конфиг правил

Файл:

```text
ib_trader/trader_rules_config.py
```

Структура:

```python
ACTIVE_RULES = [
    {
        "id": "order_type_by_session",
        "enabled": True,
        "priority": 10,
        "params": {
            ...
        },
    },
]
```

Каждое правило имеет:

```text
id        строковый идентификатор правила
enabled   включено / выключено
priority  порядок выполнения, меньше = раньше
params    параметры правила
```

### Включить правило

```python
"enabled": True
```

### Выключить правило

```python
"enabled": False
```

### Удалить правило

Удалить весь блок из `ACTIVE_RULES`.

---

## 6. Текущее правило `order_type_by_session`

Файл:

```text
ib_trader/rules/order_type_by_session.py
```

Назначение:

```text
Решает, каким типом ордера открываться:
MARKET или LIMIT.
```

Текущая логика:

```text
если signal_time_ct попадает в заданное CT-окно → LIMIT
если abs(ma_zone) >= 4 → LIMIT
иначе → MARKET
```

Пример конфига:

```python
{
    "id": "order_type_by_session",
    "enabled": True,
    "priority": 10,
    "params": {
        "limit_time_windows_ct": [
            ("08:30", "10:30"),
        ],
        "limit_if_abs_ma_zone_at_least": 4,
        "default_order_type": "MARKET",
        "limit_order_type": "LIMIT",
        "limit_offset_points": 1.0,
        "limit_ttl_seconds": 60,
    },
}
```

### Что означает `limit_time_windows_ct`

Список временных окон по Чикаго.

Пример:

```python
"limit_time_windows_ct": [
    ("08:30", "10:30"),
]
```

Означает:

```text
с 08:30 CT до 10:30 CT использовать LIMIT
```

Можно указать несколько окон:

```python
"limit_time_windows_ct": [
    ("08:30", "09:30"),
    ("14:50", "15:10"),
]
```

### Что означает `limit_if_abs_ma_zone_at_least`

```python
"limit_if_abs_ma_zone_at_least": 4
```

Если:

```text
abs(ma_zone) >= 4
```

то используется `LIMIT`.

Это заменяет отдельный расчёт текущей волатильности: зона `±4` означает, что цена вышла за обычный дальний диапазон.

Отключить это условие:

```python
"limit_if_abs_ma_zone_at_least": 0
```

### Как считается limit price

Для LONG:

```text
limit_price = entry_price - limit_offset_points
```

Для SHORT:

```text
limit_price = entry_price + limit_offset_points
```

Пример:

```python
"limit_offset_points": 1.0
```

Если сигнал LONG при `entry_price = 22000.00`, то:

```text
limit_price = 21999.00
```

Если сигнал SHORT при `entry_price = 22000.00`, то:

```text
limit_price = 22001.00
```

### TTL лимитника

```python
"limit_ttl_seconds": 60
```

Передаётся в `trade_intents.ttl_seconds`.

---

## 7. Текущее правило `zone_direction_policy`

Файл:

```text
ib_trader/rules/zone_direction_policy.py
```

Назначение:

```text
Разрешает или запрещает направление сигнала в зависимости от ma_zone.
```

Текущая политика:

```python
"allowed_directions_by_zone": {
    -4: ["LONG"],
    -3: ["LONG"],
    -2: ["LONG", "SHORT"],
    -1: ["LONG", "SHORT"],
     0: ["LONG", "SHORT"],
     1: ["LONG", "SHORT"],
     2: ["LONG", "SHORT"],
     3: ["SHORT"],
     4: ["SHORT"],
}
```

Расшифровка:

```text
-4 / -3:
    цена далеко ниже MA
    разрешён только LONG
    логика: возврат к средней

+3 / +4:
    цена далеко выше MA
    разрешён только SHORT
    логика: возврат к средней

-2 ... +2:
    пока разрешены оба направления
```

### Запретить LONG в зоне +2

```python
2: ["SHORT"],
```

### Запретить SHORT в зоне -2

```python
-2: ["LONG"],
```

### Запретить торговлю около MA

```python
0: [],
```

Если направление не разрешено, решение будет:

```text
decision_action = NO_ACTION
decision_reason = ma_zone_direction_forbidden
```

---

## 8. Текущее правило `regime_signal_strength`

Файл:

```text
ib_trader/rules/regime_signal_strength.py
```

Назначение:

```text
Классифицирует силу сигнала относительно режима.
```

Логика:

```text
LONG  при regime =  1 → STRONG
SHORT при regime = -1 → STRONG

LONG  при regime = -1 → WEAK
SHORT при regime =  1 → WEAK

regime = 0 → NEUTRAL
```

Конфиг:

```python
{
    "id": "regime_signal_strength",
    "enabled": True,
    "priority": 30,
    "params": {
        "weak_signal_policy": "ALLOW",
    },
}
```

### Разрешать слабые сигналы

```python
"weak_signal_policy": "ALLOW"
```

Тогда слабый сигнал не запрещается, но в `trade_decisions` будет:

```text
signal_strength = WEAK
```

### Запрещать слабые сигналы

```python
"weak_signal_policy": "REJECT"
```

Тогда вход против режима будет запрещён:

```text
decision_action = NO_ACTION
decision_reason = weak_signal_rejected
```

---

## 9. Что пишется в `trade_decisions`

Таблица:

```text
trade.sqlite3 → trade_decisions
```

Важные поля:

```text
decision_id
source_signal_id
instrument_code

signal_bar_ts
signal_time_utc
signal_time_ct
signal_time_msk

signal_direction
entry_price

best_pearson
candidate_score_best

potential_end_delta_points
potential_max_profit_points
potential_max_drawdown_points
potential_used

regime
ma_zone
signal_strength

order_type
order_policy_reason
limit_offset_points
limit_price
ttl_seconds
rules_json

decision_action
decision_reason

position_before_side
position_before_qty
position_after_side
position_after_qty

created_at_ts
```

### Быстрый просмотр последних решений

```sql
SELECT
    decision_id,
    instrument_code,
    signal_time_ct,
    signal_direction,
    potential_end_delta_points,
    regime,
    ma_zone,
    signal_strength,
    order_type,
    order_policy_reason,
    limit_price,
    decision_action,
    decision_reason
FROM trade_decisions
ORDER BY decision_id DESC
LIMIT 20;
```

### Посмотреть JSON правил

```sql
SELECT
    decision_id,
    rules_json
FROM trade_decisions
ORDER BY decision_id DESC
LIMIT 5;
```

`rules_json` показывает, какие правила сработали и с какими деталями.

---

## 10. Что пишется в `trade_intents`

Таблица:

```text
trade.sqlite3 → trade_intents
```

Пишется только если:

```text
decision_action != NO_ACTION
```

Важные поля:

```text
trade_intent_id
decision_id
source_signal_id
instrument_code

action
target_side
target_qty

position_before_side
position_before_qty

order_type
limit_price
limit_offset_points
ttl_seconds

status
```

`ib_execution` читает только:

```text
status = NEW
```

---

## 11. Как `ib_execution` использует правила

`ib_execution` сам ничего не решает.

Он смотрит:

```text
trade_intents.order_type
```

Если:

```text
order_type = MARKET
```

то ставит market order и ждёт исполнения.

Если:

```text
order_type = LIMIT
```

то ставит limit order и получает статус:

```text
ACCEPTED
```

Важно:

```text
ACCEPTED ≠ EXECUTED
```

`ACCEPTED` значит, что лимитный ордер выставлен. Для полноценного боевого контура дальше нужен отдельный мониторинг лимитных ордеров:

```text
ACCEPTED → EXECUTED
ACCEPTED → CANCELLED
ACCEPTED → EXPIRED
```

---

## 12. Как изменить существующее правило

### Пример 1. Сделать только первый час лимитным

Было:

```python
"limit_time_windows_ct": [
    ("08:30", "10:30"),
],
```

Стало:

```python
"limit_time_windows_ct": [
    ("08:30", "09:30"),
],
```

### Пример 2. Сделать лимитный offset 2 пункта

```python
"limit_offset_points": 2.0,
```

### Пример 3. Запретить вход против режима

```python
"weak_signal_policy": "REJECT",
```

### Пример 4. В зоне +2 разрешить только SHORT

```python
2: ["SHORT"],
```

### Пример 5. В зоне -2 разрешить только LONG

```python
-2: ["LONG"],
```

После изменения конфига нужно перезапустить:

```text
run_trader.py
```

---

## 13. Как создать новое правило

Пример: запретить сигналы, если абсолютный potential меньше заданного минимума.

### Шаг 1. Создать файл

```text
ib_trader/rules/potential_min_points.py
```

Код:

```python
from typing import Any

from ib_trader.rules.base import TraderRuleContext, TraderRuleState


class PotentialMinPointsRule:
    rule_id = "potential_min_points"

    @classmethod
    def apply(
            cls,
            *,
            context: TraderRuleContext,
            state: TraderRuleState,
            params: dict[str, Any],
    ) -> None:
        min_points = float(params.get("min_points", 0.0))
        potential = abs(float(context.signal.potential_end_delta_points))

        if potential < min_points:
            state.reject(
                rule_id=cls.rule_id,
                reason="potential_below_min_points",
                details={
                    "potential": potential,
                    "min_points": min_points,
                },
            )
            return

        state.record(
            rule_id=cls.rule_id,
            result="ALLOW",
            details={
                "potential": potential,
                "min_points": min_points,
            },
        )
```

### Шаг 2. Зарегистрировать правило

Файл:

```text
ib_trader/rules/registry.py
```

Добавить импорт:

```python
from ib_trader.rules.potential_min_points import PotentialMinPointsRule
```

Добавить в `RULE_REGISTRY`:

```python
RULE_REGISTRY = {
    ...
    PotentialMinPointsRule.rule_id: PotentialMinPointsRule,
}
```

### Шаг 3. Добавить в конфиг

Файл:

```text
ib_trader/trader_rules_config.py
```

Добавить блок:

```python
{
    "id": "potential_min_points",
    "enabled": True,
    "priority": 15,
    "params": {
        "min_points": 10.0,
    },
},
```

`priority = 15` означает, что правило выполнится после `order_type_by_session` с priority `10`, но до `zone_direction_policy` с priority `20`.

---

## 14. Что правило может делать

### Запретить сделку

```python
state.reject(
    rule_id=cls.rule_id,
    reason="some_reason",
    details={...},
)
```

После этого итоговое решение будет:

```text
decision_action = NO_ACTION
```

### Записать диагностический результат

```python
state.record(
    rule_id=cls.rule_id,
    result="ALLOW",
    details={...},
)
```

Это попадёт в:

```text
trade_decisions.rules_json
```

### Изменить тип ордера

```python
state.order_type = "LIMIT"
state.order_policy_reason = "some_reason"
state.limit_offset_points = 2.0
state.ttl_seconds = 60
```

### Изменить силу сигнала

```python
state.signal_strength = "STRONG"
```

---

## 15. Чего правило делать не должно

Правило не должно:

```text
читать SQLite
писать SQLite
отправлять ордера
создавать trade_intent
лезть в IB
менять позиции
```

Правило получает готовый `context` и изменяет только `state`.

Правильная модель:

```text
trade_store.py читает данные
rules/*.py принимают логические решения
trade_store.py пишет решение
execution исполняет
```

---

## 16. Что доступно в `context`

### `context.signal`

```text
source_signal_id
instrument_code
signal_bar_ts
signal_time_utc
signal_time_ct
signal_time_msk

direction
entry_price

best_pearson
candidate_score_best

potential_end_delta_points
potential_max_profit_points
potential_max_drawdown_points
potential_used
```

### `context.market_features`

```text
instrument_code
signal_bar_ts
feature_bar_ts
regime
ma_zone
```

### `context.position`

```text
instrument_code
side
quantity
```

---

## 17. Как отлаживать правила

### Последние решения

```sql
SELECT
    decision_id,
    signal_time_ct,
    signal_direction,
    potential_end_delta_points,
    regime,
    ma_zone,
    signal_strength,
    order_type,
    order_policy_reason,
    limit_price,
    decision_action,
    decision_reason
FROM trade_decisions
ORDER BY decision_id DESC
LIMIT 20;
```

### Только запрещённые решения

```sql
SELECT
    decision_id,
    signal_time_ct,
    signal_direction,
    regime,
    ma_zone,
    signal_strength,
    decision_reason,
    rules_json
FROM trade_decisions
WHERE decision_action = 'NO_ACTION'
ORDER BY decision_id DESC
LIMIT 20;
```

### Только созданные intents

```sql
SELECT
    trade_intent_id,
    decision_id,
    instrument_code,
    action,
    target_side,
    target_qty,
    order_type,
    limit_price,
    status
FROM trade_intents
ORDER BY trade_intent_id DESC
LIMIT 20;
```

### Разобрать, почему был LIMIT

```sql
SELECT
    decision_id,
    signal_time_ct,
    ma_zone,
    order_type,
    order_policy_reason,
    limit_offset_points,
    limit_price,
    rules_json
FROM trade_decisions
WHERE order_type = 'LIMIT'
ORDER BY decision_id DESC
LIMIT 20;
```

---

## 18. Типовые причины отказа

### `market_features_unknown`

Не удалось прочитать `regime` или `ma_zone`.

Возможные причины:

```text
job DB не пересобрана
sma_5s ещё не содержит нужные строки
bar_time_ts не найден
regime/ma_zone NULL
```

### `position_unknown`

В `positions_latest` нет строки по инструменту.

Это правильно. Робот не должен считать отсутствие строки как FLAT.

### `ma_zone_direction_forbidden`

Направление сигнала не разрешено в текущей зоне.

### `weak_signal_rejected`

Сигнал против режима, а в конфиге:

```python
"weak_signal_policy": "REJECT"
```

### `unknown_rule_id`

В `ACTIVE_RULES` указан `id`, которого нет в `RULE_REGISTRY`.

---

## 19. Практический порядок изменения правил

1. Изменить `ib_trader/trader_rules_config.py`.
2. Перезапустить `run_trader.py`.
3. Подождать новый сигнал.
4. Проверить `trade_decisions`.
5. Смотреть:
   - `decision_action`
   - `decision_reason`
   - `regime`
   - `ma_zone`
   - `signal_strength`
   - `order_type`
   - `rules_json`.

---

## 20. Что сейчас ещё не завершено

### 20.1. Мониторинг LIMIT-ордеров

Сейчас LIMIT после постановки получает:

```text
ACCEPTED
```

Но ещё нужен слой, который потом переведёт его в:

```text
EXECUTED
CANCELLED
EXPIRED
FAILED
```

Без этого LIMIT-контур неполный.

### 20.2. Риск-менеджмент

Пока нет правил:

```text
дневной лимит убытка
максимальный размер позиции
лимит сделок в день
запрет торговли после серии убытков
проверка свободных средств
```

Это будущие rule-типы.

### 20.3. Управление существующей позицией

Сейчас базовая логика:

```text
FLAT + сигнал → OPEN_POSITION
та же сторона → NO_ACTION
противоположная сторона → REVERSE_POSITION
UNKNOWN позиция → NO_ACTION
```

Позже можно добавить:

```text
ADD_TO_POSITION
REDUCE_POSITION
CLOSE_POSITION
частичный реверс
```

---

## 21. Минимальная mental model

```text
trader_rules_config.py
    управляет тем, какие правила включены и с какими параметрами

rules/*.py
    содержат код отдельных правил

registry.py
    связывает id правила с классом

rule_engine.py
    выполняет правила и собирает результат

trade_store.py
    читает данные, вызывает rule_engine, пишет trade_decisions/trade_intents

execution
    исполняет уже готовый trade_intent
```

Если коротко:

```text
Правило не торгует.
Правило только говорит: можно / нельзя / какой order_type / какая сила сигнала.
Торгует ib_execution, но только после решения ib_trader.
```
