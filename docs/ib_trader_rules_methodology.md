# Методичка по правилам `ib_trader`

## 1. Короткая схема

```text
ib_signal
    пишет signal_events

ib_trader
    читает signal_events
    читает regime / ma_zone из job DB
    читает positions_latest из trade DB
    применяет правила
    пишет trade_decisions
    если нужно исполнение — пишет trade_intents

ib_execution
    читает trade_intents
    исполняет MARKET / LIMIT
```

`ib_trader` — это слой принятия решения.  
`ib_execution` — только исполнитель.

---

## 2. Главный конфиг правил

```text
ib_trader/trader_rules_config.py
```

В нём есть две ключевые сущности:

```python
REQUIRE_MARKET_FEATURES = True / False

ACTIVE_RULES = [...]
```

---

## 3. Что такое `REQUIRE_MARKET_FEATURES`

`REQUIRE_MARKET_FEATURES` отвечает на вопрос:

```text
можно ли trader принимать решение, если regime / ma_zone неизвестны?
```

### `REQUIRE_MARKET_FEATURES = False`

```text
regime и ma_zone не обязательны
```

Используется для режима `potential_only`.

Пример:

```python
REQUIRE_MARKET_FEATURES = False
ACTIVE_RULES: list[dict] = []
```

Поведение:

```text
ib_trader входит туда, куда говорит potential / signal.direction
regime не проверяется
ma_zone не проверяется
order_type = MARKET
rules_json = []
```

Позиционная логика всё равно остаётся:

```text
UNKNOWN -> NO_ACTION
FLAT -> OPEN_POSITION
same side -> NO_ACTION
opposite side -> REVERSE_POSITION
```

### `REQUIRE_MARKET_FEATURES = True`

```text
regime и ma_zone обязательны
```

Если хотя бы одно значение не прочиталось из job DB:

```text
regime is None
или
ma_zone is None
```

то результат:

```text
decision_action = NO_ACTION
decision_reason = market_features_unknown
```

Используется для пресетов и правил, которые завязаны на:

```text
regime
ma_zone
зоны ±3/±4
силу сигнала относительно режима
```

Практическое правило:

```text
ACTIVE_RULES пустой, торгуем только по potential -> REQUIRE_MARKET_FEATURES = False

есть правила по regime / ma_zone -> REQUIRE_MARKET_FEATURES = True
```

---

## 4. Структура правила в `ACTIVE_RULES`

```python
{
    "id": "zone_direction_policy",
    "enabled": True,
    "priority": 20,
    "params": {...},
}
```

Поля:

```text
id        имя правила, должно быть в RULE_REGISTRY
enabled   включено / выключено
priority  порядок выполнения, меньше = раньше
params    параметры правила
```

---

## 5. Пресеты правил

Пресеты лежат тут:

```text
presets/trader_rules/
```

Применяются через:

```text
apply_trader_rules_preset.py
```

Список пресетов:

```powershell
.venv\Scripts\python.exe apply_trader_rules_preset.py
```

Применить пресет:

```powershell
.venv\Scripts\python.exe apply_trader_rules_preset.py potential_only
```

---

## 6. Preset: `potential_only`

Файл:

```text
presets/trader_rules/potential_only.py
```

Назначение:

```text
входить только в направлении potential / signal.direction
```

Настройки:

```python
REQUIRE_MARKET_FEATURES = False
ACTIVE_RULES: list[dict] = []
```

Итог:

```text
regime не нужен
ma_zone не нужен
order_type = MARKET
```

---

## 7. Preset: `far_limit_mean_reversion`

Файл:

```text
presets/trader_rules/far_limit_mean_reversion.py
```

Назначение:

```text
работать только в дальних зонах ±3/±4
только лимитными ордерами
только на возврат к средней
```

Ключевые настройки:

```python
REQUIRE_MARKET_FEATURES = True
```

Разрешённые направления:

```text
ma_zone +3/+4 -> SHORT
ma_zone -3/-4 -> LONG
остальные зоны -> запрет
```

Лимитник:

```text
offset = 10 пунктов
ttl = 60 секунд
```

---

## 8. Preset: `session_limit_near_market`

Файл:

```text
presets/trader_rules/session_limit_near_market.py
```

Назначение:

```text
07:00-10:00 CT -> LIMIT, offset 10
остальное время -> MARKET
разрешены зоны -1/0/+1
режим определяет силу сигнала
```

Ключевые настройки:

```python
REQUIRE_MARKET_FEATURES = True
```

Разрешённые зоны:

```text
-1, 0, +1
```

Сила сигнала:

```text
direction совпадает с regime -> STRONG
direction против regime -> WEAK
regime = 0 -> NEUTRAL
```

Сейчас `WEAK` разрешён, но помечается в:

```text
trade_decisions.signal_strength
```

---

## 9. Как добавить новое правило

### Шаг 1. Создать файл

```text
ib_trader/rules/my_rule.py
```

Пример:

```python
from typing import Any

from ib_trader.rules.base import TraderRuleContext, TraderRuleState


class MyRule:
    rule_id = "my_rule"

    @classmethod
    def apply(
            cls,
            *,
            context: TraderRuleContext,
            state: TraderRuleState,
            params: dict[str, Any],
    ) -> None:
        state.record(
            rule_id=cls.rule_id,
            result="ALLOW",
            details={},
        )
```

### Шаг 2. Зарегистрировать

```text
ib_trader/rules/registry.py
```

```python
from ib_trader.rules.my_rule import MyRule

RULE_REGISTRY = {
    ...
    MyRule.rule_id: MyRule,
}
```

### Шаг 3. Включить в конфиг

```python
{
    "id": "my_rule",
    "enabled": True,
    "priority": 25,
    "params": {},
}
```

---

## 10. Что правило может делать

Запретить сделку:

```python
state.reject(
    rule_id=cls.rule_id,
    reason="some_reason",
    details={...},
)
```

Записать диагностику:

```python
state.record(
    rule_id=cls.rule_id,
    result="ALLOW",
    details={...},
)
```

Изменить тип ордера:

```python
state.order_type = "LIMIT"
state.order_policy_reason = "my_reason"
state.limit_offset_points = 10.0
state.ttl_seconds = 60
```

Изменить силу сигнала:

```python
state.signal_strength = "STRONG"
```

---

## 11. Чего правило делать не должно

Правило не должно:

```text
читать SQLite
писать SQLite
отправлять ордера
создавать trade_intent
лезть в IB
```

Правило работает только с:

```text
context.signal
context.market_features
context.position
```

---

## 12. Где смотреть результат

```text
trade.sqlite3 -> trade_decisions
```

Полезный запрос:

```sql
SELECT
    decision_id,
    instrument_code,
    signal_time_ct,
    signal_direction,
    regime,
    ma_zone,
    signal_strength,
    order_type,
    order_policy_reason,
    limit_price,
    decision_action,
    decision_reason,
    rules_json
FROM trade_decisions
ORDER BY decision_id DESC
LIMIT 20;
```

Созданные приказы:

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

---

## 13. Типовые причины отказа

```text
market_features_unknown
    REQUIRE_MARKET_FEATURES=True, но regime или ma_zone не прочитались

position_unknown
    нет подтверждённой позиции в positions_latest

ma_zone_direction_forbidden
    направление сигнала запрещено в текущей зоне

weak_signal_rejected
    сигнал против режима и weak_signal_policy="REJECT"

unknown_rule_id
    правило указано в ACTIVE_RULES, но не зарегистрировано в RULE_REGISTRY
```
