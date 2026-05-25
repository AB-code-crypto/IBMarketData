# Правила `ib_trader`

## Главная идея

После появления сигнала `ib_trader` применяет простой набор словарных правил из:

```text
ib_trader/trader_rules_config.py
```

Больше нет:

```text
ib_trader/rules/
presets/
apply_trader_rules_preset.py
```

Вся торговая логика читается в одном файле.

## Что проверяется

### 1. Направление по зоне

```python
"long_allowed_ma_zones": [-4, -3, -2, -1, 0, 1]
"short_allowed_ma_zones": [-1, 0, 1, 2, 3, 4]
```

LONG разрешён только в зонах из `long_allowed_ma_zones`.

SHORT разрешён только в зонах из `short_allowed_ma_zones`.

### 2. Тип ордера

```python
"limit_order_ma_zones": [-4, 4]
"limit_order_time_windows_ct": [
    ("07:00", "08:00"),
    ("08:00", "09:00"),
    ("09:00", "10:00"),
]
```

LIMIT используется, если:

```text
ma_zone in limit_order_ma_zones
или
signal_time_ct попадает во временное окно
```

Иначе используется MARKET.

### 3. Сила сигнала

```python
"strong_long_regimes": [1]
"strong_short_regimes": [-1]
"neutral_regimes": [0]
```

LONG при `regime=1` — STRONG.

SHORT при `regime=-1` — STRONG.

`regime=0` — NEUTRAL.

Остальное — WEAK.

## Настройки

```python
TRADER_RULE_SETTINGS = {
    "require_market_features": True,
    "weak_signal_policy": "ALLOW",
    "default_order_type": "MARKET",
    "limit_order_type": "LIMIT",
    "limit_offset_points": 10.0,
    "limit_ttl_seconds": 60,
}
```

### `require_market_features`

Если `True`, то без `regime` или `ma_zone` сделка запрещается.

Если `False`, то market-фильтры не применяются, вход идёт только по `signal.direction`, order_type = default.

### `weak_signal_policy`

```text
ALLOW  — слабый сигнал разрешён, но пишется signal_strength=WEAK
REJECT — слабый сигнал запрещён
```

## Где смотреть результат

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
