# Preset: session_limit_near_market
#
# Время по Chicago Time:
#   07:00-08:00 CT -> LIMIT, offset 10
#   08:00-09:00 CT -> LIMIT, offset 10
#   09:00-10:00 CT -> LIMIT, offset 10
#
# Остальное время:
#   MARKET
#
# Разрешённые зоны:
#   ma_zone = -1, 0, +1
#
# Режим:
#   direction совпадает с regime -> STRONG
#   direction против regime      -> WEAK
#   regime = 0                  -> NEUTRAL
#
# Сейчас WEAK разрешён, но помечается в trade_decisions.signal_strength.
#
# Важно:
#   zone_direction_policy сейчас глобальный.
#   Поэтому зоны -1/0/+1 применяются и для LIMIT-окон, и для MARKET-времени.

# REQUIRE_MARKET_FEATURES:
#   False — ib_trader может принимать решение без regime/ma_zone.
#           Используется для режима potential_only, когда торгуем только по signal.direction / potential.
#
#   True  — regime и ma_zone обязательны.
#           Если хотя бы одно значение не прочитано из job DB, сигнал отклоняется:
#           decision_action = NO_ACTION
#           decision_reason = market_features_unknown.
#
# Практическое правило:
#   - если ACTIVE_RULES пустой и торгуем только по potential -> False;
#   - если хотя бы одно активное правило использует regime или ma_zone -> True.

REQUIRE_MARKET_FEATURES = True

ACTIVE_RULES = [
    {
        "id": "order_type_by_session",
        "enabled": True,
        "priority": 10,
        "params": {
            "limit_time_windows_ct": [
                ("07:00", "08:00"),
                ("08:00", "09:00"),
                ("09:00", "10:00"),
            ],
            "limit_if_abs_ma_zone_at_least": 0,
            "default_order_type": "MARKET",
            "limit_order_type": "LIMIT",
            "limit_offset_points": 10.0,
            "limit_ttl_seconds": 60,
        },
    },
    {
        "id": "zone_direction_policy",
        "enabled": True,
        "priority": 20,
        "params": {
            "allowed_directions_by_zone": {
                -4: [],
                -3: [],
                -2: [],
                -1: ["LONG", "SHORT"],
                 0: ["LONG", "SHORT"],
                 1: ["LONG", "SHORT"],
                 2: [],
                 3: [],
                 4: [],
            },
        },
    },
    {
        "id": "regime_signal_strength",
        "enabled": True,
        "priority": 30,
        "params": {
            "weak_signal_policy": "ALLOW",
        },
    },
]
