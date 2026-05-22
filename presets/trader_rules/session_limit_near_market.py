# Preset: session_limit_near_market
#
# 07:00-08:00 CT -> только LIMIT, offset 10 пунктов
# 08:00-09:00 CT -> только LIMIT, offset 10 пунктов
# 09:00-10:00 CT -> только LIMIT, offset 10 пунктов
#
# Остальное время -> MARKET.
#
# Разрешённые зоны: 0 и ±1.
# В этих зонах направление берётся из potential / signal.direction.
#
# Если направление совпадает с regime -> STRONG.
# Если против regime -> WEAK.
# Сейчас WEAK разрешён, но помечается в trade_decisions.signal_strength.
#
# Важно:
# zone_direction_policy сейчас глобальный, не раздельный по MARKET/LIMIT.
# Поэтому зоны 0/±1 применяются и для session LIMIT-окон, и для остального MARKET-времени.

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
