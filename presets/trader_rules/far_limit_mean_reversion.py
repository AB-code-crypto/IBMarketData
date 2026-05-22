# Preset: far_limit_mean_reversion
# Разрешены только LIMIT-ордера в дальних зонах ±3/±4,
# и только на возврат к средней.
#
# ma_zone +3/+4 -> только SHORT
# ma_zone -3/-4 -> только LONG
# остальные зоны запрещены.
#
# offset лимитника: 10 пунктов.

REQUIRE_MARKET_FEATURES = True

ACTIVE_RULES = [
    {
        "id": "order_type_by_session",
        "enabled": True,
        "priority": 10,
        "params": {
            "limit_time_windows_ct": [],
            "limit_if_abs_ma_zone_at_least": 3,
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
                -4: ["LONG"],
                -3: ["LONG"],
                -2: [],
                -1: [],
                 0: [],
                 1: [],
                 2: [],
                 3: ["SHORT"],
                 4: ["SHORT"],
            },
        },
    },
]
