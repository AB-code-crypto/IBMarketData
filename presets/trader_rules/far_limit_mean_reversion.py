# Preset: far_limit_mean_reversion
#
# Режим:
#   - разрешены только LIMIT-ордера;
#   - разрешены только зоны ±3/±4;
#   - направление только на возврат к средней.
#
# Логика:
#   ma_zone +3/+4 -> только SHORT
#   ma_zone -3/-4 -> только LONG
#   остальные зоны -> запрещены
#
# Смещение лимитника:
#   LONG  -> entry_price - 10 пунктов
#   SHORT -> entry_price + 10 пунктов

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
