# Простые правила интерпретации raw-сигнала.
#
# Здесь нет поиска сигнала. Поиск живёт в signal_config.py.
# Здесь только ответ на вопрос:
#   если raw-сигнал уже найден, разрешён ли вход и каким ордером его торговать.

SIGNAL_RULES = {
    # LONG разрешён, если цена в зоне +1 или ниже.
    "long_allowed_ma_zones": [-4, -3, -2, -1, 0, 1],

    # SHORT разрешён, если цена в зоне -1 или выше.
    "short_allowed_ma_zones": [-1, 0, 1, 2, 3, 4],

    # В этих зонах вход только лимитным ордером.
    "limit_order_ma_zones": [-4, 4],

    # В эти окна Chicago Time вход только лимитным ордером.
    "limit_order_time_windows_ct": [
        ("07:00", "08:00"),
        ("08:00", "09:00"),
        ("09:00", "10:00"),
    ],

    # Сила сигнала по режиму.
    "strong_long_regimes": [1],
    "strong_short_regimes": [-1],
    "neutral_regimes": [0],
}


SIGNAL_RULE_SETTINGS = {
    # Если True, без regime/ma_zone сигнал запрещается.
    "require_market_features": True,

    # ALLOW — слабый сигнал разрешён.
    # REJECT — слабый сигнал запрещён.
    "weak_signal_policy": "ALLOW",

    "default_order_type": "MARKET",
    "limit_order_type": "LIMIT",

    # LONG  LIMIT = entry_price - offset
    # SHORT LIMIT = entry_price + offset
    "limit_offset_points": 10.0,

    # Жизнь лимитника. Для futures execution дополнительно обрежет TTL перед 14:59:50 CT.
    "limit_ttl_seconds": 600,
}
