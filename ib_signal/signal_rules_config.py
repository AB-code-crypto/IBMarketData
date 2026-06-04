# Простые правила интерпретации raw-сигнала.
#
# Поиск сигнала живёт в signal_config.py.
# Здесь только правила: можно ли торговать уже найденный LONG/SHORT
# и каким ордером это делать.

SIGNAL_RULES = {
    # LONG разрешён, если цена в зоне +1 или ниже.
    "long_allowed_ma_zones": [-4, -3, -2, -1, 0, 1],

    # SHORT разрешён, если цена в зоне -1 или выше.
    "short_allowed_ma_zones": [-1, 0, 1, 2, 3, 4],

    # В этих зонах вход только лимитным ордером.
    "limit_order_ma_zones": [-4, 4],

    # Экстремальные зоны для закрытия уже открытой позиции.
    # LONG закрываем, если ma_zone >= +4; SHORT закрываем, если ma_zone <= -4.
    "long_position_extreme_close_ma_zone": 4,
    "short_position_extreme_close_ma_zone": -4,

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

    # ALLOW — не запрещать сигналы против текущего regime.
    # REJECT — LONG разрешён только при regime=+1 или 0, SHORT только при regime=-1 или 0.
    "regime_direction_policy": "ALLOW",

    # Если True, открытая позиция закрывается при попадании цены в экстремальную MA-зону.
    "close_position_on_extreme_ma_zone_enabled": False,

    "default_order_type": "MARKET",
    "limit_order_type": "LIMIT",

}
