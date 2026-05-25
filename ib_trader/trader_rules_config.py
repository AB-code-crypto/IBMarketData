# Конфигурация правил ib_trader.
#
# Это единственное место, где задаётся торговая логика после появления сигнала.
#
# Общая цепочка:
#   ib_signal нашёл сигнал LONG/SHORT
#   ib_trader читает signal + regime + ma_zone + position
#   ib_trader применяет правила ниже
#   ib_trader пишет trade_decisions / trade_intents
#   ib_execution только исполняет trade_intents
#
# Термины:
#   LONG  = покупка
#   SHORT = продажа
#
# ma_zone:
#    4  цена выше дальней границы upper_far
#    3  upper_far
#    2  upper_middle
#    1  upper_near
#    0  около MA / центр
#   -1  lower_near
#   -2  lower_middle
#   -3  lower_far
#   -4  цена ниже дальней границы lower_far
#
# regime:
#    1  режим вверх
#    0  флет
#   -1  режим вниз


# ============================================================
# ПРАВИЛА
# ============================================================
#
# Каждый ключ — отдельное правило.
# Значение — список допустимых значений.
#
# Логика:
#
# 1. Проверка направления по зоне:
#       LONG  разрешён, если ma_zone in long_allowed_ma_zones
#       SHORT разрешён, если ma_zone in short_allowed_ma_zones
#
# 2. Выбор типа ордера:
#       если ma_zone in limit_order_ma_zones -> LIMIT
#       если signal_time_ct попадает в limit_order_time_windows_ct -> LIMIT
#       иначе -> MARKET
#
# 3. Сила сигнала:
#       LONG  + regime in strong_long_regimes  -> STRONG
#       SHORT + regime in strong_short_regimes -> STRONG
#       regime in neutral_regimes             -> NEUTRAL
#       иначе                                 -> WEAK

TRADER_RULES = {
    # Покупка разрешена, если цена находится в зоне +1 или ниже.
    "long_allowed_ma_zones": [-4, -3, -2, -1, 0, 1],

    # Продажа разрешена, если цена находится в зоне -1 или выше.
    "short_allowed_ma_zones": [-1, 0, 1, 2, 3, 4],

    # Зоны повышенного выноса / текущей волатильности.
    # В этих зонах открываемся только лимитным ордером.
    "limit_order_ma_zones": [-4, 4],

    # Временные окна по Chicago Time, где открываемся только лимитным ордером.
    "limit_order_time_windows_ct": [
        ("07:00", "08:00"),
        ("08:00", "09:00"),
        ("09:00", "10:00"),
    ],

    # Если LONG совпал с режимом вверх, сигнал сильный.
    "strong_long_regimes": [1],

    # Если SHORT совпал с режимом вниз, сигнал сильный.
    "strong_short_regimes": [-1],

    # Если режим флетовый, сигнал нейтральный.
    "neutral_regimes": [0],
}


# ============================================================
# НАСТРОЙКИ ПОВЕДЕНИЯ
# ============================================================
#
# Это не правила допуска, а параметры исполнения и политики.

TRADER_RULE_SETTINGS = {
    # True:
    #   regime и ma_zone обязательны.
    #   Если их нет, решение = NO_ACTION.
    #
    # False:
    #   можно принимать решение без regime/ma_zone.
    #   В этом режиме вход идёт только по signal.direction / potential,
    #   а order_type остаётся default_order_type.
    "require_market_features": True,

    # Что делать со слабым сигналом:
    #   "ALLOW"  — разрешить, но пометить signal_strength = WEAK
    #   "REJECT" — запретить вход
    "weak_signal_policy": "ALLOW",

    # Тип ордера по умолчанию.
    "default_order_type": "MARKET",

    # Тип ордера при срабатывании limit_order_ma_zones
    # или limit_order_time_windows_ct.
    "limit_order_type": "LIMIT",

    # Смещение лимитника в пунктах.
    # LONG  -> entry_price - limit_offset_points
    # SHORT -> entry_price + limit_offset_points
    "limit_offset_points": 10.0,

    # Сколько секунд держим лимитник живым.
    "limit_ttl_seconds": 600,
}


# ============================================================
# БЫСТРЫЙ РЕЖИМ "ТОЛЬКО ПО ПОТЕНЦИАЛУ"
# ============================================================
#
# Если нужно временно отключить все market-фильтры:
#
#   1. Поставить:
#          "require_market_features": False
#
#   2. Очистить списки зон/режимов или просто оставить как есть:
#      при require_market_features=False evaluator не применяет zone/regime-ограничения.
#
# В этом режиме:
#   - вход туда, куда говорит signal.direction;
#   - regime не проверяется;
#   - ma_zone не проверяется;
#   - время не проверяется;
#   - order_type = default_order_type.
