# Простая конфигурация правил ib_trader.
#
# Этот файл пока НЕ подключён к ib_trader.
# Сначала проверяем, что формат понятный. Если он подходит, следующим шагом
# перепишем принятие решений на этот простой конфиг и удалим старую папку ib_trader/rules.
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
# ГЛАВНЫЙ НАБОР ПРОСТЫХ ПРАВИЛ
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

TRADER_SIMPLE_RULES = {
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

TRADER_SIMPLE_RULE_SETTINGS = {
    # True:
    #   regime и ma_zone обязательны.
    #   Если их нет, решение = NO_ACTION.
    #
    # False:
    #   можно принимать решение без regime/ma_zone.
    #   Используется только для режима potential_only.
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
    "limit_ttl_seconds": 60,
}


# ============================================================
# ПРЕСЕТ: ТОЛЬКО ПО ПОТЕНЦИАЛУ
# ============================================================
#
# Режим:
#   - входить туда, куда говорит signal.direction;
#   - не смотреть regime;
#   - не смотреть ma_zone;
#   - не смотреть время;
#   - использовать MARKET.
#
# Позиционная логика всё равно остаётся:
#   UNKNOWN -> NO_ACTION
#   FLAT -> OPEN_POSITION
#   same side -> NO_ACTION
#   opposite side -> REVERSE_POSITION

POTENTIAL_ONLY_RULES = {
    "long_allowed_ma_zones": [],
    "short_allowed_ma_zones": [],
    "limit_order_ma_zones": [],
    "limit_order_time_windows_ct": [],
    "strong_long_regimes": [],
    "strong_short_regimes": [],
    "neutral_regimes": [],
}

POTENTIAL_ONLY_SETTINGS = {
    "require_market_features": False,
    "weak_signal_policy": "ALLOW",
    "default_order_type": "MARKET",
    "limit_order_type": "LIMIT",
    "limit_offset_points": 10.0,
    "limit_ttl_seconds": 60,
}


# ============================================================
# БУДУЩАЯ ЛОГИКА ОЦЕНКИ
# ============================================================
#
# direction = signal.direction
# ma_zone = market_features.ma_zone
# regime = market_features.regime
#
# if settings["require_market_features"] and (ma_zone is None or regime is None):
#     reject("market_features_unknown")
#
# if direction == "LONG" and ma_zone not in rules["long_allowed_ma_zones"]:
#     reject("ma_zone_direction_forbidden")
#
# if direction == "SHORT" and ma_zone not in rules["short_allowed_ma_zones"]:
#     reject("ma_zone_direction_forbidden")
#
# order_type = settings["default_order_type"]
#
# if ma_zone in rules["limit_order_ma_zones"]:
#     order_type = settings["limit_order_type"]
#
# if signal_time_ct in rules["limit_order_time_windows_ct"]:
#     order_type = settings["limit_order_type"]
#
# if direction == "LONG" and regime in rules["strong_long_regimes"]:
#     signal_strength = "STRONG"
# elif direction == "SHORT" and regime in rules["strong_short_regimes"]:
#     signal_strength = "STRONG"
# elif regime in rules["neutral_regimes"]:
#     signal_strength = "NEUTRAL"
# else:
#     signal_strength = "WEAK"
#
# if signal_strength == "WEAK" and settings["weak_signal_policy"] == "REJECT":
#     reject("weak_signal_rejected")
