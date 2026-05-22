# Конфиг правил ib_trader.
# Здесь включаются/отключаются правила и задаются их параметры.
# Новое правило добавляется так:
#   1. создать файл в ib_trader/rules/
#   2. добавить класс в ib_trader/rules/registry.py
#   3. добавить блок в ACTIVE_RULES ниже

ACTIVE_RULES = [
    {
        "id": "order_type_by_session",
        "enabled": True,
        "priority": 10,
        "params": {
            # По Чикаго. В эти окна открываемся лимитниками.
            "limit_time_windows_ct": [
                ("08:30", "10:30"),
            ],

            # Если цена ушла за третью дальнюю границу MA-zone, считаем,
            # что текущая волатильность/вынос повышены, и работаем лимитником.
            "limit_if_abs_ma_zone_at_least": 4,

            "default_order_type": "MARKET",
            "limit_order_type": "LIMIT",

            # Первый простой offset лимитника в пунктах.
            # LONG  -> entry_price - offset
            # SHORT -> entry_price + offset
            "limit_offset_points": 1.0,

            # Сколько секунд держим лимитник живым.
            "limit_ttl_seconds": 60,
        },
    },
    {
        "id": "zone_direction_policy",
        "enabled": True,
        "priority": 20,
        "params": {
            # ±1 и центр — можно в любую сторону.
            # ±3/±4 — только возврат к средней.
            # ±2 пока оставлены мягкими, чтобы не зажать систему раньше тестов.
            "allowed_directions_by_zone": {
                -4: ["LONG"],
                -3: ["LONG"],
                -2: ["LONG", "SHORT"],
                -1: ["LONG", "SHORT"],
                 0: ["LONG", "SHORT"],
                 1: ["LONG", "SHORT"],
                 2: ["LONG", "SHORT"],
                 3: ["SHORT"],
                 4: ["SHORT"],
            },
        },
    },
    {
        "id": "regime_signal_strength",
        "enabled": True,
        "priority": 30,
        "params": {
            # ALLOW  — слабый сигнал пишем как WEAK, но не запрещаем.
            # REJECT — запрещаем сигнал против режима.
            "weak_signal_policy": "ALLOW",
        },
    },
]
