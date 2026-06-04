from typing import Any, Dict

# Реестр логических инструментов, их контрактов, БД и режимов загрузки.

# ==============================
# Типы
# ==============================

InstrumentRow = Dict[str, Any]
Registry = Dict[str, InstrumentRow]

# Значение 111 используем как временную заглушку conId.
# Такой conId не передаётся в IB Contract, пока ты не заменишь его на настоящий.
PLACEHOLDER_CON_ID = 111

DEFAULTS_DATA: InstrumentRow = {
    "history_enabled": False,
    "realtime_enabled": False,
    "trading_enabled": False,
    "useRTH": False,
    "barSizeSetting": "5 secs",
    "price_digits": 2,
    "mid_price_digits": 3,
    "regression_flat_delta_threshold_bps": 1.0,
    "regime_flat_delta_threshold_points": 1.0,
    "price_tick": 0.25,
    "limit_offset_points": 30.0,
    "limit_ttl_seconds": 600,
    "take_profit_points": 100.0,
}

FUT_DEFAULTS: InstrumentRow = {
    **DEFAULTS_DATA,
    "secType": "FUT",
    "exchange": "CME",
    "currency": "USD",
    "session_model": "CME_EQUITY_INDEX",
}

FX_DEFAULTS: InstrumentRow = {
    **DEFAULTS_DATA,
    "secType": "CASH",
    "exchange": "IDEALPRO",
    "session_model": "FX_24_5",
    "price_digits": 5,
    "mid_price_digits": 6,
}

CRYPTO_DEFAULTS: InstrumentRow = {
    **DEFAULTS_DATA,
    "secType": "CRYPTO",
    "currency": "USD",
    "session_model": "CRYPTO_24_7",
    # "history_lookback_days": 50,
}

# ==============================
# Реестр
# ==============================

Instrument: Registry = {
    "MNQ": {
        **FUT_DEFAULTS,
        "tradingClass": "MNQ",
        "multiplier": 2.0,
        "db_filename": "MNQ.sqlite3",
        "history_enabled": True,
        "realtime_enabled": True,
        "trading_enabled": True,
        "regression_flat_delta_threshold_bps": 3.0,
        "take_profit_points": 90.0,
        "contracts": [
            {"conId": 620730945, "localSymbol": "MNQM4", "lastTradeDateOrContractMonth": "20240621",
             "active_from_utc": "2024-03-13T22:00:00Z", "active_to_utc": "2024-06-19T17:00:00Z"},

            {"conId": 637533593, "localSymbol": "MNQU4", "lastTradeDateOrContractMonth": "20240920",
             "active_from_utc": "2024-06-19T22:00:00Z", "active_to_utc": "2024-09-18T21:00:00Z"},

            {"conId": 654503320, "localSymbol": "MNQZ4", "lastTradeDateOrContractMonth": "20241220",
             "active_from_utc": "2024-09-18T22:00:00Z", "active_to_utc": "2024-12-18T22:00:00Z"},

            {"conId": 672387468, "localSymbol": "MNQH5", "lastTradeDateOrContractMonth": "20250321",
             "active_from_utc": "2024-12-18T23:00:00Z", "active_to_utc": "2025-03-19T21:00:00Z"},

            {"conId": 691171685, "localSymbol": "MNQM5", "lastTradeDateOrContractMonth": "20250620",
             "active_from_utc": "2025-03-19T22:00:00Z", "active_to_utc": "2025-06-18T21:00:00Z"},

            {"conId": 711280073, "localSymbol": "MNQU5", "lastTradeDateOrContractMonth": "20250919",
             "active_from_utc": "2025-06-18T22:00:00Z", "active_to_utc": "2025-09-17T21:00:00Z"},

            {"conId": 730283094, "localSymbol": "MNQZ5", "lastTradeDateOrContractMonth": "20251219",
             "active_from_utc": "2025-09-17T22:00:00Z", "active_to_utc": "2025-12-17T22:00:00Z"},

            {"conId": 750150193, "localSymbol": "MNQH6", "lastTradeDateOrContractMonth": "20260320",
             "active_from_utc": "2025-12-17T23:00:00Z", "active_to_utc": "2026-03-18T21:00:00Z"},

            {"conId": 770561201, "localSymbol": "MNQM6", "lastTradeDateOrContractMonth": "20260618",
             "active_from_utc": "2026-03-18T22:00:00Z", "active_to_utc": "2026-06-16T21:00:00Z"},

            {"conId": 793356225, "localSymbol": "MNQU6", "lastTradeDateOrContractMonth": "20260918",
             "active_from_utc": "2026-06-16T22:00:00Z", "active_to_utc": "2026-09-16T21:00:00Z"},

            {"conId": 815824267, "localSymbol": "MNQZ6", "lastTradeDateOrContractMonth": "20261218",
             "active_from_utc": "2026-09-16T22:00:00Z", "active_to_utc": "2026-12-16T22:00:00Z"},
        ]
    },

    "EURUSD": {
        **FX_DEFAULTS,
        "symbol": "EUR",
        "currency": "USD",
        "db_filename": "EURUSD.sqlite3",
        "history_start_utc": "2025-09-22T01:00:00Z",
        "regression_flat_delta_threshold_bps": 2.0,
        "regime_flat_delta_threshold_points": 0.00008,
    },

    "MES": {
        **FUT_DEFAULTS,
        "tradingClass": "MES",
        "multiplier": 5.0,
        "db_filename": "MES.sqlite3",
        "history_enabled": True,
        "realtime_enabled": True,
        "trading_enabled": True,
        "regime_flat_delta_threshold_points": 0.4,
        "take_profit_points": 35.0,
        "contracts": [
            {"conId": 620731036, "localSymbol": "MESM4", "lastTradeDateOrContractMonth": "20240621",
             "active_from_utc": "2024-03-13T22:00:00Z", "active_to_utc": "2024-06-19T17:00:00Z"},

            {"conId": 637533398, "localSymbol": "MESU4", "lastTradeDateOrContractMonth": "20240920",
             "active_from_utc": "2024-06-19T22:00:00Z", "active_to_utc": "2024-09-18T21:00:00Z"},

            {"conId": 654503314, "localSymbol": "MESZ4", "lastTradeDateOrContractMonth": "20241220",
             "active_from_utc": "2024-09-18T22:00:00Z", "active_to_utc": "2024-12-18T22:00:00Z"},

            {"conId": 672387462, "localSymbol": "MESH5", "lastTradeDateOrContractMonth": "20250321",
             "active_from_utc": "2024-12-18T23:00:00Z", "active_to_utc": "2025-03-19T21:00:00Z"},

            {"conId": 691171673, "localSymbol": "MESM5", "lastTradeDateOrContractMonth": "20250620",
             "active_from_utc": "2025-03-19T22:00:00Z", "active_to_utc": "2025-06-18T21:00:00Z"},

            {"conId": 711280067, "localSymbol": "MESU5", "lastTradeDateOrContractMonth": "20250919",
             "active_from_utc": "2025-06-18T22:00:00Z", "active_to_utc": "2025-09-17T21:00:00Z"},

            {"conId": 730283085, "localSymbol": "MESZ5", "lastTradeDateOrContractMonth": "20251219",
             "active_from_utc": "2025-09-17T22:00:00Z", "active_to_utc": "2025-12-17T22:00:00Z"},

            {"conId": 750150186, "localSymbol": "MESH6", "lastTradeDateOrContractMonth": "20260320",
             "active_from_utc": "2025-12-17T23:00:00Z", "active_to_utc": "2026-03-18T21:00:00Z"},

            {"conId": 770561194, "localSymbol": "MESM6", "lastTradeDateOrContractMonth": "20260618",
             "active_from_utc": "2026-03-18T22:00:00Z", "active_to_utc": "2026-06-16T21:00:00Z"},

            {"conId": 793356217, "localSymbol": "MESU6", "lastTradeDateOrContractMonth": "20260918",
             "active_from_utc": "2026-06-16T22:00:00Z", "active_to_utc": "2026-09-16T21:00:00Z"},

            {"conId": 815824257, "localSymbol": "MESZ6", "lastTradeDateOrContractMonth": "20261218",
             "active_from_utc": "2026-09-16T22:00:00Z", "active_to_utc": "2026-12-16T22:00:00Z"},
        ]
    },

    "NQ": {
        **FUT_DEFAULTS,
        "tradingClass": "NQ",
        "multiplier": 20.0,
        "db_filename": "NQ.sqlite3",
        "contracts": [
            {"conId": 620730920, "localSymbol": "NQM4", "lastTradeDateOrContractMonth": "20240621",
             "active_from_utc": "2024-03-13T22:00:00Z", "active_to_utc": "2024-06-19T17:00:00Z"},

            {"conId": 637533450, "localSymbol": "NQU4", "lastTradeDateOrContractMonth": "20240920",
             "active_from_utc": "2024-06-19T22:00:00Z", "active_to_utc": "2024-09-18T21:00:00Z"},

            {"conId": 563947733, "localSymbol": "NQZ4", "lastTradeDateOrContractMonth": "20241220",
             "active_from_utc": "2024-09-18T22:00:00Z", "active_to_utc": "2024-12-18T22:00:00Z"},

            {"conId": 666754605, "localSymbol": "NQH5", "lastTradeDateOrContractMonth": "20250321",
             "active_from_utc": "2024-12-18T23:00:00Z", "active_to_utc": "2025-03-19T21:00:00Z"},

            {"conId": 672387474, "localSymbol": "NQM5", "lastTradeDateOrContractMonth": "20250620",
             "active_from_utc": "2025-03-19T22:00:00Z", "active_to_utc": "2025-06-18T21:00:00Z"},

            {"conId": 691171690, "localSymbol": "NQU5", "lastTradeDateOrContractMonth": "20250919",
             "active_from_utc": "2025-06-18T22:00:00Z", "active_to_utc": "2025-09-17T21:00:00Z"},

            {"conId": 563947738, "localSymbol": "NQZ5", "lastTradeDateOrContractMonth": "20251219",
             "active_from_utc": "2025-09-17T22:00:00Z", "active_to_utc": "2025-12-17T22:00:00Z"},

            {"conId": 730283097, "localSymbol": "NQH6", "lastTradeDateOrContractMonth": "20260320",
             "active_from_utc": "2025-12-17T23:00:00Z", "active_to_utc": "2026-03-18T21:00:00Z"},

            {"conId": 750150196, "localSymbol": "NQM6", "lastTradeDateOrContractMonth": "20260618",
             "active_from_utc": "2026-03-18T22:00:00Z", "active_to_utc": "2026-06-16T21:00:00Z"},

            {"conId": 770561204, "localSymbol": "NQU6", "lastTradeDateOrContractMonth": "20260918",
             "active_from_utc": "2026-06-16T22:00:00Z", "active_to_utc": "2026-09-16T21:00:00Z"},

            {"conId": 563947726, "localSymbol": "NQZ6", "lastTradeDateOrContractMonth": "20261218",
             "active_from_utc": "2026-09-16T22:00:00Z", "active_to_utc": "2026-12-16T22:00:00Z"},
        ]
    },

    "ES": {
        **FUT_DEFAULTS,
        "tradingClass": "ES",
        "multiplier": 50.0,
        "db_filename": "ES.sqlite3",
        # Для нового инструмента ограничиваем начальную загрузку двумя неделями.
        # Если нужна более длинная история, увеличь history_lookback_days вручную.
        # "history_lookback_days": 14,
        "contracts": [
            {"conId": 551601561, "localSymbol": "ESM4", "lastTradeDateOrContractMonth": "20240621",
             "active_from_utc": "2024-03-13T22:00:00Z", "active_to_utc": "2024-06-19T17:00:00Z"},

            {"conId": 568550526, "localSymbol": "ESU4", "lastTradeDateOrContractMonth": "20240920",
             "active_from_utc": "2024-06-19T22:00:00Z", "active_to_utc": "2024-09-18T21:00:00Z"},

            {"conId": 495512557, "localSymbol": "ESZ4", "lastTradeDateOrContractMonth": "20241220",
             "active_from_utc": "2024-09-18T22:00:00Z", "active_to_utc": "2024-12-18T22:00:00Z"},

            {"conId": 603558932, "localSymbol": "ESH5", "lastTradeDateOrContractMonth": "20250321",
             "active_from_utc": "2024-12-18T23:00:00Z", "active_to_utc": "2025-03-19T21:00:00Z"},

            {"conId": 620731015, "localSymbol": "ESM5", "lastTradeDateOrContractMonth": "20250620",
             "active_from_utc": "2025-03-19T22:00:00Z", "active_to_utc": "2025-06-18T21:00:00Z"},

            {"conId": 637533641, "localSymbol": "ESU5", "lastTradeDateOrContractMonth": "20250919",
             "active_from_utc": "2025-06-18T22:00:00Z", "active_to_utc": "2025-09-17T21:00:00Z"},

            {"conId": 495512563, "localSymbol": "ESZ5", "lastTradeDateOrContractMonth": "20251219",
             "active_from_utc": "2025-09-17T22:00:00Z", "active_to_utc": "2025-12-17T22:00:00Z"},

            {"conId": 649180695, "localSymbol": "ESH6", "lastTradeDateOrContractMonth": "20260320",
             "active_from_utc": "2025-12-17T23:00:00Z", "active_to_utc": "2026-03-18T21:00:00Z"},

            {"conId": 649180678, "localSymbol": "ESM6", "lastTradeDateOrContractMonth": "20260618",
             "active_from_utc": "2026-03-18T22:00:00Z", "active_to_utc": "2026-06-16T21:00:00Z"},

            {"conId": 649180671, "localSymbol": "ESU6", "lastTradeDateOrContractMonth": "20260918",
             "active_from_utc": "2026-06-16T22:00:00Z", "active_to_utc": "2026-09-16T21:00:00Z"},

            {"conId": 515416632, "localSymbol": "ESZ6", "lastTradeDateOrContractMonth": "20261218",
             "active_from_utc": "2026-09-16T22:00:00Z", "active_to_utc": "2026-12-16T22:00:00Z"},
        ]
    },

    "BTCUSD": {
        **CRYPTO_DEFAULTS,
        "symbol": "BTC",
        "exchange": "PAXOS",
        "db_filename": "BTCUSD.sqlite3",
        "history_start_utc": "2025-09-15T01:00:00Z",
        "regression_flat_delta_threshold_bps": 10.0,
        "regime_flat_delta_threshold_points": 10.0,
    },
}
