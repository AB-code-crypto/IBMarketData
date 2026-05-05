from typing import Any, Dict

# ==============================
# Типы
# ==============================

InstrumentRow = Dict[str, Any]
Registry = Dict[str, InstrumentRow]

FUT_DEFAULTS: InstrumentRow = {
    "secType": "FUT",
    "exchange": "CME",
    "currency": "USD",
    "barSizeSetting": "5 secs",
    "useRTH": False,
    "session_model": "CME_EQUITY_INDEX",
}

FX_DEFAULTS: InstrumentRow = {
    "secType": "CASH",
    "exchange": "IDEALPRO",
    "barSizeSetting": "5 secs",
    "useRTH": False,
    "session_model": "FX_24_5",
    "history_lookback_days": 50,
}

CRYPTO_DEFAULTS: InstrumentRow = {
    "secType": "CRYPTO",
    "currency": "USD",
    "barSizeSetting": "5 secs",
    "useRTH": False,
    "session_model": "CRYPTO_24_7",
    "history_lookback_days": 50,
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

    # "MES": {
    #     **FUT_DEFAULTS,
    #     "tradingClass": "MES",
    #     "multiplier": 5.0,
    #     "db_filename": "MES.sqlite3",
    #     # Для нового инструмента ограничиваем начальную загрузку двумя неделями.
    #     # Если нужна более длинная история, увеличь history_lookback_days вручную.
    #     "history_lookback_days": 14,
    #     "contracts": [
    #         {"localSymbol": "MESM4", "lastTradeDateOrContractMonth": "20240621",
    #          "active_from_utc": "2024-03-13T22:00:00Z", "active_to_utc": "2024-06-19T17:00:00Z"},
    #
    #         {"localSymbol": "MESU4", "lastTradeDateOrContractMonth": "20240920",
    #          "active_from_utc": "2024-06-19T22:00:00Z", "active_to_utc": "2024-09-18T21:00:00Z"},
    #
    #         {"localSymbol": "MESZ4", "lastTradeDateOrContractMonth": "20241220",
    #          "active_from_utc": "2024-09-18T22:00:00Z", "active_to_utc": "2024-12-18T22:00:00Z"},
    #
    #         {"localSymbol": "MESH5", "lastTradeDateOrContractMonth": "20250321",
    #          "active_from_utc": "2024-12-18T23:00:00Z", "active_to_utc": "2025-03-19T21:00:00Z"},
    #
    #         {"localSymbol": "MESM5", "lastTradeDateOrContractMonth": "20250620",
    #          "active_from_utc": "2025-03-19T22:00:00Z", "active_to_utc": "2025-06-18T21:00:00Z"},
    #
    #         {"localSymbol": "MESU5", "lastTradeDateOrContractMonth": "20250919",
    #          "active_from_utc": "2025-06-18T22:00:00Z", "active_to_utc": "2025-09-17T21:00:00Z"},
    #
    #         {"localSymbol": "MESZ5", "lastTradeDateOrContractMonth": "20251219",
    #          "active_from_utc": "2025-09-17T22:00:00Z", "active_to_utc": "2025-12-17T22:00:00Z"},
    #
    #         {"localSymbol": "MESH6", "lastTradeDateOrContractMonth": "20260320",
    #          "active_from_utc": "2025-12-17T23:00:00Z", "active_to_utc": "2026-03-18T21:00:00Z"},
    #
    #         {"localSymbol": "MESM6", "lastTradeDateOrContractMonth": "20260618",
    #          "active_from_utc": "2026-03-18T22:00:00Z", "active_to_utc": "2026-06-16T21:00:00Z"},
    #
    #         {"localSymbol": "MESU6", "lastTradeDateOrContractMonth": "20260918",
    #          "active_from_utc": "2026-06-16T22:00:00Z", "active_to_utc": "2026-09-16T21:00:00Z"},
    #
    #         {"localSymbol": "MESZ6", "lastTradeDateOrContractMonth": "20261218",
    #          "active_from_utc": "2026-09-16T22:00:00Z", "active_to_utc": "2026-12-16T22:00:00Z"},
    #     ]
    # },

    "EURUSD": {
        **FX_DEFAULTS,
        "symbol": "EUR",
        "currency": "USD",
        "db_filename": "EURUSD.sqlite3",
    },

    "BTCUSD": {
        **CRYPTO_DEFAULTS,
        "symbol": "BTC",
        # Для части аккаунтов IBKR crypto может быть доступен через ZEROHASH.
        # Если PAXOS не подходит твоему аккаунту, поменяй exchange на "ZEROHASH".
        "exchange": "PAXOS",
        "db_filename": "BTCUSD.sqlite3",
    },
}
