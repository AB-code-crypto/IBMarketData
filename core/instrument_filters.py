from contracts import Instrument


def get_trading_enabled_instrument_codes() -> list[str]:
    """Что делает: возвращает инструменты, включённые в контур сигналов и будущей торговли.
    Зачем нужна: signal и trader работают только там, где включены history, realtime и trading."""
    result = []

    for instrument_code, instrument_row in Instrument.items():
        if (
            instrument_row["history_enabled"]
            and instrument_row["realtime_enabled"]
            and instrument_row["trading_enabled"]
        ):
            result.append(instrument_code)

    return result
