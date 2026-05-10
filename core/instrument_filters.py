from contracts import Instrument


def get_live_enabled_instrument_codes() -> list[str]:
    """Что делает: возвращает инструменты, у которых включены и history, и realtime. Зачем нужна: job-data и signal работают только по полному live-контуру."""
    result = []

    for instrument_code, instrument_row in Instrument.items():
        if instrument_row["history_enabled"] and instrument_row["realtime_enabled"]:
            result.append(instrument_code)

    return result
