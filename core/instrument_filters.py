from contracts import Instrument


def get_live_enabled_instrument_codes() -> list[str]:
    # Возвращает логические инструменты, включённые в полный live-контур.
    #
    # Для live-контура нужны оба признака:
    # - history_enabled=True  -> по инструменту есть/должна быть price DB;
    # - realtime_enabled=True -> инструмент участвует в realtime-потоке.
    #
    # Новые инструменты на лету не подхватываем.
    # После изменения contracts.py перезапускаем нужные сервисы.
    result = []

    for instrument_code, instrument_row in Instrument.items():
        if instrument_row["history_enabled"] and instrument_row["realtime_enabled"]:
            result.append(instrument_code)

    return result
