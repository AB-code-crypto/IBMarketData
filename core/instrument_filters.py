from contracts import Instrument


def is_instrument_history_enabled(instrument_row) -> bool:
    # History-флаг должен быть явно задан через defaults/overrides в contracts.py.
    return instrument_row["history_enabled"]


def is_instrument_realtime_enabled(instrument_row) -> bool:
    # Realtime-флаг должен быть явно задан через defaults/overrides в contracts.py.
    return instrument_row["realtime_enabled"]


def is_instrument_live_enabled(instrument_row) -> bool:
    # Полный live-контур требует и history, и realtime.
    return (
        is_instrument_history_enabled(instrument_row)
        and is_instrument_realtime_enabled(instrument_row)
    )


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
        if is_instrument_live_enabled(instrument_row):
            result.append(instrument_code)

    return result
