from contracts import Instrument


def get_signal_enabled_instrument_codes() -> list[str]:
    # Возвращает логические инструменты, с которыми должен работать IBSignal.
    #
    # Для сигнального робота нужны оба признака:
    # - history_enabled=True  -> есть/должна быть историческая БД;
    # - realtime_enabled=True -> инструмент реально участвует в live-потоке.
    #
    # Если инструмент только добавлен, но история ещё не загружена,
    # он всё равно попадёт сюда. Следующий этап проверки уже решит,
    # есть ли price DB/job DB и можно ли с ним работать сейчас.
    result = []

    for instrument_code, instrument_row in Instrument.items():
        if not instrument_row.get("history_enabled", False):
            continue

        if not instrument_row.get("realtime_enabled", False):
            continue

        result.append(instrument_code)

    return result