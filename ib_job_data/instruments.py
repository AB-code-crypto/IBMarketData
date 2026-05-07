from contracts import Instrument


def get_job_data_enabled_instrument_codes() -> list[str]:
    # Возвращает логические инструменты, с которыми должен работать job-data сервис.
    #
    # Для подготовки job DB нужны оба признака:
    # - history_enabled=True  -> есть/должна быть историческая price DB;
    # - realtime_enabled=True -> инструмент реально участвует в live-потоке.
    #
    # Если инструмент только добавлен, но история ещё не загружена,
    # realtime_enabled лучше держать False. После загрузки истории включаем
    # realtime_enabled=True и перезапускаем run_market_data.py / run_job_data.py.
    result = []

    for instrument_code, instrument_row in Instrument.items():
        if not instrument_row.get("history_enabled", False):
            continue

        if not instrument_row.get("realtime_enabled", False):
            continue

        result.append(instrument_code)

    return result
