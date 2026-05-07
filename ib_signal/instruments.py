from contracts import Instrument


def get_signal_enabled_instrument_codes() -> list[str]:
    # Возвращает логические инструменты, с которыми должен работать signal-сервис.
    #
    # Signal-сервис работает только с инструментами, которые уже включены
    # в полный live-контур:
    # - history_enabled=True;
    # - realtime_enabled=True.
    #
    # Новые инструменты на лету не подхватываем. После изменения contracts.py
    # перезапускаем run_market_data.py, run_job_data.py и run_signal.py.
    result = []

    for instrument_code, instrument_row in Instrument.items():
        if not instrument_row.get("history_enabled", False):
            continue

        if not instrument_row.get("realtime_enabled", False):
            continue

        result.append(instrument_code)

    return result