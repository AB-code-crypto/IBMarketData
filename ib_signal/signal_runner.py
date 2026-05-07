import time

from ib_signal.job_reader import get_last_job_bar_ts, is_job_db_ready

SIGNAL_LOOP_SLEEP_SECONDS = 1


def wait_for_job_dbs(instrument_codes: list[str], log_message) -> list[str]:
    pending = set(instrument_codes)
    ready = []

    log_message(f"Жду готовности job DB для signal-сервиса: {sorted(pending)}")

    while pending:
        for instrument_code in list(pending):
            if not is_job_db_ready(instrument_code):
                continue

            log_message(f"{instrument_code}: job DB готова для signal-сервиса")
            ready.append(instrument_code)
            pending.remove(instrument_code)

        if pending:
            log_message(f"Job DB пока не готовы: {sorted(pending)}")
            time.sleep(5)

    return ready


def run_signal_loop(instrument_codes: list[str], log_message) -> None:
    # Первый каркас signal-сервиса.
    #
    # Пока не ищем Pearson и не пишем сигналы.
    # Только отслеживаем появление новых bar_time_ts в job DB.
    last_seen_ts_by_instrument: dict[str, int | None] = {
        instrument_code: None
        for instrument_code in instrument_codes
    }

    log_message(f"Запускаю signal-loop для инструментов: {instrument_codes}")

    while True:
        for instrument_code in instrument_codes:
            current_last_ts = get_last_job_bar_ts(instrument_code)
            previous_last_ts = last_seen_ts_by_instrument[instrument_code]

            if current_last_ts is None:
                continue

            if previous_last_ts is None:
                last_seen_ts_by_instrument[instrument_code] = current_last_ts
                log_message(f"{instrument_code}: начальный последний bar_time_ts={current_last_ts}")
                continue

            if current_last_ts <= previous_last_ts:
                continue

            last_seen_ts_by_instrument[instrument_code] = current_last_ts

            log_message(
                f"{instrument_code}: появился новый job bar_time_ts={current_last_ts}. "
                f"Дальше здесь будет расчёт сигнала."
            )

            # Следующий этап:
            # - построить SignalWindow;
            # - прочитать back window;
            # - найти Pearson-кандидатов;
            # - агрегировать прогноз;
            # - записать сигнал.

        time.sleep(SIGNAL_LOOP_SLEEP_SECONDS)