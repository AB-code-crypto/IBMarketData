import time

from ib_signal.job_reader import get_job_db_status, get_last_job_bar_ts

SIGNAL_LOOP_SLEEP_SECONDS = 1
JOB_DB_WAIT_SECONDS = 5


def format_job_db_status(status) -> str:
    return (
        f"ready={status.is_ready}, "
        f"reason={status.reason}, "
        f"rows={status.rows_count}, "
        f"min_ts={status.min_bar_time_ts}, "
        f"max_ts={status.max_bar_time_ts}, "
        f"lag={status.last_bar_lag_seconds}, "
        f"expected_flow={status.expected_realtime_flow}, "
        f"db={status.job_db_path}"
    )


def wait_for_job_dbs(instrument_codes: list[str], log_message) -> list[str]:
    pending = set(instrument_codes)
    ready = []

    log_message(f"Жду готовности job DB для signal-сервиса: {sorted(pending)}")

    while pending:
        for instrument_code in list(pending):
            status = get_job_db_status(instrument_code)

            if not status.is_ready:
                log_message(f"{instrument_code}: job DB пока не готова: {format_job_db_status(status)}")
                continue

            log_message(f"{instrument_code}: job DB готова: {format_job_db_status(status)}")
            ready.append(instrument_code)
            pending.remove(instrument_code)

        if pending:
            time.sleep(JOB_DB_WAIT_SECONDS)

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
            status = get_job_db_status(instrument_code)

            if not status.is_ready:
                log_message(f"{instrument_code}: пропускаю расчёт, job DB не готова: {format_job_db_status(status)}")
                continue

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
