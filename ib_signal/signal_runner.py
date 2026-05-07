import time

from ib_signal.job_reader import get_job_db_status, get_last_job_bar_ts
from datetime import datetime, timezone
from core.time_utils import CT_TIMEZONE, MSK_TIMEZONE, SQLITE_DATETIME_FORMAT

SIGNAL_LOOP_SLEEP_SECONDS = 1
JOB_DB_WAIT_SECONDS = 5


def format_ts_for_status(ts: int | None) -> str:
    if ts is None:
        return "-"

    dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
    dt_msk = dt_utc.astimezone(MSK_TIMEZONE)
    dt_ct = dt_utc.astimezone(CT_TIMEZONE)

    return (
        f"msk={dt_msk.strftime(SQLITE_DATETIME_FORMAT)}, "
        f"ct={dt_ct.strftime(SQLITE_DATETIME_FORMAT)}"
    )


def format_job_db_status(status) -> str:
    return (
        f"ready={status.is_ready}, "
        f"reason={status.reason}, "
        f"rows={status.rows_count}, "
        f"last_time=({format_ts_for_status(status.last_bar_time_ts)}), "
        f"lag={status.last_bar_lag_seconds}"
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

        time.sleep(SIGNAL_LOOP_SLEEP_SECONDS)
