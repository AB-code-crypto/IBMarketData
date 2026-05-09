import asyncio

from core.logger import get_logger, log_info, setup_logging
from ib_signal.job_reader import get_job_db_status, get_last_job_bar_ts
from ib_signal.signal_settings import SignalSettings

setup_logging()
logger = get_logger(__name__)

SIGNAL_LOOP_SLEEP_SECONDS = 1
JOB_DB_WAIT_SECONDS = 5


def format_job_db_status(status) -> str:
    return (
        f"ready={status.is_ready}, "
        f"reason={status.reason}, "
        f"rows={status.rows_count}, "
        f"last_ts={status.last_bar_time_ts}, "
        f"lag={status.last_bar_lag_seconds}"
    )


async def wait_for_job_dbs(
        instrument_codes: list[str],
        settings: SignalSettings,
) -> list[str]:
    pending = set(instrument_codes)
    ready = []

    log_info(
        logger,
        f"Жду готовности job DB для signal-сервиса: {sorted(pending)}",
        to_telegram=False,
    )

    while pending:
        for instrument_code in list(pending):
            status = get_job_db_status(
                instrument_code=instrument_code,
                max_allowed_lag_seconds=settings.last_bar_safety_seconds,
            )

            if not status.is_ready:
                log_info(
                    logger,
                    f"{instrument_code}: job DB пока не готова: {format_job_db_status(status)}",
                    to_telegram=False,
                )
                continue

            log_info(
                logger,
                f"{instrument_code}: job DB готова: {format_job_db_status(status)}",
                to_telegram=False,
            )
            ready.append(instrument_code)
            pending.remove(instrument_code)

        if pending:
            await asyncio.sleep(JOB_DB_WAIT_SECONDS)

    return ready


async def run_signal_loop(
        instrument_codes: list[str],
        settings: SignalSettings,
) -> None:
    # Пока не ищем Pearson и не пишем сигналы.
    # Только отслеживаем появление новых bar_time_ts в job DB.
    last_seen_ts_by_instrument: dict[str, int | None] = {
        instrument_code: None
        for instrument_code in instrument_codes
    }

    log_info(
        logger,
        f"Запускаю signal-loop для инструментов: {instrument_codes}",
        to_telegram=False,
    )

    while True:
        for instrument_code in instrument_codes:
            status = get_job_db_status(
                instrument_code=instrument_code,
                max_allowed_lag_seconds=settings.last_bar_safety_seconds,
            )

            if not status.is_ready:
                log_info(
                    logger,
                    f"{instrument_code}: пропускаю расчёт, job DB не готова: {format_job_db_status(status)}",
                    to_telegram=False,
                )
                continue

            current_last_ts = get_last_job_bar_ts(instrument_code)
            previous_last_ts = last_seen_ts_by_instrument[instrument_code]

            if current_last_ts is None:
                continue

            if previous_last_ts is None:
                last_seen_ts_by_instrument[instrument_code] = current_last_ts
                log_info(
                    logger,
                    f"{instrument_code}: начальный последний bar_time_ts={current_last_ts}",
                    to_telegram=False,
                )
                continue

            if current_last_ts <= previous_last_ts:
                continue

            last_seen_ts_by_instrument[instrument_code] = current_last_ts

            log_info(
                logger,
                f"{instrument_code}: появился новый job bar_time_ts={current_last_ts}. "
                f"Дальше здесь будет расчёт сигнала.",
                to_telegram=False,
            )

        await asyncio.sleep(SIGNAL_LOOP_SLEEP_SECONDS)
