import asyncio

from core.logger import get_logger, log_info, setup_logging
from ib_signal.job_reader import get_job_db_status
from ib_signal.signal_schedule import get_due_signal_bar_ts
from ib_signal.signal_settings import SignalSettings

setup_logging()
logger = get_logger(__name__)

SIGNAL_LOOP_SLEEP_SECONDS = 1
JOB_DB_WAIT_SECONDS = 5


def format_job_db_status(status) -> str:
    """Что делает: форматирует JobDbStatus в одну строку лога. Зачем нужна: ожидание и пропуски расчёта показывают конкретную причину неготовности job DB."""
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
    """Что делает: ждёт готовности job DB по всем instrument_codes. Зачем нужна: основной signal-loop стартует только после доступности рабочих данных."""
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
                max_job_bar_lag_seconds=settings.max_job_bar_lag_seconds,
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
    # last_seen нужен только для логирования появления нового бара.
    """Что делает: отслеживает новые job-бары и определяет due signal_bar_ts по активному режиму. Зачем нужна: это основной runtime-цикл signal-сервиса, пока без фактического расчёта сигнала."""
    last_seen_ts_by_instrument: dict[str, int | None] = {
        instrument_code: None
        for instrument_code in instrument_codes
    }

    # last_calculated защищает от повторного расчёта одного и того же signal bar.
    last_calculated_ts_by_instrument: dict[str, int | None] = {
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
                max_job_bar_lag_seconds=settings.max_job_bar_lag_seconds,
            )

            if not status.is_ready:
                log_info(
                    logger,
                    f"{instrument_code}: пропускаю расчёт, job DB не готова: {format_job_db_status(status)}",
                    to_telegram=False,
                )
                continue

            current_last_ts = status.last_bar_time_ts
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

            elif current_last_ts > previous_last_ts:
                last_seen_ts_by_instrument[instrument_code] = current_last_ts
                log_info(
                    logger,
                    f"{instrument_code}: появился новый job bar_time_ts={current_last_ts}",
                    to_telegram=False,
                )

            due_signal_bar_ts = get_due_signal_bar_ts(
                current_bar_ts=current_last_ts,
                settings=settings,
                last_calculated_bar_ts=last_calculated_ts_by_instrument[instrument_code],
            )

            if due_signal_bar_ts is None:
                continue

            last_calculated_ts_by_instrument[instrument_code] = due_signal_bar_ts

            log_info(
                logger,
                f"{instrument_code}: пора считать сигнал, "
                f"signal_bar_time_ts={due_signal_bar_ts}, "
                f"latest_job_bar_time_ts={current_last_ts}, "
                f"mode={settings.signal_window_mode.value}",
                to_telegram=False,
            )

            # Дальше здесь будет полный расчёт сигнала:
            # 1. построение текущего окна от signal_bar_time_ts;
            # 2. поиск исторических кандидатов;
            # 3. расчёт Pearson;
            # 4. фильтры;
            # 5. принятие торгового решения.

        await asyncio.sleep(SIGNAL_LOOP_SLEEP_SECONDS)
