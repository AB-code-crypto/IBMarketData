import asyncio

from contracts import Instrument
from core.bar_utils import get_bar_size_seconds
from core.logger import get_logger, log_info, setup_logging
from ib_signal.job_reader import get_fresh_job_bar_status, read_job_bar_time_ct
from ib_signal.signal_schedule import get_due_signal_bar_ts
from ib_signal.signal_config import SignalConfig
from ib_signal.signal_window import build_current_signal_window, format_signal_window_for_log

setup_logging()
logger = get_logger(__name__)

SIGNAL_LOOP_SLEEP_SECONDS = 1
JOB_DB_WAIT_SECONDS = 5


def format_fresh_job_bar_status(status) -> str:
    """Что делает: форматирует статус свежести последнего job-бара в одну строку лога.
    Зачем нужна: в ожидании и runtime-пропусках видно, почему signal-сервис не считает сигнал."""
    return (
        f"ready={status.is_ready}, "
        f"reason={status.reason}, "
        f"last_bar={status.last_bar_time_ct}, "
        f"lag={status.last_bar_lag_seconds}"
    )


async def wait_for_fresh_job_bars(
        instrument_codes: list[str],
        settings: SignalConfig,
) -> list[str]:
    """Что делает: ждёт свежий последний job-бар по каждому instrument_code.
    Зачем нужна: signal-сервис стартует расчёт только после того, как job-data начал обновлять рабочие данные."""
    pending = set(instrument_codes)
    ready = []

    log_info(
        logger,
        f"Жду свежих job-баров для signal-сервиса: {sorted(pending)}",
        to_telegram=True,
    )

    while pending:
        for instrument_code in list(pending):
            status = get_fresh_job_bar_status(
                instrument_code=instrument_code,
                max_job_bar_lag_seconds=settings.max_job_bar_lag_seconds,
            )

            if not status.is_ready:
                log_info(
                    logger,
                    f"{instrument_code}: свежий job-бар пока не готов: "
                    f"{format_fresh_job_bar_status(status)}",
                    to_telegram=False,
                )
                continue

            log_info(
                logger,
                f"{instrument_code}: свежий job-бар готов: "
                f"{format_fresh_job_bar_status(status)}",
                to_telegram=False,
            )
            ready.append(instrument_code)
            pending.remove(instrument_code)

        if pending:
            await asyncio.sleep(JOB_DB_WAIT_SECONDS)

    return ready


async def run_signal_loop(
        instrument_codes: list[str],
        settings: SignalConfig,
) -> None:
    """Что делает: отслеживает новые job-бары и определяет due signal_bar_ts по активному режиму.
    Зачем нужна: это основной runtime-цикл signal-сервиса, пока без фактического расчёта сигнала."""

    # last_seen_ts_by_instrument - память для логирования появления нового job-бара.
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
        to_telegram=True,
    )

    while True:
        for instrument_code in instrument_codes:
            # status - лёгкая проверка свежести job DB
            status = get_fresh_job_bar_status(
                instrument_code=instrument_code,
                max_job_bar_lag_seconds=settings.max_job_bar_lag_seconds,
            )

            if not status.is_ready:
                log_info(
                    logger,
                    f"{instrument_code}: пропускаю расчёт, job-бар не свежий: "
                    f"{format_fresh_job_bar_status(status)}",
                    to_telegram=False,
                )
                continue

            # current_last_ts - последний доступный bar_time_ts в job DB.
            current_last_ts = status.last_bar_time_ts
            # previous_last_ts - прошлое значение current_last_ts для логирования.
            previous_last_ts = last_seen_ts_by_instrument[instrument_code]

            if current_last_ts is None:
                continue

            # bar_time_ts в job DB — это время начала бара.
            # Точка принятия решения должна быть на границе закрытия этого бара.
            # Размер бара берём из contracts.py, а не дублируем в signal config.
            bar_size_seconds = get_bar_size_seconds(Instrument[instrument_code]["barSizeSetting"])
            closed_bar_ts = current_last_ts + bar_size_seconds

            # due_signal_bar_ts - реальная точка принятия решения, для которой надо считать сигнал.
            due_signal_bar_ts = get_due_signal_bar_ts(
                current_bar_ts=closed_bar_ts,
                settings=settings,
                last_calculated_bar_ts=last_calculated_ts_by_instrument[instrument_code],
            )

            if previous_last_ts is None:
                last_seen_ts_by_instrument[instrument_code] = current_last_ts
                last_calculated_ts_by_instrument[instrument_code] = due_signal_bar_ts

                log_info(
                    logger,
                    f"{instrument_code}: начальный job-bar принят: "
                    f"latest_job_bar={status.last_bar_time_ct} CT. "
                    f"Первый расчёт будет со следующей due-точки.",
                    to_telegram=False,
                )
                continue

            elif current_last_ts > previous_last_ts:
                last_seen_ts_by_instrument[instrument_code] = current_last_ts
                log_info(
                    logger,
                    f"{instrument_code}: появился новый job bar: "
                    f"{status.last_bar_time_ct} CT",
                    to_telegram=False,
                )

            if due_signal_bar_ts is None:
                continue

            '''
            signal_window - объект с границами. Он нужен как вход в следующий слой: сбор текущего паттерна, поиск кандидатов,
            pattern_start_ts
            pattern_end_ts
            trade_start_ts
            trade_end_ts
            pattern_seconds
            trade_seconds
            slot_start_ts
            slot_offset_seconds
            '''
            signal_window = build_current_signal_window(
                signal_bar_ts=due_signal_bar_ts,
                settings=settings,
            )

            last_calculated_ts_by_instrument[instrument_code] = due_signal_bar_ts

            window_text = format_signal_window_for_log(
                signal_window,
                lambda ts: read_job_bar_time_ct(instrument_code, ts),
            )

            log_info(
                logger,
                f"{instrument_code}: пора считать сигнал, "
                f"latest_job_row={status.last_bar_time_ct} CT, "
                f"window={window_text}",
                to_telegram=False,
            )

            # Дальше здесь будет полный расчёт сигнала:
            # 1. построение текущего окна от signal_bar_time_ts;
            # 2. поиск исторических кандидатов;
            # 3. расчёт Pearson;
            # 4. фильтры;
            # 5. принятие торгового решения.

        await asyncio.sleep(SIGNAL_LOOP_SLEEP_SECONDS)
