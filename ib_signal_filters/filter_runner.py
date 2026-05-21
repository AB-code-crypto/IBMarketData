import asyncio
import traceback

from core.logger import get_logger, log_info, log_warning, setup_logging
from ib_signal_filters.filter_event_store import process_latest_fresh_signal_events

setup_logging()
logger = get_logger(__name__)

SIGNAL_FILTER_LOOP_SLEEP_SECONDS = 1
SIGNAL_FILTER_MAX_SIGNAL_AGE_SECONDS = 10


async def run_signal_filters_loop() -> None:
    """Что делает: читает только последние свежие signal_events и пишет filtered_signal_latest.
    Зачем нужна: live-цепочка должна работать с актуальным срезом сигналов, а не с очередью старой истории."""
    log_info(
        logger,
        (
            "signal-filters loop started: "
            "mode=ALLOW_ALL_STUB, "
            f"max_signal_age_seconds={SIGNAL_FILTER_MAX_SIGNAL_AGE_SECONDS}"
        ),
        to_telegram=False,
    )

    while True:
        try:
            processed_count = process_latest_fresh_signal_events(
                max_signal_age_seconds=SIGNAL_FILTER_MAX_SIGNAL_AGE_SECONDS,
            )

            if processed_count > 0:
                log_info(
                    logger,
                    f"signal-filters: обновлено latest filtered signals: {processed_count}",
                    to_telegram=False,
                )

        except Exception as exc:
            log_warning(
                logger,
                f"signal-filters: ошибка обработки latest signal_events: {exc}\n"
                f"{traceback.format_exc()}",
                to_telegram=True,
            )

        await asyncio.sleep(SIGNAL_FILTER_LOOP_SLEEP_SECONDS)
