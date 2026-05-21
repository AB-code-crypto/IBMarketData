import asyncio
import traceback

from core.logger import get_logger, log_info, log_warning, setup_logging
from ib_signal_filters.filter_event_store import process_pending_signal_events

setup_logging()
logger = get_logger(__name__)

SIGNAL_FILTER_LOOP_SLEEP_SECONDS = 1
PENDING_SIGNAL_LIMIT = 100


async def run_signal_filters_loop() -> None:
    """Что делает: постоянно читает signal_events и пишет filtered_signal_events.
    Зачем нужна: это отдельный слой между ib_signal и будущим торговым модулем."""
    log_info(
        logger,
        "signal-filters loop started: mode=ALLOW_ALL_STUB",
        to_telegram=False,
    )

    while True:
        try:
            processed_count = process_pending_signal_events(
                limit=PENDING_SIGNAL_LIMIT,
            )

            if processed_count > 0:
                log_info(
                    logger,
                    f"signal-filters: обработано signal_events: {processed_count}",
                    to_telegram=False,
                )

        except Exception as exc:
            log_warning(
                logger,
                f"signal-filters: ошибка обработки signal_events: {exc}\n"
                f"{traceback.format_exc()}",
                to_telegram=True,
            )

        await asyncio.sleep(SIGNAL_FILTER_LOOP_SLEEP_SECONDS)
