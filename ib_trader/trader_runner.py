import asyncio
import traceback

from core.logger import get_logger, log_info, log_warning, setup_logging
from ib_trader.trade_store import process_signal_events_once

setup_logging()
logger = get_logger(__name__)

TRADER_LOOP_SLEEP_SECONDS = 1
TRADER_MAX_SIGNAL_AGE_SECONDS = 10


async def run_trader_loop() -> None:
    """Что делает: читает свежие signal_events и пишет решения в trade.sqlite3.
    Зачем нужна: ib_trader — слой принятия решения по сигналу, market-features и позиции."""
    log_info(
        logger,
        (
            "ib_trader loop started: "
            "source=signal_events, "
            f"max_signal_age_seconds={TRADER_MAX_SIGNAL_AGE_SECONDS}"
        ),
        to_telegram=False,
    )

    while True:
        try:
            decisions = process_signal_events_once(
                max_signal_age_seconds=TRADER_MAX_SIGNAL_AGE_SECONDS,
            )

            for decision in decisions:
                log_info(
                    logger,
                    (
                        f"{decision.instrument_code}: trader decision: "
                        f"source_signal_id={decision.source_signal_id}, "
                        f"action={decision.action.value}, "
                        f"signal_direction={decision.signal_direction}, "
                        f"potential_end={decision.potential_end_delta_points:+.2f}, "
                        f"regime={decision.regime}, "
                        f"ma_zone={decision.ma_zone}, "
                        f"position_before={decision.position_before_side.value}/{decision.position_before_qty:g}, "
                        f"position_after={decision.position_after_side.value}/{decision.position_after_qty:g}, "
                        f"reason={decision.reason}"
                    ),
                    to_telegram=False,
                )

        except Exception as exc:
            log_warning(
                logger,
                f"ib_trader: ошибка обработки signal_events: {exc}\n"
                f"{traceback.format_exc()}",
                to_telegram=True,
            )

        await asyncio.sleep(TRADER_LOOP_SLEEP_SECONDS)
