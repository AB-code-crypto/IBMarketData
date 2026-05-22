import asyncio
import traceback

from core.logger import get_logger, log_info, log_warning, setup_logging
from ib_position_sync.position_store import sync_broker_positions_once

setup_logging()
logger = get_logger(__name__)

POSITION_SYNC_LOOP_SLEEP_SECONDS = 1


def format_snapshot_for_log(snapshot) -> str:
    return (
        f"{snapshot.instrument_code}: "
        f"{snapshot.side}/{snapshot.quantity:g} "
        f"contract={snapshot.broker_contract or 'n/a'} "
        f"account={snapshot.broker_account or 'n/a'}"
    )


async def run_position_sync_loop(ib) -> None:
    """Что делает: постоянно синхронизирует broker positions в trade.sqlite3.
    Зачем нужна: ib_trader принимает решения только по positions_latest."""
    log_info(
        logger,
        "ib_position_sync loop started",
        to_telegram=False,
    )

    last_seen: dict[str, tuple[str, float]] = {}

    while True:
        try:
            snapshots = await sync_broker_positions_once(ib)

            for snapshot in snapshots:
                key = snapshot.instrument_code
                value = (snapshot.side, float(snapshot.quantity))

                if last_seen.get(key) != value:
                    log_info(
                        logger,
                        f"position_sync: {format_snapshot_for_log(snapshot)}",
                        to_telegram=False,
                    )
                    last_seen[key] = value

        except Exception as exc:
            log_warning(
                logger,
                f"ib_position_sync: ошибка синхронизации позиций: {exc}\n"
                f"{traceback.format_exc()}",
                to_telegram=True,
            )

        await asyncio.sleep(POSITION_SYNC_LOOP_SLEEP_SECONDS)
