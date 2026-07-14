import asyncio
import time
import traceback

from core.logger import get_logger, log_info, log_warning, setup_logging
from ib_position_sync.position_store import sync_broker_positions_once

setup_logging()
logger = get_logger(__name__)

POSITION_SYNC_LOOP_SLEEP_SECONDS = 2
POSITION_SYNC_HEARTBEAT_INTERVAL_SECONDS = 60

# reqPositionsAsync иногда зависает после IB reconnect без исключения.
# Таймаут не даёт position_sync-loop умереть молча.
POSITION_SYNC_REQUEST_TIMEOUT_SECONDS = 10
POSITION_SYNC_ERROR_REPORT_INTERVAL_SECONDS = 60


def format_snapshot_for_log(snapshot) -> str:
    return (
        f"{snapshot.instrument_code}: "
        f"{snapshot.side}/{snapshot.quantity:g} "
        f"contract={snapshot.broker_contract or 'n/a'} "
        f"conId={snapshot.broker_con_id or 'n/a'} "
        f"active_contract={snapshot.contract_is_active} "
        f"account={snapshot.broker_account or 'n/a'}"
    )


def is_broker_backend_ready(ib, ib_health) -> bool:
    try:
        api_connected = bool(ib.isConnected())
    except Exception:
        api_connected = False

    return api_connected and bool(ib_health.ib_backend_ok)


async def run_position_sync_loop(ib, ib_health) -> None:
    "Синхронизирует broker positions, не выдавая cached TWS state за свежий при outage."
    log_info(
        logger,
        "ib_position_sync loop started",
        to_telegram=False,
    )

    last_seen: dict[str, tuple[str, float]] = {}
    last_success_ts: int | None = None
    last_error_report_ts = 0
    waiting_for_backend = False
    next_heartbeat_ts = int(time.time()) + POSITION_SYNC_HEARTBEAT_INTERVAL_SECONDS

    while True:
        backend_ready = is_broker_backend_ready(ib, ib_health)

        if not backend_ready:
            if not waiting_for_backend:
                log_warning(
                    logger,
                    (
                        "ib_position_sync paused: IB backend unavailable; "
                        "последний подтверждённый snapshot сохраняется без обновления timestamp"
                    ),
                    to_telegram=False,
                )
                waiting_for_backend = True

        else:
            if waiting_for_backend:
                log_info(
                    logger,
                    "ib_position_sync resumed: IB backend restored",
                    to_telegram=False,
                )
                waiting_for_backend = False

            try:
                snapshots = await asyncio.wait_for(
                    sync_broker_positions_once(ib),
                    timeout=float(POSITION_SYNC_REQUEST_TIMEOUT_SECONDS),
                )
                last_success_ts = int(time.time())

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

            except TimeoutError:
                now_ts = int(time.time())
                if (
                        now_ts - last_error_report_ts
                        >= POSITION_SYNC_ERROR_REPORT_INTERVAL_SECONDS
                ):
                    log_warning(
                        logger,
                        (
                            "ib_position_sync: reqPositions timeout; "
                            f"timeout={POSITION_SYNC_REQUEST_TIMEOUT_SECONDS}s; "
                            "последний подтверждённый snapshot сохранён, loop продолжает работу"
                        ),
                        to_telegram=False,
                    )
                    last_error_report_ts = now_ts

            except Exception as exc:
                now_ts = int(time.time())
                if (
                        now_ts - last_error_report_ts
                        >= POSITION_SYNC_ERROR_REPORT_INTERVAL_SECONDS
                ):
                    log_warning(
                        logger,
                        (
                            "ib_position_sync: ошибка синхронизации позиций: "
                            f"{type(exc).__name__}: {exc}\n"
                            f"{traceback.format_exc()}"
                        ),
                        to_telegram=True,
                    )
                    last_error_report_ts = now_ts

        now_ts = int(time.time())
        if now_ts >= next_heartbeat_ts:
            positions_text = ", ".join(
                f"{instrument_code}={side}/{quantity:g}"
                for instrument_code, (side, quantity) in sorted(last_seen.items())
            ) or "none"
            last_success_age_text = (
                "never"
                if last_success_ts is None
                else f"{max(0, now_ts - last_success_ts)}s"
            )
            sync_status = "ACTIVE" if backend_ready else "PAUSED_BACKEND"
            log_info(
                logger,
                (
                    "ib_position_sync heartbeat: alive, "
                    f"sync_status={sync_status}, "
                    f"positions={positions_text}, "
                    f"last_success_age={last_success_age_text}, "
                    f"request_timeout={POSITION_SYNC_REQUEST_TIMEOUT_SECONDS}s"
                ),
                to_telegram=False,
            )
            next_heartbeat_ts = now_ts + POSITION_SYNC_HEARTBEAT_INTERVAL_SECONDS

        await asyncio.sleep(POSITION_SYNC_LOOP_SLEEP_SECONDS)
