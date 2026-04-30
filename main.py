import asyncio
from typing import Any, Dict, Optional

from config import settings_live as settings
from core.active_futures import build_active_futures
from core.db_initializer import initialize_databases
from core.ib_connector import (
    connect_ib,
    disconnect_ib,
    get_ib_server_time_text,
    heartbeat_ib_connection,
    monitor_ib_connection,
)
from core.load_history import load_history_task
from core.load_realtime import load_realtime_task
from core.logger import (
    disable_telegram_logging,
    get_logger,
    log_info,
    setup_logging,
    setup_telegram_logging,
    wait_telegram_logging,
)
from core.telegram_sender import TelegramSender
from ts.prepared_task import prepared_db_sync_task, run_prepared_sync_once


setup_logging()
logger = get_logger(__name__)

telegram_sender = TelegramSender(settings)
setup_telegram_logging(telegram_sender)


def _build_recent_backfill_state() -> Dict[str, Any]:
    return {
        "first_bid_ts": None,
        "first_ask_ts": None,
        "last_backfill_completed_sync_ts": None,
        "backfill_task": None,
    }


def _log_connection_details(*, server_time_text: str, active_futures: dict) -> None:
    log_info(logger, "IBDownload data-service started", to_telegram=True)
    log_info(logger, f"Host: {settings.ib_host}", to_telegram=False)
    log_info(logger, f"Port: {settings.ib_port}", to_telegram=False)
    log_info(logger, f"Client ID: {settings.ib_client_id}", to_telegram=True)
    log_info(logger, f"Время сервера IB: {server_time_text}", to_telegram=True)
    log_info(logger, f"Активные фьючерсы на старте: {active_futures}", to_telegram=False)
    log_info(logger, f"Price DB: {settings.price_db_path}", to_telegram=False)
    log_info(logger, f"Prepared DB: {settings.prepared_db_path}", to_telegram=False)


async def _bootstrap_data_runtime() -> None:
    """
    Разовый bootstrap data-service перед запуском realtime.

    Создаём нужные БД/таблицы и один раз синхронизируем prepared DB,
    чтобы realtime-контур стартовал уже с актуальной prepared-базой.
    """
    await initialize_databases(settings)

    await run_prepared_sync_once(
        settings=settings,
        instrument_code="MNQ",
        lookback_days=31,
    )


def _start_background_tasks(
    *,
    ib,
    ib_health,
    active_futures: dict,
    recent_backfill_state: dict,
) -> dict[str, asyncio.Task]:
    return {
        "monitor": asyncio.create_task(
            monitor_ib_connection(ib, settings, ib_health),
            name="monitor_ib_connection",
        ),
        "heartbeat": asyncio.create_task(
            heartbeat_ib_connection(ib, ib_health),
            name="heartbeat_ib_connection",
        ),
        "realtime": asyncio.create_task(
            load_realtime_task(
                ib=ib,
                ib_health=ib_health,
                settings=settings,
                active_futures=active_futures,
                recent_backfill_state=recent_backfill_state,
            ),
            name="load_realtime_task",
        ),
        "prepared_sync": asyncio.create_task(
            prepared_db_sync_task(
                settings=settings,
                instrument_code="MNQ",
                lookback_days=31,
                run_immediately=False,
            ),
            name="prepared_db_sync_task",
        ),
    }


async def _run_initial_history_load(*, ib, ib_health) -> None:
    history_task = asyncio.create_task(
        load_history_task(ib, ib_health, settings),
        name="load_history_task",
    )
    await history_task


async def _cancel_and_await(task: Optional[asyncio.Task]) -> None:
    if task is None:
        return

    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass


async def _shutdown_background_tasks(tasks: dict[str, asyncio.Task]) -> None:
    shutdown_order = (
        "prepared_sync",
        "realtime",
        "heartbeat",
        "monitor",
    )

    for task_name in shutdown_order:
        await _cancel_and_await(tasks.get(task_name))


async def _shutdown_app(*, ib, shutdown_message: str, tasks: dict[str, asyncio.Task]) -> None:
    await _shutdown_background_tasks(tasks)

    try:
        disconnect_ib(ib)
        log_info(logger, "Соединение с IB закрыто", to_telegram=False)
    except Exception:
        pass

    disable_telegram_logging()
    log_info(logger, shutdown_message)
    await wait_telegram_logging()


async def main():
    shutdown_message = "IBDownload data-service завершает работу"
    tasks: dict[str, asyncio.Task] = {}
    recent_backfill_state = _build_recent_backfill_state()

    ib, ib_health = await connect_ib(settings)

    try:
        server_time_text = await get_ib_server_time_text(ib)

        active_futures = build_active_futures(server_time_text)

        _log_connection_details(
            server_time_text=server_time_text,
            active_futures=active_futures,
        )

        await _run_initial_history_load(ib=ib, ib_health=ib_health)
        await _bootstrap_data_runtime()

        tasks = _start_background_tasks(
            ib=ib,
            ib_health=ib_health,
            active_futures=active_futures,
            recent_backfill_state=recent_backfill_state,
        )

        await asyncio.gather(
            tasks["realtime"],
            tasks["prepared_sync"],
        )

    except asyncio.CancelledError:
        shutdown_message = "IBDownload data-service остановлен пользователем"
        raise

    finally:
        await _shutdown_app(
            ib=ib,
            shutdown_message=shutdown_message,
            tasks=tasks,
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_info(logger, "IBDownload data-service остановлен пользователем")