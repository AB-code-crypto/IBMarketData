import asyncio

from config import settings_live as settings
from core.active_futures import build_active_futures
from core.db_initializer import initialize_databases_sync
from core.ib_connector import (
    connect_ib,
    disconnect_ib,
    get_ib_server_time_text,
    heartbeat_ib_connection,
    monitor_ib_connection,
)
from core.load_history import load_history_task
from core.load_realtime import load_realtime_task
from core.runtime_state import RecentBackfillState
from core.logger import (
    disable_telegram_logging,
    get_logger,
    log_info,
    setup_logging,
    setup_telegram_logging,
    wait_telegram_logging,
)
from core.telegram_sender import TelegramSender

setup_logging()
logger = get_logger(__name__)

telegram_sender = TelegramSender(settings)
setup_telegram_logging(telegram_sender)


def _log_connection_details(*, server_time_text: str, active_futures: dict) -> None:
    log_info(logger, "IBMarketData data-service started", to_telegram=True)
    log_info(logger, f"Host: {settings.ib_host}", to_telegram=False)
    log_info(logger, f"Port: {settings.ib_port}", to_telegram=False)
    log_info(logger, f"Client ID: {settings.ib_client_id}", to_telegram=True)
    log_info(logger, f"Время сервера IB: {server_time_text}", to_telegram=True)
    log_info(logger, f"Активные фьючерсы на старте: {active_futures}", to_telegram=False)
    log_info(logger, f"Price DB: {settings.price_db_path}", to_telegram=False)


def _start_background_tasks(*, ib, ib_health, active_futures: dict, recent_backfill_state: RecentBackfillState):
    monitor_task = asyncio.create_task(
        monitor_ib_connection(ib, settings, ib_health),
        name="monitor_ib_connection",
    )
    heartbeat_task = asyncio.create_task(
        heartbeat_ib_connection(ib, ib_health),
        name="heartbeat_ib_connection",
    )
    realtime_task = asyncio.create_task(
        load_realtime_task(
            ib=ib,
            ib_health=ib_health,
            settings=settings,
            active_futures=active_futures,
            recent_backfill_state=recent_backfill_state,
        ),
        name="load_realtime_task",
    )
    return realtime_task, heartbeat_task, monitor_task


async def _cancel_tasks(*tasks: asyncio.Task) -> None:
    for task in tasks:
        if task is not None:
            task.cancel()

    for task in tasks:
        if task is None:
            continue

        try:
            await task
        except asyncio.CancelledError:
            pass


async def _shutdown_app(*, ib, shutdown_message: str, tasks: tuple[asyncio.Task, ...]) -> None:
    await _cancel_tasks(*tasks)

    try:
        disconnect_ib(ib)
        log_info(logger, "Соединение с IB закрыто", to_telegram=False)
    except Exception:
        pass

    log_info(logger, shutdown_message, to_telegram=True)
    await wait_telegram_logging()

    disable_telegram_logging()

    try:
        await telegram_sender.close()
    except Exception:
        pass


async def main():
    shutdown_message = "IBMarketData data-service завершает работу"
    background_tasks: tuple[asyncio.Task, ...] = ()
    recent_backfill_state = RecentBackfillState()

    ib, ib_health = await connect_ib(settings)

    try:
        server_time_text = await get_ib_server_time_text(ib)
        active_futures = build_active_futures(server_time_text)

        _log_connection_details(
            server_time_text=server_time_text,
            active_futures=active_futures,
        )

        initialize_databases_sync(settings)
        await load_history_task(ib, ib_health, settings)

        background_tasks = _start_background_tasks(
            ib=ib,
            ib_health=ib_health,
            active_futures=active_futures,
            recent_backfill_state=recent_backfill_state,
        )

        realtime_task = background_tasks[0]
        await realtime_task

    except asyncio.CancelledError:
        shutdown_message = "IBMarketData data-service остановлен пользователем"
        raise

    finally:
        await _shutdown_app(
            ib=ib,
            shutdown_message=shutdown_message,
            tasks=background_tasks,
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
