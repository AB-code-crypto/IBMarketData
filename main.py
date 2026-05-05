import asyncio
from dataclasses import dataclass
from typing import Optional

from config import settings_live as settings
from core.active_instruments import build_active_instruments
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


@dataclass
class BackgroundTasks:
    # Фоновые задачи, которые живут после начальной загрузки истории.
    realtime: asyncio.Task
    heartbeat: asyncio.Task
    monitor: asyncio.Task

    def as_tuple(self) -> tuple[asyncio.Task, ...]:
        # Удобное представление для групповой отмены задач.
        return self.realtime, self.heartbeat, self.monitor


def _log_connection_details(*, server_time_text: str, active_instruments: dict) -> None:
    log_info(logger, "IBMarketData data-service started", to_telegram=True)
    log_info(logger, f"Host: {settings.ib_host}", to_telegram=False)
    log_info(logger, f"Port: {settings.ib_port}", to_telegram=False)
    log_info(logger, f"Client ID: {settings.ib_client_id}", to_telegram=True)
    log_info(logger, f"Время сервера IB: {server_time_text}", to_telegram=True)
    log_info(logger, f"Активные инструменты на старте: {active_instruments}", to_telegram=False)
    log_info(logger, f"Price DB dir: {settings.price_db_dir}", to_telegram=False)


def _start_background_tasks(*, ib, ib_health, active_instruments: dict):
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
            active_instruments=active_instruments,
        ),
        name="load_realtime_task",
    )

    return BackgroundTasks(
        realtime=realtime_task,
        heartbeat=heartbeat_task,
        monitor=monitor_task,
    )


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


async def _shutdown_app(*, ib, shutdown_message: str, tasks: Optional[BackgroundTasks]) -> None:
    if tasks is not None:
        await _cancel_tasks(*tasks.as_tuple())

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
    background_tasks: Optional[BackgroundTasks] = None

    ib, ib_health = await connect_ib(settings)

    try:
        server_time_text = await get_ib_server_time_text(ib)
        active_instruments = build_active_instruments(server_time_text)

        _log_connection_details(
            server_time_text=server_time_text,
            active_instruments=active_instruments,
        )

        initialize_databases_sync(settings)
        await load_history_task(ib, ib_health, settings)

        background_tasks = _start_background_tasks(
            ib=ib,
            ib_health=ib_health,
            active_instruments=active_instruments,
        )

        await background_tasks.realtime

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
