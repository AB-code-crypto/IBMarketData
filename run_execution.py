import asyncio
import traceback

from config import settings_live as app_settings
from core.ib_account import validate_ib_account_access
from core.ib_connector import (
    connect_ib,
    disconnect_ib,
    heartbeat_ib_connection,
    monitor_ib_connection,
)
from core.logger import (
    disable_telegram_logging,
    get_logger,
    log_info,
    log_warning,
    setup_logging,
    setup_telegram_logging,
    wait_telegram_logging,
)
from core.service_instance_lock import service_instance_lock
from core.telegram_sender import TelegramSender
from ib_execution.protective_execution_runner import run_execution_loop
from ib_execution.order_service import OrderService

setup_logging()
logger = get_logger(__name__)

telegram_sender = TelegramSender(app_settings, robot_name="ib_execution")
setup_telegram_logging(telegram_sender)


class ExecutionIbSettings:
    """Отдельный clientId для execution-сервиса, чтобы не конфликтовать с market-data."""

    ib_host = app_settings.ib_host
    ib_port = app_settings.ib_port
    ib_client_id = app_settings.ib_client_id + 40


async def send_service_message(message: str) -> None:
    log_info(logger, message, to_telegram=False)
    await telegram_sender.send_text(message)


async def shutdown_app(shutdown_message: str, ib=None) -> None:
    await send_service_message(shutdown_message)

    await wait_telegram_logging()
    disable_telegram_logging()

    if ib is not None:
        try:
            disconnect_ib(ib)
        except Exception:
            pass

    try:
        await telegram_sender.close()
    except Exception:
        pass


async def main() -> None:
    shutdown_message = "\n===========\nСтоп ib_execution сервиса.\n===========\n"
    ib = None

    try:
        await send_service_message(
            "\n===========\nСтарт ib_execution сервиса.\n"
            f"clientId={ExecutionIbSettings.ib_client_id}\n"
            f"account={app_settings.ib_account_id}\n"
            "mode=MARKET_FROM_TRADE_INTENTS\n"
            "===========\n"
        )

        ib, ib_health = await connect_ib(ExecutionIbSettings)
        await validate_ib_account_access(
            ib,
            expected_account_id=app_settings.ib_account_id,
        )

        monitor_task = asyncio.create_task(
            monitor_ib_connection(ib, ExecutionIbSettings, ib_health),
        )
        heartbeat_task = asyncio.create_task(
            heartbeat_ib_connection(ib, ib_health),
        )

        order_service = OrderService(
            ib,
            account_id=app_settings.ib_account_id,
        )

        try:
            await run_execution_loop(
                order_service,
                deal_telegram_sender=telegram_sender,
                deal_message_thread_id=app_settings.telegram_message_thread_id_deal,
                deal_status_message_thread_id=app_settings.telegram_message_thread_id_deal_status,
            )
        finally:
            monitor_task.cancel()
            heartbeat_task.cancel()

    except KeyboardInterrupt:
        shutdown_message = "===========\nСтоп ib_execution сервиса: остановлен пользователем\n==========="
        raise

    except asyncio.CancelledError:
        shutdown_message = "===========\nСтоп ib_execution сервиса: остановлен пользователем\n==========="
        raise

    except Exception as exc:
        shutdown_message = (
            "===========\n"
            "Стоп ib_execution сервиса: аварийная ошибка\n"
            f"{exc}\n"
            "==========="
        )
        log_warning(
            logger,
            f"ib_execution сервис завершился ошибкой: {exc}\n{traceback.format_exc()}",
            to_telegram=False,
        )
        raise

    finally:
        await shutdown_app(shutdown_message, ib=ib)


if __name__ == "__main__":
    execution_instance_key = (
        f"{ExecutionIbSettings.ib_host}:"
        f"{ExecutionIbSettings.ib_port}:"
        f"{ExecutionIbSettings.ib_client_id}:"
        f"{app_settings.ib_account_id}"
    )

    with service_instance_lock(
        "ib_execution",
        instance_key=execution_instance_key,
    ):
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            pass
