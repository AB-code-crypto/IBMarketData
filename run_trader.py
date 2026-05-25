import asyncio
import traceback

from config import settings_live as app_settings
from core.logger import (
    disable_telegram_logging,
    get_logger,
    log_info,
    log_warning,
    setup_logging,
    setup_telegram_logging,
    wait_telegram_logging,
)
from core.telegram_sender import TelegramSender
from ib_trader.trader_runner import run_trader_loop

setup_logging()
logger = get_logger(__name__)

telegram_sender = TelegramSender(app_settings, robot_name="ib_trader")
setup_telegram_logging(telegram_sender)


async def send_service_message(message: str) -> None:
    """Что делает: пишет сервисное сообщение в консоль и напрямую отправляет его в Telegram.
    Зачем нужна: стартовые и shutdown-сообщения не должны теряться."""
    log_info(logger, message, to_telegram=False)
    await telegram_sender.send_text(message)


async def shutdown_app(shutdown_message: str) -> None:
    """Что делает: завершает trader-сервис без потери Telegram-сообщений."""
    await send_service_message(shutdown_message)

    await wait_telegram_logging()
    disable_telegram_logging()

    try:
        await telegram_sender.close()
    except Exception:
        pass


async def main() -> None:
    """Что делает: запускает простой ib_trader.
    Зачем нужна: сервис принимает решения по свежим signal_events, job-features и позиции."""
    shutdown_message = "\n===========\nСтоп ib_trader сервиса.\n===========\n"

    try:
        await send_service_message(
            "\n===========\nСтарт ib_trader сервиса.\n"
            "mode=SIGNAL_EVENTS_DECISION\n"
            "===========\n"
        )

        await run_trader_loop()

    except KeyboardInterrupt:
        shutdown_message = "===========\nСтоп ib_trader сервиса: остановлен пользователем\n==========="
        raise

    except asyncio.CancelledError:
        shutdown_message = "===========\nСтоп ib_trader сервиса: остановлен пользователем\n==========="
        raise

    except Exception as exc:
        shutdown_message = (
            "===========\n"
            "Стоп ib_trader сервиса: аварийная ошибка\n"
            f"{exc}\n"
            "==========="
        )
        log_warning(
            logger,
            f"ib_trader сервис завершился ошибкой: {exc}\n{traceback.format_exc()}",
            to_telegram=False,
        )
        raise

    finally:
        await shutdown_app(shutdown_message)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
