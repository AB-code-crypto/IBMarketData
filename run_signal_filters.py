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
from ib_signal_filters.filter_runner import run_signal_filters_loop

setup_logging()
logger = get_logger(__name__)

telegram_sender = TelegramSender(app_settings, robot_name="ib_signal_filters")
setup_telegram_logging(telegram_sender)


async def send_service_message(message: str) -> None:
    """Что делает: пишет сервисное сообщение в консоль и напрямую отправляет его в Telegram.
    Зачем нужна: стартовые и shutdown-сообщения не должны теряться."""
    log_info(logger, message, to_telegram=False)
    await telegram_sender.send_text(message)


async def shutdown_app(shutdown_message: str) -> None:
    """Что делает: завершает filter-сервис без потери Telegram-сообщений."""
    await send_service_message(shutdown_message)

    await wait_telegram_logging()
    disable_telegram_logging()

    try:
        await telegram_sender.close()
    except Exception:
        pass


async def main() -> None:
    """Что делает: запускает пустой слой фильтрации signal_events.
    Зачем нужна: downstream-цепочка уже может читать filtered_signal_events, даже пока реальные фильтры не добавлены."""
    shutdown_message = "\n===========\nСтоп signal-filters сервиса.\n===========\n"

    try:
        await send_service_message(
            "\n===========\nСтарт signal-filters сервиса.\n"
            "mode=ALLOW_ALL_STUB\n"
            "===========\n"
        )

        await run_signal_filters_loop()

    except KeyboardInterrupt:
        shutdown_message = "===========\nСтоп signal-filters сервиса: остановлен пользователем\n==========="
        raise

    except asyncio.CancelledError:
        shutdown_message = "===========\nСтоп signal-filters сервиса: остановлен пользователем\n==========="
        raise

    except Exception as exc:
        shutdown_message = (
            "===========\n"
            "Стоп signal-filters сервиса: аварийная ошибка\n"
            f"{exc}\n"
            "==========="
        )
        log_warning(
            logger,
            f"Signal-filters сервис завершился ошибкой: {exc}\n{traceback.format_exc()}",
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
