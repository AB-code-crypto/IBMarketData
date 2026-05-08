import asyncio
import traceback

from config import settings_live as app_settings
from core.instrument_filters import get_live_enabled_instrument_codes
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
from ib_signal.signal_runner import run_signal_loop, wait_for_job_dbs
from ib_signal.signal_settings import SignalSettings

setup_logging()
logger = get_logger(__name__)

telegram_sender = TelegramSender(app_settings, robot_name="ib_signal")
setup_telegram_logging(telegram_sender)


async def send_service_message(message: str) -> None:
    # В консоль пишем через обычный логгер.
    # В Telegram отправляем напрямую, чтобы не зависеть от fire-and-forget задач.
    log_info(logger, message, to_telegram=False)
    await telegram_sender.send_text(message)


async def shutdown_app(shutdown_message: str) -> None:
    await send_service_message(shutdown_message)

    await wait_telegram_logging()
    disable_telegram_logging()

    try:
        await telegram_sender.close()
    except Exception:
        pass


async def main() -> None:
    shutdown_message = "\n===========\nСтоп signal-сервиса.\n===========\n"

    try:
        await send_service_message(
            "\n===========\nСтарт signal-сервиса.\n===========\n"
        )

        instrument_codes = get_live_enabled_instrument_codes()

        if not instrument_codes:
            log_warning(
                logger,
                "Нет инструментов для signal-сервиса: history_enabled=True и realtime_enabled=True не найдены.",
                to_telegram=False,
            )
            return

        signal_settings = SignalSettings.from_config()

        log_info(
            logger,
            f"Инструменты signal-сервиса: {instrument_codes}",
            to_telegram=False,
        )

        ready_instrument_codes = wait_for_job_dbs(
            instrument_codes=instrument_codes,
            settings=signal_settings,
        )

        run_signal_loop(
            instrument_codes=ready_instrument_codes,
            settings=signal_settings,
        )

    except KeyboardInterrupt:
        shutdown_message = "===========\nСтоп signal-сервиса: остановлен пользователем\n==========="
        raise

    except asyncio.CancelledError:
        shutdown_message = "===========\nСтоп signal-сервиса: остановлен пользователем\n==========="
        raise

    except Exception as exc:
        shutdown_message = (
            "===========\n"
            "Стоп signal-сервиса: аварийная ошибка\n"
            f"{exc}\n"
            "==========="
        )
        log_warning(
            logger,
            f"Signal-сервис завершился ошибкой: {exc}\n{traceback.format_exc()}",
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
