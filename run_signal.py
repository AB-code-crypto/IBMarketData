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
from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG
from ib_signal.signal_config_formatter import format_signal_config

setup_logging()
logger = get_logger(__name__)

telegram_sender = TelegramSender(app_settings, robot_name="ib_signal")
setup_telegram_logging(telegram_sender)


async def send_service_message(message: str) -> None:
    """Что делает: пишет сервисное сообщение в консоль и напрямую отправляет его в Telegram. Зачем нужна: гарантирует доставку стартовых и shutdown-сообщений без зависимости от fire-and-forget логгера."""
    log_info(logger, message, to_telegram=False)
    await telegram_sender.send_text(message)


async def shutdown_app(shutdown_message: str) -> None:
    """Что делает: отправляет финальное сообщение, дожидается Telegram-задач и закрывает TelegramSender. Зачем нужна: завершает signal-сервис аккуратно и без потерянных уведомлений."""
    await send_service_message(shutdown_message)

    await wait_telegram_logging()
    disable_telegram_logging()

    try:
        await telegram_sender.close()
    except Exception:
        pass


async def main() -> None:
    """Что делает: запускает signal-сервис, выбирает live-инструменты, сообщает настройки и входит в signal-loop. Зачем нужна: является async entrypoint для run_signal.py."""
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

        signal_config = DEFAULT_SIGNAL_CONFIG  # Блок ниже нужен также только для отладки
        # from dataclasses import replace
        # signal_config = replace(DEFAULT_SIGNAL_CONFIG,max_job_bar_lag_seconds=10 ** 9,)

        await send_service_message(format_signal_config(signal_config))

        log_info(
            logger,
            f"Инструменты signal-сервиса: {instrument_codes}",
            to_telegram=False,
        )

        ready_instrument_codes = await wait_for_job_dbs(
            instrument_codes=instrument_codes,
            settings=signal_config,
        )

        # DEBUG: временная заглушка для пошаговой отладки run_signal_loop().
        # Обходит ожидание готовности всех job DB и запускает loop только по выбранным инструментам.
        # После отладки удалить и вернуть wait_for_job_dbs().
        # ready_instrument_codes = ["MNQ"]

        await run_signal_loop(
            instrument_codes=ready_instrument_codes,
            settings=signal_config,
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
