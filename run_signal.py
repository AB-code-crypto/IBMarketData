from __future__ import annotations

import asyncio
import traceback

from config import settings_live as app_settings
from core.instrument_filters import get_trading_enabled_instrument_codes
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
from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG
from ib_signal.signal_config_formatter import format_signal_config
from ib_signal.signal_event_store import cleanup_old_signal_events
from ib_signal.signal_runner import run_signal_loop, wait_for_fresh_price_bars

setup_logging()
logger = get_logger(__name__)

telegram_sender = TelegramSender(app_settings, robot_name="ib_signal")
setup_telegram_logging(telegram_sender)


async def send_service_message(message: str) -> None:
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
            "\n===========\nСтарт rolling-only signal-сервиса.\n===========\n"
        )
        instrument_codes = get_trading_enabled_instrument_codes()
        if not instrument_codes:
            log_warning(
                logger,
                "Нет инструментов для signal-сервиса: нужны history_enabled, "
                "realtime_enabled и trading_enabled.",
                to_telegram=False,
            )
            return

        signal_config = DEFAULT_SIGNAL_CONFIG
        deleted = cleanup_old_signal_events(
            retention_days=signal_config.signal_event_retention_days,
        )
        log_info(
            logger,
            "signal_events cleanup/migration completed: "
            f"deleted={deleted}, retention_days={signal_config.signal_event_retention_days}",
            to_telegram=False,
        )
        await send_service_message(format_signal_config(signal_config))
        log_info(
            logger,
            f"Инструменты rolling-only signal-сервиса: {instrument_codes}",
            to_telegram=False,
        )
        ready_codes = await wait_for_fresh_price_bars(
            instrument_codes,
            signal_config,
        )
        await run_signal_loop(ready_codes, signal_config)

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
