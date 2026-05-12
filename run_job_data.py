'''
job_data не реагирует на конкретное событие закрытия бара. Он раз в секунду сканирует все активные job DB и догоняет их до последнего полного бара.
'''
import asyncio
import time
import traceback
from pathlib import Path

from config import settings_live as settings
from contracts import Instrument
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
from core.state_db import is_signal_ready
from core.telegram_sender import TelegramSender
from ib_job_data.job_db_updater import append_new_mid_price_rows
from ib_job_data.rebuild_mid_price import (
    get_instrument_feature_db_path,
    rebuild_instrument_mid_price_features,
)

setup_logging()
logger = get_logger(__name__)

telegram_sender = TelegramSender(settings, robot_name="ib_job_data")
setup_telegram_logging(telegram_sender)

READY_WAIT_SECONDS = 5

# Как часто обновляем job DB из price DB.
# Realtime-бары сейчас 5-секундные, но проверять можно чаще:
# если новых полных BID/ASK строк нет, append просто запишет 0 строк.
JOB_DB_UPDATE_INTERVAL_SECONDS = 1


def rebuild_job_db(instrument_code: str) -> None:
    """Что делает: пересоздаёт job DB одного инструмента из его price DB. Зачем нужна: перед live-обновлением гарантирует чистую таблицу mid/spread-признаков."""
    instrument_row = Instrument[instrument_code]

    job_db_path = Path(
        get_instrument_feature_db_path(
            instrument_code=instrument_code,
            instrument_row=instrument_row,
        )
    )

    if job_db_path.is_file():
        log_info(
            logger,
            f"{instrument_code}: пересоздаю Job DB: {job_db_path}",
            to_telegram=True,
        )
    else:
        log_info(
            logger,
            f"{instrument_code}: создаю Job DB: {job_db_path}",
            to_telegram=True,
        )

    rebuild_instrument_mid_price_features(instrument_code)


def update_job_dbs_once(instrument_codes: list[str]) -> None:
    """Что делает: один раз дописывает новые mid/spread-строки по всем готовым инструментам. Зачем нужна: выполняет инкрементальное обновление job DB в основном цикле."""
    for instrument_code in instrument_codes:
        try:
            rows_written = append_new_mid_price_rows(instrument_code)

            if rows_written > 0:
                log_info(
                    logger,
                    f"{instrument_code}: job DB обновлена, новых строк: {rows_written}",
                    to_telegram=False,
                )

        except Exception as exc:
            log_warning(
                logger,
                f"{instrument_code}: ошибка обновления job DB: {exc}\n"
                f"{traceback.format_exc()}",
                to_telegram=True,
            )


async def run_job_data_loop(instrument_codes: list[str]) -> None:
    """Что делает: ждёт готовность инструментов, пересоздаёт их job DB по мере готовности и постоянно обновляет уже активные DB. Зачем нужна: один зависший инструмент не должен блокировать job-data по остальным."""
    pending = set(instrument_codes)
    active_instrument_codes: list[str] = []
    all_ready_logged = False
    next_pending_log_monotonic = 0.0

    log_info(
        logger,
        f"Job-data сервис ждёт готовности инструментов от run_market_data: {sorted(pending)}",
        to_telegram=True,
    )

    while True:
        for instrument_code in list(pending):
            if not is_signal_ready(instrument_code):
                continue

            log_info(
                logger,
                f"{instrument_code}: инструмент готов для подготовки Job DB",
                to_telegram=True,
            )

            try:
                rebuild_job_db(instrument_code)
            except Exception as exc:
                log_warning(
                    logger,
                    f"{instrument_code}: ошибка пересоздания Job DB: {exc}\n"
                    f"{traceback.format_exc()}",
                    to_telegram=True,
                )
                continue

            active_instrument_codes.append(instrument_code)
            pending.remove(instrument_code)

        if active_instrument_codes:
            update_job_dbs_once(active_instrument_codes)

        if pending:
            now_monotonic = time.monotonic()
            if now_monotonic >= next_pending_log_monotonic:
                log_info(
                    logger,
                    f"Job DB пока не готовы: {sorted(pending)}. "
                    f"Уже обновляются: {active_instrument_codes}",
                    to_telegram=False,
                )
                next_pending_log_monotonic = now_monotonic + READY_WAIT_SECONDS

        elif not all_ready_logged:
            log_info(
                logger,
                f"Все Job DB пересозданы и обновляются: {active_instrument_codes}",
                to_telegram=True,
            )
            all_ready_logged = True

        sleep_seconds = (
            JOB_DB_UPDATE_INTERVAL_SECONDS
            if active_instrument_codes
            else READY_WAIT_SECONDS
        )
        await asyncio.sleep(sleep_seconds)


async def shutdown_app(shutdown_message: str) -> None:
    """Что делает: отправляет сообщение остановки, ждёт Telegram-логи и закрывает TelegramSender. Зачем нужна: завершает job-data сервис без потери shutdown-сообщения."""
    log_info(logger, shutdown_message, to_telegram=True)
    await wait_telegram_logging()

    disable_telegram_logging()

    try:
        await telegram_sender.close()
    except Exception:
        pass


async def main() -> None:
    """Что делает: запускает job-data сервис, выбирает live-инструменты и входит в цикл подготовки/обновления job DB. Зачем нужна: является async entrypoint для run_job_data.py."""
    shutdown_message = "\n===========\nСтоп job-data сервиса.\n===========\n"

    try:
        # run_job_data намеренно не подхватывает новые инструменты на лету.
        # Чтобы добавить инструмент в job-data контур:
        # 1. history_enabled=True — закачать историю;
        # 2. realtime_enabled=True — включить live-котировки;
        # 3. trading_enabled=True — включить job-data/signal/будущую торговлю;
        # 4. перезапустить run_market_data.py и run_job_data.py.
        instrument_codes = get_trading_enabled_instrument_codes()

        log_info(
            logger,
            "\n===========\nСтарт job-data сервиса.\n===========\n",
            to_telegram=True,
        )

        if not instrument_codes:
            log_warning(
                logger,
                "Нет инструментов для job-data сервиса: history_enabled=True, realtime_enabled=True и trading_enabled=True не найдены.",
                to_telegram=True,
            )
            return

        log_info(
            logger,
            f"Инструменты job-data сервиса: {instrument_codes}",
            to_telegram=True,
        )

        await run_job_data_loop(instrument_codes)

    except asyncio.CancelledError:
        shutdown_message = "===========\nСтоп job-data сервиса: остановлен пользователем\n==========="
        raise

    except Exception as exc:
        shutdown_message = (
            "===========\n"
            "Стоп job-data сервиса: аварийная ошибка\n"
            f"{exc}\n"
            "==========="
        )
        log_warning(
            logger,
            f"Job-data сервис завершился ошибкой: {exc}\n{traceback.format_exc()}",
            to_telegram=True,
        )
        raise

    finally:
        await shutdown_app(shutdown_message)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
