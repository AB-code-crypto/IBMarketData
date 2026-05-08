import asyncio
import traceback
from pathlib import Path

from config import settings_live as settings
from contracts import Instrument
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


async def wait_for_ready_instruments(instrument_codes: list[str]) -> list[str]:
    pending = set(instrument_codes)
    ready = []

    log_info(
        logger,
        f"Job-data сервис ждёт готовности инструментов от run_market_data: {sorted(pending)}",
        to_telegram=True,
    )

    while pending:
        for instrument_code in list(pending):
            if not is_signal_ready(instrument_code):
                continue

            log_info(
                logger,
                f"{instrument_code}: Job DB стала доступна для подготовки рабочих данных",
                to_telegram=True,
            )
            ready.append(instrument_code)
            pending.remove(instrument_code)

        if pending:
            # В консоль пишем текущее ожидание, в Telegram не спамим.
            log_info(
                logger,
                f"Job DB пока не готовы: {sorted(pending)}",
                to_telegram=False,
            )
            await asyncio.sleep(READY_WAIT_SECONDS)

    return ready


def update_job_dbs_once(instrument_codes: list[str]) -> None:
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


async def run_job_db_update_loop(instrument_codes: list[str]) -> None:
    log_info(
        logger,
        f"Запускаю обновление job DB для инструментов: {instrument_codes}",
        to_telegram=True,
    )

    while True:
        update_job_dbs_once(instrument_codes)
        await asyncio.sleep(JOB_DB_UPDATE_INTERVAL_SECONDS)


async def shutdown_app(shutdown_message: str) -> None:
    log_info(logger, shutdown_message, to_telegram=True)
    await wait_telegram_logging()

    disable_telegram_logging()

    try:
        await telegram_sender.close()
    except Exception:
        pass


async def main() -> None:
    shutdown_message = "\n===========\nСтоп job-data сервиса.\n===========\n"

    try:
        # run_job_data намеренно не подхватывает новые инструменты на лету.
        # Чтобы добавить инструмент в job-data контур:
        # 1. history_enabled=True — закачать историю;
        # 2. realtime_enabled=True — включить live-контур когда история закачалась;
        # 3. перезапустить run_market_data.py и run_job_data.py.
        instrument_codes = get_live_enabled_instrument_codes()

        log_info(
            logger,
            "\n===========\nСтарт job-data сервиса.\n===========\n",
            to_telegram=True,
        )

        if not instrument_codes:
            log_warning(
                logger,
                "Нет инструментов для job-data сервиса: history_enabled=True и realtime_enabled=True не найдены.",
                to_telegram=True,
            )
            return

        log_info(
            logger,
            f"Инструменты job-data сервиса: {instrument_codes}",
            to_telegram=True,
        )

        ready_instrument_codes = await wait_for_ready_instruments(instrument_codes)

        for instrument_code in ready_instrument_codes:
            rebuild_job_db(instrument_code)

        log_info(
            logger,
            f"Все Job DB пересозданы: {ready_instrument_codes}",
            to_telegram=True,
        )

        # Перед входом в основной цикл сразу один раз догоняем job DB.
        update_job_dbs_once(ready_instrument_codes)

        await run_job_db_update_loop(ready_instrument_codes)

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
