import time
import traceback
from pathlib import Path

from contracts import Instrument
from core.instrument_filters import get_live_enabled_instrument_codes
from core.logger import get_logger, log_info, log_warning, setup_logging
from core.state_db import is_signal_ready
from ib_job_data.job_db_updater import append_new_mid_price_rows
from ib_job_data.rebuild_mid_price import (
    get_instrument_feature_db_path,
    rebuild_instrument_mid_price_features,
)

setup_logging()
logger = get_logger(__name__)

READY_WAIT_SECONDS = 5

# Как часто обновляем job DB из price DB.
# Realtime-бары сейчас 5-секундные, но проверять можно чаще:
# если новых полных BID/ASK строк нет, append просто запишет 0 строк.
JOB_DB_UPDATE_INTERVAL_SECONDS = 1


def ensure_job_db_exists(instrument_code: str) -> None:
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
            f"{instrument_code}: Job DB уже есть: {job_db_path}",
            to_telegram=False,
        )
        return

    log_info(
        logger,
        f"{instrument_code}: Job DB не найдена, создаю: {job_db_path}",
        to_telegram=False,
    )
    rebuild_instrument_mid_price_features(instrument_code)


def wait_for_ready_instruments(instrument_codes: list[str]) -> list[str]:
    pending = set(instrument_codes)
    ready = []

    log_info(
        logger,
        f"Жду готовности инструментов от run_market_data: {sorted(pending)}",
        to_telegram=False,
    )

    while pending:
        for instrument_code in list(pending):
            if not is_signal_ready(instrument_code):
                continue

            log_info(
                logger,
                f"{instrument_code}: инструмент готов для job-data сервиса",
                to_telegram=False,
            )
            ready.append(instrument_code)
            pending.remove(instrument_code)

        if pending:
            log_info(
                logger,
                f"Пока не готовы: {sorted(pending)}",
                to_telegram=False,
            )
            time.sleep(READY_WAIT_SECONDS)

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
                to_telegram=False,
            )


def run_job_db_update_loop(instrument_codes: list[str]) -> None:
    log_info(
        logger,
        f"Запускаю обновление job DB для инструментов: {instrument_codes}",
        to_telegram=False,
    )

    while True:
        update_job_dbs_once(instrument_codes)
        time.sleep(JOB_DB_UPDATE_INTERVAL_SECONDS)


def main() -> None:
    # run_job_data намеренно не подхватывает новые инструменты на лету.
    # Чтобы добавить инструмент в job-data контур:
    # 1. history_enabled=True — закачать историю;
    # 2. realtime_enabled=True — включить live-контур когда история закачалась;
    # 3. перезапустить run_market_data.py и run_job_data.py.
    instrument_codes = get_live_enabled_instrument_codes()

    if not instrument_codes:
        log_warning(
            logger,
            "Нет инструментов для job-data сервиса: history_enabled=True и realtime_enabled=True не найдены.",
            to_telegram=False,
        )
        return

    log_info(
        logger,
        f"Инструменты job-data сервиса: {instrument_codes}",
        to_telegram=False,
    )

    ready_instrument_codes = wait_for_ready_instruments(instrument_codes)

    for instrument_code in ready_instrument_codes:
        ensure_job_db_exists(instrument_code)

    log_info(logger, "Job DB готовы.", to_telegram=False)

    # Перед входом в основной цикл сразу один раз догоняем job DB.
    update_job_dbs_once(ready_instrument_codes)

    run_job_db_update_loop(ready_instrument_codes)


if __name__ == "__main__":
    main()