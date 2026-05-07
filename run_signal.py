import time
import traceback
from pathlib import Path

from contracts import Instrument
from core.state_db import is_signal_ready
from ib_signal.instruments import get_signal_enabled_instrument_codes
from ib_signal.job_db_updater import append_new_mid_price_rows
from ib_signal.rebuild_mid_price import (
    get_instrument_feature_db_path,
    rebuild_instrument_mid_price_features,
)

SIGNAL_READY_WAIT_SECONDS = 5

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
        print(f"Job DB уже есть: {job_db_path}")
        return

    print(f"Job DB не найдена, создаю: {job_db_path}")
    rebuild_instrument_mid_price_features(instrument_code)


def wait_for_signal_ready_instruments(instrument_codes: list[str]) -> list[str]:
    pending = set(instrument_codes)
    ready = []

    print(f"Жду готовности инструментов от run_market_data: {sorted(pending)}")

    while pending:
        for instrument_code in list(pending):
            if not is_signal_ready(instrument_code):
                continue

            print(f"Инструмент готов для IBSignal: {instrument_code}")
            ready.append(instrument_code)
            pending.remove(instrument_code)

        if pending:
            print(f"Пока не готовы: {sorted(pending)}")
            time.sleep(SIGNAL_READY_WAIT_SECONDS)

    return ready


def update_job_dbs_once(instrument_codes: list[str]) -> None:
    for instrument_code in instrument_codes:
        try:
            rows_written = append_new_mid_price_rows(instrument_code)

            if rows_written > 0:
                print(f"{instrument_code}: job DB обновлена, новых строк: {rows_written}")

        except Exception as exc:
            print(
                f"{instrument_code}: ошибка обновления job DB: {exc}\n"
                f"{traceback.format_exc()}"
            )


def run_job_db_update_loop(instrument_codes: list[str]) -> None:
    print(f"Запускаю обновление job DB для инструментов: {instrument_codes}")

    while True:
        update_job_dbs_once(instrument_codes)

        # Дальше здесь будет следующая логика:
        # - проверить, появился ли новый расчётный timestamp;
        # - построить окно анализа;
        # - найти кандидатов по Pearson;
        # - записать сигнал.
        time.sleep(JOB_DB_UPDATE_INTERVAL_SECONDS)


def main() -> None:
    # IBSignal намеренно не подхватывает новые инструменты на лету.
    # Чтобы добавить инструмент в сигнальный контур:
    # 1. history_enabled=True — закачать историю;
    # 2. realtime_enabled=True — включить live-контур когда история закачалась;
    # 3. перезапустить run_market_data.py и run_signal.py.
    instrument_codes = get_signal_enabled_instrument_codes()

    if not instrument_codes:
        print("Нет инструментов для IBSignal: history_enabled=True и realtime_enabled=True не найдены.")
        return

    print(f"Инструменты IBSignal: {instrument_codes}")

    ready_instrument_codes = wait_for_signal_ready_instruments(instrument_codes)

    for instrument_code in ready_instrument_codes:
        ensure_job_db_exists(instrument_code)

    print("Job DB готовы.")

    # Перед входом в основной цикл сразу один раз догоняем job DB.
    update_job_dbs_once(ready_instrument_codes)

    run_job_db_update_loop(ready_instrument_codes)


if __name__ == "__main__":
    main()
