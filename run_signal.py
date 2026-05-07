import time
from pathlib import Path

from contracts import Instrument
from core.state_db import is_signal_ready
from ib_signal.instruments import get_signal_enabled_instrument_codes
from ib_signal.rebuild_mid_price import (
    get_instrument_feature_db_path,
    rebuild_instrument_mid_price_features,
)

SIGNAL_READY_WAIT_SECONDS = 5


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


def main() -> None:
    instrument_codes = get_signal_enabled_instrument_codes()

    if not instrument_codes:
        print("Нет инструментов для IBSignal: history_enabled=True и realtime_enabled=True не найдены.")
        return

    print(f"Инструменты IBSignal: {instrument_codes}")

    ready_instrument_codes = wait_for_signal_ready_instruments(instrument_codes)

    for instrument_code in ready_instrument_codes:
        ensure_job_db_exists(instrument_code)

    print("Job DB готовы. Дальше будет запуск сигнальной логики.")


if __name__ == "__main__":
    main()
