from pathlib import Path

from contracts import Instrument
from ib_signal.rebuild_mid_price import (
    get_instrument_feature_db_path,
    rebuild_instrument_mid_price_features,
)


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


TARGETS = ["MNQ"]


def main() -> None:
    for instrument_code in TARGETS:
        ensure_job_db_exists(instrument_code)

    print("Job DB готовы. Дальше запускаем сигнальную логику.")


if __name__ == "__main__":
    main()
