from pathlib import Path

from contracts import Instrument
from core.bar_utils import get_bar_size_seconds
from core.sqlite_utils import open_sqlite_connection
from ib_job_data.feature_db_sql import MID_PRICE_TABLE_NAME, quote_identifier
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path


# Меняем только это значение.
INSTRUMENT_CODE = "MNQ"


def main() -> None:
    instrument_code = INSTRUMENT_CODE.strip().upper()

    if instrument_code not in Instrument:
        raise ValueError(
            f"Инструмент {instrument_code!r} не найден в contracts.py"
        )

    instrument_row = Instrument[instrument_code]
    bar_size_seconds = get_bar_size_seconds(
        instrument_row["barSizeSetting"]
    )

    job_db_path = Path(
        get_instrument_feature_db_path(
            instrument_code=instrument_code,
            instrument_row=instrument_row,
        )
    )

    if not job_db_path.is_file():
        raise FileNotFoundError(f"Job DB не найдена: {job_db_path}")

    conn = open_sqlite_connection(
        str(job_db_path),
        require_existing_file=True,
        use_wal=False,
    )

    try:
        rows = conn.execute(
            f"""
            SELECT
                bar_time_ts,
                bar_time_ct,
                bar_time_msk
            FROM {quote_identifier(MID_PRICE_TABLE_NAME)}
            ORDER BY bar_time_ts ASC
            """
        ).fetchall()
    finally:
        conn.close()

    if len(rows) < 2:
        print(f"{instrument_code}: недостаточно баров для проверки.")
        return

    print(f"Инструмент : {instrument_code}")
    print(f"Job DB     : {job_db_path}")
    print(f"Таблица    : {MID_PRICE_TABLE_NAME}")
    print(f"Шаг бара   : {bar_size_seconds} сек.")
    print(f"Всего баров: {len(rows)}")
    print()

    gap_count = 0
    previous_row = rows[0]

    for current_row in rows[1:]:
        previous_ts = int(previous_row[0])
        current_ts = int(current_row[0])
        delta_seconds = current_ts - previous_ts

        if delta_seconds > bar_size_seconds:
            gap_count += 1
            missing_bars = max(
                0,
                delta_seconds // bar_size_seconds - 1,
            )

            print(f"ДЫРКА #{gap_count}")
            print(
                f"  Предыдущий бар: {previous_row[1]} CT | "
                f"{previous_row[2]} MSK | ts={previous_ts}"
            )
            print(
                f"  Следующий бар : {current_row[1]} CT | "
                f"{current_row[2]} MSK | ts={current_ts}"
            )
            print(f"  Разрыв        : {delta_seconds} сек.")
            print(f"  Пропущено     : {missing_bars} баров")
            print()

        previous_row = current_row

    if gap_count == 0:
        print("Дырок между соседними барами не найдено.")
    else:
        print(f"Всего найдено дырок: {gap_count}")


if __name__ == "__main__":
    main()
