from pathlib import Path

from contracts import Instrument
from core.sqlite_utils import open_sqlite_connection
from ib_job_data.feature_db_sql import MID_PRICE_TABLE_NAME, quote_identifier
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path


def get_signal_job_db_path(instrument_code: str) -> Path:
    if instrument_code not in Instrument:
        raise ValueError(f"Инструмент {instrument_code!r} не найден в contracts.py")

    return get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=Instrument[instrument_code],
    )


def is_job_db_ready(instrument_code: str) -> bool:
    # Проверяем минимальную готовность job DB для signal-сервиса.
    #
    # На этом этапе не валидируем всю БД.
    # Достаточно:
    # - файл есть;
    # - таблица mid_price_5s есть;
    # - в таблице есть хотя бы одна строка.
    job_db_path = get_signal_job_db_path(instrument_code)

    if not job_db_path.is_file():
        return False

    conn = open_sqlite_connection(
        str(job_db_path),
        require_existing_file=True,
        use_wal=False,
    )

    try:
        table_row = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name = ?
            """,
            (MID_PRICE_TABLE_NAME,),
        ).fetchone()

        if table_row is None:
            return False

        count_row = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {quote_identifier(MID_PRICE_TABLE_NAME)}
            """
        ).fetchone()

        return int(count_row[0] or 0) > 0

    finally:
        conn.close()


def get_last_job_bar_ts(instrument_code: str) -> int | None:
    # Возвращает последний доступный bar_time_ts в job DB.
    job_db_path = get_signal_job_db_path(instrument_code)

    conn = open_sqlite_connection(
        str(job_db_path),
        require_existing_file=True,
        use_wal=False,
    )

    try:
        row = conn.execute(
            f"""
            SELECT MAX(bar_time_ts)
            FROM {quote_identifier(MID_PRICE_TABLE_NAME)}
            """
        ).fetchone()

        if row is None or row[0] is None:
            return None

        return int(row[0])

    finally:
        conn.close()