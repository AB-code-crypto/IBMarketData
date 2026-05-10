from pathlib import Path

from config import settings_live as settings
from contracts import Instrument
from core.instrument_db import get_instrument_db_path, get_instrument_table_name
from core.sqlite_utils import open_sqlite_connection
from ib_job_data.feature_db_sql import (
    MID_PRICE_TABLE_NAME,
    create_mid_price_table_sql,
    insert_new_mid_price_from_attached_price_db_sql,
    quote_identifier,
)
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path

PRICE_DB_SCHEMA_NAME = "price_src"


def get_last_job_bar_ts(conn) -> int:
    """Что делает: читает последний рассчитанный bar_time_ts в job DB. Зачем нужна: инкрементальное обновление знает, от какой границы дописывать новые строки."""
    row = conn.execute(
        f"""
        SELECT MAX(bar_time_ts)
        FROM {quote_identifier(MID_PRICE_TABLE_NAME)}
        """
    ).fetchone()

    if row is None or row[0] is None:
        return 0

    return int(row[0])


def append_new_mid_price_rows(instrument_code: str) -> int:
    """Что делает: дописывает новые mid/spread-строки одного инструмента из price DB в job DB. Зачем нужна: поддерживает рабочую feature DB актуальной для signal-сервиса."""
    if instrument_code not in Instrument:
        raise ValueError(f"Инструмент {instrument_code!r} не найден в contracts.py")

    instrument_row = Instrument[instrument_code]

    price_db_path = Path(
        get_instrument_db_path(
            settings=settings,
            instrument_code=instrument_code,
            instrument_row=instrument_row,
        )
    )

    if not price_db_path.is_file():
        raise FileNotFoundError(f"Price DB не найдена: {price_db_path}")

    feature_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    if not feature_db_path.is_file():
        raise FileNotFoundError(f"Job DB не найдена: {feature_db_path}")

    price_table_name = get_instrument_table_name(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    conn = open_sqlite_connection(
        str(feature_db_path),
        create_parent_dir=True,
        use_wal=False,
    )

    try:
        # На случай, если job DB файл есть, но таблицы нет.
        conn.execute(create_mid_price_table_sql())

        last_job_bar_ts = get_last_job_bar_ts(conn)

        conn.execute(
            f"ATTACH DATABASE ? AS {quote_identifier(PRICE_DB_SCHEMA_NAME)}",
            (str(price_db_path),),
        )

        insert_sql = insert_new_mid_price_from_attached_price_db_sql(
            attached_schema_name=PRICE_DB_SCHEMA_NAME,
            source_table_name=price_table_name,
            price_digits=instrument_row["price_digits"],
            mid_price_digits=instrument_row["mid_price_digits"],
        )

        changes_before = conn.total_changes
        conn.execute(insert_sql, (last_job_bar_ts,))
        conn.commit()

        return conn.total_changes - changes_before

    finally:
        try:
            conn.execute(f"DETACH DATABASE {quote_identifier(PRICE_DB_SCHEMA_NAME)}")
        except Exception:
            pass

        conn.close()
