"""
Полный rebuild рабочей БД с mid/spread-ценами.

Скрипт:
1. Берёт исходную price DB инструмента из data/prices.
2. Создаёт отдельную job DB в data/features.
3. Полностью пересоздаёт таблицу mid_price_5s.
4. Заполняет её mid/spread-ценами из BID/ASK OHLC.

Price DB не меняется.
Job DB можно безопасно удалять и пересобирать заново.
"""

from pathlib import Path

from config import settings_live as settings
from contracts import Instrument
from core.instrument_db import get_instrument_db_path, get_instrument_table_name
from core.sqlite_utils import open_sqlite_connection
from ib_job_data.feature_db_sql import (
    MID_PRICE_TABLE_NAME,
    create_mid_price_table_sql,
    insert_mid_price_from_attached_price_db_sql,
    quote_identifier,
)

PRICE_DB_SCHEMA_NAME = "price_src"


# ============================================================
# ПУТИ
# ============================================================


def get_instrument_feature_db_path(instrument_code: str, instrument_row: dict) -> Path:
    """Что делает: строит путь к job DB инструмента на основе price DB filename. Зачем нужна: все job-data и signal модули обращаются к одному месту хранения features."""
    price_db_filename = instrument_row["db_filename"]
    feature_db_stem = Path(price_db_filename).stem.lower()
    feature_db_filename = f"{feature_db_stem}_job.sqlite3"

    return Path(settings.price_db_dir).parent / "features" / feature_db_filename


# ============================================================
# REBUILD
# ============================================================

def rebuild_instrument_mid_price_features(instrument_code: str) -> None:
    """Что делает: полностью пересоздаёт mid_price_5s для одного инструмента. Зачем нужна: старт job-data получает чистую рабочую БД, синхронизированную с price DB."""
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

    price_table_name = get_instrument_table_name(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    feature_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    if not price_db_path.is_file():
        raise FileNotFoundError(f"Price DB не найдена: {price_db_path}")

    feature_db_path.parent.mkdir(parents=True, exist_ok=True)

    print()
    print("=" * 80)
    print(f"Инструмент : {instrument_code}")
    print(f"Price DB   : {price_db_path}")
    print(f"Price table: {price_table_name}")
    print(f"Job DB     : {feature_db_path}")
    print(f"Job table  : {MID_PRICE_TABLE_NAME}")
    print("=" * 80)

    conn = open_sqlite_connection(
        str(feature_db_path),
        create_parent_dir=True,
        use_wal=False,
    )

    try:
        conn.execute(
            f"ATTACH DATABASE ? AS {quote_identifier(PRICE_DB_SCHEMA_NAME)}",
            (str(price_db_path),),
        )

        conn.execute(f"DROP TABLE IF EXISTS {quote_identifier(MID_PRICE_TABLE_NAME)}")
        conn.execute(create_mid_price_table_sql())

        insert_sql = insert_mid_price_from_attached_price_db_sql(
            attached_schema_name=PRICE_DB_SCHEMA_NAME,
            source_table_name=price_table_name,
            price_digits=instrument_row["price_digits"],
            mid_price_digits=instrument_row["mid_price_digits"],
        )
        conn.execute(insert_sql)

        conn.commit()
        print("Готово.")

    finally:
        try:
            conn.execute(f"DETACH DATABASE {quote_identifier(PRICE_DB_SCHEMA_NAME)}")
        except Exception:
            pass

        conn.close()
