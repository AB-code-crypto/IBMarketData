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
from ib_signal.feature_db_sql import (
    MID_PRICE_TABLE_NAME,
    create_mid_price_table_sql,
    insert_mid_price_from_attached_price_db_sql,
    quote_identifier,
)

# ============================================================
# НАСТРОЙКИ ЗАПУСКА
# ============================================================

TARGETS = ["MNQ", "EURUSD"]

PRICE_DB_SCHEMA_NAME = "price_src"


# ============================================================
# ПУТИ
# ============================================================

def get_feature_db_dir() -> Path:
    # Feature/job-БД лежат рядом с data/prices, в data/features.
    return Path(settings.price_db_dir).parent / "features"


def get_instrument_feature_db_path(instrument_code: str, instrument_row: dict) -> Path:
    # Price DB:   data/prices/MNQ.sqlite3
    # Feature DB: data/features/mnq_job.sqlite3
    price_db_filename = instrument_row.get("db_filename", f"{instrument_code}.sqlite3")
    feature_db_stem = Path(price_db_filename).stem.lower()
    feature_db_filename = f"{feature_db_stem}_job.sqlite3"

    return get_feature_db_dir() / feature_db_filename


# ============================================================
# REBUILD
# ============================================================

def rebuild_instrument_mid_price_features(instrument_code: str) -> None:
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
    print(f"Feature DB : {feature_db_path}")
    print(f"Feature tbl: {MID_PRICE_TABLE_NAME}")
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

        conn.execute(f"DROP TABLE IF EXISTS {MID_PRICE_TABLE_NAME}")
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


def main() -> None:
    if not TARGETS:
        raise ValueError("TARGETS не должен быть пустым")

    for instrument_code in TARGETS:
        rebuild_instrument_mid_price_features(instrument_code)


if __name__ == "__main__":
    main()
