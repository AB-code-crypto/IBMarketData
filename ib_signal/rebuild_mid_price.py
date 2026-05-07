"""
Полный rebuild feature-БД с mid/spread-ценами.

Что делает скрипт:
1. Берёт исходную price DB инструмента из data/prices.
2. Создаёт отдельную feature DB в data/features.
3. Полностью пересоздаёт таблицу mid_price_5s.
4. Заполняет её mid/spread-ценами из BID/ASK OHLC.

Price DB не меняется.
Feature DB можно безопасно удалять и пересобирать заново.
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
# НАСТРОЙКИ РАЗОВОГО ЗАПУСКА
# ============================================================

# Какие инструменты пересобираем.
# Можно указать один:
#   TARGETS = ["MNQ"]
#
# Можно несколько:
#   TARGETS = ["MNQ", "MES", "EURUSD"]
TARGETS = ["MNQ"]

# Если True, перед сборкой печатаем план, но БД не меняем.
DRY_RUN = False

# Имя attached-схемы для исходной price DB.
PRICE_DB_SCHEMA_NAME = "price_src"


# ============================================================
# ПУТИ
# ============================================================

def get_feature_db_dir() -> Path:
    # Feature-БД лежат рядом с data/prices, в data/features.
    #
    # Если settings.price_db_dir = "data/prices",
    # то feature dir будет "data/features".
    return Path(settings.price_db_dir).parent / "features"


def get_instrument_feature_db_path(instrument_code: str, instrument_row: dict) -> str:
    # Строит путь к feature-БД конкретного логического инструмента.
    db_filename = instrument_row.get("db_filename", f"{instrument_code}.sqlite3")
    return str(get_feature_db_dir() / db_filename)


# ============================================================
# SQL HELPERS
# ============================================================

def get_source_rows_count(conn, source_table_name: str) -> int:
    table_ref = (
        f"{quote_identifier(PRICE_DB_SCHEMA_NAME)}."
        f"{quote_identifier(source_table_name)}"
    )

    row = conn.execute(f"SELECT COUNT(*) FROM {table_ref}").fetchone()
    return int(row[0] or 0)


def get_valid_source_rows_count(conn, source_table_name: str) -> int:
    table_ref = (
        f"{quote_identifier(PRICE_DB_SCHEMA_NAME)}."
        f"{quote_identifier(source_table_name)}"
    )

    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_ref}
        WHERE bid_open IS NOT NULL
          AND ask_open IS NOT NULL
          AND bid_high IS NOT NULL
          AND ask_high IS NOT NULL
          AND bid_low IS NOT NULL
          AND ask_low IS NOT NULL
          AND bid_close IS NOT NULL
          AND ask_close IS NOT NULL
        """
    ).fetchone()

    return int(row[0] or 0)


def get_feature_rows_count(conn) -> int:
    row = conn.execute(
        f"SELECT COUNT(*) FROM {MID_PRICE_TABLE_NAME}"
    ).fetchone()

    return int(row[0] or 0)


# ============================================================
# REBUILD
# ============================================================

def rebuild_instrument_mid_price_features(instrument_code: str) -> None:
    if instrument_code not in Instrument:
        raise ValueError(f"Инструмент {instrument_code!r} не найден в contracts.py")

    instrument_row = Instrument[instrument_code]

    price_db_path = get_instrument_db_path(
        settings=settings,
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    price_table_name = get_instrument_table_name(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    feature_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    price_db_path_obj = Path(price_db_path)
    feature_db_path_obj = Path(feature_db_path)

    if not price_db_path_obj.is_file():
        raise FileNotFoundError(f"Price DB не найдена: {price_db_path_obj}")

    print()
    print("=" * 80)
    print(f"Инструмент : {instrument_code}")
    print(f"Price DB   : {price_db_path_obj}")
    print(f"Price table: {price_table_name}")
    print(f"Feature DB : {feature_db_path_obj}")
    print(f"Feature tbl: {MID_PRICE_TABLE_NAME}")
    print("=" * 80)

    if DRY_RUN:
        print("DRY_RUN=True: rebuild не выполняется.")
        return

    feature_db_path_obj.parent.mkdir(parents=True, exist_ok=True)

    conn = open_sqlite_connection(
        str(feature_db_path_obj),
        create_parent_dir=True,
        use_wal=False,
    )

    try:
        conn.execute(
            f"ATTACH DATABASE ? AS {quote_identifier(PRICE_DB_SCHEMA_NAME)}",
            (str(price_db_path_obj),),
        )

        source_rows_count = get_source_rows_count(
            conn=conn,
            source_table_name=price_table_name,
        )
        valid_source_rows_count = get_valid_source_rows_count(
            conn=conn,
            source_table_name=price_table_name,
        )

        print(f"Строк в price table всего      : {source_rows_count}")
        print(f"Строк с полным BID/ASK OHLC   : {valid_source_rows_count}")

        conn.execute(f"DROP TABLE IF EXISTS {MID_PRICE_TABLE_NAME}")
        conn.execute(create_mid_price_table_sql(MID_PRICE_TABLE_NAME))

        insert_sql = insert_mid_price_from_attached_price_db_sql(
            target_table_name=MID_PRICE_TABLE_NAME,
            attached_schema_name=PRICE_DB_SCHEMA_NAME,
            source_table_name=price_table_name,
        )
        conn.execute(insert_sql)

        conn.commit()

        feature_rows_count = get_feature_rows_count(conn)
        skipped_rows_count = source_rows_count - feature_rows_count

        print(f"Записано строк в feature table : {feature_rows_count}")
        print(f"Пропущено строк                : {skipped_rows_count}")

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
