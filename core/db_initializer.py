import sqlite3
from typing import Iterable
from pathlib import Path
from contracts import Instrument
from core.contract_utils import build_table_name
from core.db_sql import create_quotes_table_sql
from core.logger import get_logger, log_info

logger = get_logger(__name__)


def create_db_objects_if_missing(db_path, sql_list: Iterable[str]):
    db_path_obj = Path(db_path)
    db_path_obj.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path_obj))
    try:
        conn.execute("PRAGMA busy_timeout=5000;")
        for sql in sql_list:
            conn.execute(sql)
        conn.commit()
    finally:
        conn.close()


def initialize_price_database(settings):
    """Создаём таблицы ценовой БД только для FUT-инструментов."""
    for instrument_code, instrument_row in Instrument.items():
        if instrument_row["secType"] != "FUT":
            continue

        table_name = build_table_name(
            instrument_code=instrument_code,
            bar_size_setting=instrument_row["barSizeSetting"],
        )
        create_db_objects_if_missing(
            settings.price_db_path,
            [create_quotes_table_sql(table_name)],
        )
        log_info(
            logger,
            f"Проверил таблицу цен {table_name} в БД {settings.price_db_path}: FUT/BID-ASK",
            to_telegram=False,
        )


def initialize_databases_sync(settings):
    """Синхронная точка входа инициализации price DB."""
    log_info(logger, "Запускаю инициализацию price DB", to_telegram=False)
    initialize_price_database(settings)
    log_info(logger, "Инициализация price DB завершена", to_telegram=False)
