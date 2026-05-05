from typing import Iterable

from contracts import Instrument
from core.db_sql import create_quotes_table_sql
from core.instrument_db import get_instrument_db_path, get_instrument_table_name
from core.logger import get_logger, log_info
from core.sqlite_utils import open_sqlite_connection

logger = get_logger(__name__)


def create_db_objects_if_missing(db_path, sql_list: Iterable[str]):
    conn = open_sqlite_connection(
        db_path,
        create_parent_dir=True,
        use_wal=False,
    )
    try:
        for sql in sql_list:
            conn.execute(sql)
        conn.commit()
    finally:
        conn.close()


def initialize_price_databases(settings):
    """Создаём таблицы ценовой БД для всех инструментов из contracts.py."""
    for instrument_code, instrument_row in Instrument.items():
        db_path = get_instrument_db_path(settings, instrument_code, instrument_row)
        table_name = get_instrument_table_name(instrument_code, instrument_row)

        create_db_objects_if_missing(
            db_path,
            [create_quotes_table_sql(table_name)],
        )

        log_info(
            logger,
            f"Проверил таблицу цен {table_name} в БД {db_path}: "
            f"secType={instrument_row['secType']}, BID/ASK",
            to_telegram=False,
        )


def initialize_databases_sync(settings):
    """Синхронная точка входа инициализации price DB."""
    log_info(logger, "Запускаю инициализацию price DB", to_telegram=False)
    initialize_price_databases(settings)
    log_info(logger, "Инициализация price DB завершена", to_telegram=False)
