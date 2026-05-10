from pathlib import Path

from core.contract_utils import build_table_name


def get_price_db_dir(settings) -> Path:
    # Возвращает каталог, где лежат SQLite-БД инструментов.
    return Path(settings.price_db_dir)


def get_instrument_db_path(settings, instrument_code, instrument_row) -> str:
    # Строит путь к БД конкретного логического инструмента.
    db_filename = instrument_row["db_filename"]
    return str(get_price_db_dir(settings) / db_filename)


def get_instrument_table_name(instrument_code, instrument_row) -> str:
    # Таблица остаётся в едином формате: код инструмента + размер бара.
    return build_table_name(
        instrument_code=instrument_code,
        bar_size_setting=instrument_row["barSizeSetting"],
    )
