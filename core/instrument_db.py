from pathlib import Path

from core.contract_utils import build_table_name


def get_instrument_db_path(settings, instrument_code, instrument_row) -> str:
    """Что делает: строит путь к price DB конкретного инструмента. Зачем нужна: все loaders должны использовать один источник имени файла из contracts.py."""
    db_filename = instrument_row["db_filename"]
    return str(Path(settings.price_db_dir) / db_filename)


def get_instrument_table_name(instrument_code, instrument_row) -> str:
    """Что делает: строит имя price-таблицы инструмента. Зачем нужна: имя таблицы должно совпадать между инициализацией, history, realtime и repair."""
    return build_table_name(
        instrument_code=instrument_code,
        bar_size_setting=instrument_row["barSizeSetting"],
    )
