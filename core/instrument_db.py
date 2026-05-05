from pathlib import Path

from core.contract_utils import build_table_name


def get_price_db_dir(settings) -> Path:
    # Возвращает каталог, где лежат SQLite-БД инструментов.
    price_db_dir = getattr(settings, "price_db_dir", None)

    if price_db_dir:
        return Path(price_db_dir)

    # Fallback на старую настройку, если где-то осталась старая конфигурация.
    legacy_price_db_path = Path(settings.price_db_path)
    return legacy_price_db_path.parent / "prices"


def get_instrument_db_path(settings, instrument_code, instrument_row) -> str:
    # Строит путь к БД конкретного логического инструмента.
    db_filename = instrument_row.get("db_filename", f"{instrument_code}.sqlite3")
    return str(get_price_db_dir(settings) / db_filename)


def get_instrument_table_name(instrument_code, instrument_row) -> str:
    # Таблица остаётся в едином формате: код инструмента + размер бара.
    return build_table_name(
        instrument_code=instrument_code,
        bar_size_setting=instrument_row["barSizeSetting"],
    )
