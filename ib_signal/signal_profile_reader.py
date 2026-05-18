import sqlite3
from dataclasses import dataclass

from contracts import Instrument
from core.sqlite_utils import open_sqlite_connection
from ib_job_data.feature_db_sql import quote_identifier
from ib_job_data.profile_features import (
    PROFILE_TABLE_NAME,
    PROFILE_VOL_MINUS_NAME,
    PROFILE_VOL_PLUS_NAME,
)
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path


@dataclass(frozen=True)
class ProfileValues:
    """Что делает: хранит редкие profile-характеристики инструмента для signal/PNG.
    Зачем нужна: отрисовка не должна знать SQL-детали таблицы profile."""
    ewm_avg_vol_plus: float | None = None
    ewm_avg_vol_minus: float | None = None


def convert_profile_value(value) -> float | None:
    """Что делает: переводит SQLite-значение profile в float или None.
    Зачем нужна: отсутствующие значения должны безопасно отображаться как n/a на картинке."""
    if value is None:
        return None

    return float(value)


def read_profile_values(*, instrument_code: str) -> ProfileValues:
    """Что делает: читает из job DB средние положительные/отрицательные отклонения от SMA600.
    Зачем нужна: PNG должен показывать актуальную норму отклонения цены от SMA без повторного расчёта."""
    instrument_row = Instrument[instrument_code]
    feature_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    conn = open_sqlite_connection(
        str(feature_db_path),
        require_existing_file=True,
        use_wal=False,
    )

    try:
        try:
            rows = conn.execute(
                f"""
                SELECT name, value
                FROM {quote_identifier(PROFILE_TABLE_NAME)}
                WHERE name IN (?, ?)
                """,
                (PROFILE_VOL_PLUS_NAME, PROFILE_VOL_MINUS_NAME),
            ).fetchall()
        except sqlite3.Error:
            return ProfileValues()

    finally:
        conn.close()

    profile_values = {
        str(row[0]): row[1]
        for row in rows
    }

    return ProfileValues(
        ewm_avg_vol_plus=convert_profile_value(
            profile_values.get(PROFILE_VOL_PLUS_NAME),
        ),
        ewm_avg_vol_minus=convert_profile_value(
            profile_values.get(PROFILE_VOL_MINUS_NAME),
        ),
    )
