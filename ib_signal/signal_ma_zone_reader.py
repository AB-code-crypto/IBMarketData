
import sqlite3
from dataclasses import dataclass

from contracts import Instrument
from core.sqlite_utils import open_sqlite_connection
from ib_job_data.feature_db_sql import quote_identifier
from ib_job_data.job_features_config import (
    MA_ZONE_COLUMN_NAME,
    MA_ZONE_LOWER_RANGE_COLUMN_NAME,
    MA_ZONE_UPPER_RANGE_COLUMN_NAME,
)
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path
from ib_job_data.sma_features import SMA_TABLE_NAME
from ib_signal.signal_window import SignalWindow


@dataclass(frozen=True)
class MaZoneRangeValues:
    """Что делает: хранит верхний/нижний zone-range для текущего signal-window.
    Зачем нужна: signal_plot рисует линии зон без повторного расчёта MA-zone."""
    upper_range_points: list[float | None]
    lower_range_points: list[float | None]


def read_current_ma_zone_ranges(
        *,
        instrument_code: str,
        signal_window: SignalWindow,
        expected_points: int,
) -> MaZoneRangeValues:
    expected_points = int(expected_points)

    if expected_points <= 0:
        return MaZoneRangeValues([], [])

    instrument_row = Instrument[instrument_code]
    feature_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    conn = open_sqlite_connection(str(feature_db_path), require_existing_file=True, use_wal=False)

    try:
        rows = conn.execute(
            f"""
            SELECT
                {quote_identifier(MA_ZONE_UPPER_RANGE_COLUMN_NAME)},
                {quote_identifier(MA_ZONE_LOWER_RANGE_COLUMN_NAME)}
            FROM {quote_identifier(SMA_TABLE_NAME)}
            WHERE bar_time_ts <= ?
            ORDER BY bar_time_ts DESC
            LIMIT ?
            """,
            (int(signal_window.signal_bar_ts), expected_points),
        ).fetchall()
    except sqlite3.Error as exc:
        print(f"[signal_plot] ma_zone range read warning: {exc}")
        return MaZoneRangeValues([None] * expected_points, [None] * expected_points)
    finally:
        conn.close()

    upper_values = [None if row[0] is None else float(row[0]) for row in reversed(rows)]
    lower_values = [None if row[1] is None else float(row[1]) for row in reversed(rows)]

    if len(upper_values) < expected_points:
        missing = expected_points - len(upper_values)
        upper_values = [None] * missing + upper_values
        lower_values = [None] * missing + lower_values

    return MaZoneRangeValues(
        upper_range_points=upper_values,
        lower_range_points=lower_values,
    )

def read_current_ma_zone_values(
        *,
        instrument_code: str,
        signal_window: SignalWindow,
        expected_points: int,
) -> list[int | None]:
    """Что делает: читает ma_zone значения для текущего signal-window из sma_5s.
    Зачем нужна: PNG должна показывать текущую зону в правом столбике не только линиями."""
    expected_points = int(expected_points)

    if expected_points <= 0:
        return []

    instrument_row = Instrument[instrument_code]
    feature_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    conn = open_sqlite_connection(str(feature_db_path), require_existing_file=True, use_wal=False)

    try:
        rows = conn.execute(
            f"""
            SELECT {quote_identifier(MA_ZONE_COLUMN_NAME)}
            FROM {quote_identifier(SMA_TABLE_NAME)}
            WHERE bar_time_ts <= ?
            ORDER BY bar_time_ts DESC
            LIMIT ?
            """,
            (int(signal_window.signal_bar_ts), expected_points),
        ).fetchall()
    except sqlite3.Error as exc:
        print(f"[signal_plot] ma_zone value read warning: {exc}")
        return [None] * expected_points
    finally:
        conn.close()

    values = [
        None if row[0] is None else int(row[0])
        for row in reversed(rows)
    ]

    if len(values) < expected_points:
        values = [None] * (expected_points - len(values)) + values

    return values
