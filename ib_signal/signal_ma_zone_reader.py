import logging
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

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class MaZoneValues:
    zone_values: list[int | None]
    upper_range_points: list[float | None]
    lower_range_points: list[float | None]


def pad_left(values: list, *, expected_points: int, fill_value):
    if len(values) >= expected_points:
        return values

    return [fill_value] * (expected_points - len(values)) + values


def read_current_ma_zone_values(
        *,
        instrument_code: str,
        signal_window: SignalWindow,
        expected_points: int,
) -> MaZoneValues:
    expected_points = int(expected_points)

    if expected_points <= 0:
        return MaZoneValues([], [], [])

    instrument_row = Instrument[instrument_code]
    feature_db_path = get_instrument_feature_db_path(
        instrument_code=instrument_code,
        instrument_row=instrument_row,
    )

    conn = open_sqlite_connection(str(feature_db_path), require_existing_file=True, use_wal=False)

    try:
        sql = (
            f"SELECT "
            f"{quote_identifier(MA_ZONE_COLUMN_NAME)}, "
            f"{quote_identifier(MA_ZONE_UPPER_RANGE_COLUMN_NAME)}, "
            f"{quote_identifier(MA_ZONE_LOWER_RANGE_COLUMN_NAME)} "
            f"FROM {quote_identifier(SMA_TABLE_NAME)} "
            "WHERE bar_time_ts <= ? "
            "ORDER BY bar_time_ts DESC "
            "LIMIT ?"
        )
        rows = conn.execute(
            sql,
            (int(signal_window.signal_bar_ts), expected_points),
        ).fetchall()
    except sqlite3.Error as exc:
        log.warning("[signal_plot] ma_zone read warning: %s", exc)
        return MaZoneValues(
            zone_values=[None] * expected_points,
            upper_range_points=[None] * expected_points,
            lower_range_points=[None] * expected_points,
        )
    finally:
        conn.close()

    zone_values = [
        None if row[0] is None else int(row[0])
        for row in reversed(rows)
    ]
    upper_values = [
        None if row[1] is None else float(row[1])
        for row in reversed(rows)
    ]
    lower_values = [
        None if row[2] is None else float(row[2])
        for row in reversed(rows)
    ]

    return MaZoneValues(
        zone_values=pad_left(zone_values, expected_points=expected_points, fill_value=None),
        upper_range_points=pad_left(upper_values, expected_points=expected_points, fill_value=None),
        lower_range_points=pad_left(lower_values, expected_points=expected_points, fill_value=None),
    )
