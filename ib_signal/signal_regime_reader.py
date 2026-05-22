import sqlite3

from contracts import Instrument
from core.sqlite_utils import open_sqlite_connection
from ib_job_data.feature_db_sql import quote_identifier
from ib_job_data.job_features_config import REGIME_COLUMN_NAME
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path
from ib_job_data.sma_features import SMA_TABLE_NAME
from ib_signal.signal_window import SignalWindow


def read_signal_regime_values(
        *,
        instrument_code: str,
        signal_window: SignalWindow,
        expected_points: int,
) -> list[int | None]:
    """Что делает: читает regime-значения для текущего signal-window из sma_5s.
    Зачем нужна: PNG должна показывать режим под графиком без повторного расчёта индикатора."""
    expected_points = int(expected_points)

    if expected_points <= 0:
        return []

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
                SELECT {quote_identifier(REGIME_COLUMN_NAME)}
                FROM {quote_identifier(SMA_TABLE_NAME)}
                WHERE bar_time_ts <= ?
                ORDER BY bar_time_ts DESC
                LIMIT ?
                """,
                (
                    int(signal_window.signal_bar_ts),
                    expected_points,
                ),
            ).fetchall()
        except sqlite3.Error:
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
