from pathlib import Path

import numpy as np

from contracts import Instrument
from core.bar_utils import get_bar_size_seconds
from core.sqlite_utils import open_sqlite_connection
from ib_job_data.rebuild_mid_price import get_instrument_feature_db_path
from ib_job_data.feature_db_sql import quote_identifier
from ib_job_data.sma_features import SMA_PERIODS, SMA_TABLE_NAME, get_sma_column_name
from ib_signal.signal_window import SignalWindow


def read_current_sma_lines(
        *,
        instrument_code: str,
        signal_window: SignalWindow,
) -> dict[int, np.ndarray]:
    """Что делает: читает SMA 120/600/1200 для текущего pattern window.
    Зачем нужна: signal и PNG используют готовые SMA из job DB без повторного расчёта на лету."""
    instrument_row = Instrument[instrument_code]
    bar_size_seconds = get_bar_size_seconds(instrument_row["barSizeSetting"])

    if signal_window.pattern_seconds % bar_size_seconds != 0:
        return {}

    pattern_points = signal_window.pattern_seconds // bar_size_seconds

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
        select_columns_sql = ", ".join(
            quote_identifier(get_sma_column_name(period_bars))
            for period_bars in SMA_PERIODS
        )

        rows = conn.execute(
            f"""
            SELECT
                bar_time_ts,
                {select_columns_sql}
            FROM {quote_identifier(SMA_TABLE_NAME)}
            WHERE bar_time_ts >= ?
              AND bar_time_ts < ?
            ORDER BY bar_time_ts
            """,
            (signal_window.pattern_start_ts, signal_window.pattern_end_ts),
        ).fetchall()

    finally:
        conn.close()

    if len(rows) != pattern_points:
        return {}

    sma_lines: dict[int, np.ndarray] = {}

    for column_offset, period_bars in enumerate(SMA_PERIODS, start=1):
        values = np.empty((pattern_points,), dtype=float)
        is_complete = True

        for index, row in enumerate(rows):
            bar_time_ts = int(row[0])
            expected_ts = signal_window.pattern_start_ts + index * bar_size_seconds
            value = row[column_offset]

            if bar_time_ts != expected_ts or value is None:
                is_complete = False
                break

            values[index] = float(value)

        if is_complete:
            sma_lines[period_bars] = values

    return sma_lines


def read_current_sma_values(
        *,
        instrument_code: str,
        signal_window: SignalWindow,
        period_bars: int,
) -> np.ndarray | None:
    """Что делает: читает одну SMA-линию для текущего pattern window.
    Зачем нужна: market-regime и лог регрессии работают с выбранной SMA, сейчас это SMA 600."""
    return read_current_sma_lines(
        instrument_code=instrument_code,
        signal_window=signal_window,
    ).get(int(period_bars))
