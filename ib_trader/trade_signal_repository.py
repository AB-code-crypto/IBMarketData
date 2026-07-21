from __future__ import annotations

import time

from core.sqlite_utils import open_sqlite_connection
from core.state_db import STATE_DB_PATH, initialize_state_db
from ib_signal.signal_event_store import (
    SIGNAL_EVENTS_TABLE_NAME,
    initialize_signal_events_table,
)
from ib_trader.trade_models import TraderSignalEvent


def read_latest_signal_events(*, max_signal_age_seconds: int) -> list[TraderSignalEvent]:
    initialize_state_db()
    conn = open_sqlite_connection(
        str(STATE_DB_PATH),
        create_parent_dir=True,
        use_wal=True,
    )
    try:
        initialize_signal_events_table(conn)
        max_age = int(max_signal_age_seconds)
        if max_age <= 0:
            return []
        min_signal_bar_ts = int(time.time()) - max_age
        rows = conn.execute(
            f"""
            SELECT
                se.signal_id,
                se.instrument_code,
                se.signal_bar_ts,
                se.signal_time_utc,
                se.signal_time_ct,
                se.signal_time_msk,
                se.direction,
                se.entry_price,
                se.potential_end_delta_points
            FROM {SIGNAL_EVENTS_TABLE_NAME} AS se
            WHERE se.signal_bar_ts >= ?
              AND se.signal_id = (
                  SELECT se2.signal_id
                  FROM {SIGNAL_EVENTS_TABLE_NAME} AS se2
                  WHERE se2.instrument_code = se.instrument_code
                    AND se2.signal_bar_ts >= ?
                  ORDER BY se2.signal_bar_ts DESC, se2.signal_id DESC
                  LIMIT 1
              )
            ORDER BY se.instrument_code
            """,
            (min_signal_bar_ts, min_signal_bar_ts),
        ).fetchall()
        return [
            TraderSignalEvent(
                source_signal_id=int(row[0]),
                instrument_code=str(row[1]),
                signal_bar_ts=int(row[2]),
                signal_time_utc=str(row[3]),
                signal_time_ct=None if row[4] is None else str(row[4]),
                signal_time_msk=str(row[5]),
                direction=str(row[6]),
                entry_price=float(row[7]),
                potential_end_delta_points=float(row[8]),
            )
            for row in rows
        ]
    finally:
        conn.close()


__all__ = ["read_latest_signal_events"]
