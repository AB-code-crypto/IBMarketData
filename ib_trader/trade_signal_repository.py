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

        max_signal_age_seconds = int(max_signal_age_seconds)
        if max_signal_age_seconds <= 0:
            return []

        now_ts = int(time.time())
        min_signal_bar_ts = now_ts - max_signal_age_seconds

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

                se.best_pearson,
                se.candidate_score_best,

                se.potential_end_delta_points,
                se.potential_max_profit_points,
                se.potential_max_drawdown_points,
                se.potential_used,

                se.feature_bar_ts,
                se.regime,
                se.ma_zone,

                se.signal_allowed,
                se.signal_reject_reason,

                se.signal_strength,

                se.order_type,
                se.order_policy_reason,
                se.limit_offset_points,
                se.limit_price,
                se.ttl_seconds,

                se.signal_rules_json
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
            ORDER BY se.instrument_code ASC
            """,
            (
                min_signal_bar_ts,
                min_signal_bar_ts,
            ),
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
                best_pearson=float(row[8]),
                candidate_score_best=None if row[9] is None else float(row[9]),
                potential_end_delta_points=float(row[10]),
                potential_max_profit_points=float(row[11]),
                potential_max_drawdown_points=float(row[12]),
                potential_used=int(row[13]),
                feature_bar_ts=None if row[14] is None else int(row[14]),
                regime=None if row[15] is None else int(row[15]),
                ma_zone=None if row[16] is None else int(row[16]),
                signal_allowed=bool(int(row[17])),
                signal_reject_reason=None if row[18] is None else str(row[18]),
                signal_strength=str(row[19]),
                order_type=str(row[20]).upper(),
                order_policy_reason=str(row[21]),
                limit_offset_points=None if row[22] is None else float(row[22]),
                limit_price=None if row[23] is None else float(row[23]),
                ttl_seconds=None if row[24] is None else int(row[24]),
                signal_rules_json=str(row[25]),
            )
            for row in rows
        ]

    finally:
        conn.close()

__all__ = ['read_latest_signal_events']
