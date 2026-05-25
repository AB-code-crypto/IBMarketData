import time

from core.sqlite_utils import open_sqlite_connection
from core.state_db import STATE_DB_PATH, initialize_state_db
from ib_signal.signal_event import SignalEvent

SIGNAL_EVENTS_TABLE_NAME = "signal_events"


def create_signal_events_table_sql() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {SIGNAL_EVENTS_TABLE_NAME} (
        signal_id INTEGER PRIMARY KEY AUTOINCREMENT,

        instrument_code TEXT NOT NULL,

        signal_bar_ts INTEGER NOT NULL,
        signal_time_utc TEXT NOT NULL,
        signal_time_ct TEXT,
        signal_time_msk TEXT NOT NULL,
        created_at_ts INTEGER NOT NULL,

        direction TEXT NOT NULL,
        entry_price REAL NOT NULL,

        signal_window_mode TEXT NOT NULL,
        market_regime_filter_mode TEXT NOT NULL,

        best_pearson REAL NOT NULL,
        candidate_score_best REAL,

        potential_end_delta_points REAL NOT NULL,
        potential_max_profit_points REAL NOT NULL,
        potential_max_drawdown_points REAL NOT NULL,
        potential_used INTEGER NOT NULL,

        settings_json TEXT NOT NULL,

        feature_bar_ts INTEGER,
        regime INTEGER,
        ma_zone INTEGER,

        signal_allowed INTEGER NOT NULL,
        signal_reject_reason TEXT,

        signal_strength TEXT NOT NULL,

        order_type TEXT NOT NULL,
        order_policy_reason TEXT NOT NULL,
        limit_offset_points REAL,
        limit_price REAL,
        ttl_seconds INTEGER,

        signal_rules_json TEXT NOT NULL,

        UNIQUE (
            instrument_code,
            signal_bar_ts,
            signal_window_mode,
            market_regime_filter_mode
        )
    );
    """


def initialize_signal_events_table(conn) -> None:
    conn.execute(create_signal_events_table_sql())
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_signal_events_created_at_ts
        ON {SIGNAL_EVENTS_TABLE_NAME}(created_at_ts);
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_signal_events_instrument_ts
        ON {SIGNAL_EVENTS_TABLE_NAME}(instrument_code, signal_bar_ts);
        """
    )


def write_signal_event(event: SignalEvent) -> int:
    initialize_state_db()

    conn = open_sqlite_connection(
        str(STATE_DB_PATH),
        create_parent_dir=True,
        use_wal=True,
    )

    try:
        initialize_signal_events_table(conn)

        conn.execute(
            f"""
            INSERT INTO {SIGNAL_EVENTS_TABLE_NAME} (
                instrument_code,

                signal_bar_ts,
                signal_time_utc,
                signal_time_ct,
                signal_time_msk,
                created_at_ts,

                direction,
                entry_price,

                signal_window_mode,
                market_regime_filter_mode,

                best_pearson,
                candidate_score_best,

                potential_end_delta_points,
                potential_max_profit_points,
                potential_max_drawdown_points,
                potential_used,

                settings_json,

                feature_bar_ts,
                regime,
                ma_zone,

                signal_allowed,
                signal_reject_reason,

                signal_strength,

                order_type,
                order_policy_reason,
                limit_offset_points,
                limit_price,
                ttl_seconds,

                signal_rules_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

            ON CONFLICT (
                instrument_code,
                signal_bar_ts,
                signal_window_mode,
                market_regime_filter_mode
            ) DO UPDATE SET
                signal_time_utc = excluded.signal_time_utc,
                signal_time_ct = excluded.signal_time_ct,
                signal_time_msk = excluded.signal_time_msk,
                created_at_ts = excluded.created_at_ts,

                direction = excluded.direction,
                entry_price = excluded.entry_price,

                best_pearson = excluded.best_pearson,
                candidate_score_best = excluded.candidate_score_best,

                potential_end_delta_points = excluded.potential_end_delta_points,
                potential_max_profit_points = excluded.potential_max_profit_points,
                potential_max_drawdown_points = excluded.potential_max_drawdown_points,
                potential_used = excluded.potential_used,

                settings_json = excluded.settings_json,

                feature_bar_ts = excluded.feature_bar_ts,
                regime = excluded.regime,
                ma_zone = excluded.ma_zone,

                signal_allowed = excluded.signal_allowed,
                signal_reject_reason = excluded.signal_reject_reason,

                signal_strength = excluded.signal_strength,

                order_type = excluded.order_type,
                order_policy_reason = excluded.order_policy_reason,
                limit_offset_points = excluded.limit_offset_points,
                limit_price = excluded.limit_price,
                ttl_seconds = excluded.ttl_seconds,

                signal_rules_json = excluded.signal_rules_json
            ;
            """,
            (
                event.instrument_code,
                event.signal_bar_ts,
                event.signal_time_utc,
                event.signal_time_ct,
                event.signal_time_msk,
                event.created_at_ts,
                event.direction,
                event.entry_price,
                event.signal_window_mode,
                event.market_regime_filter_mode,
                event.best_pearson,
                event.candidate_score_best,
                event.potential_end_delta_points,
                event.potential_max_profit_points,
                event.potential_max_drawdown_points,
                event.potential_used,
                event.settings_json,
                event.feature_bar_ts,
                event.regime,
                event.ma_zone,
                1 if event.signal_allowed else 0,
                event.signal_reject_reason,
                event.signal_strength,
                event.order_type,
                event.order_policy_reason,
                event.limit_offset_points,
                event.limit_price,
                event.ttl_seconds,
                event.signal_rules_json,
            ),
        )

        row = conn.execute(
            f"""
            SELECT signal_id
            FROM {SIGNAL_EVENTS_TABLE_NAME}
            WHERE instrument_code = ?
              AND signal_bar_ts = ?
              AND signal_window_mode = ?
              AND market_regime_filter_mode = ?
            """,
            (
                event.instrument_code,
                event.signal_bar_ts,
                event.signal_window_mode,
                event.market_regime_filter_mode,
            ),
        ).fetchone()

        if row is None or row[0] is None:
            raise RuntimeError("SignalEvent был записан, но signal_id не найден")

        conn.commit()
        return int(row[0])

    finally:
        conn.close()


def cleanup_old_signal_events(*, retention_days: int) -> int:
    retention_days = int(retention_days)

    if retention_days <= 0:
        return 0

    cutoff_ts = int(time.time()) - retention_days * 24 * 60 * 60

    initialize_state_db()

    conn = open_sqlite_connection(
        str(STATE_DB_PATH),
        create_parent_dir=True,
        use_wal=True,
    )

    try:
        initialize_signal_events_table(conn)

        changes_before = conn.total_changes
        conn.execute(
            f"""
            DELETE FROM {SIGNAL_EVENTS_TABLE_NAME}
            WHERE created_at_ts < ?
            """,
            (cutoff_ts,),
        )
        deleted_rows = conn.total_changes - changes_before

        conn.commit()
        return int(deleted_rows)

    finally:
        conn.close()
