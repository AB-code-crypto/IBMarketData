from __future__ import annotations

import time
import uuid

from core.sqlite_utils import open_sqlite_connection
from core.state_db import STATE_DB_PATH, initialize_state_db
from ib_signal.signal_event import SignalEvent

SIGNAL_EVENTS_TABLE_NAME = "signal_events"
SIGNAL_EVENT_COLUMNS = (
    "signal_id",
    "instrument_code",
    "signal_bar_ts",
    "signal_time_utc",
    "signal_time_ct",
    "signal_time_msk",
    "created_at_ts",
    "direction",
    "entry_price",
    "best_pearson",
    "candidate_score_best",
    "potential_end_delta_points",
    "potential_max_profit_points",
    "potential_max_drawdown_points",
    "potential_used",
    "settings_json",
)


def quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def create_signal_events_table_sql(
        table_name: str = SIGNAL_EVENTS_TABLE_NAME,
) -> str:
    table_ref = quote_identifier(table_name)
    return f"""
    CREATE TABLE IF NOT EXISTS {table_ref} (
        signal_id INTEGER PRIMARY KEY AUTOINCREMENT,
        instrument_code TEXT NOT NULL,
        signal_bar_ts INTEGER NOT NULL,
        signal_time_utc TEXT NOT NULL,
        signal_time_ct TEXT,
        signal_time_msk TEXT NOT NULL,
        created_at_ts INTEGER NOT NULL,
        direction TEXT NOT NULL,
        entry_price REAL NOT NULL,
        best_pearson REAL NOT NULL,
        candidate_score_best REAL,
        potential_end_delta_points REAL NOT NULL,
        potential_max_profit_points REAL NOT NULL,
        potential_max_drawdown_points REAL NOT NULL,
        potential_used INTEGER NOT NULL,
        settings_json TEXT NOT NULL,
        UNIQUE (instrument_code, signal_bar_ts)
    );
    """


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table_name),),
    ).fetchone()
    return row is not None


def _read_column_names(conn, table_name: str) -> tuple[str, ...]:
    return tuple(
        str(row[1])
        for row in conn.execute(
            f"PRAGMA table_info({quote_identifier(table_name)})"
        ).fetchall()
    )


def _create_indexes(conn) -> None:
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_signal_events_created_at_ts
        ON {quote_identifier(SIGNAL_EVENTS_TABLE_NAME)}(created_at_ts)
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_signal_events_instrument_ts
        ON {quote_identifier(SIGNAL_EVENTS_TABLE_NAME)}(
            instrument_code, signal_bar_ts
        )
        """
    )


def _migrate_legacy_signal_events(conn, legacy_columns: tuple[str, ...]) -> None:
    missing = [column for column in SIGNAL_EVENT_COLUMNS if column not in legacy_columns]
    if missing:
        raise RuntimeError(
            "Legacy signal_events cannot be migrated because required columns "
            f"are missing: {missing}; actual={legacy_columns}"
        )

    legacy_max_signal_id = int(
        conn.execute(
            f"SELECT COALESCE(MAX(signal_id), 0) "
            f"FROM {quote_identifier(SIGNAL_EVENTS_TABLE_NAME)}"
        ).fetchone()[0]
    )
    legacy_sequence_row = conn.execute(
        "SELECT seq FROM sqlite_sequence WHERE name = ?",
        (SIGNAL_EVENTS_TABLE_NAME,),
    ).fetchone()
    legacy_id_high_water = max(
        legacy_max_signal_id,
        0 if legacy_sequence_row is None else int(legacy_sequence_row[0]),
    )
    temporary_name = f"signal_events_rolling_{uuid.uuid4().hex}"
    conn.execute(create_signal_events_table_sql(temporary_name))

    filters = ["UPPER(direction) IN ('LONG', 'SHORT')"]
    if "signal_window_mode" in legacy_columns:
        filters.append("UPPER(COALESCE(signal_window_mode, 'ROLLING')) = 'ROLLING'")
    if "market_regime_filter_mode" in legacy_columns:
        filters.append("UPPER(COALESCE(market_regime_filter_mode, 'OFF')) = 'OFF'")
    if "signal_allowed" in legacy_columns:
        filters.append("COALESCE(signal_allowed, 1) = 1")

    columns_sql = ", ".join(quote_identifier(column) for column in SIGNAL_EVENT_COLUMNS)
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {quote_identifier(temporary_name)} ({columns_sql})
        SELECT {columns_sql}
        FROM {quote_identifier(SIGNAL_EVENTS_TABLE_NAME)}
        WHERE {' AND '.join(filters)}
        ORDER BY signal_id
        """
    )
    conn.execute(f"DROP TABLE {quote_identifier(SIGNAL_EVENTS_TABLE_NAME)}")
    conn.execute(
        f"ALTER TABLE {quote_identifier(temporary_name)} "
        f"RENAME TO {quote_identifier(SIGNAL_EVENTS_TABLE_NAME)}"
    )

    # Filtered legacy rows are intentionally not copied, but their IDs must
    # never be reused: old trade_intents may still reference them for audit.
    conn.execute(
        "DELETE FROM sqlite_sequence WHERE name = ?",
        (SIGNAL_EVENTS_TABLE_NAME,),
    )
    conn.execute(
        "INSERT INTO sqlite_sequence(name, seq) VALUES (?, ?)",
        (SIGNAL_EVENTS_TABLE_NAME, legacy_id_high_water),
    )


def initialize_signal_events_table(conn) -> None:
    if not _table_exists(conn, SIGNAL_EVENTS_TABLE_NAME):
        conn.execute(create_signal_events_table_sql())
        _create_indexes(conn)
        return

    actual_columns = _read_column_names(conn, SIGNAL_EVENTS_TABLE_NAME)
    if actual_columns != SIGNAL_EVENT_COLUMNS:
        _migrate_legacy_signal_events(conn, actual_columns)
    _create_indexes(conn)


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
            INSERT INTO {quote_identifier(SIGNAL_EVENTS_TABLE_NAME)} (
                instrument_code,
                signal_bar_ts,
                signal_time_utc,
                signal_time_ct,
                signal_time_msk,
                created_at_ts,
                direction,
                entry_price,
                best_pearson,
                candidate_score_best,
                potential_end_delta_points,
                potential_max_profit_points,
                potential_max_drawdown_points,
                potential_used,
                settings_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (instrument_code, signal_bar_ts) DO UPDATE SET
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
                settings_json = excluded.settings_json
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
                event.best_pearson,
                event.candidate_score_best,
                event.potential_end_delta_points,
                event.potential_max_profit_points,
                event.potential_max_drawdown_points,
                event.potential_used,
                event.settings_json,
            ),
        )
        row = conn.execute(
            f"""
            SELECT signal_id
            FROM {quote_identifier(SIGNAL_EVENTS_TABLE_NAME)}
            WHERE instrument_code = ? AND signal_bar_ts = ?
            """,
            (event.instrument_code, event.signal_bar_ts),
        ).fetchone()
        if row is None or row[0] is None:
            raise RuntimeError("SignalEvent был записан, но signal_id не найден")
        conn.commit()
        return int(row[0])
    finally:
        conn.close()


def cleanup_old_signal_events(*, retention_days: int) -> int:
    days = int(retention_days)
    if days <= 0:
        return 0
    cutoff_ts = int(time.time()) - days * 24 * 60 * 60
    initialize_state_db()
    conn = open_sqlite_connection(
        str(STATE_DB_PATH),
        create_parent_dir=True,
        use_wal=True,
    )
    try:
        initialize_signal_events_table(conn)
        before = conn.total_changes
        conn.execute(
            f"DELETE FROM {quote_identifier(SIGNAL_EVENTS_TABLE_NAME)} "
            "WHERE created_at_ts < ?",
            (cutoff_ts,),
        )
        deleted = int(conn.total_changes - before)
        conn.commit()
        return deleted
    finally:
        conn.close()


__all__ = [
    "SIGNAL_EVENTS_TABLE_NAME",
    "SIGNAL_EVENT_COLUMNS",
    "create_signal_events_table_sql",
    "initialize_signal_events_table",
    "write_signal_event",
    "cleanup_old_signal_events",
]
