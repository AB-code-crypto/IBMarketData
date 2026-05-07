import time
from pathlib import Path
from typing import Optional

from core.sqlite_utils import open_sqlite_connection

STATE_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "state.sqlite3"
INSTRUMENT_STATE_TABLE = "instrument_state"


def get_state_db_path() -> Path:
    # Внутренняя БД состояния всего робота.
    # Не выносится в конфиг: это служебная БД проекта.
    return STATE_DB_PATH


def initialize_state_db() -> None:
    conn = open_sqlite_connection(
        str(get_state_db_path()),
        create_parent_dir=True,
        use_wal=True,
    )

    try:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {INSTRUMENT_STATE_TABLE} (
                instrument_code TEXT PRIMARY KEY,

                history_ready INTEGER NOT NULL DEFAULT 0,
                realtime_started INTEGER NOT NULL DEFAULT 0,
                first_synced_bid_ask_ts INTEGER,
                recent_backfill_ready INTEGER NOT NULL DEFAULT 0,

                signal_ready INTEGER NOT NULL DEFAULT 0,

                updated_at_ts INTEGER NOT NULL,
                error_text TEXT
            );
            """
        )
        conn.commit()

    finally:
        conn.close()


def _now_ts() -> int:
    return int(time.time())


def reset_instrument_state(instrument_code: str) -> None:
    # Сбрасывает состояние инструмента перед новым стартом market-data процесса.
    initialize_state_db()

    conn = open_sqlite_connection(str(get_state_db_path()), use_wal=True)

    try:
        conn.execute(
            f"""
            INSERT INTO {INSTRUMENT_STATE_TABLE} (
                instrument_code,
                history_ready,
                realtime_started,
                first_synced_bid_ask_ts,
                recent_backfill_ready,
                signal_ready,
                updated_at_ts,
                error_text
            )
            VALUES (?, 0, 0, NULL, 0, 0, ?, NULL)

            ON CONFLICT(instrument_code) DO UPDATE SET
                history_ready = 0,
                realtime_started = 0,
                first_synced_bid_ask_ts = NULL,
                recent_backfill_ready = 0,
                signal_ready = 0,
                updated_at_ts = excluded.updated_at_ts,
                error_text = NULL
            ;
            """,
            (instrument_code, _now_ts()),
        )
        conn.commit()

    finally:
        conn.close()


def mark_history_ready(instrument_code: str) -> None:
    initialize_state_db()

    conn = open_sqlite_connection(str(get_state_db_path()), use_wal=True)

    try:
        conn.execute(
            f"""
            INSERT INTO {INSTRUMENT_STATE_TABLE} (
                instrument_code,
                history_ready,
                updated_at_ts
            )
            VALUES (?, 1, ?)

            ON CONFLICT(instrument_code) DO UPDATE SET
                history_ready = 1,
                updated_at_ts = excluded.updated_at_ts,
                error_text = NULL
            ;
            """,
            (instrument_code, _now_ts()),
        )
        conn.commit()

    finally:
        conn.close()


def mark_realtime_started(instrument_code: str) -> None:
    initialize_state_db()

    conn = open_sqlite_connection(str(get_state_db_path()), use_wal=True)

    try:
        conn.execute(
            f"""
            INSERT INTO {INSTRUMENT_STATE_TABLE} (
                instrument_code,
                realtime_started,
                updated_at_ts
            )
            VALUES (?, 1, ?)

            ON CONFLICT(instrument_code) DO UPDATE SET
                realtime_started = 1,
                updated_at_ts = excluded.updated_at_ts,
                error_text = NULL
            ;
            """,
            (instrument_code, _now_ts()),
        )
        conn.commit()

    finally:
        conn.close()


def mark_first_synced_bid_ask(instrument_code: str, sync_ts: int) -> None:
    initialize_state_db()

    conn = open_sqlite_connection(str(get_state_db_path()), use_wal=True)

    try:
        conn.execute(
            f"""
            INSERT INTO {INSTRUMENT_STATE_TABLE} (
                instrument_code,
                first_synced_bid_ask_ts,
                updated_at_ts
            )
            VALUES (?, ?, ?)

            ON CONFLICT(instrument_code) DO UPDATE SET
                first_synced_bid_ask_ts = excluded.first_synced_bid_ask_ts,
                updated_at_ts = excluded.updated_at_ts,
                error_text = NULL
            ;
            """,
            (instrument_code, int(sync_ts), _now_ts()),
        )
        conn.commit()

    finally:
        conn.close()


def mark_signal_ready(instrument_code: str, sync_ts: Optional[int] = None) -> None:
    # Инструмент готов для job-data и будущих сигналов:
    # - history завершена;
    # - realtime стартовал;
    # - первый синхронный BID/ASK бар получен;
    # - recent-backfill последнего часа завершён.
    initialize_state_db()

    conn = open_sqlite_connection(str(get_state_db_path()), use_wal=True)

    try:
        conn.execute(
            f"""
            INSERT INTO {INSTRUMENT_STATE_TABLE} (
                instrument_code,
                first_synced_bid_ask_ts,
                recent_backfill_ready,
                signal_ready,
                updated_at_ts
            )
            VALUES (?, ?, 1, 1, ?)

            ON CONFLICT(instrument_code) DO UPDATE SET
                first_synced_bid_ask_ts = COALESCE(
                    excluded.first_synced_bid_ask_ts,
                    first_synced_bid_ask_ts
                ),
                recent_backfill_ready = 1,
                signal_ready = 1,
                updated_at_ts = excluded.updated_at_ts,
                error_text = NULL
            ;
            """,
            (
                instrument_code,
                None if sync_ts is None else int(sync_ts),
                _now_ts(),
            ),
        )
        conn.commit()

    finally:
        conn.close()


def mark_instrument_error(instrument_code: str, error_text: str) -> None:
    initialize_state_db()

    conn = open_sqlite_connection(str(get_state_db_path()), use_wal=True)

    try:
        conn.execute(
            f"""
            INSERT INTO {INSTRUMENT_STATE_TABLE} (
                instrument_code,
                signal_ready,
                updated_at_ts,
                error_text
            )
            VALUES (?, 0, ?, ?)

            ON CONFLICT(instrument_code) DO UPDATE SET
                signal_ready = 0,
                updated_at_ts = excluded.updated_at_ts,
                error_text = excluded.error_text
            ;
            """,
            (instrument_code, _now_ts(), str(error_text)),
        )
        conn.commit()

    finally:
        conn.close()


def is_signal_ready(instrument_code: str) -> bool:
    initialize_state_db()

    conn = open_sqlite_connection(str(get_state_db_path()), use_wal=True)

    try:
        row = conn.execute(
            f"""
            SELECT signal_ready
            FROM {INSTRUMENT_STATE_TABLE}
            WHERE instrument_code = ?
            """,
            (instrument_code,),
        ).fetchone()

        if row is None:
            return False

        return int(row[0]) == 1

    finally:
        conn.close()