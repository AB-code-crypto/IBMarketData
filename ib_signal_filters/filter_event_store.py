from dataclasses import dataclass
import json
import time

from core.sqlite_utils import open_sqlite_connection
from core.state_db import STATE_DB_PATH, initialize_state_db
from ib_signal.signal_event_store import (
    SIGNAL_EVENTS_TABLE_NAME,
    initialize_signal_events_table,
)

FILTERED_SIGNAL_EVENTS_TABLE_NAME = "filtered_signal_events"


@dataclass(frozen=True)
class PendingSignalEvent:
    """Что делает: хранит signal_event, который ещё не обработан signal_filters.
    Зачем нужна: фильтры работают с нормализованным событием из state DB, а не с внутренностями ib_signal."""
    signal_id: int
    instrument_code: str
    signal_bar_ts: int
    signal_time_utc: str
    signal_time_ct: str | None
    source_signal_created_at_ts: int
    direction: str
    entry_price: float


@dataclass(frozen=True)
class FilteredSignalEvent:
    """Что делает: хранит результат фильтрации одного SignalEvent.
    Зачем нужна: следующий слой читает уже разрешённый или запрещённый сигнал, не запуская фильтры заново."""
    signal_id: int
    instrument_code: str
    signal_bar_ts: int
    signal_time_utc: str
    signal_time_ct: str | None
    source_signal_created_at_ts: int
    created_at_ts: int

    direction: str
    entry_price: float

    allowed: bool
    rejected_by: str | None
    reject_reason: str | None
    filter_details_json: str


def create_filtered_signal_events_table_sql() -> str:
    """Что делает: возвращает SQL создания filtered_signal_events.
    Зачем нужна: это выходной контракт ib_signal_filters для следующего торгового слоя."""
    return f"""
    CREATE TABLE IF NOT EXISTS {FILTERED_SIGNAL_EVENTS_TABLE_NAME} (
        filtered_signal_id INTEGER PRIMARY KEY AUTOINCREMENT,

        signal_id INTEGER NOT NULL UNIQUE,

        instrument_code TEXT NOT NULL,
        signal_bar_ts INTEGER NOT NULL,
        signal_time_utc TEXT NOT NULL,
        signal_time_ct TEXT,

        source_signal_created_at_ts INTEGER NOT NULL,
        created_at_ts INTEGER NOT NULL,

        direction TEXT NOT NULL,
        entry_price REAL NOT NULL,

        allowed INTEGER NOT NULL,
        rejected_by TEXT,
        reject_reason TEXT,
        filter_details_json TEXT NOT NULL
    );
    """


def initialize_filtered_signal_events_table(conn) -> None:
    """Что делает: создаёт filtered_signal_events и индексы.
    Зачем нужна: runner может запускаться отдельно и сам готовит state DB."""
    conn.execute(create_filtered_signal_events_table_sql())
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_filtered_signal_events_signal_id
        ON {FILTERED_SIGNAL_EVENTS_TABLE_NAME}(signal_id);
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_filtered_signal_events_allowed_ts
        ON {FILTERED_SIGNAL_EVENTS_TABLE_NAME}(allowed, signal_bar_ts);
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_filtered_signal_events_created_at_ts
        ON {FILTERED_SIGNAL_EVENTS_TABLE_NAME}(created_at_ts);
        """
    )


def read_pending_signal_events(conn, *, limit: int) -> list[PendingSignalEvent]:
    """Что делает: читает signal_events, для которых ещё нет актуального результата фильтрации.
    Зачем нужна: signal_filters должен быть idempotent и не обрабатывать один и тот же сигнал бесконечно."""
    initialize_signal_events_table(conn)
    initialize_filtered_signal_events_table(conn)

    rows = conn.execute(
        f"""
        SELECT
            se.signal_id,
            se.instrument_code,
            se.signal_bar_ts,
            se.signal_time_utc,
            se.signal_time_ct,
            se.created_at_ts,
            se.direction,
            se.entry_price
        FROM {SIGNAL_EVENTS_TABLE_NAME} AS se
        LEFT JOIN {FILTERED_SIGNAL_EVENTS_TABLE_NAME} AS fe
          ON fe.signal_id = se.signal_id
        WHERE fe.signal_id IS NULL
           OR fe.source_signal_created_at_ts < se.created_at_ts
        ORDER BY se.signal_bar_ts ASC, se.signal_id ASC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()

    return [
        PendingSignalEvent(
            signal_id=int(row[0]),
            instrument_code=str(row[1]),
            signal_bar_ts=int(row[2]),
            signal_time_utc=str(row[3]),
            signal_time_ct=None if row[4] is None else str(row[4]),
            source_signal_created_at_ts=int(row[5]),
            direction=str(row[6]),
            entry_price=float(row[7]),
        )
        for row in rows
    ]


def build_allow_all_filtered_event(signal_event: PendingSignalEvent) -> FilteredSignalEvent:
    """Что делает: временно разрешает любой входной SignalEvent.
    Зачем нужна: слой фильтров должен уже работать как сервис, хотя реальные IBP-фильтры добавим позже."""
    details = {
        "mode": "ALLOW_ALL_STUB",
        "filters": [],
    }

    return FilteredSignalEvent(
        signal_id=signal_event.signal_id,
        instrument_code=signal_event.instrument_code,
        signal_bar_ts=signal_event.signal_bar_ts,
        signal_time_utc=signal_event.signal_time_utc,
        signal_time_ct=signal_event.signal_time_ct,
        source_signal_created_at_ts=signal_event.source_signal_created_at_ts,
        created_at_ts=int(time.time()),
        direction=signal_event.direction,
        entry_price=signal_event.entry_price,
        allowed=True,
        rejected_by=None,
        reject_reason=None,
        filter_details_json=json.dumps(
            details,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


def write_filtered_signal_event(conn, event: FilteredSignalEvent) -> int:
    """Что делает: пишет результат фильтрации и возвращает filtered_signal_id.
    Зачем нужна: следующий слой читает filtered_signal_events, а не сырые signal_events."""
    initialize_filtered_signal_events_table(conn)

    conn.execute(
        f"""
        INSERT INTO {FILTERED_SIGNAL_EVENTS_TABLE_NAME} (
            signal_id,

            instrument_code,
            signal_bar_ts,
            signal_time_utc,
            signal_time_ct,

            source_signal_created_at_ts,
            created_at_ts,

            direction,
            entry_price,

            allowed,
            rejected_by,
            reject_reason,
            filter_details_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

        ON CONFLICT(signal_id) DO UPDATE SET
            instrument_code = excluded.instrument_code,
            signal_bar_ts = excluded.signal_bar_ts,
            signal_time_utc = excluded.signal_time_utc,
            signal_time_ct = excluded.signal_time_ct,

            source_signal_created_at_ts = excluded.source_signal_created_at_ts,
            created_at_ts = excluded.created_at_ts,

            direction = excluded.direction,
            entry_price = excluded.entry_price,

            allowed = excluded.allowed,
            rejected_by = excluded.rejected_by,
            reject_reason = excluded.reject_reason,
            filter_details_json = excluded.filter_details_json
        ;
        """,
        (
            event.signal_id,
            event.instrument_code,
            event.signal_bar_ts,
            event.signal_time_utc,
            event.signal_time_ct,
            event.source_signal_created_at_ts,
            event.created_at_ts,
            event.direction,
            event.entry_price,
            1 if event.allowed else 0,
            event.rejected_by,
            event.reject_reason,
            event.filter_details_json,
        ),
    )

    row = conn.execute(
        f"""
        SELECT filtered_signal_id
        FROM {FILTERED_SIGNAL_EVENTS_TABLE_NAME}
        WHERE signal_id = ?
        """,
        (event.signal_id,),
    ).fetchone()

    if row is None or row[0] is None:
        raise RuntimeError("FilteredSignalEvent был записан, но filtered_signal_id не найден")

    return int(row[0])


def process_pending_signal_events(*, limit: int) -> int:
    """Что делает: читает новые signal_events и пишет allow-all filtered_signal_events.
    Зачем нужна: это минимальная рабочая пустышка слоя фильтров."""
    initialize_state_db()

    conn = open_sqlite_connection(
        str(STATE_DB_PATH),
        create_parent_dir=True,
        use_wal=True,
    )

    try:
        pending_events = read_pending_signal_events(conn, limit=limit)

        for signal_event in pending_events:
            filtered_event = build_allow_all_filtered_event(signal_event)
            write_filtered_signal_event(conn, filtered_event)

        conn.commit()
        return len(pending_events)

    finally:
        conn.close()


def cleanup_old_filtered_signal_events(*, retention_days: int) -> int:
    """Что делает: удаляет старые filtered_signal_events.
    Зачем нужна: результат фильтрации — такой же короткий live event-log, как и signal_events."""
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
        initialize_filtered_signal_events_table(conn)

        changes_before = conn.total_changes
        conn.execute(
            f"""
            DELETE FROM {FILTERED_SIGNAL_EVENTS_TABLE_NAME}
            WHERE created_at_ts < ?
            """,
            (cutoff_ts,),
        )
        deleted_rows = conn.total_changes - changes_before

        conn.commit()
        return int(deleted_rows)

    finally:
        conn.close()
