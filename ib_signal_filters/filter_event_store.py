from dataclasses import dataclass
import time

from core.sqlite_utils import open_sqlite_connection
from core.state_db import STATE_DB_PATH, initialize_state_db
from ib_signal.signal_event_store import (
    SIGNAL_EVENTS_TABLE_NAME,
    initialize_signal_events_table,
)

FILTERED_SIGNAL_LATEST_TABLE_NAME = "filtered_signal_latest"


@dataclass(frozen=True)
class LatestSignalEvent:
    """Что делает: хранит последний свежий signal_event одного инструмента.
    Зачем нужна: live-фильтры не разгребают всю историю signal_events, а работают только с актуальным срезом."""
    signal_id: int
    instrument_code: str
    signal_bar_ts: int
    signal_time_utc: str
    signal_time_ct: str | None
    signal_time_msk: str
    direction: str


@dataclass(frozen=True)
class FilteredSignalLatest:
    """Что делает: хранит последний разрешённый сигнал по одному инструменту.
    Зачем нужна: следующий слой видит только актуальный сигнал, который можно рассматривать для открытия сделки."""
    instrument_code: str
    source_signal_id: int

    signal_bar_ts: int
    signal_time_utc: str
    signal_time_ct: str | None
    signal_time_msk: str

    direction: str


def create_filtered_signal_latest_table_sql() -> str:
    """Что делает: возвращает SQL создания filtered_signal_latest.
    Зачем нужна: таблица хранит только последний разрешённый свежий сигнал по инструменту."""
    return f"""
    CREATE TABLE IF NOT EXISTS {FILTERED_SIGNAL_LATEST_TABLE_NAME} (
        instrument_code TEXT PRIMARY KEY,

        source_signal_id INTEGER NOT NULL,

        signal_bar_ts INTEGER NOT NULL,
        signal_time_utc TEXT NOT NULL,
        signal_time_ct TEXT,
        signal_time_msk TEXT NOT NULL,

        direction TEXT NOT NULL
    );
    """


def initialize_filtered_signal_latest_table(conn) -> None:
    """Что делает: создаёт filtered_signal_latest и индексы.
    Зачем нужна: runner может запускаться отдельно и сам готовить чистую state DB."""
    conn.execute(create_filtered_signal_latest_table_sql())
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_filtered_signal_latest_source_signal_id
        ON {FILTERED_SIGNAL_LATEST_TABLE_NAME}(source_signal_id);
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_filtered_signal_latest_signal_bar_ts
        ON {FILTERED_SIGNAL_LATEST_TABLE_NAME}(signal_bar_ts);
        """
    )


def delete_stale_filtered_signal_latest(
        conn,
        *,
        max_signal_age_seconds: int,
        now_ts: int | None = None,
) -> int:
    """Что делает: удаляет из filtered_signal_latest сигналы старше max_signal_age_seconds.
    Зачем нужна: следующий слой не должен открыть сделку по старому сигналу."""
    initialize_filtered_signal_latest_table(conn)

    max_signal_age_seconds = int(max_signal_age_seconds)
    if max_signal_age_seconds <= 0:
        return 0

    now_ts = int(time.time() if now_ts is None else now_ts)
    min_signal_bar_ts = now_ts - max_signal_age_seconds

    changes_before = conn.total_changes
    conn.execute(
        f"""
        DELETE FROM {FILTERED_SIGNAL_LATEST_TABLE_NAME}
        WHERE signal_bar_ts < ?
        """,
        (min_signal_bar_ts,),
    )

    return int(conn.total_changes - changes_before)


def delete_filtered_signal_latest_for_instrument(conn, *, instrument_code: str) -> int:
    """Что делает: удаляет текущий разрешённый сигнал инструмента.
    Зачем нужна: когда будущий фильтр запретит свежий signal_event, строка не должна оставаться разрешённой."""
    initialize_filtered_signal_latest_table(conn)

    changes_before = conn.total_changes
    conn.execute(
        f"""
        DELETE FROM {FILTERED_SIGNAL_LATEST_TABLE_NAME}
        WHERE instrument_code = ?
        """,
        (str(instrument_code),),
    )

    return int(conn.total_changes - changes_before)


def read_latest_fresh_signal_events(
        conn,
        *,
        max_signal_age_seconds: int,
        now_ts: int | None = None,
) -> list[LatestSignalEvent]:
    """Что делает: читает последний свежий signal_event по каждому инструменту.
    Зачем нужна: фильтры работают с актуальным сигналом, а не с очередью старых событий."""
    initialize_signal_events_table(conn)
    initialize_filtered_signal_latest_table(conn)

    max_signal_age_seconds = int(max_signal_age_seconds)
    if max_signal_age_seconds <= 0:
        return []

    now_ts = int(time.time() if now_ts is None else now_ts)
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
            se.direction
        FROM {SIGNAL_EVENTS_TABLE_NAME} AS se
        LEFT JOIN {FILTERED_SIGNAL_LATEST_TABLE_NAME} AS fl
          ON fl.instrument_code = se.instrument_code
        WHERE se.signal_bar_ts >= ?
          AND se.signal_id = (
              SELECT se2.signal_id
              FROM {SIGNAL_EVENTS_TABLE_NAME} AS se2
              WHERE se2.instrument_code = se.instrument_code
                AND se2.signal_bar_ts >= ?
              ORDER BY se2.signal_bar_ts DESC, se2.signal_id DESC
              LIMIT 1
          )
          AND (
              fl.instrument_code IS NULL
              OR fl.source_signal_id != se.signal_id
          )
        ORDER BY se.instrument_code ASC
        """,
        (min_signal_bar_ts, min_signal_bar_ts),
    ).fetchall()

    return [
        LatestSignalEvent(
            signal_id=int(row[0]),
            instrument_code=str(row[1]),
            signal_bar_ts=int(row[2]),
            signal_time_utc=str(row[3]),
            signal_time_ct=None if row[4] is None else str(row[4]),
            signal_time_msk=str(row[5]),
            direction=str(row[6]),
        )
        for row in rows
    ]


def is_signal_allowed(signal_event: LatestSignalEvent) -> bool:
    """Что делает: временно разрешает любой свежий сигнал.
    Зачем нужна: это пустышка слоя фильтров; реальные IBP-фильтры добавим позже."""
    return True


def build_filtered_signal_latest(signal_event: LatestSignalEvent) -> FilteredSignalLatest:
    """Что делает: собирает запись последнего разрешённого сигнала.
    Зачем нужна: в filtered_signal_latest пишем только разрешённые фильтрами сигналы."""
    return FilteredSignalLatest(
        instrument_code=signal_event.instrument_code,
        source_signal_id=signal_event.signal_id,
        signal_bar_ts=signal_event.signal_bar_ts,
        signal_time_utc=signal_event.signal_time_utc,
        signal_time_ct=signal_event.signal_time_ct,
        signal_time_msk=signal_event.signal_time_msk,
        direction=signal_event.direction,
    )


def write_filtered_signal_latest(conn, event: FilteredSignalLatest) -> None:
    """Что делает: перезаписывает последний разрешённый сигнал по инструменту.
    Зачем нужна: дальше по цепочке нужен только актуальный filtered-сигнал, а не вся история."""
    initialize_filtered_signal_latest_table(conn)

    conn.execute(
        f"""
        INSERT INTO {FILTERED_SIGNAL_LATEST_TABLE_NAME} (
            instrument_code,
            source_signal_id,

            signal_bar_ts,
            signal_time_utc,
            signal_time_ct,
            signal_time_msk,

            direction
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)

        ON CONFLICT(instrument_code) DO UPDATE SET
            source_signal_id = excluded.source_signal_id,

            signal_bar_ts = excluded.signal_bar_ts,
            signal_time_utc = excluded.signal_time_utc,
            signal_time_ct = excluded.signal_time_ct,
            signal_time_msk = excluded.signal_time_msk,

            direction = excluded.direction
        ;
        """,
        (
            event.instrument_code,
            event.source_signal_id,
            event.signal_bar_ts,
            event.signal_time_utc,
            event.signal_time_ct,
            event.signal_time_msk,
            event.direction,
        ),
    )


def process_latest_fresh_signal_events(*, max_signal_age_seconds: int) -> int:
    """Что делает: обрабатывает только последние свежие signal_events по инструментам.
    Зачем нужна: live-фильтры не должны передавать дальше старые сигналы из истории."""
    initialize_state_db()

    conn = open_sqlite_connection(
        str(STATE_DB_PATH),
        create_parent_dir=True,
        use_wal=True,
    )

    try:
        now_ts = int(time.time())

        delete_stale_filtered_signal_latest(
            conn,
            max_signal_age_seconds=max_signal_age_seconds,
            now_ts=now_ts,
        )

        latest_events = read_latest_fresh_signal_events(
            conn,
            max_signal_age_seconds=max_signal_age_seconds,
            now_ts=now_ts,
        )

        processed_count = 0

        for signal_event in latest_events:
            if is_signal_allowed(signal_event):
                filtered_event = build_filtered_signal_latest(signal_event)
                write_filtered_signal_latest(conn, filtered_event)
            else:
                delete_filtered_signal_latest_for_instrument(
                    conn,
                    instrument_code=signal_event.instrument_code,
                )

            processed_count += 1

        conn.commit()
        return processed_count

    finally:
        conn.close()
