from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from core.sqlite_schema import require_exact_table_schema
from core.sqlite_utils import open_sqlite_connection
from core.state_db import STATE_DB_PATH
from core.time_utils import MSK_TIMEZONE


DAILY_TRADING_GUARD_TABLE_NAME = "daily_trading_guard"

DAILY_GUARD_STATUS_MONITORING = "MONITORING"
DAILY_GUARD_STATUS_TRIGGERED = "TRIGGERED"
DAILY_GUARD_STATUS_CLOSING = "CLOSING"
DAILY_GUARD_STATUS_HALTED = "HALTED"

DAILY_GUARD_BLOCKING_STATUSES = {
    DAILY_GUARD_STATUS_TRIGGERED,
    DAILY_GUARD_STATUS_CLOSING,
    DAILY_GUARD_STATUS_HALTED,
}

DAILY_TRADING_GUARD_SCHEMA = (
    ("account_id", "TEXT", 1, None, 1),
    ("moscow_day", "TEXT", 1, None, 2),
    ("status", "TEXT", 1, None, 0),
    ("target_usd", "REAL", 1, None, 0),
    ("realized_pnl_usd", "REAL", 0, None, 0),
    ("unrealized_pnl_usd", "REAL", 0, None, 0),
    ("total_pnl_usd", "REAL", 0, None, 0),
    ("triggered_at_ts", "INTEGER", 0, None, 0),
    ("cleanup_completed_at_ts", "INTEGER", 0, None, 0),
    ("updated_at_ts", "INTEGER", 1, None, 0),
    ("error_text", "TEXT", 0, None, 0),
)


@dataclass(frozen=True)
class MoscowDayContext:
    day_key: str
    start_ts: int
    end_ts: int
    next_day_start_msk: str


@dataclass(frozen=True)
class DailyTradingGuardState:
    account_id: str
    moscow_day: str
    status: str
    target_usd: float
    realized_pnl_usd: float | None
    unrealized_pnl_usd: float | None
    total_pnl_usd: float | None
    triggered_at_ts: int | None
    cleanup_completed_at_ts: int | None
    updated_at_ts: int
    error_text: str | None

    @property
    def blocks_trading(self) -> bool:
        return self.status in DAILY_GUARD_BLOCKING_STATUSES


@dataclass(frozen=True)
class EffectiveDailyTradingHalt:
    state: DailyTradingGuardState
    current_day: MoscowDayContext
    is_current_moscow_day: bool


def get_moscow_day_context(now_ts: int | None = None) -> MoscowDayContext:
    now_value = int(time.time() if now_ts is None else now_ts)
    now_msk = datetime.fromtimestamp(now_value, tz=timezone.utc).astimezone(
        MSK_TIMEZONE
    )
    start_msk = now_msk.replace(hour=0, minute=0, second=0, microsecond=0)
    end_msk = start_msk + timedelta(days=1)

    return MoscowDayContext(
        day_key=start_msk.strftime("%Y-%m-%d"),
        start_ts=int(start_msk.astimezone(timezone.utc).timestamp()),
        end_ts=int(end_msk.astimezone(timezone.utc).timestamp()),
        next_day_start_msk=end_msk.strftime("%Y-%m-%d %H:%M:%S MSK"),
    )


def create_daily_trading_guard_table_sql() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {DAILY_TRADING_GUARD_TABLE_NAME} (
        account_id TEXT NOT NULL,
        moscow_day TEXT NOT NULL,
        status TEXT NOT NULL,
        target_usd REAL NOT NULL,
        realized_pnl_usd REAL,
        unrealized_pnl_usd REAL,
        total_pnl_usd REAL,
        triggered_at_ts INTEGER,
        cleanup_completed_at_ts INTEGER,
        updated_at_ts INTEGER NOT NULL,
        error_text TEXT,
        PRIMARY KEY (account_id, moscow_day)
    );
    """


def open_daily_guard_connection():
    return open_sqlite_connection(
        str(STATE_DB_PATH),
        create_parent_dir=True,
        use_wal=True,
        synchronous="FULL",
    )


def initialize_daily_trading_guard_db(conn) -> None:
    conn.execute(create_daily_trading_guard_table_sql())
    require_exact_table_schema(
        conn,
        table_name=DAILY_TRADING_GUARD_TABLE_NAME,
        expected_schema=DAILY_TRADING_GUARD_SCHEMA,
    )
    conn.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_daily_trading_guard_status
        ON {DAILY_TRADING_GUARD_TABLE_NAME}(account_id, status, moscow_day);"""
    )


def _row_to_state(row) -> DailyTradingGuardState:
    return DailyTradingGuardState(
        account_id=str(row[0]),
        moscow_day=str(row[1]),
        status=str(row[2]).upper(),
        target_usd=float(row[3]),
        realized_pnl_usd=None if row[4] is None else float(row[4]),
        unrealized_pnl_usd=None if row[5] is None else float(row[5]),
        total_pnl_usd=None if row[6] is None else float(row[6]),
        triggered_at_ts=None if row[7] is None else int(row[7]),
        cleanup_completed_at_ts=None if row[8] is None else int(row[8]),
        updated_at_ts=int(row[9]),
        error_text=None if row[10] is None else str(row[10]),
    )


def read_daily_guard_state(
        *,
        account_id: str,
        moscow_day: str,
) -> DailyTradingGuardState | None:
    conn = open_daily_guard_connection()
    try:
        initialize_daily_trading_guard_db(conn)
        row = conn.execute(
            f"""
            SELECT
                account_id,
                moscow_day,
                status,
                target_usd,
                realized_pnl_usd,
                unrealized_pnl_usd,
                total_pnl_usd,
                triggered_at_ts,
                cleanup_completed_at_ts,
                updated_at_ts,
                error_text
            FROM {DAILY_TRADING_GUARD_TABLE_NAME}
            WHERE account_id = ?
              AND moscow_day = ?
            LIMIT 1
            """,
            (str(account_id), str(moscow_day)),
        ).fetchone()
        return None if row is None else _row_to_state(row)
    finally:
        conn.close()


def read_effective_daily_trading_halt(
        *,
        account_id: str,
        now_ts: int | None = None,
) -> EffectiveDailyTradingHalt | None:
    day = get_moscow_day_context(now_ts)
    conn = open_daily_guard_connection()
    try:
        initialize_daily_trading_guard_db(conn)

        row = conn.execute(
            f"""
            SELECT
                account_id,
                moscow_day,
                status,
                target_usd,
                realized_pnl_usd,
                unrealized_pnl_usd,
                total_pnl_usd,
                triggered_at_ts,
                cleanup_completed_at_ts,
                updated_at_ts,
                error_text
            FROM {DAILY_TRADING_GUARD_TABLE_NAME}
            WHERE account_id = ?
              AND moscow_day = ?
              AND status IN (?, ?, ?)
            LIMIT 1
            """,
            (
                str(account_id),
                day.day_key,
                DAILY_GUARD_STATUS_TRIGGERED,
                DAILY_GUARD_STATUS_CLOSING,
                DAILY_GUARD_STATUS_HALTED,
            ),
        ).fetchone()
        if row is not None:
            return EffectiveDailyTradingHalt(
                state=_row_to_state(row),
                current_day=day,
                is_current_moscow_day=True,
            )

        # An unfinished prior-day liquidation remains blocking after midnight.
        row = conn.execute(
            f"""
            SELECT
                account_id,
                moscow_day,
                status,
                target_usd,
                realized_pnl_usd,
                unrealized_pnl_usd,
                total_pnl_usd,
                triggered_at_ts,
                cleanup_completed_at_ts,
                updated_at_ts,
                error_text
            FROM {DAILY_TRADING_GUARD_TABLE_NAME}
            WHERE account_id = ?
              AND moscow_day < ?
              AND status IN (?, ?)
              AND cleanup_completed_at_ts IS NULL
            ORDER BY moscow_day DESC
            LIMIT 1
            """,
            (
                str(account_id),
                day.day_key,
                DAILY_GUARD_STATUS_TRIGGERED,
                DAILY_GUARD_STATUS_CLOSING,
            ),
        ).fetchone()
        if row is None:
            return None

        return EffectiveDailyTradingHalt(
            state=_row_to_state(row),
            current_day=day,
            is_current_moscow_day=False,
        )
    finally:
        conn.close()


def upsert_daily_guard_monitoring(
        *,
        account_id: str,
        moscow_day: str,
        target_usd: float,
        realized_pnl_usd: float | None,
        unrealized_pnl_usd: float | None,
        total_pnl_usd: float | None,
        error_text: str | None = None,
        now_ts: int | None = None,
) -> DailyTradingGuardState:
    now_value = int(time.time() if now_ts is None else now_ts)
    conn = open_daily_guard_connection()
    try:
        initialize_daily_trading_guard_db(conn)
        conn.execute(
            f"""
            INSERT INTO {DAILY_TRADING_GUARD_TABLE_NAME} (
                account_id,
                moscow_day,
                status,
                target_usd,
                realized_pnl_usd,
                unrealized_pnl_usd,
                total_pnl_usd,
                triggered_at_ts,
                cleanup_completed_at_ts,
                updated_at_ts,
                error_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
            ON CONFLICT(account_id, moscow_day) DO UPDATE SET
                target_usd = excluded.target_usd,
                realized_pnl_usd = excluded.realized_pnl_usd,
                unrealized_pnl_usd = excluded.unrealized_pnl_usd,
                total_pnl_usd = excluded.total_pnl_usd,
                updated_at_ts = excluded.updated_at_ts,
                error_text = CASE
                    WHEN {DAILY_TRADING_GUARD_TABLE_NAME}.status = ?
                    THEN excluded.error_text
                    ELSE {DAILY_TRADING_GUARD_TABLE_NAME}.error_text
                END
            """,
            (
                str(account_id),
                str(moscow_day),
                DAILY_GUARD_STATUS_MONITORING,
                float(target_usd),
                realized_pnl_usd,
                unrealized_pnl_usd,
                total_pnl_usd,
                now_value,
                error_text,
                DAILY_GUARD_STATUS_MONITORING,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    state = read_daily_guard_state(
        account_id=account_id,
        moscow_day=moscow_day,
    )
    if state is None:
        raise RuntimeError(
            "daily trading guard row was upserted, but cannot be read: "
            f"account={account_id}, day={moscow_day}"
        )
    return state


def trigger_daily_trading_halt(
        *,
        account_id: str,
        moscow_day: str,
        target_usd: float,
        realized_pnl_usd: float,
        unrealized_pnl_usd: float,
        total_pnl_usd: float,
        now_ts: int | None = None,
) -> tuple[DailyTradingGuardState, bool]:
    now_value = int(time.time() if now_ts is None else now_ts)
    conn = open_daily_guard_connection()
    try:
        initialize_daily_trading_guard_db(conn)
        before = conn.execute(
            f"""
            SELECT status
            FROM {DAILY_TRADING_GUARD_TABLE_NAME}
            WHERE account_id = ? AND moscow_day = ?
            LIMIT 1
            """,
            (str(account_id), str(moscow_day)),
        ).fetchone()
        was_blocking = (
            before is not None
            and str(before[0]).upper() in DAILY_GUARD_BLOCKING_STATUSES
        )

        conn.execute(
            f"""
            INSERT INTO {DAILY_TRADING_GUARD_TABLE_NAME} (
                account_id,
                moscow_day,
                status,
                target_usd,
                realized_pnl_usd,
                unrealized_pnl_usd,
                total_pnl_usd,
                triggered_at_ts,
                cleanup_completed_at_ts,
                updated_at_ts,
                error_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL)
            ON CONFLICT(account_id, moscow_day) DO UPDATE SET
                status = CASE
                    WHEN {DAILY_TRADING_GUARD_TABLE_NAME}.status = ?
                    THEN ?
                    ELSE {DAILY_TRADING_GUARD_TABLE_NAME}.status
                END,
                target_usd = excluded.target_usd,
                realized_pnl_usd = excluded.realized_pnl_usd,
                unrealized_pnl_usd = excluded.unrealized_pnl_usd,
                total_pnl_usd = excluded.total_pnl_usd,
                triggered_at_ts = COALESCE(
                    {DAILY_TRADING_GUARD_TABLE_NAME}.triggered_at_ts,
                    excluded.triggered_at_ts
                ),
                updated_at_ts = excluded.updated_at_ts,
                error_text = NULL
            """,
            (
                str(account_id),
                str(moscow_day),
                DAILY_GUARD_STATUS_TRIGGERED,
                float(target_usd),
                float(realized_pnl_usd),
                float(unrealized_pnl_usd),
                float(total_pnl_usd),
                now_value,
                now_value,
                DAILY_GUARD_STATUS_MONITORING,
                DAILY_GUARD_STATUS_TRIGGERED,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    state = read_daily_guard_state(
        account_id=account_id,
        moscow_day=moscow_day,
    )
    if state is None:
        raise RuntimeError(
            "daily trading halt was triggered, but state cannot be read: "
            f"account={account_id}, day={moscow_day}"
        )
    return state, not was_blocking


def update_daily_guard_runtime_state(
        *,
        account_id: str,
        moscow_day: str,
        status: str,
        error_text: str | None = None,
        cleanup_completed: bool = False,
        now_ts: int | None = None,
) -> DailyTradingGuardState:
    status_value = str(status).upper()
    if status_value not in DAILY_GUARD_BLOCKING_STATUSES:
        raise ValueError(f"Unsupported blocking daily guard status: {status!r}")

    now_value = int(time.time() if now_ts is None else now_ts)
    conn = open_daily_guard_connection()
    try:
        initialize_daily_trading_guard_db(conn)
        changes_before = conn.total_changes
        conn.execute(
            f"""
            UPDATE {DAILY_TRADING_GUARD_TABLE_NAME}
            SET
                status = ?,
                cleanup_completed_at_ts = CASE
                    WHEN ? THEN COALESCE(cleanup_completed_at_ts, ?)
                    ELSE cleanup_completed_at_ts
                END,
                updated_at_ts = ?,
                error_text = ?
            WHERE account_id = ?
              AND moscow_day = ?
            """,
            (
                status_value,
                int(bool(cleanup_completed)),
                now_value,
                now_value,
                error_text,
                str(account_id),
                str(moscow_day),
            ),
        )
        if conn.total_changes == changes_before:
            raise RuntimeError(
                "daily trading guard row not found for runtime update: "
                f"account={account_id}, day={moscow_day}"
            )
        conn.commit()
    finally:
        conn.close()

    state = read_daily_guard_state(
        account_id=account_id,
        moscow_day=moscow_day,
    )
    if state is None:
        raise RuntimeError("daily trading guard row disappeared after update")
    return state


def format_daily_halt_warning(halt: EffectiveDailyTradingHalt) -> str:
    state = halt.state
    pnl_text = (
        "pending"
        if state.total_pnl_usd is None
        else f"{state.total_pnl_usd:+.2f} USD"
    )
    resume_text = (
        halt.current_day.next_day_start_msk
        if halt.is_current_moscow_day
        else "after unfinished prior-day cleanup reaches broker FLAT"
    )

    return (
        "DAILY TAKE PROFIT HALT: new trading is blocked\n"
        f"account: {state.account_id}\n"
        f"moscow_day: {state.moscow_day}\n"
        f"status: {state.status}\n"
        f"target: {state.target_usd:.2f} USD\n"
        f"last_total_pnl: {pnl_text}\n"
        f"resume: {resume_text}"
    )
