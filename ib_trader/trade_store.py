import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from contracts import Instrument
from core.sqlite_utils import open_sqlite_connection
from core.state_db import STATE_DB_PATH, initialize_state_db
from core.time_utils import CT_TIMEZONE, MSK_TIMEZONE, SQLITE_DATETIME_FORMAT
from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG, SignalWindowMode
from ib_signal.signal_event_store import SIGNAL_EVENTS_TABLE_NAME, initialize_signal_events_table
from ib_signal.signal_interpretation import MarketSnapshot, read_market_snapshot
from ib_signal.signal_rules_config import SIGNAL_RULES, SIGNAL_RULE_SETTINGS
from ib_signal.signal_schedule import get_slot_start_ts
from ib_execution.slot_loss_extension_store import has_active_slot_loss_extension_for_instrument
from ib_trader.trade_models import (
    PositionSide,
    PositionSnapshot,
    PositionSnapshotFreshness,
    TradeDecisionAction,
    TradeIntentCreated,
    TradeIntentRejected,
    TradeProcessResult,
    TraderSignalEvent,
)

TRADE_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "trade.sqlite3"

POSITIONS_LATEST_TABLE_NAME = "positions_latest"
TRADE_INTENTS_TABLE_NAME = "trade_intents"
ORDER_REF_PREFIX = "IBMD_INTENT"

SLOT_CLOSE_SOURCE_SIGNAL_ID = -1
SLOT_CLOSE_INTENT_SOURCE = "SLOT_CLOSE"
SLOT_CLOSE_REASON = "slot_close_before_slot_end"

FUTURES_DAILY_FLAT_SOURCE_SIGNAL_ID = -2
FUTURES_DAILY_FLAT_INTENT_SOURCE = "FUTURES_DAILY_FLAT"
FUTURES_DAILY_FLAT_REASON = "futures_daily_flat_before_no_trade_hour"

EXTREME_MA_ZONE_CLOSE_SOURCE_SIGNAL_ID = -3
EXTREME_MA_ZONE_CLOSE_INTENT_SOURCE = "EXTREME_MA_ZONE_CLOSE"
EXTREME_MA_ZONE_CLOSE_REASON = "extreme_ma_zone_close_position"
FUTURES_NO_NEW_TRADES_HOUR_CT = 15
FUTURES_CLEARING_HOUR_CT = 16

TRADER_MAX_NEW_INTENT_AGE_SECONDS = 10
STALE_NEW_INTENT_ERROR_TEXT = "stale trade_intent before trader decision"
POSITION_SNAPSHOT_MAX_AGE_SECONDS = 10



@dataclass(frozen=True)
class TradeIntentDraft:
    source_signal_id: int
    instrument_code: str
    signal_bar_ts: int
    signal_time_ct: str | None

    intent_source: str
    action: TradeDecisionAction
    reason: str

    signal_direction: str
    entry_price: float
    potential_end_delta_points: float

    regime: int | None
    ma_zone: int | None
    signal_strength: str

    order_type: str
    limit_price: float | None
    limit_offset_points: float | None
    ttl_seconds: int | None

    position_before_side: PositionSide
    position_before_qty: float

    position_after_side: PositionSide
    position_after_qty: float


def get_trade_db_connection():
    return open_sqlite_connection(
        str(TRADE_DB_PATH),
        create_parent_dir=True,
        use_wal=True,
    )


def create_positions_latest_table_sql() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {POSITIONS_LATEST_TABLE_NAME} (
        instrument_code TEXT PRIMARY KEY,
        side TEXT NOT NULL,
        quantity REAL NOT NULL,
        updated_at_ts INTEGER NOT NULL,
        updated_at_utc TEXT NOT NULL
    );
    """


def create_trade_intents_table_sql() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {TRADE_INTENTS_TABLE_NAME} (
        trade_intent_id INTEGER PRIMARY KEY AUTOINCREMENT,

        source_signal_id INTEGER NOT NULL,
        instrument_code TEXT NOT NULL,
        signal_bar_ts INTEGER NOT NULL,
        signal_time_utc TEXT NOT NULL,
        signal_time_ct TEXT NOT NULL,
        signal_time_msk TEXT NOT NULL,

        entry_regime INTEGER,
        entry_ma_zone INTEGER,

        intent_source TEXT NOT NULL,

        action TEXT NOT NULL,
        action_reason TEXT NOT NULL,

        target_side TEXT NOT NULL,
        target_qty REAL NOT NULL,

        position_before_side TEXT NOT NULL,
        position_before_qty REAL NOT NULL,

        order_type TEXT NOT NULL,
        limit_price REAL,
        limit_offset_points REAL,
        ttl_seconds INTEGER,

        status TEXT NOT NULL,

        cancel_requested INTEGER NOT NULL DEFAULT 0,
        cancel_reason TEXT,
        cancel_source_signal_id INTEGER,
        cancel_requested_at_ts INTEGER,

        order_ref TEXT,
        order_id INTEGER,
        order_action TEXT,
        order_quantity INTEGER,
        avg_fill_price REAL,
        total_commission REAL,
        realized_pnl REAL,
        error_text TEXT,

        created_at_ts INTEGER NOT NULL,
        updated_at_ts INTEGER NOT NULL,
        sent_at_ts INTEGER,
        finished_at_ts INTEGER,

        UNIQUE (
            instrument_code,
            source_signal_id,
            signal_bar_ts,
            action
        )
    );
    """


def table_columns(conn, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def ensure_table_column(conn, *, table_name: str, column_name: str, column_sql: str) -> None:
    if column_name in table_columns(conn, table_name):
        return

    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")


def ensure_trade_intents_runtime_columns(conn) -> None:
    ensure_table_column(
        conn,
        table_name=TRADE_INTENTS_TABLE_NAME,
        column_name="entry_regime",
        column_sql="entry_regime INTEGER",
    )
    ensure_table_column(
        conn,
        table_name=TRADE_INTENTS_TABLE_NAME,
        column_name="entry_ma_zone",
        column_sql="entry_ma_zone INTEGER",
    )
    ensure_table_column(
        conn,
        table_name=TRADE_INTENTS_TABLE_NAME,
        column_name="cancel_requested",
        column_sql="cancel_requested INTEGER NOT NULL DEFAULT 0",
    )
    ensure_table_column(
        conn,
        table_name=TRADE_INTENTS_TABLE_NAME,
        column_name="cancel_reason",
        column_sql="cancel_reason TEXT",
    )
    ensure_table_column(
        conn,
        table_name=TRADE_INTENTS_TABLE_NAME,
        column_name="cancel_source_signal_id",
        column_sql="cancel_source_signal_id INTEGER",
    )
    ensure_table_column(
        conn,
        table_name=TRADE_INTENTS_TABLE_NAME,
        column_name="cancel_requested_at_ts",
        column_sql="cancel_requested_at_ts INTEGER",
    )


def initialize_trade_db(conn) -> None:
    conn.execute(create_positions_latest_table_sql())
    conn.execute(create_trade_intents_table_sql())
    ensure_trade_intents_runtime_columns(conn)

    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_trade_intents_status
        ON {TRADE_INTENTS_TABLE_NAME}(status, created_at_ts);
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_trade_intents_instrument_time
        ON {TRADE_INTENTS_TABLE_NAME}(instrument_code, created_at_ts);
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_trade_intents_source_signal
        ON {TRADE_INTENTS_TABLE_NAME}(instrument_code, source_signal_id, signal_bar_ts);
        """
    )


def build_time_text_fields_from_ts(ts: int) -> tuple[str, str, str]:
    dt_utc = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    dt_ct = dt_utc.astimezone(CT_TIMEZONE)
    dt_msk = dt_utc.astimezone(MSK_TIMEZONE)

    return (
        dt_utc.strftime(SQLITE_DATETIME_FORMAT),
        dt_ct.strftime(SQLITE_DATETIME_FORMAT),
        dt_msk.strftime(SQLITE_DATETIME_FORMAT),
    )


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


def read_position_snapshot(conn, *, instrument_code: str) -> PositionSnapshot:
    initialize_trade_db(conn)

    row = conn.execute(
        f"""
        SELECT side, quantity
        FROM {POSITIONS_LATEST_TABLE_NAME}
        WHERE instrument_code = ?
        """,
        (str(instrument_code),),
    ).fetchone()

    if row is None:
        return PositionSnapshot(
            instrument_code=str(instrument_code),
            side=PositionSide.UNKNOWN,
            quantity=0.0,
        )

    quantity = float(row[1])
    side = PositionSide(str(row[0]).upper())

    if side == PositionSide.UNKNOWN:
        return PositionSnapshot(str(instrument_code), PositionSide.UNKNOWN, 0.0)

    if quantity <= 0.0:
        return PositionSnapshot(str(instrument_code), PositionSide.FLAT, 0.0)

    return PositionSnapshot(str(instrument_code), side, quantity)


def read_open_position_snapshots(conn) -> list[PositionSnapshot]:
    initialize_trade_db(conn)

    rows = conn.execute(
        f"""
        SELECT instrument_code, side, quantity
        FROM {POSITIONS_LATEST_TABLE_NAME}
        WHERE side IN ('LONG', 'SHORT')
          AND quantity > 0
        ORDER BY instrument_code
        """
    ).fetchall()

    return [
        PositionSnapshot(
            instrument_code=str(row[0]),
            side=PositionSide(str(row[1]).upper()),
            quantity=float(row[2]),
        )
        for row in rows
    ]


def read_position_updated_at_ts(conn, *, instrument_code: str) -> int | None:
    row = conn.execute(
        f"""
        SELECT updated_at_ts
        FROM {POSITIONS_LATEST_TABLE_NAME}
        WHERE instrument_code = ?
        """,
        (str(instrument_code),),
    ).fetchone()

    if row is None or row[0] is None:
        return None

    return int(row[0])


def read_position_snapshot_freshness(
        conn,
        *,
        instrument_code: str,
        now_ts: int,
        max_age_seconds: int = POSITION_SNAPSHOT_MAX_AGE_SECONDS,
) -> PositionSnapshotFreshness:
    row = conn.execute(
        f"""
        SELECT updated_at_ts, updated_at_utc
        FROM {POSITIONS_LATEST_TABLE_NAME}
        WHERE instrument_code = ?
        """,
        (str(instrument_code),),
    ).fetchone()

    if row is None or row[0] is None:
        return PositionSnapshotFreshness(
            instrument_code=str(instrument_code),
            updated_at_ts=None,
            updated_at_utc=None,
            age_seconds=None,
            max_age_seconds=int(max_age_seconds),
            is_stale=True,
        )

    updated_at_ts = int(row[0])
    age_seconds = max(0, int(now_ts) - updated_at_ts)

    return PositionSnapshotFreshness(
        instrument_code=str(instrument_code),
        updated_at_ts=updated_at_ts,
        updated_at_utc=None if row[1] is None else str(row[1]),
        age_seconds=age_seconds,
        max_age_seconds=int(max_age_seconds),
        is_stale=age_seconds > int(max_age_seconds),
    )


def build_stale_position_open_rejected_event(
        *,
        signal: TraderSignalEvent,
        position: PositionSnapshot,
        freshness: PositionSnapshotFreshness,
        draft: TradeIntentDraft,
) -> TradeIntentRejected:
    return TradeIntentRejected(
        instrument_code=signal.instrument_code,
        source_signal_id=int(signal.source_signal_id),
        signal_bar_ts=int(signal.signal_bar_ts),
        signal_time_utc=signal.signal_time_utc,
        signal_time_ct=signal.signal_time_ct,
        signal_time_msk=signal.signal_time_msk,
        reason="positions_latest_stale_trade_rejected",
        action=draft.action,
        signal_direction=draft.signal_direction,
        order_type=draft.order_type,
        position_before_side=position.side,
        position_before_qty=float(position.quantity),
        positions_latest_updated_at_ts=freshness.updated_at_ts,
        positions_latest_updated_at_utc=freshness.updated_at_utc,
        positions_latest_age_seconds=freshness.age_seconds,
        max_allowed_age_seconds=freshness.max_age_seconds,
    )


def expire_stale_new_trade_intents(
        conn,
        *,
        max_age_seconds: int = TRADER_MAX_NEW_INTENT_AGE_SECONDS,
        now_ts: int | None = None,
) -> int:
    """Переводит старые NEW trade_intents в FAILED."""
    max_age_seconds = int(max_age_seconds)

    if max_age_seconds <= 0:
        return 0

    now_ts = int(time.time() if now_ts is None else now_ts)
    min_created_at_ts = now_ts - max_age_seconds

    changes_before = conn.total_changes

    conn.execute(
        f"""
        UPDATE {TRADE_INTENTS_TABLE_NAME}
        SET
            status = 'FAILED',
            error_text = ?,
            updated_at_ts = ?,
            finished_at_ts = ?
        WHERE status = 'NEW'
          AND created_at_ts < ?
        """,
        (
            f"{STALE_NEW_INTENT_ERROR_TEXT}; max_age_seconds={max_age_seconds}",
            now_ts,
            now_ts,
            min_created_at_ts,
        ),
    )

    return int(conn.total_changes - changes_before)


def read_latest_non_failed_trade_intent(conn, *, instrument_code: str) -> dict | None:
    row = conn.execute(
        f"""
        SELECT
            trade_intent_id,
            action,
            target_side,
            target_qty,
            status,
            created_at_ts,
            updated_at_ts,
            sent_at_ts,
            finished_at_ts
        FROM {TRADE_INTENTS_TABLE_NAME}
        WHERE instrument_code = ?
          AND status != 'FAILED'
        ORDER BY
            COALESCE(finished_at_ts, updated_at_ts, sent_at_ts, created_at_ts) DESC,
            trade_intent_id DESC
        LIMIT 1
        """,
        (str(instrument_code),),
    ).fetchone()

    if row is None:
        return None

    return {
        "trade_intent_id": int(row[0]),
        "action": str(row[1]),
        "target_side": str(row[2]),
        "target_qty": float(row[3]),
        "status": str(row[4]),
        "created_at_ts": int(row[5]),
        "updated_at_ts": int(row[6]),
        "sent_at_ts": None if row[7] is None else int(row[7]),
        "finished_at_ts": None if row[8] is None else int(row[8]),
        "event_ts": int(row[8] or row[6] or row[7] or row[5]),
    }


def has_unresolved_trade_intent_for_instrument(conn, *, instrument_code: str) -> bool:
    """Не даём создать новый приказ, пока предыдущий активен или позиция после EXECUTED ещё не синхронизирована."""
    latest_intent = read_latest_non_failed_trade_intent(
        conn,
        instrument_code=instrument_code,
    )

    if latest_intent is None:
        return False

    if latest_intent["status"] in {"NEW", "SENDING", "ACCEPTED"}:
        return True

    if latest_intent["status"] == "EXECUTED":
        position_updated_at_ts = read_position_updated_at_ts(
            conn,
            instrument_code=instrument_code,
        )
        return position_updated_at_ts is None or position_updated_at_ts <= latest_intent["event_ts"]

    return False


def has_trade_intent_for_signal(
        conn,
        *,
        instrument_code: str,
        source_signal_id: int,
        signal_bar_ts: int,
) -> bool:
    row = conn.execute(
        f"""
        SELECT 1
        FROM {TRADE_INTENTS_TABLE_NAME}
        WHERE instrument_code = ?
          AND source_signal_id = ?
          AND signal_bar_ts = ?
        LIMIT 1
        """,
        (
            str(instrument_code),
            int(source_signal_id),
            int(signal_bar_ts),
        ),
    ).fetchone()

    return row is not None



def read_pending_entry_limit_intent_for_opposite_signal(conn, *, signal: TraderSignalEvent) -> dict | None:
    signal_direction = str(signal.direction).upper()

    if signal_direction not in {"LONG", "SHORT"}:
        return None

    row = conn.execute(
        f"""
        SELECT
            trade_intent_id,
            instrument_code,
            source_signal_id,
            signal_bar_ts,
            action,
            target_side,
            target_qty,
            order_type,
            status,
            order_id,
            order_action,
            order_quantity,
            cancel_requested
        FROM {TRADE_INTENTS_TABLE_NAME}
        WHERE instrument_code = ?
          AND action = 'OPEN_POSITION'
          AND order_type = 'LIMIT'
          AND status IN ('NEW', 'SENDING', 'ACCEPTED')
          AND target_side != ?
        ORDER BY trade_intent_id DESC
        LIMIT 1
        """,
        (
            str(signal.instrument_code),
            signal_direction,
        ),
    ).fetchone()

    if row is None:
        return None

    return {
        "trade_intent_id": int(row[0]),
        "instrument_code": str(row[1]),
        "source_signal_id": int(row[2]),
        "signal_bar_ts": int(row[3]),
        "action": str(row[4]),
        "target_side": str(row[5]),
        "target_qty": float(row[6]),
        "order_type": str(row[7]),
        "status": str(row[8]),
        "order_id": None if row[9] is None else int(row[9]),
        "order_action": None if row[10] is None else str(row[10]),
        "order_quantity": None if row[11] is None else int(row[11]),
        "cancel_requested": bool(int(row[12] or 0)),
    }


def request_cancel_pending_entry_limit_for_opposite_signal(conn, *, signal: TraderSignalEvent) -> dict | None:
    pending_intent = read_pending_entry_limit_intent_for_opposite_signal(
        conn,
        signal=signal,
    )

    if pending_intent is None:
        return None

    now_ts = int(time.time())
    reason = (
        f"cancel pending LIMIT entry by opposite signal: "
        f"source_signal_id={signal.source_signal_id}, "
        f"direction={signal.direction}"
    )

    if pending_intent["status"] == "NEW":
        conn.execute(
            f"""
            UPDATE {TRADE_INTENTS_TABLE_NAME}
            SET
                status = 'CANCELLED',
                cancel_requested = 1,
                cancel_reason = ?,
                cancel_source_signal_id = ?,
                cancel_requested_at_ts = ?,
                error_text = ?,
                updated_at_ts = ?,
                finished_at_ts = ?
            WHERE trade_intent_id = ?
              AND status = 'NEW'
            """,
            (
                reason,
                int(signal.source_signal_id),
                now_ts,
                reason,
                now_ts,
                now_ts,
                int(pending_intent["trade_intent_id"]),
            ),
        )

    else:
        conn.execute(
            f"""
            UPDATE {TRADE_INTENTS_TABLE_NAME}
            SET
                cancel_requested = 1,
                cancel_reason = ?,
                cancel_source_signal_id = ?,
                cancel_requested_at_ts = ?,
                updated_at_ts = ?
            WHERE trade_intent_id = ?
              AND status IN ('SENDING', 'ACCEPTED')
            """,
            (
                reason,
                int(signal.source_signal_id),
                now_ts,
                now_ts,
                int(pending_intent["trade_intent_id"]),
            ),
        )

    pending_intent["cancel_reason"] = reason
    pending_intent["cancel_source_signal_id"] = int(signal.source_signal_id)
    return pending_intent


def build_trade_order_ref(*, trade_intent_id: int, instrument_code: str) -> str:
    return f"{ORDER_REF_PREFIX}_{int(trade_intent_id)}_{str(instrument_code)}"


def write_trade_intent(conn, draft: TradeIntentDraft) -> int:
    initialize_trade_db(conn)

    now_ts = int(time.time())
    signal_time_utc, signal_time_ct, signal_time_msk = build_time_text_fields_from_ts(
        int(draft.signal_bar_ts),
    )

    if draft.action in {TradeDecisionAction.OPEN_POSITION, TradeDecisionAction.REVERSE_POSITION}:
        entry_regime = draft.regime
        entry_ma_zone = draft.ma_zone
    else:
        entry_regime = None
        entry_ma_zone = None

    conn.execute(
        f"""
        INSERT INTO {TRADE_INTENTS_TABLE_NAME} (
            source_signal_id,
            instrument_code,
            signal_bar_ts,
            signal_time_utc,
            signal_time_ct,
            signal_time_msk,

            entry_regime,
            entry_ma_zone,

            intent_source,

            action,
            action_reason,

            target_side,
            target_qty,

            position_before_side,
            position_before_qty,

            order_type,
            limit_price,
            limit_offset_points,
            ttl_seconds,

            status,

            created_at_ts,
            updated_at_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

        ON CONFLICT (
            instrument_code,
            source_signal_id,
            signal_bar_ts,
            action
        ) DO NOTHING
        """,
        (
            int(draft.source_signal_id),
            draft.instrument_code,
            int(draft.signal_bar_ts),
            signal_time_utc,
            signal_time_ct,
            signal_time_msk,
            entry_regime,
            entry_ma_zone,
            draft.intent_source,
            draft.action.value,
            draft.reason,
            draft.position_after_side.value,
            float(draft.position_after_qty),
            draft.position_before_side.value,
            float(draft.position_before_qty),
            draft.order_type,
            draft.limit_price,
            draft.limit_offset_points,
            draft.ttl_seconds,
            "NEW",
            now_ts,
            now_ts,
        ),
    )

    row = conn.execute(
        f"""
        SELECT trade_intent_id
        FROM {TRADE_INTENTS_TABLE_NAME}
        WHERE instrument_code = ?
          AND source_signal_id = ?
          AND signal_bar_ts = ?
          AND action = ?
        """,
        (
            draft.instrument_code,
            int(draft.source_signal_id),
            int(draft.signal_bar_ts),
            draft.action.value,
        ),
    ).fetchone()

    if row is None or row[0] is None:
        raise RuntimeError("TradeIntent был записан, но trade_intent_id не найден")

    trade_intent_id = int(row[0])
    order_ref = build_trade_order_ref(
        trade_intent_id=trade_intent_id,
        instrument_code=draft.instrument_code,
    )

    conn.execute(
        f"""
        UPDATE {TRADE_INTENTS_TABLE_NAME}
        SET order_ref = ?
        WHERE trade_intent_id = ?
        """,
        (
            order_ref,
            trade_intent_id,
        ),
    )

    return trade_intent_id


def build_created_event(*, trade_intent_id: int, draft: TradeIntentDraft) -> TradeIntentCreated:
    return TradeIntentCreated(
        trade_intent_id=int(trade_intent_id),
        source_signal_id=int(draft.source_signal_id),
        instrument_code=draft.instrument_code,
        signal_bar_ts=int(draft.signal_bar_ts),
        signal_time_ct=draft.signal_time_ct,
        intent_source=draft.intent_source,
        action=draft.action,
        reason=draft.reason,
        signal_direction=draft.signal_direction,
        entry_price=float(draft.entry_price),
        potential_end_delta_points=float(draft.potential_end_delta_points),
        regime=draft.regime,
        ma_zone=draft.ma_zone,
        signal_strength=draft.signal_strength,
        order_type=draft.order_type,
        limit_price=draft.limit_price,
        limit_offset_points=draft.limit_offset_points,
        ttl_seconds=draft.ttl_seconds,
        position_before_side=draft.position_before_side,
        position_before_qty=float(draft.position_before_qty),
        position_after_side=draft.position_after_side,
        position_after_qty=float(draft.position_after_qty),
    )


def write_trade_intent_and_event(conn, draft: TradeIntentDraft) -> TradeIntentCreated:
    trade_intent_id = write_trade_intent(conn, draft)
    return build_created_event(
        trade_intent_id=trade_intent_id,
        draft=draft,
    )


def build_signal_trade_intent_draft(
        *,
        signal: TraderSignalEvent,
        position: PositionSnapshot,
) -> TradeIntentDraft | None:
    if not signal.signal_allowed:
        return None

    if position.side == PositionSide.UNKNOWN:
        return None

    signal_direction = str(signal.direction).upper()
    if signal_direction not in {"LONG", "SHORT"}:
        return None

    signal_order_type = str(signal.order_type).upper()

    if position.side == PositionSide.FLAT or position.quantity <= 0.0:
        action = TradeDecisionAction.OPEN_POSITION
        reason = "flat_position_open_by_signal"
        after_side = PositionSide(signal_direction)
        after_qty = 1.0
        order_type = signal_order_type
        limit_price = signal.limit_price if signal_order_type == "LIMIT" else None
        limit_offset_points = signal.limit_offset_points
        ttl_seconds = signal.ttl_seconds

    elif position.side.value == signal_direction:
        return None

    else:
        if signal_order_type == "LIMIT":
            action = TradeDecisionAction.CLOSE_POSITION
            reason = "opposite_limit_signal_close_position_only"
            after_side = PositionSide.FLAT
            after_qty = 0.0
            order_type = "MARKET"
            limit_price = None
            limit_offset_points = None
            ttl_seconds = None

        else:
            action = TradeDecisionAction.REVERSE_POSITION
            reason = "opposite_signal_reverse_position"
            after_side = PositionSide(signal_direction)
            after_qty = max(1.0, float(position.quantity))
            order_type = "MARKET"
            limit_price = None
            limit_offset_points = None
            ttl_seconds = None

    return TradeIntentDraft(
        source_signal_id=signal.source_signal_id,
        instrument_code=signal.instrument_code,
        signal_bar_ts=signal.signal_bar_ts,
        signal_time_ct=signal.signal_time_ct,
        intent_source="SIGNAL",
        action=action,
        reason=reason,
        signal_direction=signal_direction,
        entry_price=float(signal.entry_price),
        potential_end_delta_points=float(signal.potential_end_delta_points),
        regime=signal.regime,
        ma_zone=signal.ma_zone,
        signal_strength=signal.signal_strength,
        order_type=order_type,
        limit_price=limit_price,
        limit_offset_points=limit_offset_points,
        ttl_seconds=ttl_seconds,
        position_before_side=position.side,
        position_before_qty=float(position.quantity),
        position_after_side=after_side,
        position_after_qty=float(after_qty),
    )


def get_slot_close_context(*, now_ts: int) -> dict | None:
    settings = DEFAULT_SIGNAL_CONFIG

    if settings.signal_window_mode != SignalWindowMode.SLOT:
        return None

    slot_step_seconds = int(settings.slot_step_minutes) * 60
    close_before_seconds = int(settings.slot_close_before_end_seconds)

    if slot_step_seconds <= 0:
        return None

    if close_before_seconds < 0 or close_before_seconds >= slot_step_seconds:
        return None

    slot_start_ts = get_slot_start_ts(
        current_bar_ts=int(now_ts),
        slot_step_minutes=int(settings.slot_step_minutes),
        slot_start_minute_of_day=int(settings.slot_start_minute_of_day),
    )
    slot_end_ts = int(slot_start_ts) + slot_step_seconds
    close_at_ts = int(slot_end_ts) - close_before_seconds

    if close_at_ts <= int(now_ts) < slot_end_ts:
        return {
            "slot_start_ts": int(slot_start_ts),
            "slot_end_ts": int(slot_end_ts),
            "close_at_ts": int(close_at_ts),
            "close_before_seconds": int(close_before_seconds),
        }

    return None


def get_slot_entry_context(*, now_ts: int) -> dict | None:
    settings = DEFAULT_SIGNAL_CONFIG

    if settings.signal_window_mode != SignalWindowMode.SLOT:
        return None

    slot_step_seconds = int(settings.slot_step_minutes) * 60
    slot_back_seconds = int(settings.slot_back_minutes) * 60
    slot_entry_seconds = int(settings.slot_entry_minutes) * 60
    close_before_seconds = int(settings.slot_close_before_end_seconds)

    if slot_step_seconds <= 0:
        return None

    if slot_back_seconds < 0 or slot_entry_seconds < 0:
        return None

    if close_before_seconds < 0 or close_before_seconds >= slot_step_seconds:
        return None

    if slot_back_seconds + slot_entry_seconds > slot_step_seconds:
        return None

    slot_start_ts = get_slot_start_ts(
        current_bar_ts=int(now_ts),
        slot_step_minutes=int(settings.slot_step_minutes),
        slot_start_minute_of_day=int(settings.slot_start_minute_of_day),
    )
    slot_end_ts = int(slot_start_ts) + slot_step_seconds
    entry_start_ts = int(slot_start_ts) + slot_back_seconds
    entry_end_ts = entry_start_ts + slot_entry_seconds
    close_at_ts = int(slot_end_ts) - close_before_seconds
    entry_end_ts = min(entry_end_ts, close_at_ts)

    if entry_start_ts <= int(now_ts) < entry_end_ts:
        return {
            "slot_start_ts": int(slot_start_ts),
            "slot_end_ts": int(slot_end_ts),
            "entry_start_ts": int(entry_start_ts),
            "entry_end_ts": int(entry_end_ts),
            "close_at_ts": int(close_at_ts),
            "slot_back_seconds": int(slot_back_seconds),
            "slot_entry_seconds": int(slot_entry_seconds),
            "close_before_seconds": int(close_before_seconds),
        }

    return None


def is_slot_entry_allowed_now(*, now_ts: int) -> bool:
    settings = DEFAULT_SIGNAL_CONFIG

    if settings.signal_window_mode != SignalWindowMode.SLOT:
        return True

    return get_slot_entry_context(now_ts=now_ts) is not None


def is_risk_increasing_trade_action(action: TradeDecisionAction) -> bool:
    return action in {
        TradeDecisionAction.OPEN_POSITION,
        TradeDecisionAction.REVERSE_POSITION,
    }


def build_slot_close_trade_intent_draft(
        *,
        position: PositionSnapshot,
        close_context: dict,
        now_ts: int,
) -> TradeIntentDraft:
    _, signal_time_ct, _ = build_time_text_fields_from_ts(now_ts)

    return TradeIntentDraft(
        source_signal_id=SLOT_CLOSE_SOURCE_SIGNAL_ID,
        instrument_code=position.instrument_code,
        signal_bar_ts=int(now_ts),
        signal_time_ct=signal_time_ct,
        intent_source=SLOT_CLOSE_INTENT_SOURCE,
        action=TradeDecisionAction.CLOSE_POSITION,
        reason=SLOT_CLOSE_REASON,
        signal_direction="CLOSE",
        entry_price=0.0,
        potential_end_delta_points=0.0,
        regime=None,
        ma_zone=None,
        signal_strength="SLOT_CLOSE",
        order_type="MARKET",
        limit_price=None,
        limit_offset_points=None,
        ttl_seconds=None,
        position_before_side=position.side,
        position_before_qty=float(position.quantity),
        position_after_side=PositionSide.FLAT,
        position_after_qty=0.0,
    )


def get_ct_day_ts(*, now_ts: int, hour: int, minute: int = 0, second: int = 0) -> int:
    now_ct = datetime.fromtimestamp(int(now_ts), tz=timezone.utc).astimezone(CT_TIMEZONE)
    target_ct = now_ct.replace(
        hour=int(hour),
        minute=int(minute),
        second=int(second),
        microsecond=0,
    )
    return int(target_ct.astimezone(timezone.utc).timestamp())


def get_futures_daily_flat_context(*, now_ts: int) -> dict | None:
    settings = DEFAULT_SIGNAL_CONFIG
    close_before_seconds = int(settings.slot_close_before_end_seconds)

    no_new_trades_ts = get_ct_day_ts(
        now_ts=now_ts,
        hour=FUTURES_NO_NEW_TRADES_HOUR_CT,
    )
    clearing_ts = get_ct_day_ts(
        now_ts=now_ts,
        hour=FUTURES_CLEARING_HOUR_CT,
    )
    close_at_ts = no_new_trades_ts - close_before_seconds

    if close_at_ts <= int(now_ts) < clearing_ts:
        return {
            "no_new_trades_ts": int(no_new_trades_ts),
            "clearing_ts": int(clearing_ts),
            "close_at_ts": int(close_at_ts),
            "close_before_seconds": int(close_before_seconds),
        }

    return None


def is_futures_instrument(instrument_code: str) -> bool:
    instrument_row = Instrument.get(str(instrument_code))

    if instrument_row is None:
        return False

    return str(instrument_row.get("secType", "")).upper() == "FUT"


def build_futures_daily_flat_trade_intent_draft(
        *,
        position: PositionSnapshot,
        close_context: dict,
        now_ts: int,
) -> TradeIntentDraft:
    _, signal_time_ct, _ = build_time_text_fields_from_ts(now_ts)

    return TradeIntentDraft(
        source_signal_id=FUTURES_DAILY_FLAT_SOURCE_SIGNAL_ID,
        instrument_code=position.instrument_code,
        signal_bar_ts=int(now_ts),
        signal_time_ct=signal_time_ct,
        intent_source=FUTURES_DAILY_FLAT_INTENT_SOURCE,
        action=TradeDecisionAction.CLOSE_POSITION,
        reason=FUTURES_DAILY_FLAT_REASON,
        signal_direction="CLOSE",
        entry_price=0.0,
        potential_end_delta_points=0.0,
        regime=None,
        ma_zone=None,
        signal_strength="FUTURES_DAILY_FLAT",
        order_type="MARKET",
        limit_price=None,
        limit_offset_points=None,
        ttl_seconds=None,
        position_before_side=position.side,
        position_before_qty=float(position.quantity),
        position_after_side=PositionSide.FLAT,
        position_after_qty=0.0,
    )


def is_extreme_ma_zone_close_enabled() -> bool:
    return bool(SIGNAL_RULE_SETTINGS.get("close_position_on_extreme_ma_zone_enabled", True))


def get_long_position_extreme_close_ma_zone() -> int:
    return int(SIGNAL_RULES["long_position_extreme_close_ma_zone"])


def get_short_position_extreme_close_ma_zone() -> int:
    return int(SIGNAL_RULES["short_position_extreme_close_ma_zone"])


def should_close_position_on_extreme_ma_zone(
        *,
        position: PositionSnapshot,
        ma_zone: int | None,
) -> bool:
    if ma_zone is None:
        return False

    ma_zone_value = int(ma_zone)

    if position.side == PositionSide.LONG:
        return ma_zone_value >= get_long_position_extreme_close_ma_zone()

    if position.side == PositionSide.SHORT:
        return ma_zone_value <= get_short_position_extreme_close_ma_zone()

    return False


def build_extreme_ma_zone_close_trade_intent_draft(
        *,
        position: PositionSnapshot,
        market: MarketSnapshot,
        now_ts: int,
) -> TradeIntentDraft:
    _, signal_time_ct, _ = build_time_text_fields_from_ts(now_ts)

    return TradeIntentDraft(
        source_signal_id=EXTREME_MA_ZONE_CLOSE_SOURCE_SIGNAL_ID,
        instrument_code=position.instrument_code,
        signal_bar_ts=int(now_ts),
        signal_time_ct=signal_time_ct,
        intent_source=EXTREME_MA_ZONE_CLOSE_INTENT_SOURCE,
        action=TradeDecisionAction.CLOSE_POSITION,
        reason=EXTREME_MA_ZONE_CLOSE_REASON,
        signal_direction="CLOSE",
        entry_price=0.0,
        potential_end_delta_points=0.0,
        regime=market.regime,
        ma_zone=market.ma_zone,
        signal_strength="EXTREME_MA_ZONE_CLOSE",
        order_type="MARKET",
        limit_price=None,
        limit_offset_points=None,
        ttl_seconds=None,
        position_before_side=position.side,
        position_before_qty=float(position.quantity),
        position_after_side=PositionSide.FLAT,
        position_after_qty=0.0,
    )


def process_extreme_ma_zone_close_once(conn, *, now_ts: int | None = None) -> list[TradeIntentCreated]:
    if not is_extreme_ma_zone_close_enabled():
        return []

    now_ts = int(time.time() if now_ts is None else now_ts)

    created: list[TradeIntentCreated] = []

    for position in read_open_position_snapshots(conn):
        if has_unresolved_trade_intent_for_instrument(
                conn,
                instrument_code=position.instrument_code,
        ):
            continue

        market = read_market_snapshot(
            instrument_code=position.instrument_code,
            signal_bar_ts=now_ts,
        )

        if not should_close_position_on_extreme_ma_zone(
                position=position,
                ma_zone=market.ma_zone,
        ):
            continue

        draft = build_extreme_ma_zone_close_trade_intent_draft(
            position=position,
            market=market,
            now_ts=now_ts,
        )
        created.append(write_trade_intent_and_event(conn, draft))

    return created


def process_slot_close_once(conn, *, now_ts: int | None = None) -> list[TradeIntentCreated]:
    now_ts = int(time.time() if now_ts is None else now_ts)

    # Если включён second-chance механизм, штатным slot-close владеет ib_execution.
    # Это убирает race: trader не создаёт CLOSE_POSITION, пока execution решает
    # закрывать позицию сразу или переводить её в SLOT_LOSS_EXTENSION.
    if bool(getattr(DEFAULT_SIGNAL_CONFIG, "slot_loss_extension_enabled", False)):
        return []

    close_context = get_slot_close_context(now_ts=now_ts)

    if close_context is None:
        return []

    created: list[TradeIntentCreated] = []

    for position in read_open_position_snapshots(conn):
        if has_unresolved_trade_intent_for_instrument(
                conn,
                instrument_code=position.instrument_code,
        ):
            continue

        draft = build_slot_close_trade_intent_draft(
            position=position,
            close_context=close_context,
            now_ts=now_ts,
        )
        created.append(write_trade_intent_and_event(conn, draft))

    return created


def process_futures_daily_flat_once(conn, *, now_ts: int | None = None) -> list[TradeIntentCreated]:
    now_ts = int(time.time() if now_ts is None else now_ts)
    close_context = get_futures_daily_flat_context(now_ts=now_ts)

    if close_context is None:
        return []

    created: list[TradeIntentCreated] = []

    for position in read_open_position_snapshots(conn):
        if not is_futures_instrument(position.instrument_code):
            continue

        if has_unresolved_trade_intent_for_instrument(
                conn,
                instrument_code=position.instrument_code,
        ):
            continue

        draft = build_futures_daily_flat_trade_intent_draft(
            position=position,
            close_context=close_context,
            now_ts=now_ts,
        )
        created.append(write_trade_intent_and_event(conn, draft))

    return created


def process_signal_events_once(*, max_signal_age_seconds: int) -> TradeProcessResult:
    signals = read_latest_signal_events(max_signal_age_seconds=max_signal_age_seconds)

    conn = get_trade_db_connection()

    try:
        initialize_trade_db(conn)

        # ВАЖНО: ib_trader — producer trade_intents, а не владелец исполнения.
        # Не переводим NEW intents в FAILED здесь. Иначе, если ib_execution временно
        # не успел забрать intent, trader сам гасит заявку и создаёт новую по следующему
        # сигналу, засоряя trade_intents пачкой FAILED записей без order_id.
        # Просрочку NEW/SENDING/ACCEPTED должен делать ib_execution через execution_store.
        now_ts = int(time.time())
        created: list[TradeIntentCreated] = []
        rejected: list[TradeIntentRejected] = []
        created.extend(process_futures_daily_flat_once(conn, now_ts=now_ts))
        created.extend(process_slot_close_once(conn, now_ts=now_ts))
        created.extend(process_extreme_ma_zone_close_once(conn, now_ts=now_ts))

        for signal in signals:
            if has_trade_intent_for_signal(
                    conn,
                    instrument_code=signal.instrument_code,
                    source_signal_id=signal.source_signal_id,
                    signal_bar_ts=signal.signal_bar_ts,
            ):
                continue

            position = read_position_snapshot(conn, instrument_code=signal.instrument_code)
            freshness = read_position_snapshot_freshness(
                conn,
                instrument_code=signal.instrument_code,
                now_ts=now_ts,
            )

            if has_active_slot_loss_extension_for_instrument(
                    conn,
                    instrument_code=signal.instrument_code,
            ):
                continue

            # Если позиции ещё нет, но висит LIMIT-вход в противоположную сторону,
            # новый сигнал важнее старого лимитника: просим execution отменить pending order.
            cancelled_pending_limit = request_cancel_pending_entry_limit_for_opposite_signal(
                conn,
                signal=signal,
            )
            if cancelled_pending_limit is not None:
                continue

            if has_unresolved_trade_intent_for_instrument(
                    conn,
                    instrument_code=signal.instrument_code,
            ):
                continue

            draft = build_signal_trade_intent_draft(
                signal=signal,
                position=position,
            )

            if draft is None:
                continue

            # Нельзя доверять ни FLAT, ни LONG/SHORT, если positions_latest протухла.
            # Иначе можно не только пропустить вход, но и открыть фантомный reverse по старой позиции.
            if freshness.is_stale:
                rejected.append(
                    build_stale_position_open_rejected_event(
                        signal=signal,
                        position=position,
                        freshness=freshness,
                        draft=draft,
                    )
                )
                continue

            if is_risk_increasing_trade_action(draft.action) and not is_slot_entry_allowed_now(now_ts=now_ts):
                continue

            created.append(write_trade_intent_and_event(conn, draft))

        conn.commit()
        return TradeProcessResult(created=created, rejected=rejected)

    finally:
        conn.close()
