from __future__ import annotations

import time

from ib_trader.trade_models import (
    TradeDecisionAction,
    TradeIntentCreated,
    TraderSignalEvent,
)
from ib_trader.trade_position_repository import read_position_updated_at_ts
from ib_trader.trade_schema import (
    ORDER_REF_PREFIX,
    TRADE_INTENTS_TABLE_NAME,
    TradeIntentDraft,
    build_time_text_fields_from_ts,
    initialize_trade_db,
)


TRADER_MAX_NEW_INTENT_AGE_SECONDS = 10
STALE_NEW_INTENT_ERROR_TEXT = "stale trade_intent before trader decision"

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

    if latest_intent["status"] in {"NEW", "SENDING", "ACCEPTED", "RECONCILING"}:
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
          AND status IN ('NEW', 'SENDING', 'ACCEPTED', 'RECONCILING')
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
              AND status IN ('SENDING', 'ACCEPTED', 'RECONCILING')
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

__all__ = ['TRADER_MAX_NEW_INTENT_AGE_SECONDS', 'STALE_NEW_INTENT_ERROR_TEXT', 'expire_stale_new_trade_intents', 'read_latest_non_failed_trade_intent', 'has_unresolved_trade_intent_for_instrument', 'has_trade_intent_for_signal', 'read_pending_entry_limit_intent_for_opposite_signal', 'request_cancel_pending_entry_limit_for_opposite_signal', 'build_trade_order_ref', 'write_trade_intent', 'build_created_event', 'write_trade_intent_and_event']
