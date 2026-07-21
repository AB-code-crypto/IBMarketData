from __future__ import annotations

import time

from ib_trader.trade_models import TradeIntentCreated
from ib_trader.trade_position_repository import read_position_updated_at_ts
from ib_trader.trade_schema import (
    ORDER_REF_PREFIX,
    TRADE_INTENTS_TABLE_NAME,
    TradeIntentDraft,
    build_time_text_fields_from_ts,
    initialize_trade_db,
)


def read_latest_non_failed_trade_intent(conn, *, instrument_code: str) -> dict | None:
    row = conn.execute(
        f"""
        SELECT
            trade_intent_id, action, target_side, target_qty, status,
            created_at_ts, updated_at_ts, sent_at_ts, finished_at_ts
        FROM {TRADE_INTENTS_TABLE_NAME}
        WHERE instrument_code = ? AND status != 'FAILED'
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
    latest = read_latest_non_failed_trade_intent(
        conn,
        instrument_code=instrument_code,
    )
    if latest is None:
        return False
    if latest["status"] in {"NEW", "SENDING", "ACCEPTED", "RECONCILING"}:
        return True
    if latest["status"] == "EXECUTED":
        position_updated_at_ts = read_position_updated_at_ts(
            conn,
            instrument_code=instrument_code,
        )
        return (
            position_updated_at_ts is None
            or position_updated_at_ts <= latest["event_ts"]
        )
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
        (str(instrument_code), int(source_signal_id), int(signal_bar_ts)),
    ).fetchone()
    return row is not None


def build_trade_order_ref(*, trade_intent_id: int, instrument_code: str) -> str:
    return f"{ORDER_REF_PREFIX}_{int(trade_intent_id)}_{str(instrument_code)}"


def write_trade_intent(conn, draft: TradeIntentDraft) -> int:
    initialize_trade_db(conn)
    now_ts = int(time.time())
    signal_time_utc, signal_time_ct, signal_time_msk = build_time_text_fields_from_ts(
        int(draft.signal_bar_ts)
    )
    order_type = str(draft.order_type).upper()
    if order_type != "MARKET":
        raise ValueError(
            f"Rolling-only trader creates MARKET intents only: {order_type!r}"
        )

    conn.execute(
        f"""
        INSERT INTO {TRADE_INTENTS_TABLE_NAME} (
            source_signal_id,
            instrument_code,
            signal_bar_ts,
            signal_time_utc,
            signal_time_ct,
            signal_time_msk,
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, 'NEW', ?, ?)
        ON CONFLICT (
            instrument_code, source_signal_id, signal_bar_ts, action
        ) DO NOTHING
        """,
        (
            int(draft.source_signal_id),
            draft.instrument_code,
            int(draft.signal_bar_ts),
            signal_time_utc,
            signal_time_ct,
            signal_time_msk,
            draft.intent_source,
            draft.action.value,
            draft.reason,
            draft.position_after_side.value,
            float(draft.position_after_qty),
            draft.position_before_side.value,
            float(draft.position_before_qty),
            order_type,
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
    conn.execute(
        f"UPDATE {TRADE_INTENTS_TABLE_NAME} SET order_ref = ? WHERE trade_intent_id = ?",
        (
            build_trade_order_ref(
                trade_intent_id=trade_intent_id,
                instrument_code=draft.instrument_code,
            ),
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
        order_type=str(draft.order_type).upper(),
        position_before_side=draft.position_before_side,
        position_before_qty=float(draft.position_before_qty),
        position_after_side=draft.position_after_side,
        position_after_qty=float(draft.position_after_qty),
    )


def write_trade_intent_and_event(conn, draft: TradeIntentDraft) -> TradeIntentCreated:
    return build_created_event(
        trade_intent_id=write_trade_intent(conn, draft),
        draft=draft,
    )


__all__ = [
    "read_latest_non_failed_trade_intent",
    "has_unresolved_trade_intent_for_instrument",
    "has_trade_intent_for_signal",
    "build_trade_order_ref",
    "write_trade_intent",
    "build_created_event",
    "write_trade_intent_and_event",
]
