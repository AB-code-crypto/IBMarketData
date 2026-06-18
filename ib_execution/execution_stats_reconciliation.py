import time
from types import SimpleNamespace
from typing import Any

from ib_execution.execution_logic import (
    are_commission_reports_final_for_fills,
    collect_trade_fill_statistics,
    refresh_ib_executions_if_possible,
)
from ib_execution.execution_store import get_trade_db_connection, initialize_execution_db
from ib_trader.trade_store import TRADE_INTENTS_TABLE_NAME


MISSING_EXECUTION_STATS_RECONCILE_LIMIT = 50
MISSING_EXECUTION_STATS_MAX_AGE_SECONDS = 2 * 24 * 60 * 60


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _safe_float_or_none(value):
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _event_ts_expr() -> str:
    return "COALESCE(finished_at_ts, updated_at_ts, sent_at_ts, created_at_ts)"


def read_trade_intents_missing_execution_stats(
        conn,
        *,
        limit: int = MISSING_EXECUTION_STATS_RECONCILE_LIMIT,
        max_age_seconds: int = MISSING_EXECUTION_STATS_MAX_AGE_SECONDS,
        now_ts: int | None = None,
) -> list[dict[str, Any]]:
    now_ts = int(time.time() if now_ts is None else now_ts)
    max_age_seconds = int(max_age_seconds)
    min_event_ts = now_ts - max_age_seconds if max_age_seconds > 0 else 0
    event_ts = _event_ts_expr()

    rows = conn.execute(
        f"""
        SELECT
            trade_intent_id,
            instrument_code,
            intent_source,
            action,
            order_ref,
            order_id,
            avg_fill_price,
            total_commission,
            realized_pnl,
            {event_ts} AS event_ts
        FROM {TRADE_INTENTS_TABLE_NAME}
        WHERE status = 'EXECUTED'
          AND order_id IS NOT NULL
          AND (
                total_commission IS NULL
             OR realized_pnl IS NULL
          )
          AND {event_ts} >= ?
        ORDER BY event_ts ASC, trade_intent_id ASC
        LIMIT ?
        """,
        (
            int(min_event_ts),
            int(limit),
        ),
    ).fetchall()

    return [
        {
            "trade_intent_id": int(row[0]),
            "instrument_code": str(row[1]),
            "intent_source": str(row[2]),
            "action": str(row[3]),
            "order_ref": "" if row[4] is None else str(row[4]),
            "order_id": int(row[5]),
            "avg_fill_price": _safe_float_or_none(row[6]),
            "total_commission": _safe_float_or_none(row[7]),
            "realized_pnl": _safe_float_or_none(row[8]),
            "event_ts": int(row[9]),
        }
        for row in rows
    ]


def _fill_exec_id(fill) -> str:
    execution = getattr(fill, "execution", None)

    if execution is None:
        return ""

    return str(getattr(execution, "execId", "") or "").strip()


def _fill_order_id(fill) -> int:
    execution = getattr(fill, "execution", None)

    if execution is None:
        return 0

    return _safe_int(getattr(execution, "orderId", 0) or 0)


def _fill_dedupe_key(fill) -> str:
    exec_id = _fill_exec_id(fill)

    if exec_id:
        return f"exec:{exec_id}"

    execution = getattr(fill, "execution", None)

    if execution is None:
        return f"obj:{id(fill)}"

    return (
        "fallback:"
        f"{getattr(execution, 'orderId', '')}:"
        f"{getattr(execution, 'time', '')}:"
        f"{getattr(execution, 'shares', '')}:"
        f"{getattr(execution, 'price', '')}:"
        f"{id(fill)}"
    )


def _append_unique_fill(result: list, seen: set[str], fill) -> None:
    execution = getattr(fill, "execution", None)

    if execution is None:
        return

    key = _fill_dedupe_key(fill)

    if key in seen:
        return

    seen.add(key)
    result.append(fill)


def collect_known_ib_fills_for_order(ib, *, order_id: int) -> list:
    expected_order_id = int(order_id)
    result: list = []
    seen: set[str] = set()

    fills_method = getattr(ib, "fills", None)

    if fills_method is not None:
        try:
            fills = list(fills_method() or [])
        except Exception:
            fills = []

        for fill in fills:
            if _fill_order_id(fill) != expected_order_id:
                continue

            _append_unique_fill(result, seen, fill)

    for method_name in ("trades", "openTrades"):
        method = getattr(ib, method_name, None)

        if method is None:
            continue

        try:
            trades = list(method() or [])
        except Exception:
            continue

        for trade in trades:
            order = getattr(trade, "order", None)

            if order is None:
                continue

            trade_order_id = _safe_int(getattr(order, "orderId", 0) or 0)

            if trade_order_id != expected_order_id:
                continue

            for fill in list(getattr(trade, "fills", []) or []):
                _append_unique_fill(result, seen, fill)

    return result


def write_trade_intent_execution_stats_backfill(
        conn,
        *,
        trade_intent_id: int,
        avg_fill_price: float | None,
        total_commission: float | None,
        realized_pnl: float | None,
) -> int:
    now_ts = int(time.time())
    before = conn.total_changes

    conn.execute(
        f"""
        UPDATE {TRADE_INTENTS_TABLE_NAME}
        SET
            avg_fill_price = COALESCE(avg_fill_price, ?),
            total_commission = COALESCE(total_commission, ?),
            realized_pnl = COALESCE(realized_pnl, ?),
            updated_at_ts = ?
        WHERE trade_intent_id = ?
          AND status = 'EXECUTED'
          AND (
                total_commission IS NULL
             OR realized_pnl IS NULL
             OR avg_fill_price IS NULL
          )
        """,
        (
            avg_fill_price,
            total_commission,
            realized_pnl,
            now_ts,
            int(trade_intent_id),
        ),
    )

    return int(conn.total_changes - before)


def _base_event(intent: dict[str, Any], *, event: str) -> dict[str, Any]:
    return {
        "event": str(event),
        "trade_intent_id": int(intent["trade_intent_id"]),
        "instrument_code": str(intent["instrument_code"]),
        "intent_source": str(intent["intent_source"]),
        "action": str(intent["action"]),
        "order_ref": str(intent["order_ref"]),
        "order_id": int(intent["order_id"]),
        "event_ts": int(intent["event_ts"]),
    }


def _format_optional_price(value) -> str:
    if value is None:
        return "pending"

    return f"{float(value):.2f}"


def _format_optional_money(value, *, signed: bool = False) -> str:
    if value is None:
        return "pending"

    number = float(value)

    if signed:
        return f"{number:+.2f}"

    return f"{number:.2f}"


def format_backfilled_execution_stats_message(event: dict[str, Any]) -> str:
    return (
        "♻️ Execution stats backfilled\n"
        f"instrument: {event['instrument_code']}\n"
        f"trade_intent_id: {event['trade_intent_id']}\n"
        f"intent_source: {event['intent_source']}\n"
        f"action: {event['action']}\n"
        f"order_id: {event['order_id']}\n"
        f"avg_fill: {_format_optional_price(event.get('avg_fill_price'))}\n"
        f"commission: {_format_optional_money(event.get('total_commission'))}\n"
        f"realized_pnl: {_format_optional_money(event.get('realized_pnl'), signed=True)}"
    )


async def reconcile_missing_execution_stats_once(
        *,
        order_service,
        limit: int = MISSING_EXECUTION_STATS_RECONCILE_LIMIT,
        max_age_seconds: int = MISSING_EXECUTION_STATS_MAX_AGE_SECONDS,
) -> list[dict[str, Any]]:
    conn = get_trade_db_connection()

    try:
        initialize_execution_db(conn)

        pending_intents = read_trade_intents_missing_execution_stats(
            conn,
            limit=int(limit),
            max_age_seconds=int(max_age_seconds),
        )

        if not pending_intents:
            return []

        await refresh_ib_executions_if_possible(order_service.ib)

        events: list[dict[str, Any]] = []

        for intent in pending_intents:
            fills = collect_known_ib_fills_for_order(
                order_service.ib,
                order_id=int(intent["order_id"]),
            )

            if not fills:
                events.append(_base_event(intent, event="NO_FILLS"))
                continue

            avg_fill_price, total_commission, realized_pnl, filled_qty = collect_trade_fill_statistics(
                SimpleNamespace(fills=fills),
            )

            if not are_commission_reports_final_for_fills(fills):
                if avg_fill_price is not None and intent.get("avg_fill_price") is None:
                    write_trade_intent_execution_stats_backfill(
                        conn,
                        trade_intent_id=int(intent["trade_intent_id"]),
                        avg_fill_price=avg_fill_price,
                        total_commission=None,
                        realized_pnl=None,
                    )
                    conn.commit()

                event = _base_event(intent, event="WAITING_COMMISSION")
                event.update({
                    "avg_fill_price": avg_fill_price,
                    "total_commission": total_commission,
                    "realized_pnl": realized_pnl,
                    "filled_qty": filled_qty,
                })
                events.append(event)
                continue

            changes = write_trade_intent_execution_stats_backfill(
                conn,
                trade_intent_id=int(intent["trade_intent_id"]),
                avg_fill_price=avg_fill_price,
                total_commission=total_commission,
                realized_pnl=realized_pnl,
            )
            conn.commit()

            event = _base_event(
                intent,
                event="BACKFILLED" if changes > 0 else "ALREADY_FINAL",
            )
            event.update({
                "avg_fill_price": avg_fill_price,
                "total_commission": total_commission,
                "realized_pnl": realized_pnl,
                "filled_qty": filled_qty,
                "updated_rows": changes,
            })
            events.append(event)

        return events

    finally:
        conn.close()