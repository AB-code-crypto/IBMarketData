from __future__ import annotations

import time
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from ib_execution.execution_logic import (
    are_commission_reports_final_for_fills,
    collect_trade_fill_statistics,
    refresh_ib_executions_if_possible,
)
from ib_execution.execution_models import ExecutionResult, ExecutionStatus, TradeIntent
from ib_execution.execution_stats_reconciliation import collect_known_ib_fills_for_order
from ib_execution.protective_order_reconciliation import refresh_ib_open_orders_if_possible
from ib_execution.execution_store import (
    get_trade_db_connection,
    initialize_execution_db,
    write_trade_intent_execution_result,
)
from ib_trader.trade_store import TRADE_INTENTS_TABLE_NAME


UNCERTAIN_EXECUTION_LIMIT = 20

LIVE_ORDER_STATUSES = {
    "ApiPending",
    "PendingSubmit",
    "PreSubmitted",
    "Submitted",
    "PendingCancel",
}

CANCELLED_ORDER_STATUSES = {
    "Cancelled",
    "ApiCancelled",
}

FAILED_ORDER_STATUSES = {
    "Inactive",
    "Rejected",
}


@dataclass(frozen=True)
class UncertainExecutionEvent:
    event: str
    message: str
    intent: TradeIntent
    result: ExecutionResult | None = None
    needs_protection: bool = False
    log_level: str = "INFO"


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _row_to_intent(row) -> TradeIntent:
    return TradeIntent(
        trade_intent_id=int(row[0]),
        source_signal_id=int(row[1]),
        instrument_code=str(row[2]),
        order_ref=str(row[3] or ""),
        action=str(row[4]),
        target_side=str(row[5]),
        target_qty=float(row[6]),
        position_before_side=str(row[7]),
        position_before_qty=float(row[8]),
        order_type=str(row[9]).upper(),
        limit_price=None if row[10] is None else float(row[10]),
        limit_offset_points=None if row[11] is None else float(row[11]),
        ttl_seconds=None if row[12] is None else int(row[12]),
        status=str(row[13]).upper(),
        created_at_ts=int(row[14]),
    )


def read_uncertain_execution_rows(
        conn,
        *,
        limit: int = UNCERTAIN_EXECUTION_LIMIT,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT
            trade_intent_id,
            source_signal_id,
            instrument_code,
            order_ref,
            action,
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
            order_id,
            order_action,
            order_quantity,
            error_text
        FROM {TRADE_INTENTS_TABLE_NAME}
        WHERE status IN ('SENDING', 'ACCEPTED', 'RECONCILING')
        ORDER BY COALESCE(sent_at_ts, updated_at_ts, created_at_ts), trade_intent_id
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()

    return [
        {
            "intent": _row_to_intent(row),
            "order_id": None if row[15] is None else int(row[15]),
            "order_action": None if row[16] is None else str(row[16]).upper(),
            "order_quantity": None if row[17] is None else int(row[17]),
            "error_text": None if row[18] is None else str(row[18]),
        }
        for row in rows
    ]


def find_known_trade(
        ib,
        *,
        order_id: int | None,
        order_ref: str,
):
    expected_order_id = None if order_id is None else int(order_id)
    expected_order_ref = str(order_ref)

    for method_name in ("openTrades", "trades"):
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
            trade_order_ref = str(getattr(order, "orderRef", "") or "")

            if expected_order_id is not None and trade_order_id == expected_order_id:
                return trade
            if expected_order_ref and trade_order_ref == expected_order_ref:
                return trade

    return None


def persist_resolved_order_identity(
        conn,
        *,
        trade_intent_id: int,
        order_id: int,
        order_action: str | None,
        order_quantity: int | None,
) -> None:
    conn.execute(
        f"""
        UPDATE {TRADE_INTENTS_TABLE_NAME}
        SET
            order_id = COALESCE(order_id, ?),
            order_action = COALESCE(order_action, ?),
            order_quantity = COALESCE(order_quantity, ?),
            status = CASE
                WHEN status = 'SENDING' THEN 'RECONCILING'
                ELSE status
            END,
            updated_at_ts = ?
        WHERE trade_intent_id = ?
        """,
        (
            int(order_id),
            None if order_action is None else str(order_action).upper(),
            None if order_quantity is None else int(order_quantity),
            int(time.time()),
            int(trade_intent_id),
        ),
    )


def find_known_order_status(
        ib,
        *,
        order_id: int,
        order_ref: str,
) -> str:
    expected_order_id = int(order_id)
    expected_order_ref = str(order_ref)

    # openTrades() is the source of truth for a live broker order.
    try:
        open_trades = list(ib.openTrades() or [])
    except Exception:
        open_trades = []

    for trade in open_trades:
        order = getattr(trade, "order", None)
        if order is None:
            continue

        trade_order_id = _safe_int(getattr(order, "orderId", 0) or 0)
        trade_order_ref = str(getattr(order, "orderRef", "") or "")

        if (
                trade_order_id == expected_order_id
                or (
                    expected_order_ref
                    and trade_order_ref == expected_order_ref
                )
        ):
            return str(
                getattr(
                    getattr(trade, "orderStatus", None),
                    "status",
                    "",
                ) or ""
            )

    # Historical trades() is allowed to prove only a terminal state.
    # A stale historical Submitted status must not keep an intent ACCEPTED forever.
    terminal_statuses = {
        "Filled",
        "Cancelled",
        "ApiCancelled",
        "Inactive",
        "Rejected",
    }

    try:
        historical_trades = list(ib.trades() or [])
    except Exception:
        historical_trades = []

    for trade in historical_trades:
        order = getattr(trade, "order", None)
        if order is None:
            continue

        trade_order_id = _safe_int(getattr(order, "orderId", 0) or 0)
        trade_order_ref = str(getattr(order, "orderRef", "") or "")

        if not (
                trade_order_id == expected_order_id
                or (
                    expected_order_ref
                    and trade_order_ref == expected_order_ref
                )
        ):
            continue

        status = str(
            getattr(
                getattr(trade, "orderStatus", None),
                "status",
                "",
            ) or ""
        )
        if status in terminal_statuses:
            return status

    return ""


def write_nonterminal_execution_state(
        conn,
        *,
        trade_intent_id: int,
        status: ExecutionStatus,
        error_text: str | None,
) -> int:
    if status not in {
        ExecutionStatus.ACCEPTED,
        ExecutionStatus.RECONCILING,
    }:
        raise ValueError(f"Unsupported nonterminal status: {status}")

    now_ts = int(time.time())
    before = conn.total_changes

    conn.execute(
        f"""
        UPDATE {TRADE_INTENTS_TABLE_NAME}
        SET
            status = ?,
            error_text = ?,
            updated_at_ts = ?,
            finished_at_ts = NULL
        WHERE trade_intent_id = ?
          AND status IN ('SENDING', 'ACCEPTED', 'RECONCILING')
        """,
        (
            status.value,
            error_text,
            now_ts,
            int(trade_intent_id),
        ),
    )

    return int(conn.total_changes - before)


def build_terminal_result(
        *,
        row: dict[str, Any],
        status: ExecutionStatus,
        avg_fill_price: float | None = None,
        total_commission: float | None = None,
        realized_pnl: float | None = None,
        error_text: str | None = None,
) -> ExecutionResult:
    return ExecutionResult(
        trade_intent_id=int(row["intent"].trade_intent_id),
        order_id=int(row["order_id"]),
        order_action=row["order_action"],
        order_quantity=row["order_quantity"],
        status=status,
        avg_fill_price=avg_fill_price,
        total_commission=total_commission,
        realized_pnl=realized_pnl,
        error_text=error_text,
    )


async def reconcile_uncertain_trade_intents_once(
        *,
        order_service,
        limit: int = UNCERTAIN_EXECUTION_LIMIT,
) -> list[UncertainExecutionEvent]:
    conn = get_trade_db_connection()

    try:
        initialize_execution_db(conn)
        pending_rows = read_uncertain_execution_rows(
            conn,
            limit=int(limit),
        )

        if not pending_rows:
            return []

        executions_refreshed = await refresh_ib_executions_if_possible(
            order_service.ib
        )
        open_orders_refreshed, _ = await refresh_ib_open_orders_if_possible(
            order_service
        )
        events: list[UncertainExecutionEvent] = []

        for row in pending_rows:
            intent = row["intent"]
            known_trade = find_known_trade(
                order_service.ib,
                order_id=row["order_id"],
                order_ref=intent.order_ref,
            )

            if row["order_id"] is None and known_trade is not None:
                known_order = getattr(known_trade, "order", None)
                resolved_order_id = _safe_int(
                    getattr(known_order, "orderId", 0) or 0
                )
                resolved_order_action = str(
                    getattr(known_order, "action", "") or ""
                ).upper() or row["order_action"]
                resolved_order_quantity = _safe_int(
                    getattr(known_order, "totalQuantity", 0) or 0
                ) or row["order_quantity"]

                if resolved_order_id > 0:
                    persist_resolved_order_identity(
                        conn,
                        trade_intent_id=intent.trade_intent_id,
                        order_id=resolved_order_id,
                        order_action=resolved_order_action,
                        order_quantity=resolved_order_quantity,
                    )
                    conn.commit()
                    row["order_id"] = resolved_order_id
                    row["order_action"] = resolved_order_action
                    row["order_quantity"] = resolved_order_quantity

            if row["order_id"] is None:
                error_text = (
                    "broker submission identity is still unknown; "
                    f"order_ref={intent.order_ref}; "
                    f"executions_refreshed={executions_refreshed}; "
                    f"open_orders_refreshed={open_orders_refreshed}"
                )
                changed = write_nonterminal_execution_state(
                    conn,
                    trade_intent_id=intent.trade_intent_id,
                    status=ExecutionStatus.RECONCILING,
                    error_text=error_text,
                )
                conn.commit()

                if changed > 0 and str(intent.status).upper() != "RECONCILING":
                    events.append(
                        UncertainExecutionEvent(
                            event="RECONCILING_WITHOUT_ORDER_ID",
                            message=(
                                f"{intent.instrument_code}: trade_intent is RECONCILING by order_ref: "
                                f"trade_intent_id={intent.trade_intent_id}, "
                                f"order_ref={intent.order_ref}"
                            ),
                            intent=intent,
                            log_level="WARNING",
                        )
                    )
                continue

            order_id = int(row["order_id"])
            expected_qty = int(row["order_quantity"] or 0)

            trade_fills = list(getattr(known_trade, "fills", []) or []) if known_trade is not None else []
            fills = trade_fills or collect_known_ib_fills_for_order(
                order_service.ib,
                order_id=order_id,
            )

            if fills:
                (
                    avg_fill_price,
                    total_commission,
                    realized_pnl,
                    filled_qty,
                ) = collect_trade_fill_statistics(
                    SimpleNamespace(fills=fills),
                )

                if expected_qty > 0 and filled_qty >= float(expected_qty):
                    if not are_commission_reports_final_for_fills(fills):
                        total_commission = None
                        realized_pnl = None

                    result = build_terminal_result(
                        row=row,
                        status=ExecutionStatus.EXECUTED,
                        avg_fill_price=avg_fill_price,
                        total_commission=total_commission,
                        realized_pnl=realized_pnl,
                        error_text=None,
                    )
                    write_trade_intent_execution_result(conn, result=result)
                    conn.commit()

                    events.append(
                        UncertainExecutionEvent(
                            event="EXECUTED_RECOVERED",
                            message=(
                                f"{intent.instrument_code}: uncertain execution recovered as EXECUTED: "
                                f"trade_intent_id={intent.trade_intent_id}, "
                                f"order_id={order_id}, filled_qty={filled_qty:g}, "
                                f"avg_fill={avg_fill_price}"
                            ),
                            intent=intent,
                            result=result,
                            needs_protection=(
                                str(intent.action).upper()
                                in {"OPEN_POSITION", "REVERSE_POSITION"}
                                and str(intent.target_side).upper()
                                in {"LONG", "SHORT"}
                            ),
                        )
                    )
                    continue

                if filled_qty > 0.0:
                    error_text = (
                        "partial fill requires broker reconciliation; "
                        f"filled_qty={filled_qty:g}; expected_qty={expected_qty}; "
                        f"executions_refreshed={executions_refreshed}"
                    )
                    changed = write_nonterminal_execution_state(
                        conn,
                        trade_intent_id=intent.trade_intent_id,
                        status=ExecutionStatus.RECONCILING,
                        error_text=error_text,
                    )
                    conn.commit()

                    if changed > 0 and row["error_text"] != error_text:
                        events.append(
                            UncertainExecutionEvent(
                                event="PARTIAL_FILL_RECONCILING",
                                message=(
                                    f"{intent.instrument_code}: partial fill remains RECONCILING: "
                                    f"trade_intent_id={intent.trade_intent_id}, "
                                    f"order_id={order_id}, filled_qty={filled_qty:g}, "
                                    f"expected_qty={expected_qty}"
                                ),
                                intent=intent,
                                log_level="WARNING",
                            )
                        )
                    continue

            broker_status = find_known_order_status(
                order_service.ib,
                order_id=order_id,
                order_ref=intent.order_ref,
            )

            if broker_status in LIVE_ORDER_STATUSES:
                write_nonterminal_execution_state(
                    conn,
                    trade_intent_id=intent.trade_intent_id,
                    status=ExecutionStatus.ACCEPTED,
                    error_text=f"broker order is still live: status={broker_status}",
                )
                conn.commit()
                continue

            if broker_status in CANCELLED_ORDER_STATUSES:
                result = build_terminal_result(
                    row=row,
                    status=ExecutionStatus.CANCELLED,
                    error_text=f"reconciled broker terminal status={broker_status}",
                )
                write_trade_intent_execution_result(conn, result=result)
                conn.commit()
                events.append(
                    UncertainExecutionEvent(
                        event="CANCELLED_RECOVERED",
                        message=(
                            f"{intent.instrument_code}: uncertain execution recovered as CANCELLED: "
                            f"trade_intent_id={intent.trade_intent_id}, "
                            f"order_id={order_id}, broker_status={broker_status}"
                        ),
                        intent=intent,
                        result=result,
                        log_level="WARNING",
                    )
                )
                continue

            if broker_status in FAILED_ORDER_STATUSES:
                result = build_terminal_result(
                    row=row,
                    status=ExecutionStatus.FAILED,
                    error_text=f"reconciled broker terminal status={broker_status}",
                )
                write_trade_intent_execution_result(conn, result=result)
                conn.commit()
                events.append(
                    UncertainExecutionEvent(
                        event="FAILED_RECOVERED",
                        message=(
                            f"{intent.instrument_code}: uncertain execution recovered as FAILED: "
                            f"trade_intent_id={intent.trade_intent_id}, "
                            f"order_id={order_id}, broker_status={broker_status}"
                        ),
                        intent=intent,
                        result=result,
                        log_level="WARNING",
                    )
                )
                continue

            error_text = (
                "broker result is still unknown; "
                f"broker_status={broker_status or 'missing'}; "
                f"executions_refreshed={executions_refreshed}; "
                f"open_orders_refreshed={open_orders_refreshed}"
            )
            changed = write_nonterminal_execution_state(
                conn,
                trade_intent_id=intent.trade_intent_id,
                status=ExecutionStatus.RECONCILING,
                error_text=error_text,
            )
            conn.commit()

            if changed > 0 and str(intent.status).upper() != "RECONCILING":
                events.append(
                    UncertainExecutionEvent(
                        event="RECONCILING_STARTED",
                        message=(
                            f"{intent.instrument_code}: trade_intent moved to RECONCILING: "
                            f"trade_intent_id={intent.trade_intent_id}, "
                            f"order_id={order_id}, "
                            f"broker_status={broker_status or 'missing'}"
                        ),
                        intent=intent,
                        log_level="WARNING",
                    )
                )

        return events

    finally:
        conn.close()
