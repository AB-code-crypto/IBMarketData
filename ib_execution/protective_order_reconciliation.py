from __future__ import annotations

import asyncio
import inspect
import time
from types import SimpleNamespace
from typing import Any

from ib_execution.broker_state_service import get_broker_state_service
from ib_execution.execution_logic import (
    are_commission_reports_final_for_fills,
    collect_trade_fill_statistics,
    should_finalize_with_pending_commission,
)
from ib_execution.protective_order_store import (
    PROTECTIVE_ORDERS_TABLE_NAME,
    PROTECTIVE_ORDER_ROLE_STOP_LOSS,
    PROTECTIVE_ORDER_ROLE_TAKE_PROFIT,
    PROTECTIVE_ORDER_STATUS_ACTIVE,
    PROTECTIVE_ORDER_STATUS_CANCELLED,
    PROTECTIVE_ORDER_STATUS_FILLED,
    PROTECTIVE_ORDER_STATUS_FAILED,
    PROTECTIVE_ORDER_STATUS_UNPROTECTED,
    get_trade_db_connection,
    initialize_protective_order_db,
    read_active_protective_orders,
)
from ib_position_sync.position_store import sync_broker_positions_once
from ib_trader.trade_store import TRADE_INTENTS_TABLE_NAME, build_time_text_fields_from_ts


PROTECTIVE_CLOSE_SOURCE_SIGNAL_ID_BASES = {
    PROTECTIVE_ORDER_ROLE_TAKE_PROFIT: -400_000_000,
    PROTECTIVE_ORDER_ROLE_STOP_LOSS: -500_000_000,
}

PROTECTIVE_INTENT_SOURCES = {
    PROTECTIVE_ORDER_ROLE_TAKE_PROFIT: "TAKE_PROFIT",
    PROTECTIVE_ORDER_ROLE_STOP_LOSS: "STOP_LOSS",
}

PROTECTIVE_REASONS = {
    PROTECTIVE_ORDER_ROLE_TAKE_PROFIT: "take_profit_filled",
    PROTECTIVE_ORDER_ROLE_STOP_LOSS: "stop_loss_filled",
}

OPEN_ORDER_REFRESH_SETTLE_SECONDS = 0.25
BROKER_POSITION_SYNC_TIMEOUT_SECONDS = 8.0

PROTECTIVE_LIVE_ORDER_STATUSES = {
    "ApiPending",
    "PendingSubmit",
    "PreSubmitted",
    "Submitted",
    "PendingCancel",
}


def normalize_protective_role(role: str) -> str:
    return str(role).upper()


async def refresh_ib_executions_if_possible(order_service) -> bool:
    return await order_service.broker_state.refresh_executions(
        force=False,
    )


async def refresh_ib_open_orders_if_possible(order_service) -> tuple[bool, str | None]:
    return await order_service.broker_state.refresh_open_orders(
        force=True,
    )


def iter_known_ib_trades(order_service):
    seen_order_ids: set[int] = set()

    for method_name in ("trades", "openTrades"):
        method = getattr(order_service.ib, method_name, None)

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

            order_id = int(getattr(order, "orderId", 0) or 0)

            if order_id in seen_order_ids:
                continue

            seen_order_ids.add(order_id)
            yield trade


def find_ib_trade_for_protective_order(order_service, protective_order: dict[str, Any]):
    order_id = int(protective_order["order_id"])
    order_ref = str(protective_order["order_ref"])

    for trade in iter_known_ib_trades(order_service):
        order = getattr(trade, "order", None)

        if order is None:
            continue

        trade_order_id = int(getattr(order, "orderId", 0) or 0)
        trade_order_ref = str(getattr(order, "orderRef", "") or "")

        if trade_order_id == order_id:
            return trade

        if trade_order_ref and trade_order_ref == order_ref:
            return trade

    return None


def collect_ib_fills_for_order(order_service, *, order_id: int) -> list:
    fills_method = getattr(order_service.ib, "fills", None)

    if fills_method is None:
        return []

    try:
        fills = list(fills_method() or [])
    except Exception:
        return []

    result = []
    order_id = int(order_id)

    for fill in fills:
        execution = getattr(fill, "execution", None)

        if execution is None:
            continue

        fill_order_id = int(getattr(execution, "orderId", 0) or 0)

        if fill_order_id == order_id:
            result.append(fill)

    return result


def latest_fill_ts_from_fills(fills: list) -> int | None:
    latest_ts = None

    for fill in list(fills or []):
        execution = getattr(fill, "execution", None)

        if execution is None:
            continue

        fill_time = getattr(execution, "time", None)

        if fill_time is None:
            continue

        try:
            fill_ts = int(fill_time.timestamp())
        except Exception:
            continue

        latest_ts = fill_ts if latest_ts is None else max(latest_ts, fill_ts)

    return latest_ts


def collect_protective_order_fill_statistics(order_service, protective_order: dict[str, Any]) -> dict[str, Any]:
    expected_qty = float(protective_order["order_quantity"])
    order_id = int(protective_order["order_id"])
    trade = find_ib_trade_for_protective_order(order_service, protective_order)

    if trade is not None:
        avg_fill_price, total_commission, realized_pnl, filled_qty = collect_trade_fill_statistics(trade)
        status = str(getattr(getattr(trade, "orderStatus", None), "status", "") or "")
        trade_fills = list(getattr(trade, "fills", []) or [])
        filled_at_ts = latest_fill_ts_from_fills(trade_fills) or int(time.time())

        if filled_qty >= expected_qty:
            if not are_commission_reports_final_for_fills(trade_fills):
                if should_finalize_with_pending_commission(filled_at_ts):
                    return {
                        "filled": True,
                        "cancelled": False,
                        "commission_pending": True,
                        "status": status,
                        "avg_fill_price": avg_fill_price,
                        "total_commission": total_commission,
                        "realized_pnl": realized_pnl,
                        "filled_qty": filled_qty,
                        "filled_at_ts": filled_at_ts,
                    }

                return {
                    "filled": False,
                    "cancelled": False,
                    "waiting_commission": True,
                    "status": status,
                    "avg_fill_price": avg_fill_price,
                    "total_commission": total_commission,
                    "realized_pnl": realized_pnl,
                    "filled_qty": filled_qty,
                    "filled_at_ts": filled_at_ts,
                }

            return {
                "filled": True,
                "cancelled": False,
                "status": status,
                "avg_fill_price": avg_fill_price,
                "total_commission": total_commission,
                "realized_pnl": realized_pnl,
                "filled_qty": filled_qty,
                "filled_at_ts": filled_at_ts,
            }

        if status in {"Cancelled", "ApiCancelled"}:
            return {
                "filled": False,
                "cancelled": True,
                "status": status,
                "avg_fill_price": avg_fill_price,
                "total_commission": total_commission,
                "realized_pnl": realized_pnl,
                "filled_qty": filled_qty,
                "filled_at_ts": filled_at_ts,
            }

        if status in {"Inactive", "Rejected"}:
            return {
                "filled": False,
                "cancelled": False,
                "rejected": True,
                "status": status,
                "avg_fill_price": avg_fill_price,
                "total_commission": total_commission,
                "realized_pnl": realized_pnl,
                "filled_qty": filled_qty,
                "filled_at_ts": filled_at_ts,
            }

    fills = collect_ib_fills_for_order(order_service, order_id=order_id)

    if fills:
        avg_fill_price, total_commission, realized_pnl, filled_qty = collect_trade_fill_statistics(
            SimpleNamespace(fills=fills),
        )
        filled_at_ts = latest_fill_ts_from_fills(fills) or int(time.time())

        if filled_qty >= expected_qty:
            if not are_commission_reports_final_for_fills(fills):
                if should_finalize_with_pending_commission(filled_at_ts):
                    return {
                        "filled": True,
                        "cancelled": False,
                        "commission_pending": True,
                        "status": "Filled",
                        "avg_fill_price": avg_fill_price,
                        "total_commission": total_commission,
                        "realized_pnl": realized_pnl,
                        "filled_qty": filled_qty,
                        "filled_at_ts": filled_at_ts,
                    }

                return {
                    "filled": False,
                    "cancelled": False,
                    "waiting_commission": True,
                    "status": "Filled",
                    "avg_fill_price": avg_fill_price,
                    "total_commission": total_commission,
                    "realized_pnl": realized_pnl,
                    "filled_qty": filled_qty,
                    "filled_at_ts": filled_at_ts,
                }

            return {
                "filled": True,
                "cancelled": False,
                "status": "Filled",
                "avg_fill_price": avg_fill_price,
                "total_commission": total_commission,
                "realized_pnl": realized_pnl,
                "filled_qty": filled_qty,
                "filled_at_ts": filled_at_ts,
            }

    return {
        "filled": False,
        "cancelled": False,
        "status": "",
        "avg_fill_price": None,
        "total_commission": None,
        "realized_pnl": None,
        "filled_qty": 0.0,
        "filled_at_ts": int(time.time()),
    }


def build_protective_close_source_signal_id(*, role: str, order_id: int) -> int:
    role_value = normalize_protective_role(role)
    base = PROTECTIVE_CLOSE_SOURCE_SIGNAL_ID_BASES.get(role_value)

    if base is None:
        raise ValueError(f"Unknown protective order role: {role!r}")

    return int(base) - int(order_id)


def mark_protective_cancelled(conn, *, protective_order: dict[str, Any], status_text: str) -> None:
    now_ts = int(time.time())

    conn.execute(
        f"""
        UPDATE {PROTECTIVE_ORDERS_TABLE_NAME}
        SET
            status = ?,
            error_text = ?,
            updated_at_ts = ?,
            finished_at_ts = ?
        WHERE order_id = ?
        """,
        (
            PROTECTIVE_ORDER_STATUS_CANCELLED,
            f"broker terminal status={status_text}",
            now_ts,
            now_ts,
            int(protective_order["order_id"]),
        ),
    )


def mark_oca_siblings_cancelled_after_fill(
        conn,
        *,
        protective_order: dict[str, Any],
        filled_order_id: int,
        filled_at_ts: int,
) -> list[dict[str, Any]]:
    # Если TP/SL входит в OCA-пару и один sibling filled, второй ACTIVE sibling локально уже не активен.
    oca_group = protective_order.get("oca_group")

    if not oca_group:
        return []

    parent_trade_intent_id = int(protective_order["parent_trade_intent_id"])
    filled_order_id = int(filled_order_id)
    now_ts = int(time.time())
    finished_at_ts = int(filled_at_ts)
    error_text = f"cancelled by OCA after sibling filled: order_id={filled_order_id}"

    sibling_rows = conn.execute(
        f"""
        SELECT
            instrument_code,
            parent_trade_intent_id,
            role,
            order_id
        FROM {PROTECTIVE_ORDERS_TABLE_NAME}
        WHERE status = ?
          AND oca_group = ?
          AND parent_trade_intent_id = ?
          AND order_id != ?
        """,
        (
            PROTECTIVE_ORDER_STATUS_ACTIVE,
            str(oca_group),
            parent_trade_intent_id,
            filled_order_id,
        ),
    ).fetchall()

    if not sibling_rows:
        return []

    conn.execute(
        f"""
        UPDATE {PROTECTIVE_ORDERS_TABLE_NAME}
        SET
            status = ?,
            error_text = ?,
            updated_at_ts = ?,
            finished_at_ts = ?
        WHERE status = ?
          AND oca_group = ?
          AND parent_trade_intent_id = ?
          AND order_id != ?
        """,
        (
            PROTECTIVE_ORDER_STATUS_CANCELLED,
            error_text,
            now_ts,
            finished_at_ts,
            PROTECTIVE_ORDER_STATUS_ACTIVE,
            str(oca_group),
            parent_trade_intent_id,
            filled_order_id,
        ),
    )

    return [
        {
            "event": "CANCELLED",
            "role": str(row[2]),
            "instrument_code": str(row[0]),
            "order_id": int(row[3]),
            "parent_trade_intent_id": int(row[1]),
            "synthetic_trade_intent_id": None,
            "filled_qty": 0.0,
            "avg_fill_price": None,
            "realized_pnl": None,
        }
        for row in sibling_rows
    ]


def get_cached_broker_order_status(order_service, protective_order: dict[str, Any]) -> str:
    # Для live-проверки используем только openTrades(). Исторический trades()
    # может хранить старый Submitted-status уже несуществующего ордера.
    try:
        trades = list(order_service.ib.openTrades() or [])
    except Exception:
        return ""

    order_id = int(protective_order["order_id"])
    order_ref = str(protective_order.get("order_ref", "") or "")

    for trade in trades:
        order = getattr(trade, "order", None)
        if order is None:
            continue

        trade_order_id = int(getattr(order, "orderId", 0) or 0)
        trade_order_ref = str(getattr(order, "orderRef", "") or "")

        if trade_order_id == order_id or (order_ref and trade_order_ref == order_ref):
            return str(getattr(getattr(trade, "orderStatus", None), "status", "") or "")

    return ""


def is_cached_broker_order_live(order_service, protective_order: dict[str, Any]) -> bool:
    return get_cached_broker_order_status(order_service, protective_order) in PROTECTIVE_LIVE_ORDER_STATUSES


def mark_orphan_protective_cancelled(
        conn,
        *,
        protective_order: dict[str, Any],
        reason: str,
) -> None:
    now_ts = int(time.time())

    conn.execute(
        f"""
        UPDATE {PROTECTIVE_ORDERS_TABLE_NAME}
        SET
            status = ?,
            error_text = ?,
            updated_at_ts = ?,
            finished_at_ts = ?
        WHERE order_id = ?
          AND status IN (?, ?)
        """,
        (
            PROTECTIVE_ORDER_STATUS_CANCELLED,
            str(reason),
            now_ts,
            now_ts,
            int(protective_order["order_id"]),
            PROTECTIVE_ORDER_STATUS_ACTIVE,
            PROTECTIVE_ORDER_STATUS_UNPROTECTED,
        ),
    )


def mark_protective_runtime_state(
        conn,
        *,
        protective_order: dict[str, Any],
        status: str,
        reason: str,
) -> None:
    now_ts = int(time.time())
    status_value = str(status).upper()
    finished_at_ts = (
        None
        if status_value in {
            PROTECTIVE_ORDER_STATUS_ACTIVE,
            PROTECTIVE_ORDER_STATUS_UNPROTECTED,
        }
        else now_ts
    )

    conn.execute(
        f"""
        UPDATE {PROTECTIVE_ORDERS_TABLE_NAME}
        SET
            status = ?,
            error_text = ?,
            updated_at_ts = ?,
            finished_at_ts = ?
        WHERE order_id = ?
        """,
        (
            status_value,
            str(reason),
            now_ts,
            finished_at_ts,
            int(protective_order["order_id"]),
        ),
    )


def build_reconciled_event(
        *,
        event: str,
        protective_order: dict[str, Any],
        synthetic_trade_intent_id: int | None = None,
        filled_qty: float = 0.0,
        avg_fill_price: float | None = None,
        realized_pnl: float | None = None,
        requires_market_close: bool = False,
        reason: str | None = None,
) -> dict[str, Any]:
    return {
        "event": str(event).upper(),
        "role": protective_order["role"],
        "instrument_code": protective_order["instrument_code"],
        "order_id": int(protective_order["order_id"]),
        "parent_trade_intent_id": int(protective_order["parent_trade_intent_id"]),
        "synthetic_trade_intent_id": synthetic_trade_intent_id,
        "filled_qty": float(filled_qty),
        "avg_fill_price": avg_fill_price,
        "realized_pnl": realized_pnl,
        "requires_market_close": bool(requires_market_close),
        "reason": reason,
    }


def reconcile_terminal_protective_order(
        conn,
        *,
        protective_order: dict[str, Any],
        statistics: dict[str, Any],
) -> list[dict[str, Any]] | None:
    if statistics["filled"]:
        synthetic_trade_intent_id = create_protective_close_trade_intent(
            conn,
            protective_order=protective_order,
            statistics=statistics,
        )
        sibling_cancelled_events = mark_oca_siblings_cancelled_after_fill(
            conn,
            protective_order=protective_order,
            filled_order_id=int(protective_order["order_id"]),
            filled_at_ts=int(statistics["filled_at_ts"]),
        )
        conn.commit()

        return [
            build_reconciled_event(
                event="FILLED",
                protective_order=protective_order,
                synthetic_trade_intent_id=synthetic_trade_intent_id,
                filled_qty=float(statistics["filled_qty"]),
                avg_fill_price=statistics["avg_fill_price"],
                realized_pnl=statistics["realized_pnl"],
            ),
            *sibling_cancelled_events,
        ]

    if statistics["cancelled"]:
        mark_protective_cancelled(
            conn,
            protective_order=protective_order,
            status_text=str(statistics["status"]),
        )
        conn.commit()

        return [
            build_reconciled_event(
                event="CANCELLED",
                protective_order=protective_order,
                filled_qty=float(statistics["filled_qty"]),
                avg_fill_price=statistics["avg_fill_price"],
                realized_pnl=statistics["realized_pnl"],
            )
        ]

    if statistics.get("rejected"):
        role = normalize_protective_role(protective_order["role"])
        requires_market_close = role == PROTECTIVE_ORDER_ROLE_STOP_LOSS
        target_status = (
            PROTECTIVE_ORDER_STATUS_UNPROTECTED
            if requires_market_close
            else PROTECTIVE_ORDER_STATUS_FAILED
        )
        reason = (
            f"broker protective order rejected/inactive: "
            f"status={statistics.get('status') or 'missing'}"
        )
        mark_protective_runtime_state(
            conn,
            protective_order=protective_order,
            status=target_status,
            reason=reason,
        )
        conn.commit()

        return [
            build_reconciled_event(
                event="UNPROTECTED" if requires_market_close else "FAILED",
                protective_order=protective_order,
                filled_qty=float(statistics.get("filled_qty") or 0.0),
                avg_fill_price=statistics.get("avg_fill_price"),
                realized_pnl=statistics.get("realized_pnl"),
                requires_market_close=requires_market_close,
                reason=reason,
            )
        ]

    return None


def create_protective_close_trade_intent(
        conn,
        *,
        protective_order: dict[str, Any],
        statistics: dict[str, Any],
) -> int | None:
    existing_synthetic_id = protective_order.get("synthetic_trade_intent_id")

    if existing_synthetic_id is not None:
        return int(existing_synthetic_id)

    role = normalize_protective_role(protective_order["role"])
    parent_trade_intent_id = int(protective_order["parent_trade_intent_id"])
    parent = conn.execute(
        f"""
        SELECT
            instrument_code,
            target_side,
            target_qty
        FROM {TRADE_INTENTS_TABLE_NAME}
        WHERE trade_intent_id = ?
        LIMIT 1
        """,
        (parent_trade_intent_id,),
    ).fetchone()

    order_id = int(protective_order["order_id"])
    now_ts = int(time.time())
    filled_at_ts = int(statistics["filled_at_ts"])

    if parent is None:
        conn.execute(
            f"""
            UPDATE {PROTECTIVE_ORDERS_TABLE_NAME}
            SET
                status = ?,
                filled_qty = ?,
                avg_fill_price = ?,
                total_commission = ?,
                realized_pnl = ?,
                filled_at_ts = ?,
                error_text = ?,
                updated_at_ts = ?,
                finished_at_ts = ?
            WHERE order_id = ?
            """,
            (
                PROTECTIVE_ORDER_STATUS_FILLED,
                float(statistics["filled_qty"]),
                statistics["avg_fill_price"],
                statistics["total_commission"],
                statistics["realized_pnl"],
                filled_at_ts,
                f"filled, but parent trade_intent not found: {parent_trade_intent_id}",
                now_ts,
                filled_at_ts,
                order_id,
            ),
        )
        return None

    instrument_code = str(parent[0])
    position_before_side = str(parent[1]).upper()
    position_before_qty = float(parent[2])
    source_signal_id = build_protective_close_source_signal_id(role=role, order_id=order_id)
    signal_time_utc, signal_time_ct, signal_time_msk = build_time_text_fields_from_ts(filled_at_ts)
    intent_source = PROTECTIVE_INTENT_SOURCES.get(role, role)
    reason = PROTECTIVE_REASONS.get(role, f"{role.lower()}_filled")
    price_for_trade_intent = protective_order.get("limit_price") or protective_order.get("stop_price")

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

            order_ref,
            order_id,
            order_action,
            order_quantity,
            avg_fill_price,
            total_commission,
            realized_pnl,
            error_text,

            created_at_ts,
            updated_at_ts,
            sent_at_ts,
            finished_at_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

        ON CONFLICT (
            instrument_code,
            source_signal_id,
            signal_bar_ts,
            action
        ) DO NOTHING
        """,
        (
            source_signal_id,
            instrument_code,
            filled_at_ts,
            signal_time_utc,
            signal_time_ct,
            signal_time_msk,
            None,
            None,
            intent_source,
            "CLOSE_POSITION",
            reason,
            "FLAT",
            0.0,
            position_before_side,
            position_before_qty,
            str(protective_order["order_type"]).upper(),
            None if price_for_trade_intent is None else float(price_for_trade_intent),
            None,
            None,
            "EXECUTED",
            str(protective_order["order_ref"]),
            order_id,
            str(protective_order["order_action"]),
            int(protective_order["order_quantity"]),
            statistics["avg_fill_price"],
            statistics["total_commission"],
            statistics["realized_pnl"],
            None,
            now_ts,
            now_ts,
            filled_at_ts,
            filled_at_ts,
        ),
    )

    row = conn.execute(
        f"""
        SELECT trade_intent_id
        FROM {TRADE_INTENTS_TABLE_NAME}
        WHERE instrument_code = ?
          AND source_signal_id = ?
          AND signal_bar_ts = ?
          AND action = 'CLOSE_POSITION'
        LIMIT 1
        """,
        (
            instrument_code,
            source_signal_id,
            filled_at_ts,
        ),
    ).fetchone()

    synthetic_trade_intent_id = None if row is None else int(row[0])

    conn.execute(
        f"""
        UPDATE {PROTECTIVE_ORDERS_TABLE_NAME}
        SET
            status = ?,
            filled_qty = ?,
            avg_fill_price = ?,
            total_commission = ?,
            realized_pnl = ?,
            filled_at_ts = ?,
            synthetic_trade_intent_id = ?,
            error_text = NULL,
            updated_at_ts = ?,
            finished_at_ts = ?
        WHERE order_id = ?
        """,
        (
            PROTECTIVE_ORDER_STATUS_FILLED,
            float(statistics["filled_qty"]),
            statistics["avg_fill_price"],
            statistics["total_commission"],
            statistics["realized_pnl"],
            filled_at_ts,
            synthetic_trade_intent_id,
            now_ts,
            filled_at_ts,
            order_id,
        ),
    )

    return synthetic_trade_intent_id


async def reconcile_protective_orders_once(*, order_service) -> list[dict[str, Any]]:
    conn = get_trade_db_connection()

    try:
        initialize_protective_order_db(conn)
        active_orders = read_active_protective_orders(conn)

        if not active_orders:
            return []

        executions_refreshed = await refresh_ib_executions_if_possible(order_service)

        reconciled: list[dict[str, Any]] = []
        unresolved_orders: list[dict[str, Any]] = []

        # Первый проход использует текущий IB cache. Не вызываем reqOpenOrders
        # на каждой штатной итерации, когда все orders уже известны.
        for protective_order in active_orders:
            statistics = collect_protective_order_fill_statistics(order_service, protective_order)
            terminal_events = reconcile_terminal_protective_order(
                conn,
                protective_order=protective_order,
                statistics=statistics,
            )

            if terminal_events is not None:
                reconciled.extend(terminal_events)
                continue

            if (
                    statistics.get("waiting_commission")
                    or float(statistics.get("filled_qty") or 0.0) > 0.0
            ):
                continue

            if is_cached_broker_order_live(order_service, protective_order):
                if str(protective_order.get("status", "")).upper() == PROTECTIVE_ORDER_STATUS_UNPROTECTED:
                    mark_protective_runtime_state(
                        conn,
                        protective_order=protective_order,
                        status=PROTECTIVE_ORDER_STATUS_ACTIVE,
                        reason="broker protective order became live again",
                    )
                    conn.commit()
                continue

            unresolved_orders.append(protective_order)

        if not unresolved_orders:
            return reconciled

        # Только для неразрешённых локальных ACTIVE записей явно обновляем
        # broker open-orders snapshot и проверяем их второй раз.
        open_orders_refreshed, _ = await refresh_ib_open_orders_if_possible(order_service)

        if not open_orders_refreshed:
            return reconciled

        still_unresolved: list[dict[str, Any]] = []

        for protective_order in unresolved_orders:
            statistics = collect_protective_order_fill_statistics(order_service, protective_order)
            terminal_events = reconcile_terminal_protective_order(
                conn,
                protective_order=protective_order,
                statistics=statistics,
            )

            if terminal_events is not None:
                reconciled.extend(terminal_events)
                continue

            if (
                    statistics.get("waiting_commission")
                    or float(statistics.get("filled_qty") or 0.0) > 0.0
            ):
                continue

            if is_cached_broker_order_live(order_service, protective_order):
                if str(protective_order.get("status", "")).upper() == PROTECTIVE_ORDER_STATUS_UNPROTECTED:
                    mark_protective_runtime_state(
                        conn,
                        protective_order=protective_order,
                        status=PROTECTIVE_ORDER_STATUS_ACTIVE,
                        reason="broker protective order became live again",
                    )
                    conn.commit()
                continue

            still_unresolved.append(protective_order)

        if not still_unresolved:
            return reconciled

        try:
            snapshots = await asyncio.wait_for(
                sync_broker_positions_once(
                    order_service.ib,
                    expected_account_id=order_service.account_id,
                    force_refresh=True,
                ),
                timeout=float(BROKER_POSITION_SYNC_TIMEOUT_SECONDS),
            )
        except Exception:
            # Без подтверждённой broker-позиции локальный order не гасим.
            return reconciled

        snapshots_by_instrument = {
            str(snapshot.instrument_code): snapshot
            for snapshot in snapshots
        }

        for protective_order in still_unresolved:
            instrument_code = str(protective_order["instrument_code"])
            snapshot = snapshots_by_instrument.get(instrument_code)

            if snapshot is None:
                continue

            side = str(snapshot.side).upper()
            quantity = float(snapshot.quantity)
            role = normalize_protective_role(protective_order["role"])

            if side in {"LONG", "SHORT"} and quantity > 0.0:
                reason = (
                    "protective order absent from refreshed IB open orders "
                    "while broker position is open"
                )
                requires_market_close = role == PROTECTIVE_ORDER_ROLE_STOP_LOSS
                target_status = (
                    PROTECTIVE_ORDER_STATUS_UNPROTECTED
                    if requires_market_close
                    else PROTECTIVE_ORDER_STATUS_FAILED
                )
                mark_protective_runtime_state(
                    conn,
                    protective_order=protective_order,
                    status=target_status,
                    reason=reason,
                )
                conn.commit()
                reconciled.append(
                    build_reconciled_event(
                        event="UNPROTECTED" if requires_market_close else "FAILED",
                        protective_order=protective_order,
                        requires_market_close=requires_market_close,
                        reason=reason,
                    )
                )
                continue

            # Если executions refresh не подтвердился, FLAT ещё не означает orphan:
            # TP/SL мог исполниться, но fill пока не попал в cache.
            if not executions_refreshed:
                continue

            orphan_reason = (
                "orphan protective order: absent from refreshed IB open orders "
                "and broker position is FLAT"
            )
            mark_orphan_protective_cancelled(
                conn,
                protective_order=protective_order,
                reason=orphan_reason,
            )
            conn.commit()
            reconciled.append(
                build_reconciled_event(
                    event="ORPHANED",
                    protective_order=protective_order,
                    reason=orphan_reason,
                )
            )

        return reconciled

    finally:
        conn.close()


