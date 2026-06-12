from __future__ import annotations

import time
from types import SimpleNamespace
from typing import Any

from ib_execution.execution_logic import collect_trade_fill_statistics
from ib_execution.protective_order_store import (
    PROTECTIVE_ORDERS_TABLE_NAME,
    PROTECTIVE_ORDER_ROLE_STOP_LOSS,
    PROTECTIVE_ORDER_ROLE_TAKE_PROFIT,
    PROTECTIVE_ORDER_STATUS_CANCELLED,
    PROTECTIVE_ORDER_STATUS_FILLED,
    get_trade_db_connection,
    initialize_protective_order_db,
    read_active_protective_orders,
)
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


def normalize_protective_role(role: str) -> str:
    return str(role).upper()


async def refresh_ib_executions_if_possible(order_service) -> None:
    try:
        req_async = getattr(order_service.ib, "reqExecutionsAsync", None)

        if req_async is not None:
            await req_async()
            return

        req_sync = getattr(order_service.ib, "reqExecutions", None)

        if req_sync is not None:
            req_sync()

    except Exception:
        return


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

    fills = collect_ib_fills_for_order(order_service, order_id=order_id)

    if fills:
        avg_fill_price, total_commission, realized_pnl, filled_qty = collect_trade_fill_statistics(
            SimpleNamespace(fills=fills),
        )
        filled_at_ts = latest_fill_ts_from_fills(fills) or int(time.time())

        if filled_qty >= expected_qty:
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

        await refresh_ib_executions_if_possible(order_service)

        reconciled: list[dict[str, Any]] = []

        for protective_order in active_orders:
            statistics = collect_protective_order_fill_statistics(order_service, protective_order)

            if statistics["filled"]:
                synthetic_trade_intent_id = create_protective_close_trade_intent(
                    conn,
                    protective_order=protective_order,
                    statistics=statistics,
                )
                conn.commit()
                reconciled.append({
                    "event": "FILLED",
                    "role": protective_order["role"],
                    "instrument_code": protective_order["instrument_code"],
                    "order_id": int(protective_order["order_id"]),
                    "parent_trade_intent_id": int(protective_order["parent_trade_intent_id"]),
                    "synthetic_trade_intent_id": synthetic_trade_intent_id,
                    "filled_qty": float(statistics["filled_qty"]),
                    "avg_fill_price": statistics["avg_fill_price"],
                    "realized_pnl": statistics["realized_pnl"],
                })
                continue

            if statistics["cancelled"]:
                mark_protective_cancelled(
                    conn,
                    protective_order=protective_order,
                    status_text=str(statistics["status"]),
                )
                conn.commit()
                reconciled.append({
                    "event": "CANCELLED",
                    "role": protective_order["role"],
                    "instrument_code": protective_order["instrument_code"],
                    "order_id": int(protective_order["order_id"]),
                    "parent_trade_intent_id": int(protective_order["parent_trade_intent_id"]),
                    "synthetic_trade_intent_id": None,
                    "filled_qty": float(statistics["filled_qty"]),
                    "avg_fill_price": statistics["avg_fill_price"],
                    "realized_pnl": statistics["realized_pnl"],
                })

        return reconciled

    finally:
        conn.close()
