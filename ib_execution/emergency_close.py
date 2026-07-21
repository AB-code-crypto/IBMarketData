from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from ib_execution.execution_logic import execute_trade_intent
from ib_execution.execution_models import ExecutionResult, ExecutionStatus, TradeIntent
from ib_execution.execution_store import (
    get_trade_db_connection,
    initialize_execution_db,
    mark_trade_intent_order_submitted,
    mark_trade_intent_sending,
    read_trade_intent_submission_state,
    write_trade_intent_execution_result,
)
from ib_execution.order_cancellation import (
    OrderFilledDuringCancellationError,
    cancel_order_and_require_terminal,
)
from ib_execution.order_service import OrderService
from ib_execution.protective_order_reconciliation import reconcile_protective_orders_once
from ib_execution.protective_order_store import (
    PROTECTIVE_ORDER_STATUS_ACTIVE,
    PROTECTIVE_ORDER_STATUS_CANCELLED,
    initialize_protective_order_db,
    mark_protective_order_status,
    read_active_protective_orders,
)
from ib_position_sync.position_models import BrokerPositionSnapshot
from ib_position_sync.position_store import (
    is_same_contract_for_instrument,
    sync_broker_positions_once,
)
from ib_trader.trade_intent_repository import write_trade_intent
from ib_trader.trade_models import PositionSide, TradeDecisionAction
from ib_trader.trade_schema import (
    ORDER_REF_PREFIX,
    TRADE_INTENTS_TABLE_NAME,
    TradeIntentDraft,
    build_time_text_fields_from_ts,
)


@dataclass(frozen=True)
class EmergencyCloseEvent:
    instrument_code: str
    event: str
    message: str
    intent: TradeIntent | None = None
    result: ExecutionResult | None = None
    log_level: str = "INFO"


LEGACY_AUXILIARY_EXIT_SUFFIXES = ("_EXT_TP", "_EXT_SL")


def collect_live_legacy_auxiliary_exit_orders(
        order_service: OrderService,
) -> list[dict[str, Any]]:
    """Return broker orders left by the removed loss-extension subsystem."""
    result: list[dict[str, Any]] = []
    seen: set[int] = set()
    for trade in list(order_service.ib.openTrades() or []):
        order = getattr(trade, "order", None)
        if order is None:
            continue
        order_ref = str(getattr(order, "orderRef", "") or "")
        if not order_ref.startswith(ORDER_REF_PREFIX):
            continue
        if not order_ref.endswith(LEGACY_AUXILIARY_EXIT_SUFFIXES):
            continue
        account = str(getattr(order, "account", "") or "").strip()
        if account and account != order_service.account_id:
            continue
        order_id = int(getattr(order, "orderId", 0) or 0)
        if order_id <= 0 or order_id in seen:
            continue
        seen.add(order_id)
        result.append(
            {
                "trade": trade,
                "order_id": order_id,
                "order_ref": order_ref,
                "status": str(
                    getattr(getattr(trade, "orderStatus", None), "status", "")
                    or ""
                ),
            }
        )
    return result


async def cancel_legacy_auxiliary_exit_orders_once(
        order_service: OrderService,
) -> list[dict[str, Any]]:
    """Cancel every live auxiliary exit left by the removed subsystem.

    Losing local knowledge of a live exit order is worse than refusing startup.
    Therefore any refresh/cancellation ambiguity aborts execution startup.
    """
    refresh_ok, refresh_error = await order_service.broker_state.refresh_open_orders(
        force=True
    )
    if not refresh_ok:
        raise RuntimeError(
            "cannot verify legacy auxiliary exit orders during startup: "
            f"{refresh_error}"
        )

    cancelled: list[dict[str, Any]] = []
    for item in collect_live_legacy_auxiliary_exit_orders(order_service):
        try:
            cancellation = await cancel_order_and_require_terminal(
                order_service,
                order_id=int(item["order_id"]),
                context=(
                    "rolling-only startup cleanup of removed auxiliary exit: "
                    f"order_ref={item['order_ref']}"
                ),
            )
        except OrderFilledDuringCancellationError as exc:
            await reconcile_protective_orders_once(order_service=order_service)
            raise RuntimeError(
                "legacy auxiliary exit filled during startup cleanup; "
                "broker positions must be inspected before restarting: "
                f"order_id={item['order_id']}, order_ref={item['order_ref']}, "
                f"error={exc}"
            ) from exc
        cancelled.append(
            {
                "order_id": int(item["order_id"]),
                "order_ref": str(item["order_ref"]),
                "terminal_status": str(cancellation.terminal_status),
            }
        )

    refresh_ok, refresh_error = await order_service.broker_state.refresh_open_orders(
        force=True
    )
    if not refresh_ok:
        raise RuntimeError(
            "cannot verify auxiliary exit cleanup after cancellation: "
            f"{refresh_error}"
        )
    remaining = collect_live_legacy_auxiliary_exit_orders(order_service)
    if remaining:
        raise RuntimeError(
            "legacy auxiliary exit orders remain live after cancellation: "
            f"{[{k: v for k, v in item.items() if k != 'trade'} for item in remaining]}"
        )
    return cancelled


def is_open_snapshot(snapshot: BrokerPositionSnapshot) -> bool:
    return (
        str(snapshot.side).upper() in {"LONG", "SHORT"}
        and float(snapshot.quantity) > 0.0
    )


def find_snapshot(
        snapshots: list[BrokerPositionSnapshot],
        *,
        instrument_code: str,
) -> BrokerPositionSnapshot | None:
    for snapshot in snapshots:
        if str(snapshot.instrument_code) == str(instrument_code):
            return snapshot
    return None


def read_trade_intent_by_id(conn, *, trade_intent_id: int) -> TradeIntent:
    row = conn.execute(
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
            created_at_ts
        FROM {TRADE_INTENTS_TABLE_NAME}
        WHERE trade_intent_id = ?
        LIMIT 1
        """,
        (int(trade_intent_id),),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"trade_intent not found: trade_intent_id={trade_intent_id}")
    order_ref = "" if row[3] is None else str(row[3]).strip()
    if not order_ref:
        raise RuntimeError(
            f"trade_intent without order_ref: trade_intent_id={trade_intent_id}"
        )
    return TradeIntent(
        trade_intent_id=int(row[0]),
        source_signal_id=int(row[1]),
        instrument_code=str(row[2]),
        order_ref=order_ref,
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


def build_market_close_trade_intent_draft(
        *,
        snapshot: BrokerPositionSnapshot,
        source_signal_id: int,
        intent_source: str,
        reason: str,
        now_ts: int,
) -> TradeIntentDraft:
    _, signal_time_ct, _ = build_time_text_fields_from_ts(now_ts)
    return TradeIntentDraft(
        source_signal_id=int(source_signal_id),
        instrument_code=str(snapshot.instrument_code),
        signal_bar_ts=int(now_ts),
        signal_time_ct=signal_time_ct,
        intent_source=str(intent_source),
        action=TradeDecisionAction.CLOSE_POSITION,
        reason=str(reason),
        signal_direction="CLOSE",
        entry_price=0.0,
        potential_end_delta_points=0.0,
        order_type="MARKET",
        position_before_side=PositionSide(str(snapshot.side).upper()),
        position_before_qty=float(snapshot.quantity),
        position_after_side=PositionSide.FLAT,
        position_after_qty=0.0,
    )


async def refresh_open_orders_for_close(
        order_service: OrderService,
) -> tuple[bool, str | None]:
    return await order_service.broker_state.refresh_open_orders(force=True)


def collect_live_exit_orders(
        *,
        order_service: OrderService,
        instrument_code: str,
        now_ts: int,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen_order_ids: set[int] = set()
    for trade in list(order_service.ib.openTrades() or []):
        order = getattr(trade, "order", None)
        contract = getattr(trade, "contract", None)
        if order is None or contract is None:
            continue
        order_ref = str(getattr(order, "orderRef", "") or "")
        if not order_ref.startswith(ORDER_REF_PREFIX):
            continue
        if not order_ref.endswith(("_TP", "_SL")):
            continue
        if not is_same_contract_for_instrument(
            position_contract=contract,
            instrument_code=instrument_code,
            now_ts=now_ts,
        ):
            continue
        order_id = int(getattr(order, "orderId", 0) or 0)
        if order_id <= 0 or order_id in seen_order_ids:
            continue
        seen_order_ids.add(order_id)
        result.append({"order_id": order_id, "order_ref": order_ref})
    return result


async def cancel_exit_orders_for_instrument(
        *,
        order_service: OrderService,
        instrument_code: str,
        now_ts: int,
        reason: str,
) -> tuple[bool, list[EmergencyCloseEvent]]:
    events: list[EmergencyCloseEvent] = []
    cancelled_order_ids: set[int] = set()

    pre_events = await reconcile_protective_orders_once(order_service=order_service)
    if any(str(event.get("event", "")).upper() == "FILLED" for event in pre_events):
        events.append(
            EmergencyCloseEvent(
                instrument_code=instrument_code,
                event="EXIT_ORDER_FILLED_BEFORE_CANCEL",
                message=(
                    f"{instrument_code}: an exit order filled before cancellation; "
                    "broker position must be re-read before another close action"
                ),
                log_level="WARNING",
            )
        )
        return False, events

    conn = get_trade_db_connection()
    try:
        initialize_execution_db(conn)
        initialize_protective_order_db(conn)
        for protective_order in read_active_protective_orders(
            conn,
            instrument_code=instrument_code,
        ):
            order_id = int(protective_order["order_id"])
            try:
                cancellation = await cancel_order_and_require_terminal(
                    order_service,
                    order_id=order_id,
                    context=f"{instrument_code} protective cleanup: {reason}",
                )
            except OrderFilledDuringCancellationError as exc:
                await reconcile_protective_orders_once(order_service=order_service)
                events.append(
                    EmergencyCloseEvent(
                        instrument_code=instrument_code,
                        event="EXIT_ORDER_FILLED_DURING_CANCEL",
                        message=(
                            f"{instrument_code}: protective order filled during cancel; "
                            f"order_id={order_id}, error={exc}"
                        ),
                        log_level="WARNING",
                    )
                )
                return False, events
            except Exception as exc:
                mark_protective_order_status(
                    conn,
                    order_id=order_id,
                    status=PROTECTIVE_ORDER_STATUS_ACTIVE,
                    error_text=f"cancel not confirmed: {type(exc).__name__}: {exc}",
                )
                conn.commit()
                events.append(
                    EmergencyCloseEvent(
                        instrument_code=instrument_code,
                        event="EXIT_ORDER_CANCEL_FAILED",
                        message=(
                            f"{instrument_code}: failed to confirm protective cancel "
                            f"order_id={order_id}: {type(exc).__name__}: {exc}"
                        ),
                        log_level="WARNING",
                    )
                )
                return False, events

            mark_protective_order_status(
                conn,
                order_id=order_id,
                status=PROTECTIVE_ORDER_STATUS_CANCELLED,
                error_text=f"{reason}; broker_status={cancellation.terminal_status}",
            )
            conn.commit()
            cancelled_order_ids.add(order_id)
            events.append(
                EmergencyCloseEvent(
                    instrument_code=instrument_code,
                    event="EXIT_ORDER_CANCELLED",
                    message=(
                        f"{instrument_code}: protective cancellation confirmed "
                        f"order_id={order_id}, broker_status={cancellation.terminal_status}: "
                        f"{reason}"
                    ),
                )
            )
    finally:
        conn.close()

    refresh_ok, refresh_error = await refresh_open_orders_for_close(order_service)
    if not refresh_ok:
        events.append(
            EmergencyCloseEvent(
                instrument_code=instrument_code,
                event="OPEN_ORDER_REFRESH_FAILED",
                message=(
                    f"{instrument_code}: cannot refresh broker open orders before "
                    f"exit cleanup: {refresh_error}"
                ),
                log_level="WARNING",
            )
        )
        return False, events

    for live_order in collect_live_exit_orders(
        order_service=order_service,
        instrument_code=instrument_code,
        now_ts=now_ts,
    ):
        order_id = int(live_order["order_id"])
        if order_id in cancelled_order_ids:
            continue
        try:
            cancellation = await cancel_order_and_require_terminal(
                order_service,
                order_id=order_id,
                context=f"{instrument_code} broker-only exit cleanup: {reason}",
            )
            cancelled_order_ids.add(order_id)
            events.append(
                EmergencyCloseEvent(
                    instrument_code=instrument_code,
                    event="LIVE_EXIT_ORDER_CANCELLED",
                    message=(
                        f"{instrument_code}: live exit cancellation confirmed "
                        f"order_id={order_id}, order_ref={live_order['order_ref']}, "
                        f"broker_status={cancellation.terminal_status}"
                    ),
                )
            )
        except OrderFilledDuringCancellationError as exc:
            await reconcile_protective_orders_once(order_service=order_service)
            events.append(
                EmergencyCloseEvent(
                    instrument_code=instrument_code,
                    event="LIVE_EXIT_ORDER_FILLED_DURING_CANCEL",
                    message=(
                        f"{instrument_code}: live exit filled during cancel; "
                        f"order_id={order_id}, order_ref={live_order['order_ref']}, "
                        f"error={exc}"
                    ),
                    log_level="WARNING",
                )
            )
            return False, events
        except Exception as exc:
            events.append(
                EmergencyCloseEvent(
                    instrument_code=instrument_code,
                    event="LIVE_EXIT_ORDER_CANCEL_FAILED",
                    message=(
                        f"{instrument_code}: failed to confirm live exit cancel "
                        f"order_id={order_id}, order_ref={live_order['order_ref']}: "
                        f"{type(exc).__name__}: {exc}"
                    ),
                    log_level="WARNING",
                )
            )
            return False, events

    refresh_ok, refresh_error = await refresh_open_orders_for_close(order_service)
    if not refresh_ok:
        events.append(
            EmergencyCloseEvent(
                instrument_code=instrument_code,
                event="OPEN_ORDER_REFRESH_AFTER_CANCEL_FAILED",
                message=(
                    f"{instrument_code}: cannot refresh broker open orders after "
                    f"exit-order cancel: {refresh_error}"
                ),
                log_level="WARNING",
            )
        )
        return False, events

    remaining = collect_live_exit_orders(
        order_service=order_service,
        instrument_code=instrument_code,
        now_ts=now_ts,
    )
    if remaining:
        events.append(
            EmergencyCloseEvent(
                instrument_code=instrument_code,
                event="LIVE_EXIT_ORDERS_STILL_OPEN",
                message=(
                    f"{instrument_code}: live exit orders are still open; "
                    f"market close is blocked: {remaining}"
                ),
                log_level="WARNING",
            )
        )
        return False, events

    return True, events


async def create_and_execute_market_close(
        *,
        order_service: OrderService,
        snapshot: BrokerPositionSnapshot,
        source_signal_id: int,
        intent_source: str,
        reason: str,
        now_ts: int,
) -> EmergencyCloseEvent:
    conn = get_trade_db_connection()
    try:
        initialize_execution_db(conn)
        trade_intent_id = write_trade_intent(
            conn,
            build_market_close_trade_intent_draft(
                snapshot=snapshot,
                source_signal_id=source_signal_id,
                intent_source=intent_source,
                reason=reason,
                now_ts=now_ts,
            ),
        )
        conn.commit()
        intent = read_trade_intent_by_id(conn, trade_intent_id=trade_intent_id)
        if str(intent.status).upper() != ExecutionStatus.NEW.value:
            return EmergencyCloseEvent(
                instrument_code=str(snapshot.instrument_code),
                event="MARKET_CLOSE_SKIPPED_EXISTING_INTENT",
                message=(
                    f"{snapshot.instrument_code}: market close skipped because "
                    f"trade_intent_id={trade_intent_id} already has status={intent.status}"
                ),
                intent=intent,
                log_level="WARNING",
            )

        mark_trade_intent_sending(conn, trade_intent_id=intent.trade_intent_id)
        conn.commit()

        async def on_order_submitted(
                order_id: int,
                order_action: str,
                order_quantity: int,
        ) -> None:
            mark_trade_intent_order_submitted(
                conn,
                trade_intent_id=intent.trade_intent_id,
                order_id=order_id,
                order_action=order_action,
                order_quantity=order_quantity,
            )
            conn.commit()

        try:
            result = await execute_trade_intent(
                order_service=order_service,
                intent=intent,
                order_submitted_callback=on_order_submitted,
            )
        except Exception as exc:
            submission_state = read_trade_intent_submission_state(
                conn,
                trade_intent_id=intent.trade_intent_id,
            ) or {}
            order_id = submission_state.get("order_id") or getattr(exc, "order_id", None)
            result = ExecutionResult(
                trade_intent_id=intent.trade_intent_id,
                order_id=order_id,
                order_action=(
                    submission_state.get("order_action")
                    or getattr(exc, "order_action", None)
                ),
                order_quantity=(
                    submission_state.get("order_quantity")
                    or getattr(exc, "order_quantity", None)
                ),
                status=(
                    ExecutionStatus.RECONCILING
                    if order_id is not None
                    else ExecutionStatus.FAILED
                ),
                avg_fill_price=None,
                total_commission=None,
                realized_pnl=None,
                error_text=f"{type(exc).__name__}: {exc}",
            )

        write_trade_intent_execution_result(conn, result=result)
        conn.commit()
        return EmergencyCloseEvent(
            instrument_code=str(snapshot.instrument_code),
            event="MARKET_CLOSE_SENT",
            message=(
                f"{snapshot.instrument_code}: market close result: "
                f"intent_source={intent_source}, trade_intent_id={intent.trade_intent_id}, "
                f"status={result.status.value}, order_id={result.order_id}, "
                f"order_action={result.order_action}, order_qty={result.order_quantity}, "
                f"avg_fill={result.avg_fill_price}, reason={reason}, "
                f"error={result.error_text}"
            ),
            intent=intent,
            result=result,
            log_level=(
                "INFO" if result.status == ExecutionStatus.EXECUTED else "WARNING"
            ),
        )
    finally:
        conn.close()


async def close_market_safely(
        *,
        order_service: OrderService,
        snapshot: BrokerPositionSnapshot,
        source_signal_id: int,
        intent_source: str,
        reason: str,
        now_ts: int,
) -> list[EmergencyCloseEvent]:
    instrument_code = str(snapshot.instrument_code)
    events: list[EmergencyCloseEvent] = []
    cancel_ok, cancel_events = await cancel_exit_orders_for_instrument(
        order_service=order_service,
        instrument_code=instrument_code,
        now_ts=now_ts,
        reason=f"before {intent_source}: {reason}",
    )
    events.extend(cancel_events)
    if not cancel_ok:
        events.append(
            EmergencyCloseEvent(
                instrument_code=instrument_code,
                event="MARKET_CLOSE_SKIPPED_EXIT_ORDERS_NOT_CANCELLED",
                message=(
                    f"{instrument_code}: market close skipped because existing "
                    f"exit orders could not be safely cancelled; reason={reason}"
                ),
                log_level="WARNING",
            )
        )
        return events

    snapshots = await sync_broker_positions_once(
        order_service.ib,
        expected_account_id=order_service.account_id,
        force_refresh=True,
    )
    current_snapshot = find_snapshot(
        snapshots,
        instrument_code=instrument_code,
    )
    if current_snapshot is None or not is_open_snapshot(current_snapshot):
        events.append(
            EmergencyCloseEvent(
                instrument_code=instrument_code,
                event="MARKET_CLOSE_SKIPPED_ALREADY_FLAT",
                message=(
                    f"{instrument_code}: market close skipped because broker "
                    "position is already FLAT after exit-order cancellation"
                ),
            )
        )
        return events

    events.append(
        await create_and_execute_market_close(
            order_service=order_service,
            snapshot=current_snapshot,
            source_signal_id=source_signal_id,
            intent_source=intent_source,
            reason=reason,
            now_ts=int(time.time()),
        )
    )
    return events


__all__ = [
    "EmergencyCloseEvent",
    "LEGACY_AUXILIARY_EXIT_SUFFIXES",
    "collect_live_legacy_auxiliary_exit_orders",
    "cancel_legacy_auxiliary_exit_orders_once",
    "is_open_snapshot",
    "find_snapshot",
    "read_trade_intent_by_id",
    "build_market_close_trade_intent_draft",
    "collect_live_exit_orders",
    "cancel_exit_orders_for_instrument",
    "create_and_execute_market_close",
    "close_market_safely",
]
