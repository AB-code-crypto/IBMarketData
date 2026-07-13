from __future__ import annotations

from ib_execution.slot_loss_extension import *

async def refresh_open_orders_for_extension(
        order_service: OrderService,
) -> tuple[bool, str | None]:
    return await order_service.broker_state.refresh_open_orders(
        force=True,
    )

def collect_live_exit_orders(
        *,
        order_service: OrderService,
        instrument_code: str,
        now_ts: int,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen_order_ids: set[int] = set()

    try:
        trades = list(order_service.ib.openTrades() or [])
    except Exception:
        return result

    for trade in trades:
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
        result.append({
            "order_id": order_id,
            "order_ref": order_ref,
        })

    return result

async def cancel_exit_orders_for_instrument(
        *,
        order_service: OrderService,
        instrument_code: str,
        now_ts: int,
        reason: str,
) -> tuple[bool, list[SlotLossExtensionEvent]]:
    events: list[SlotLossExtensionEvent] = []
    cancelled_order_ids: set[int] = set()

    conn = get_trade_db_connection()
    try:
        initialize_execution_db(conn)
        initialize_protective_order_db(conn)

        for protective_order in read_active_protective_orders(conn, instrument_code=instrument_code):
            order_id = int(protective_order["order_id"])
            try:
                await order_service.cancel_order_id(order_id)
                cancelled_order_ids.add(order_id)
                mark_protective_order_status(
                    conn,
                    order_id=order_id,
                    status=PROTECTIVE_ORDER_STATUS_CANCELLED,
                    error_text=reason,
                )
                events.append(SlotLossExtensionEvent(
                    instrument_code=instrument_code,
                    event="EXIT_ORDER_CANCELLED",
                    message=f"{instrument_code}: slot-loss extension cancelled protective order_id={order_id}: {reason}",
                ))
            except Exception as exc:
                mark_protective_order_status(
                    conn,
                    order_id=order_id,
                    status=PROTECTIVE_ORDER_STATUS_ACTIVE,
                    error_text=f"cancel failed: {type(exc).__name__}: {exc}",
                )
                events.append(SlotLossExtensionEvent(
                    instrument_code=instrument_code,
                    event="EXIT_ORDER_CANCEL_FAILED",
                    message=f"{instrument_code}: failed to cancel protective order_id={order_id}: {type(exc).__name__}: {exc}",
                    log_level="WARNING",
                ))

        conn.commit()

    finally:
        conn.close()

    refresh_ok, refresh_error = await refresh_open_orders_for_extension(order_service)
    if not refresh_ok:
        events.append(SlotLossExtensionEvent(
            instrument_code=instrument_code,
            event="OPEN_ORDER_REFRESH_FAILED",
            message=f"{instrument_code}: cannot refresh broker open orders before slot-loss extension action: {refresh_error}",
            log_level="WARNING",
        ))
        return False, events

    live_exit_orders = collect_live_exit_orders(
        order_service=order_service,
        instrument_code=instrument_code,
        now_ts=now_ts,
    )

    for live_order in live_exit_orders:
        order_id = int(live_order["order_id"])
        if order_id in cancelled_order_ids:
            continue
        try:
            await order_service.cancel_order_id(order_id)
            cancelled_order_ids.add(order_id)
            events.append(SlotLossExtensionEvent(
                instrument_code=instrument_code,
                event="LIVE_EXIT_ORDER_CANCELLED",
                message=f"{instrument_code}: slot-loss extension cancelled live exit order_id={order_id}, order_ref={live_order['order_ref']}",
            ))
        except Exception as exc:
            events.append(SlotLossExtensionEvent(
                instrument_code=instrument_code,
                event="LIVE_EXIT_ORDER_CANCEL_FAILED",
                message=(
                    f"{instrument_code}: failed to cancel live exit order_id={order_id}, "
                    f"order_ref={live_order['order_ref']}: {type(exc).__name__}: {exc}"
                ),
                log_level="WARNING",
            ))

    refresh_ok, refresh_error = await refresh_open_orders_for_extension(order_service)
    if not refresh_ok:
        events.append(SlotLossExtensionEvent(
            instrument_code=instrument_code,
            event="OPEN_ORDER_REFRESH_AFTER_CANCEL_FAILED",
            message=f"{instrument_code}: cannot refresh broker open orders after exit-order cancel: {refresh_error}",
            log_level="WARNING",
        ))
        return False, events

    remaining_live_orders = collect_live_exit_orders(
        order_service=order_service,
        instrument_code=instrument_code,
        now_ts=now_ts,
    )
    if remaining_live_orders:
        events.append(SlotLossExtensionEvent(
            instrument_code=instrument_code,
            event="LIVE_EXIT_ORDERS_STILL_OPEN",
            message=f"{instrument_code}: live exit orders are still open; market close/extension is blocked: {remaining_live_orders}",
            log_level="WARNING",
        ))
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
) -> SlotLossExtensionEvent:
    conn = get_trade_db_connection()
    intent: TradeIntent | None = None

    try:
        initialize_execution_db(conn)
        draft = build_market_close_trade_intent_draft(
            snapshot=snapshot,
            source_signal_id=source_signal_id,
            intent_source=intent_source,
            reason=reason,
            now_ts=now_ts,
        )
        trade_intent_id = write_trade_intent(conn, draft)
        conn.commit()

        intent = read_trade_intent_by_id(conn, trade_intent_id=trade_intent_id)
        if str(intent.status).upper() != ExecutionStatus.NEW.value:
            return SlotLossExtensionEvent(
                instrument_code=str(snapshot.instrument_code),
                event="MARKET_CLOSE_SKIPPED_EXISTING_INTENT",
                message=(
                    f"{snapshot.instrument_code}: slot-loss extension market close skipped because "
                    f"trade_intent_id={trade_intent_id} already has status={intent.status}"
                ),
                intent=intent,
                result=None,
                log_level="WARNING",
            )

        mark_trade_intent_sending(conn, trade_intent_id=intent.trade_intent_id)
        conn.commit()

        async def on_order_submitted(order_id: int, order_action: str, order_quantity: int) -> None:
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
            order_id = (
                submission_state.get("order_id")
                or getattr(exc, "order_id", None)
            )
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

        log_level = "INFO" if result.status == ExecutionStatus.EXECUTED else "WARNING"
        return SlotLossExtensionEvent(
            instrument_code=str(snapshot.instrument_code),
            event="MARKET_CLOSE_SENT",
            message=(
                f"{snapshot.instrument_code}: slot-loss extension market close result: "
                f"intent_source={intent_source}, trade_intent_id={intent.trade_intent_id}, "
                f"status={result.status.value}, order_id={result.order_id}, "
                f"order_action={result.order_action}, order_qty={result.order_quantity}, "
                f"avg_fill={result.avg_fill_price}, reason={reason}, error={result.error_text}"
            ),
            intent=intent,
            result=result,
            log_level=log_level,
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
) -> list[SlotLossExtensionEvent]:
    instrument_code = str(snapshot.instrument_code)
    events: list[SlotLossExtensionEvent] = []

    cancel_ok, cancel_events = await cancel_exit_orders_for_instrument(
        order_service=order_service,
        instrument_code=instrument_code,
        now_ts=now_ts,
        reason=f"before {intent_source}: {reason}",
    )
    events.extend(cancel_events)

    if not cancel_ok:
        events.append(SlotLossExtensionEvent(
            instrument_code=instrument_code,
            event="MARKET_CLOSE_SKIPPED_EXIT_ORDERS_NOT_CANCELLED",
            message=f"{instrument_code}: market close skipped because existing exit orders could not be safely cancelled; reason={reason}",
            log_level="WARNING",
        ))
        return events

    snapshots = await sync_broker_positions_once(
                    order_service.ib,
                    expected_account_id=order_service.account_id,
                    force_refresh=True,
                )
    current_snapshot = find_snapshot(snapshots, instrument_code=instrument_code)
    if current_snapshot is None or not is_open_snapshot(current_snapshot):
        events.append(SlotLossExtensionEvent(
            instrument_code=instrument_code,
            event="MARKET_CLOSE_SKIPPED_ALREADY_FLAT",
            message=f"{instrument_code}: market close skipped because broker position is already FLAT after exit-order cancellation",
        ))
        return events

    events.append(await create_and_execute_market_close(
        order_service=order_service,
        snapshot=current_snapshot,
        source_signal_id=source_signal_id,
        intent_source=intent_source,
        reason=reason,
        now_ts=int(time.time()),
    ))
    return events
