import asyncio
import time
import traceback
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any

from core.logger import get_logger, log_info, log_warning, setup_logging
from contracts import Instrument
from ib_execution.contract_resolver import build_execution_contract
from ib_execution.execution_logic import execute_trade_intent
from ib_execution.execution_models import ExecutionResult, ExecutionStatus
from ib_execution.execution_runner import (
    read_executed_trade_intent_and_result_for_notification,
    send_deal_status_notification,
    send_executed_deal_notification,
)
from ib_execution.execution_store import (
    get_trade_db_connection,
    initialize_execution_db,
    mark_trade_intent_order_submitted,
    mark_trade_intent_sending,
    read_new_trade_intents,
    write_trade_intent_execution_result,
)
from ib_execution.order_service import OrderService
from ib_execution.protective_order_reconciliation import reconcile_protective_orders_once
from ib_execution.protective_order_store import (
    PROTECTIVE_ORDER_ROLE_STOP_LOSS,
    PROTECTIVE_ORDER_ROLE_TAKE_PROFIT,
    PROTECTIVE_ORDER_STATUS_ACTIVE,
    mark_protective_order_status,
    read_active_protective_orders,
    record_protective_order,
)
from ib_execution.slot_close_recovery import (
    SLOT_CLOSE_RECOVERY_INTERVAL_SECONDS,
    run_slot_close_recovery_once,
)

setup_logging()
logger = get_logger(__name__)

EXECUTION_LOOP_SLEEP_SECONDS = 1
NEW_INTENTS_LIMIT = 20
MAX_NEW_INTENT_AGE_SECONDS = 10
EXECUTION_HEARTBEAT_INTERVAL_SECONDS = 60

TAKE_PROFIT_ORDER_REF_SUFFIX = "_TP"
STOP_LOSS_ORDER_REF_SUFFIX = "_SL"
PROTECTIVE_OCA_GROUP_PREFIX = "IBMD_OCA"
PROTECTIVE_ORDER_TIME_IN_FORCE = "DAY"


def normalize_price_to_tick_floor(*, price: Decimal, price_tick: Decimal) -> float:
    if price_tick <= Decimal("0"):
        raise ValueError(f"price_tick must be positive: {price_tick!r}")

    steps = (price / price_tick).to_integral_value(rounding=ROUND_FLOOR)
    return float(steps * price_tick)


def normalize_price_to_tick_ceiling(*, price: Decimal, price_tick: Decimal) -> float:
    if price_tick <= Decimal("0"):
        raise ValueError(f"price_tick must be positive: {price_tick!r}")

    steps = (price / price_tick).to_integral_value(rounding=ROUND_CEILING)
    return float(steps * price_tick)


def calculate_take_profit_price(*, instrument_code: str, position_side: str, avg_fill_price: float) -> float | None:
    instrument_row = Instrument.get(str(instrument_code))

    if instrument_row is None:
        return None

    take_profit_points = Decimal(str(instrument_row.get("take_profit_points", 0.0) or 0.0))

    if take_profit_points <= Decimal("0"):
        return None

    avg_fill_price_decimal = Decimal(str(avg_fill_price))
    position_side = str(position_side).upper()

    if position_side == "LONG":
        raw_take_profit_price = avg_fill_price_decimal + take_profit_points

    elif position_side == "SHORT":
        raw_take_profit_price = avg_fill_price_decimal - take_profit_points

    else:
        return None

    price_tick = Decimal(str(instrument_row.get("price_tick", 0.0) or 0.0))
    return normalize_price_to_tick_floor(
        price=raw_take_profit_price,
        price_tick=price_tick,
    )


def calculate_stop_loss_price(*, instrument_code: str, position_side: str, avg_fill_price: float) -> float | None:
    instrument_row = Instrument.get(str(instrument_code))

    if instrument_row is None:
        return None

    stop_loss_points = Decimal(str(instrument_row.get("stop_loss_points", 0.0) or 0.0))

    if stop_loss_points <= Decimal("0"):
        return None

    avg_fill_price_decimal = Decimal(str(avg_fill_price))
    position_side = str(position_side).upper()
    price_tick = Decimal(str(instrument_row.get("price_tick", 0.0) or 0.0))

    if position_side == "LONG":
        raw_stop_loss_price = avg_fill_price_decimal - stop_loss_points
        return normalize_price_to_tick_ceiling(
            price=raw_stop_loss_price,
            price_tick=price_tick,
        )

    if position_side == "SHORT":
        raw_stop_loss_price = avg_fill_price_decimal + stop_loss_points
        return normalize_price_to_tick_floor(
            price=raw_stop_loss_price,
            price_tick=price_tick,
        )

    return None


def get_protective_order_action(position_side: str) -> str | None:
    position_side = str(position_side).upper()

    if position_side == "LONG":
        return "SELL"

    if position_side == "SHORT":
        return "BUY"

    return None


def get_protective_order_quantity(intent) -> int | None:
    target_qty = float(intent.target_qty)

    if target_qty <= 0.0:
        return None

    if target_qty != int(target_qty):
        return None

    return int(target_qty)


def build_take_profit_order_ref(intent) -> str:
    return f"{intent.order_ref}{TAKE_PROFIT_ORDER_REF_SUFFIX}"


def build_stop_loss_order_ref(intent) -> str:
    return f"{intent.order_ref}{STOP_LOSS_ORDER_REF_SUFFIX}"


def build_protective_oca_group(intent) -> str:
    return f"{PROTECTIVE_OCA_GROUP_PREFIX}_{int(intent.trade_intent_id)}_{str(intent.instrument_code)}"


def protective_role_text(role: str) -> str:
    return str(role).lower().replace("_", "-")


async def cancel_protective_orders_before_position_change(*, conn, order_service: OrderService, intent) -> None:
    action = str(intent.action).upper()

    # Перед OPEN_POSITION тоже чистим старые ACTIVE защитные ордера из прошлых запусков/сессий.
    if action not in {"OPEN_POSITION", "CLOSE_POSITION", "REVERSE_POSITION"}:
        return

    instrument_code = str(intent.instrument_code)
    active_orders = read_active_protective_orders(
        conn,
        instrument_code=instrument_code,
    )

    if not active_orders:
        return

    for active_order in active_orders:
        order_id = int(active_order["order_id"])
        role_text = protective_role_text(str(active_order.get("role", "PROTECTIVE")))

        try:
            await order_service.cancel_order_id(order_id)
            mark_protective_order_status(
                conn,
                order_id=order_id,
                status="CANCELLED",
                error_text=f"cancelled before {action}",
            )
            log_info(
                logger,
                f"{instrument_code}: {role_text} order cancelled before {action}: order_id={order_id}",
                to_telegram=False,
            )

        except Exception as exc:
            mark_protective_order_status(
                conn,
                order_id=order_id,
                status=PROTECTIVE_ORDER_STATUS_ACTIVE,
                error_text=f"cancel failed before {action}: {type(exc).__name__}: {exc}",
            )
            log_warning(
                logger,
                (
                    f"{instrument_code}: {role_text} cancel failed before {action}: "
                    f"order_id={order_id}, {type(exc).__name__}: {exc}"
                ),
                to_telegram=False,
            )


async def place_protective_orders_after_entry(*, conn, order_service: OrderService, intent, result) -> None:
    if result.status != ExecutionStatus.EXECUTED:
        return

    action = str(intent.action).upper()

    if action not in {"OPEN_POSITION", "REVERSE_POSITION"}:
        return

    if result.avg_fill_price is None:
        return

    instrument_code = str(intent.instrument_code)
    target_side = str(intent.target_side).upper()
    order_action = get_protective_order_action(target_side)

    if order_action is None:
        return

    quantity = get_protective_order_quantity(intent)

    if quantity is None:
        log_warning(
            logger,
            (
                f"{instrument_code}: protective orders skipped: unsupported target_qty={intent.target_qty!r} "
                f"for trade_intent={intent.trade_intent_id}"
            ),
            to_telegram=False,
        )
        return

    try:
        take_profit_price = calculate_take_profit_price(
            instrument_code=instrument_code,
            position_side=target_side,
            avg_fill_price=float(result.avg_fill_price),
        )
        stop_loss_price = calculate_stop_loss_price(
            instrument_code=instrument_code,
            position_side=target_side,
            avg_fill_price=float(result.avg_fill_price),
        )

        if take_profit_price is None and stop_loss_price is None:
            return

        specs: list[dict[str, Any]] = []

        # SL ставим первым: если TP почему-то не поставится, позиция всё равно останется защищённой.
        if stop_loss_price is not None:
            stop_order = order_service.api.build_stop(
                action=order_action,
                quantity=quantity,
                stop_price=float(stop_loss_price),
                time_in_force=PROTECTIVE_ORDER_TIME_IN_FORCE,
            )
            specs.append({
                "role": PROTECTIVE_ORDER_ROLE_STOP_LOSS,
                "order": stop_order,
                "order_ref": build_stop_loss_order_ref(intent),
                "order_type": "STP",
                "limit_price": None,
                "stop_price": float(stop_loss_price),
                "price": float(stop_loss_price),
            })

        if take_profit_price is not None:
            take_profit_order = order_service.api.build_limit(
                action=order_action,
                quantity=quantity,
                limit_price=float(take_profit_price),
                time_in_force=PROTECTIVE_ORDER_TIME_IN_FORCE,
            )
            specs.append({
                "role": PROTECTIVE_ORDER_ROLE_TAKE_PROFIT,
                "order": take_profit_order,
                "order_ref": build_take_profit_order_ref(intent),
                "order_type": "LIMIT",
                "limit_price": float(take_profit_price),
                "stop_price": None,
                "price": float(take_profit_price),
            })

        oca_group = build_protective_oca_group(intent) if len(specs) >= 2 else None

        if oca_group is not None:
            order_service.api.apply_oca_group(
                [spec["order"] for spec in specs],
                oca_group=oca_group,
                oca_type=1,
            )

        contract = build_execution_contract(instrument_code=instrument_code)
        contract_q = await order_service.qualify(contract)

        for spec in specs:
            receipt = await order_service.api.place_order(
                contract_q,
                spec["order"],
                order_ref=str(spec["order_ref"]),
            )
            order_id = int(receipt.order_id)

            record_protective_order(
                conn,
                instrument_code=instrument_code,
                parent_trade_intent_id=int(intent.trade_intent_id),
                role=str(spec["role"]),
                order_ref=str(spec["order_ref"]),
                order_id=order_id,
                order_action=order_action,
                order_quantity=quantity,
                order_type=str(spec["order_type"]),
                limit_price=spec["limit_price"],
                stop_price=spec["stop_price"],
                oca_group=oca_group,
            )

            role_text = protective_role_text(str(spec["role"]))
            log_info(
                logger,
                (
                    f"{instrument_code}: {role_text} order submitted: "
                    f"parent_trade_intent={intent.trade_intent_id}, "
                    f"order_id={order_id}, action={order_action}, qty={quantity}, "
                    f"price={spec['price']}, order_ref={spec['order_ref']}, oca_group={oca_group}"
                ),
                to_telegram=False,
            )

    except Exception as exc:
        log_warning(
            logger,
            (
                f"{instrument_code}: protective order submit failed: "
                f"trade_intent={intent.trade_intent_id}, "
                f"{type(exc).__name__}: {exc}"
            ),
            to_telegram=False,
        )


async def reconcile_and_notify_protective_orders(
        *,
        order_service: OrderService,
        deal_telegram_sender=None,
        deal_message_thread_id=None,
) -> None:
    reconciled_protective_orders = await reconcile_protective_orders_once(
        order_service=order_service,
    )

    for reconciled_protective_order in reconciled_protective_orders:
        role_text = protective_role_text(str(reconciled_protective_order.get("role", "PROTECTIVE")))
        log_info(
            logger,
            (
                f"{reconciled_protective_order['instrument_code']}: "
                f"{role_text} {reconciled_protective_order['event'].lower()}: "
                f"order_id={reconciled_protective_order['order_id']}, "
                f"parent_trade_intent_id={reconciled_protective_order['parent_trade_intent_id']}, "
                f"synthetic_trade_intent_id={reconciled_protective_order['synthetic_trade_intent_id']}, "
                f"filled_qty={reconciled_protective_order['filled_qty']}, "
                f"avg_fill={reconciled_protective_order['avg_fill_price']}, "
                f"realized_pnl={reconciled_protective_order['realized_pnl']}"
            ),
            to_telegram=False,
        )

        if str(reconciled_protective_order.get("event", "")).upper() != "FILLED":
            continue

        synthetic_trade_intent_id = reconciled_protective_order.get("synthetic_trade_intent_id")

        if synthetic_trade_intent_id is None:
            continue

        try:
            synthetic_intent, synthetic_result = read_executed_trade_intent_and_result_for_notification(
                trade_intent_id=int(synthetic_trade_intent_id),
            )

            if synthetic_intent is None or synthetic_result is None:
                continue

            await send_executed_deal_notification(
                telegram_sender=deal_telegram_sender,
                message_thread_id=deal_message_thread_id,
                intent=synthetic_intent,
                result=synthetic_result,
            )

        except Exception as notification_exc:
            log_warning(
                logger,
                (
                    f"protective-order deal notification failed "
                    f"synthetic_trade_intent_id={synthetic_trade_intent_id}: "
                    f"{type(notification_exc).__name__}: {notification_exc}"
                ),
                to_telegram=True,
            )


async def run_execution_loop(
        order_service: OrderService,
        *,
        deal_telegram_sender=None,
        deal_message_thread_id=None,
        deal_status_message_thread_id=None,
) -> None:
    log_info(
        logger,
        (
            "ib_execution loop started: "
            "order_type=FROM_TRADE_INTENT, "
            f"max_new_intent_age_seconds={MAX_NEW_INTENT_AGE_SECONDS}"
        ),
        to_telegram=False,
    )

    next_heartbeat_ts = int(time.time()) + EXECUTION_HEARTBEAT_INTERVAL_SECONDS
    next_slot_close_recovery_ts = 0

    while True:
        try:
            await reconcile_and_notify_protective_orders(
                order_service=order_service,
                deal_telegram_sender=deal_telegram_sender,
                deal_message_thread_id=deal_message_thread_id,
            )

        except Exception as exc:
            log_warning(
                logger,
                f"protective-order reconciliation failed: {type(exc).__name__}: {exc}\n{traceback.format_exc()}",
                to_telegram=True,
            )

        now_ts = int(time.time())
        if now_ts >= next_slot_close_recovery_ts:
            next_slot_close_recovery_ts = now_ts + SLOT_CLOSE_RECOVERY_INTERVAL_SECONDS

            try:
                recovery_events = await run_slot_close_recovery_once(
                    order_service=order_service,
                )

                for recovery_event in recovery_events:
                    if str(recovery_event.log_level).upper() == "WARNING":
                        log_warning(
                            logger,
                            recovery_event.message,
                            to_telegram=True,
                        )
                    else:
                        log_info(
                            logger,
                            recovery_event.message,
                            to_telegram=False,
                        )

                    if recovery_event.intent is None or recovery_event.result is None:
                        continue

                    try:
                        await send_executed_deal_notification(
                            telegram_sender=deal_telegram_sender,
                            message_thread_id=deal_message_thread_id,
                            intent=recovery_event.intent,
                            result=recovery_event.result,
                        )
                    except Exception as notification_exc:
                        log_warning(
                            logger,
                            (
                                f"slot-close recovery deal notification failed "
                                f"trade_intent={recovery_event.intent.trade_intent_id}: "
                                f"{type(notification_exc).__name__}: {notification_exc}"
                            ),
                            to_telegram=True,
                        )

                    try:
                        await send_deal_status_notification(
                            telegram_sender=deal_telegram_sender,
                            message_thread_id=deal_status_message_thread_id,
                            intent=recovery_event.intent,
                            result=recovery_event.result,
                        )
                    except Exception as notification_exc:
                        log_warning(
                            logger,
                            (
                                f"slot-close recovery status notification failed "
                                f"trade_intent={recovery_event.intent.trade_intent_id}: "
                                f"{type(notification_exc).__name__}: {notification_exc}"
                            ),
                            to_telegram=True,
                        )

            except Exception as exc:
                log_warning(
                    logger,
                    f"slot-close recovery failed: {type(exc).__name__}: {exc}\n{traceback.format_exc()}",
                    to_telegram=True,
                )

        intents = read_new_trade_intents(
            limit=NEW_INTENTS_LIMIT,
            max_age_seconds=MAX_NEW_INTENT_AGE_SECONDS,
        )

        for intent in intents:
            conn = get_trade_db_connection()
            try:
                initialize_execution_db(conn)

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

                await cancel_protective_orders_before_position_change(
                    conn=conn,
                    order_service=order_service,
                    intent=intent,
                )
                conn.commit()

                result = await execute_trade_intent(
                    order_service=order_service,
                    intent=intent,
                    order_submitted_callback=on_order_submitted,
                )

                write_trade_intent_execution_result(conn, result=result)
                conn.commit()

                await place_protective_orders_after_entry(
                    conn=conn,
                    order_service=order_service,
                    intent=intent,
                    result=result,
                )
                conn.commit()

                log_info(
                    logger,
                    (
                        f"{intent.instrument_code}: executed trade_intent={intent.trade_intent_id}, "
                        f"action={intent.action}, order_type={intent.order_type}, "
                        f"order_id={result.order_id}, order_action={result.order_action}, "
                        f"qty={result.order_quantity}, avg_fill={result.avg_fill_price}, "
                        f"realized_pnl={result.realized_pnl}, commission={result.total_commission}"
                    ),
                    to_telegram=False,
                )

                try:
                    await send_executed_deal_notification(
                        telegram_sender=deal_telegram_sender,
                        message_thread_id=deal_message_thread_id,
                        intent=intent,
                        result=result,
                    )
                except Exception as notification_exc:
                    log_warning(
                        logger,
                        (
                            f"deal notification failed "
                            f"trade_intent={intent.trade_intent_id}: "
                            f"{type(notification_exc).__name__}: {notification_exc}"
                        ),
                        to_telegram=True,
                    )

                try:
                    await send_deal_status_notification(
                        telegram_sender=deal_telegram_sender,
                        message_thread_id=deal_status_message_thread_id,
                        intent=intent,
                        result=result,
                    )
                except Exception as notification_exc:
                    log_warning(
                        logger,
                        (
                            f"deal status notification failed "
                            f"trade_intent={intent.trade_intent_id}: "
                            f"{type(notification_exc).__name__}: {notification_exc}"
                        ),
                        to_telegram=True,
                    )

            except Exception as exc:
                error_text = f"{type(exc).__name__}: {exc}"

                try:
                    failure_result = ExecutionResult(
                        trade_intent_id=intent.trade_intent_id,
                        order_id=getattr(exc, "order_id", None),
                        order_action=None,
                        order_quantity=None,
                        status=ExecutionStatus.FAILED,
                        avg_fill_price=None,
                        total_commission=None,
                        realized_pnl=None,
                        error_text=error_text,
                    )
                    write_trade_intent_execution_result(conn, result=failure_result)
                    conn.commit()

                    await send_deal_status_notification(
                        telegram_sender=deal_telegram_sender,
                        message_thread_id=deal_status_message_thread_id,
                        intent=intent,
                        result=failure_result,
                    )
                finally:
                    log_warning(
                        logger,
                        f"ib_execution failed trade_intent={intent.trade_intent_id}: {error_text}\n"
                        f"{traceback.format_exc()}",
                        to_telegram=True,
                    )

            finally:
                conn.close()

        now_ts = int(time.time())
        if now_ts >= next_heartbeat_ts:
            log_info(
                logger,
                (
                    "ib_execution heartbeat: alive, "
                    f"new_intents_limit={NEW_INTENTS_LIMIT}, "
                    f"max_new_intent_age_seconds={MAX_NEW_INTENT_AGE_SECONDS}"
                ),
                to_telegram=False,
            )
            next_heartbeat_ts = now_ts + EXECUTION_HEARTBEAT_INTERVAL_SECONDS

        await asyncio.sleep(EXECUTION_LOOP_SLEEP_SECONDS)
