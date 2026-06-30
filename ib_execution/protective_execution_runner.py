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
from ib_execution.execution_stats_reconciliation import (
    format_backfilled_execution_stats_message,
    reconcile_missing_execution_stats_once,
)
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
from ib_execution.slot_loss_extension import (
    close_market_safely,
    find_snapshot,
    read_first_level_touch_row,
    read_latest_feature_bar,
    run_slot_loss_extension_once,
)
from ib_position_sync.position_store import sync_broker_positions_once
from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG

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

PROTECTIVE_ORDER_SAFETY_SOURCE_SIGNAL_ID = -10
PROTECTIVE_ORDER_SAFETY_INTENT_SOURCE = "PROTECTIVE_ORDER_SAFETY"
PROTECTIVE_ORDER_SAFETY_SUBMIT_FAILED_REASON = "protective_order_submit_failed_market_close"
PROTECTIVE_ORDER_SAFETY_STOP_BREACHED_REASON = "protective_order_watchdog_stop_price_breached"
PROTECTIVE_ORDER_SAFETY_PRICE_PATH_STALE_REASON = "protective_order_watchdog_price_path_stale"

PROTECTIVE_EXIT_ORDER_ACCEPTED_STATUSES = {
    "PreSubmitted",
    "Submitted",
}
PROTECTIVE_EXIT_ORDER_FILLED_STATUSES = {"Filled"}
PROTECTIVE_EXIT_ORDER_REJECTED_STATUSES = {
    "ApiCancelled",
    "Cancelled",
    "Inactive",
    "Rejected",
}


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



def get_protective_order_accept_timeout_seconds() -> float:
    return float(getattr(DEFAULT_SIGNAL_CONFIG, "protective_order_accept_timeout_seconds", 5.0))


def is_protective_order_price_watchdog_enabled() -> bool:
    return bool(getattr(DEFAULT_SIGNAL_CONFIG, "protective_order_price_watchdog_enabled", True))


def is_protective_order_stale_price_fail_safe_enabled() -> bool:
    return bool(getattr(DEFAULT_SIGNAL_CONFIG, "protective_order_price_watchdog_stale_close_enabled", True))


def get_protective_order_price_stale_max_seconds() -> int:
    configured = getattr(DEFAULT_SIGNAL_CONFIG, "protective_order_price_stale_max_seconds", None)
    if configured is not None:
        return max(1, int(configured))
    return max(1, int(getattr(DEFAULT_SIGNAL_CONFIG, "max_job_bar_lag_seconds", 15)))


def apply_protective_order_safety_flags(order) -> None:
    if hasattr(order, "outsideRth"):
        setattr(order, "outsideRth", True)


def describe_ib_error(error) -> str:
    if error is None:
        return "missing"

    parts = []
    for attr_name in ("id", "code", "message"):
        value = getattr(error, attr_name, None)
        if value is not None:
            parts.append(f"{attr_name}={value}")
    return ", ".join(parts) if parts else str(error)


def find_known_trade_order_status(order_service: OrderService, *, order_id: int) -> str | None:
    order_id = int(order_id)

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

            trade_order_id = int(getattr(order, "orderId", 0) or 0)
            if trade_order_id != order_id:
                continue

            return str(getattr(getattr(trade, "orderStatus", None), "status", "") or "")

    return None


async def wait_for_protective_exit_order_working_or_done(
        *,
        order_service: OrderService,
        trade,
        role: str,
        order_ref: str,
) -> str:
    loop_time = asyncio.get_running_loop().time
    timeout_seconds = get_protective_order_accept_timeout_seconds()
    poll_interval_seconds = 0.10
    deadline = loop_time() + float(timeout_seconds)
    order_id = int(getattr(getattr(trade, "order", None), "orderId", 0) or 0)

    while True:
        status = str(getattr(getattr(trade, "orderStatus", None), "status", "") or "")

        if status in PROTECTIVE_EXIT_ORDER_ACCEPTED_STATUSES:
            return status

        if status in PROTECTIVE_EXIT_ORDER_FILLED_STATUSES:
            return status

        if status in PROTECTIVE_EXIT_ORDER_REJECTED_STATUSES:
            raise RuntimeError(
                f"protective {role} order was rejected/cancelled by broker: "
                f"order_id={order_id}, order_ref={order_ref}, status={status}, "
                f"ib_error={describe_ib_error(order_service.monitor.last_error(order_id))}"
            )

        if loop_time() >= deadline:
            raise RuntimeError(
                f"protective {role} order was not accepted by broker before timeout: "
                f"order_id={order_id}, order_ref={order_ref}, status={status}, "
                f"accepted_statuses={sorted(PROTECTIVE_EXIT_ORDER_ACCEPTED_STATUSES)}, "
                f"timeout_seconds={timeout_seconds}, "
                f"ib_error={describe_ib_error(order_service.monitor.last_error(order_id))}"
            )

        await asyncio.sleep(float(poll_interval_seconds))


def side_closed_by_exit_order_action(order_action: str) -> str | None:
    action = str(order_action).upper()
    if action == "SELL":
        return "LONG"
    if action == "BUY":
        return "SHORT"
    return None


async def log_safety_close_events(events) -> None:
    for event in events:
        if str(getattr(event, "log_level", "")).upper() == "WARNING":
            log_warning(logger, event.message, to_telegram=True)
        else:
            log_info(logger, event.message, to_telegram=False)


async def emergency_close_position_after_protective_failure(
        *,
        order_service: OrderService,
        instrument_code: str,
        reason: str,
) -> None:
    snapshots = await sync_broker_positions_once(order_service.ib)
    snapshot = find_snapshot(snapshots, instrument_code=str(instrument_code))

    if snapshot is None or str(snapshot.side).upper() not in {"LONG", "SHORT"} or float(snapshot.quantity) <= 0.0:
        log_warning(
            logger,
            f"{instrument_code}: protective safety market close skipped because broker position is already FLAT; reason={reason}",
            to_telegram=True,
        )
        return

    close_events = await close_market_safely(
        order_service=order_service,
        snapshot=snapshot,
        source_signal_id=PROTECTIVE_ORDER_SAFETY_SOURCE_SIGNAL_ID,
        intent_source=PROTECTIVE_ORDER_SAFETY_INTENT_SOURCE,
        reason=reason,
        now_ts=int(time.time()),
    )
    await log_safety_close_events(close_events)


async def run_protective_order_price_watchdog_once(*, order_service: OrderService) -> None:
    if not is_protective_order_price_watchdog_enabled():
        return

    now_ts = int(time.time())
    conn = get_trade_db_connection()

    try:
        active_orders = read_active_protective_orders(conn)
    finally:
        conn.close()

    stop_orders = [
        order for order in active_orders
        if str(order.get("role", "")).upper() == PROTECTIVE_ORDER_ROLE_STOP_LOSS
        and order.get("stop_price") is not None
    ]

    if not stop_orders:
        return

    for stop_order in stop_orders:
        instrument_code = str(stop_order["instrument_code"])
        side = side_closed_by_exit_order_action(str(stop_order.get("order_action", "")))

        if side is None:
            continue

        start_ts = int(stop_order.get("created_at_ts") or now_ts)
        latest_bar = read_latest_feature_bar(
            instrument_code=instrument_code,
            start_ts=start_ts,
            now_ts=now_ts,
        )

        if latest_bar is None:
            if not is_protective_order_stale_price_fail_safe_enabled():
                continue

            reason = (
                f"{PROTECTIVE_ORDER_SAFETY_PRICE_PATH_STALE_REASON}; "
                f"protective_order_id={stop_order['protective_order_id']}; "
                f"order_id={stop_order['order_id']}; latest_bar_ts=missing"
            )
            log_warning(
                logger,
                (
                    f"{instrument_code}: protective STOP watchdog cannot read fresh price path: "
                    f"order_id={stop_order['order_id']}, stop_loss={float(stop_order['stop_price']):.2f}; "
                    "market close will be sent"
                ),
                to_telegram=True,
            )
            await emergency_close_position_after_protective_failure(
                order_service=order_service,
                instrument_code=instrument_code,
                reason=reason,
            )
            continue

        latest_age_seconds = int(now_ts) - int(latest_bar["bar_time_ts"])
        stale_max_seconds = get_protective_order_price_stale_max_seconds()

        if latest_age_seconds > stale_max_seconds:
            if not is_protective_order_stale_price_fail_safe_enabled():
                continue

            reason = (
                f"{PROTECTIVE_ORDER_SAFETY_PRICE_PATH_STALE_REASON}; "
                f"protective_order_id={stop_order['protective_order_id']}; "
                f"order_id={stop_order['order_id']}; "
                f"latest_bar_ts={int(latest_bar['bar_time_ts'])}; "
                f"age_seconds={latest_age_seconds}; max_age_seconds={stale_max_seconds}"
            )
            log_warning(
                logger,
                (
                    f"{instrument_code}: protective STOP watchdog price path is stale: "
                    f"order_id={stop_order['order_id']}, latest_bar_ts={int(latest_bar['bar_time_ts'])}, "
                    f"age_seconds={latest_age_seconds}, max_age_seconds={stale_max_seconds}; "
                    "market close will be sent"
                ),
                to_telegram=True,
            )
            await emergency_close_position_after_protective_failure(
                order_service=order_service,
                instrument_code=instrument_code,
                reason=reason,
            )
            continue

        touch_row = read_first_level_touch_row(
            instrument_code=instrument_code,
            side=side,
            level_kind="STOP",
            level_price=float(stop_order["stop_price"]),
            start_ts=start_ts,
            now_ts=now_ts,
        )

        if touch_row is None:
            continue

        reason = (
            f"{PROTECTIVE_ORDER_SAFETY_STOP_BREACHED_REASON}; "
            f"protective_order_id={stop_order['protective_order_id']}; "
            f"order_id={stop_order['order_id']}; "
            f"stop_loss={float(stop_order['stop_price']):.2f}; "
            f"trigger_price={float(touch_row['trigger_price']):.2f}; "
            f"trigger_bar_ts={int(touch_row['bar_time_ts'])}"
        )
        log_warning(
            logger,
            (
                f"{instrument_code}: protective STOP watchdog detected STOP breach: "
                f"order_id={stop_order['order_id']}, "
                f"stop_loss={float(stop_order['stop_price']):.2f}, "
                f"trigger_price={float(touch_row['trigger_price']):.2f}, "
                f"trigger_bar_ts={int(touch_row['bar_time_ts'])}; market close will be sent"
            ),
            to_telegram=True,
        )
        await emergency_close_position_after_protective_failure(
            order_service=order_service,
            instrument_code=instrument_code,
            reason=reason,
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
            to_telegram=True,
        )
        await emergency_close_position_after_protective_failure(
            order_service=order_service,
            instrument_code=instrument_code,
            reason="protective_order_unsupported_quantity_market_close",
        )
        return

    placed_order_ids: list[int] = []

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
            log_warning(
                logger,
                (
                    f"{instrument_code}: protective orders skipped because both TP and SL are disabled; "
                    f"trade_intent={intent.trade_intent_id}; market close will be sent"
                ),
                to_telegram=True,
            )
            await emergency_close_position_after_protective_failure(
                order_service=order_service,
                instrument_code=instrument_code,
                reason="protective_orders_missing_tp_sl_market_close",
            )
            return

        specs: list[dict[str, Any]] = []

        # SL ставим первым: если TP почему-то не поставится, позиция всё равно сначала защищена.
        if stop_loss_price is not None:
            stop_order = order_service.api.build_stop(
                action=order_action,
                quantity=quantity,
                stop_price=float(stop_loss_price),
                time_in_force=PROTECTIVE_ORDER_TIME_IN_FORCE,
            )
            apply_protective_order_safety_flags(stop_order)
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
            apply_protective_order_safety_flags(take_profit_order)
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
            placed_order_ids.append(order_id)

            status = await wait_for_protective_exit_order_working_or_done(
                order_service=order_service,
                trade=receipt.trade,
                role=protective_role_text(str(spec["role"])),
                order_ref=str(spec["order_ref"]),
            )

            if status in PROTECTIVE_EXIT_ORDER_FILLED_STATUSES:
                raise RuntimeError(
                    f"protective {protective_role_text(str(spec['role']))} order filled during placement: "
                    f"order_id={order_id}, order_ref={spec['order_ref']}"
                )

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
            conn.commit()

            role_text = protective_role_text(str(spec["role"]))
            log_info(
                logger,
                (
                    f"{instrument_code}: {role_text} order accepted: "
                    f"parent_trade_intent={intent.trade_intent_id}, "
                    f"order_id={order_id}, action={order_action}, qty={quantity}, "
                    f"price={spec['price']}, order_ref={spec['order_ref']}, "
                    f"oca_group={oca_group}, broker_status={status}"
                ),
                to_telegram=False,
            )

    except Exception as exc:
        for order_id in placed_order_ids:
            try:
                known_status = find_known_trade_order_status(
                    order_service,
                    order_id=order_id,
                )
                if known_status in PROTECTIVE_EXIT_ORDER_FILLED_STATUSES:
                    continue

                await order_service.cancel_order_id(order_id)
                mark_protective_order_status(
                    conn,
                    order_id=order_id,
                    status="CANCELLED",
                    error_text="cancelled after failed protective order placement",
                )
            except Exception:
                pass

        conn.commit()

        log_warning(
            logger,
            (
                f"{instrument_code}: protective order submit failed; "
                f"trade_intent={intent.trade_intent_id}, "
                f"{type(exc).__name__}: {exc}; market close will be sent"
            ),
            to_telegram=True,
        )
        await emergency_close_position_after_protective_failure(
            order_service=order_service,
            instrument_code=instrument_code,
            reason=f"{PROTECTIVE_ORDER_SAFETY_SUBMIT_FAILED_REASON}; trade_intent_id={intent.trade_intent_id}; {type(exc).__name__}: {exc}",
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

        try:
            await run_protective_order_price_watchdog_once(
                order_service=order_service,
            )

        except Exception as exc:
            log_warning(
                logger,
                f"protective-order watchdog failed: {type(exc).__name__}: {exc}\n{traceback.format_exc()}",
                to_telegram=True,
            )

        try:
            slot_loss_extension_events = await run_slot_loss_extension_once(
                order_service=order_service,
            )

            for extension_event in slot_loss_extension_events:
                if str(extension_event.log_level).upper() == "WARNING":
                    log_warning(
                        logger,
                        extension_event.message,
                        to_telegram=True,
                    )
                else:
                    log_info(
                        logger,
                        extension_event.message,
                        to_telegram=False,
                    )

                if extension_event.intent is None or extension_event.result is None:
                    continue

                try:
                    await send_executed_deal_notification(
                        telegram_sender=deal_telegram_sender,
                        message_thread_id=deal_message_thread_id,
                        intent=extension_event.intent,
                        result=extension_event.result,
                    )
                except Exception as notification_exc:
                    log_warning(
                        logger,
                        (
                            f"slot-loss extension deal notification failed "
                            f"trade_intent={extension_event.intent.trade_intent_id}: "
                            f"{type(notification_exc).__name__}: {notification_exc}"
                        ),
                        to_telegram=True,
                    )

                try:
                    await send_deal_status_notification(
                        telegram_sender=deal_telegram_sender,
                        message_thread_id=deal_status_message_thread_id,
                        intent=extension_event.intent,
                        result=extension_event.result,
                    )
                except Exception as notification_exc:
                    log_warning(
                        logger,
                        (
                            f"slot-loss extension status notification failed "
                            f"trade_intent={extension_event.intent.trade_intent_id}: "
                            f"{type(notification_exc).__name__}: {notification_exc}"
                        ),
                        to_telegram=True,
                    )

        except Exception as exc:
            log_warning(
                logger,
                f"slot-loss extension failed: {type(exc).__name__}: {exc}\n{traceback.format_exc()}",
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

        try:
            execution_stats_events = await reconcile_missing_execution_stats_once(
                order_service=order_service,
            )

            for execution_stats_event in execution_stats_events:
                if str(execution_stats_event.get("event", "")).upper() != "BACKFILLED":
                    continue

                log_info(
                    logger,
                    (
                        f"{execution_stats_event['instrument_code']}: execution stats backfilled: "
                        f"trade_intent_id={execution_stats_event['trade_intent_id']}, "
                        f"order_id={execution_stats_event['order_id']}, "
                        f"avg_fill={execution_stats_event.get('avg_fill_price')}, "
                        f"commission={execution_stats_event.get('total_commission')}, "
                        f"realized_pnl={execution_stats_event.get('realized_pnl')}"
                    ),
                    to_telegram=False,
                )

                if deal_telegram_sender is None:
                    continue

                try:
                    await deal_telegram_sender.send_text(
                        format_backfilled_execution_stats_message(execution_stats_event),
                        message_thread_id=deal_message_thread_id,
                    )
                except Exception as notification_exc:
                    log_warning(
                        logger,
                        (
                            f"execution-stats backfill notification failed "
                            f"trade_intent={execution_stats_event['trade_intent_id']}: "
                            f"{type(notification_exc).__name__}: {notification_exc}"
                        ),
                        to_telegram=True,
                    )

        except Exception as exc:
            log_warning(
                logger,
                f"execution-stats reconciliation failed: {type(exc).__name__}: {exc}\n{traceback.format_exc()}",
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
