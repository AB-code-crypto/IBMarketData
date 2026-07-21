import asyncio
import time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any

from core.logger import get_logger, log_info, log_warning, setup_logging
from contracts import Instrument
from ib_execution.contract_resolver import build_execution_contract
from ib_execution.execution_models import ExecutionStatus
from ib_execution.execution_runner import (
    read_executed_trade_intent_and_result_for_notification,
    send_executed_deal_notification,
)
from ib_execution.execution_store import (
    get_trade_db_connection,
    initialize_execution_db,
)
from ib_execution.order_cancellation import (
    OrderFilledDuringCancellationError,
    cancel_order_and_require_terminal,
)
from ib_execution.order_service import OrderService
from ib_execution.uncertain_execution_reconciliation import (
    reconcile_uncertain_trade_intents_once,
)
from ib_execution.protective_order_reconciliation import (
    reconcile_protective_orders_once,
    refresh_ib_open_orders_if_possible,
)
from ib_execution.protective_order_store import (
    PROTECTIVE_ORDER_ROLE_STOP_LOSS,
    PROTECTIVE_ORDER_ROLE_TAKE_PROFIT,
    PROTECTIVE_ORDER_STATUS_ACTIVE,
    mark_protective_order_status,
    PROTECTIVE_ORDERS_TABLE_NAME,
    initialize_protective_order_db,
    read_active_protective_orders,
    record_protective_order,
    has_active_protective_stop_for_parent,
)
from ib_execution.executable_price_reader import (
    read_first_executable_level_touch_row as read_first_level_touch_row,
    read_latest_executable_bar as read_latest_price_bar,
)
from ib_execution.emergency_close import (
    close_market_safely,
    find_snapshot,
)
from ib_position_sync.position_store import sync_broker_positions_once
from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG
from ib_trader.trade_schema import TRADE_INTENTS_TABLE_NAME

setup_logging()
logger = get_logger(__name__)

EXECUTION_LOOP_SLEEP_SECONDS = 1
NEW_INTENTS_LIMIT = 20
MAX_NEW_INTENT_AGE_SECONDS = int(
    DEFAULT_SIGNAL_CONFIG.decision_pipeline_max_age_seconds
)
EXECUTION_HEARTBEAT_INTERVAL_SECONDS = 60

# Hard safety timeouts: execution-loop не должен зависать из-за IB disconnect,
# зависшего reqPositionsAsync/reqExecutionsAsync или auxiliary recovery task.
PROTECTIVE_RECONCILE_BROKER_TIMEOUT_SECONDS = 20.0
PROTECTIVE_RECONCILE_TOTAL_TIMEOUT_SECONDS = 60.0
PROTECTIVE_NOTIFICATION_TIMEOUT_SECONDS = 8.0
PROTECTIVE_WATCHDOG_TIMEOUT_SECONDS = 20.0
PROTECTIVE_EMERGENCY_SYNC_TIMEOUT_SECONDS = 8.0
PROTECTIVE_EMERGENCY_CLOSE_TIMEOUT_SECONDS = 45.0
EXECUTION_STATS_RECONCILE_TIMEOUT_SECONDS = 20.0
UNCERTAIN_EXECUTION_RECONCILE_TIMEOUT_SECONDS = 20.0
UNCERTAIN_EXECUTION_RECONCILE_INTERVAL_SECONDS = 2
PROTECTIVE_RECONCILE_INTERVAL_SECONDS = 2
EXECUTION_STATS_RECONCILE_INTERVAL_SECONDS = 30
MARKET_INTENT_EXECUTION_TIMEOUT_SECONDS = 90.0
LIMIT_INTENT_EXECUTION_TIMEOUT_EXTRA_SECONDS = 30.0
DEFAULT_LIMIT_INTENT_EXECUTION_TIMEOUT_SECONDS = 630.0

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


def calculate_take_profit_price(
        *,
        instrument_code: str,
        position_side: str,
        avg_fill_price: float,
) -> float | None:
    instrument_row = Instrument.get(str(instrument_code))
    if instrument_row is None:
        return None
    points = Decimal(str(instrument_row.get("take_profit_points", 0.0) or 0.0))
    if points <= Decimal("0"):
        return None
    entry = Decimal(str(avg_fill_price))
    tick = Decimal(str(instrument_row.get("price_tick", 0.0) or 0.0))
    side = str(position_side).upper()
    if side == "LONG":
        return normalize_price_to_tick_floor(price=entry + points, price_tick=tick)
    if side == "SHORT":
        return normalize_price_to_tick_ceiling(price=entry - points, price_tick=tick)
    return None


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
    if action not in {"OPEN_POSITION", "CLOSE_POSITION", "REVERSE_POSITION"}:
        return

    instrument_code = str(intent.instrument_code)

    # Resolve a fill/cancel that arrived just before this position change.
    pre_events = await reconcile_protective_orders_once(
        order_service=order_service,
    )
    if any(str(event.get("event", "")).upper() == "FILLED" for event in pre_events):
        raise RuntimeError(
            f"{instrument_code}: protective order filled immediately before "
            f"{action}; live broker position must be re-evaluated"
        )

    active_orders = read_active_protective_orders(
        conn,
        instrument_code=instrument_code,
    )
    if not active_orders:
        return

    for active_order in active_orders:
        order_id = int(active_order["order_id"])
        role_text = protective_role_text(
            str(active_order.get("role", "PROTECTIVE"))
        )
        context = (
            f"{instrument_code} {role_text} before {action}"
        )

        try:
            cancellation = await cancel_order_and_require_terminal(
                order_service,
                order_id=order_id,
                context=context,
            )
        except OrderFilledDuringCancellationError:
            # Persist the fill/synthetic close before aborting this stale intent.
            await reconcile_protective_orders_once(
                order_service=order_service,
            )
            raise
        except Exception as exc:
            mark_protective_order_status(
                conn,
                order_id=order_id,
                status=PROTECTIVE_ORDER_STATUS_ACTIVE,
                error_text=(
                    f"cancel not confirmed before {action}: "
                    f"{type(exc).__name__}: {exc}"
                ),
            )
            conn.commit()
            raise

        mark_protective_order_status(
            conn,
            order_id=order_id,
            status="CANCELLED",
            error_text=(
                f"broker terminal status={cancellation.terminal_status} "
                f"before {action}"
            ),
        )
        conn.commit()
        log_info(
            logger,
            (
                f"{instrument_code}: {role_text} order cancellation confirmed "
                f"before {action}: order_id={order_id}, "
                f"broker_status={cancellation.terminal_status}"
            ),
            to_telegram=False,
        )



def get_protective_order_accept_timeout_seconds() -> float:
    value = float(DEFAULT_SIGNAL_CONFIG.protective_order_accept_timeout_seconds)
    if value <= 0.0:
        raise ValueError(f"protective_order_accept_timeout_seconds must be positive: {value}")
    return value


def is_protective_order_price_watchdog_enabled() -> bool:
    return bool(DEFAULT_SIGNAL_CONFIG.protective_order_price_watchdog_enabled)


def is_protective_order_stale_price_fail_safe_enabled() -> bool:
    return bool(DEFAULT_SIGNAL_CONFIG.protective_order_price_watchdog_stale_close_enabled)


def get_protective_order_price_stale_max_seconds() -> int:
    value = int(DEFAULT_SIGNAL_CONFIG.protective_order_price_stale_max_seconds)
    if value <= 0:
        raise ValueError(f"protective_order_price_stale_max_seconds must be positive: {value}")
    return value


def apply_protective_order_safety_flags(
        order,
        *,
        instrument_code: str,
        role: str,
) -> None:
    instrument_row = Instrument.get(str(instrument_code))
    sec_type = "" if instrument_row is None else str(instrument_row.get("secType", "")).upper()
    role_value = str(role).upper()

    # Для FUT outsideRth критичен для STOP: без него IB может удерживать стоп
    # до начала RTH и присылать code=399. Для LIMIT take-profit этот флаг
    # игнорируется IB и создаёт только benign code=2109, поэтому его не ставим.
    if sec_type == "FUT" and role_value != PROTECTIVE_ORDER_ROLE_STOP_LOSS:
        return

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


def find_known_trade_order_status(
        order_service: OrderService,
        *,
        order_id: int,
) -> str | None:
    order_id = int(order_id)
    for trade in list(order_service.ib.openTrades() or []):
        order = getattr(trade, "order", None)
        if order is not None and int(getattr(order, "orderId", 0) or 0) == order_id:
            return str(getattr(getattr(trade, "orderStatus", None), "status", "") or "")

    terminal = PROTECTIVE_EXIT_ORDER_FILLED_STATUSES | PROTECTIVE_EXIT_ORDER_REJECTED_STATUSES
    for trade in list(order_service.ib.trades() or []):
        order = getattr(trade, "order", None)
        if order is None or int(getattr(order, "orderId", 0) or 0) != order_id:
            continue
        status = str(getattr(getattr(trade, "orderStatus", None), "status", "") or "")
        if status in terminal:
            return status
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
        role_value = str(role).lower()
        held_until_rth_error = order_service.monitor.find_error(order_id, code=399)

        # code=399 для protective STOP означает, что ордер принят в PreSubmitted,
        # но будет реально выставлен только после начала RTH. Такая позиция сейчас
        # не защищена, поэтому это не benign warning.
        if (
                role_value in {"stop-loss", "stop_loss"}
                and held_until_rth_error is not None
        ):
            raise RuntimeError(
                f"protective {role} order is held until RTH by broker: "
                f"order_id={order_id}, order_ref={order_ref}, status={status}, "
                f"ib_error={describe_ib_error(held_until_rth_error)}"
            )

        if status in PROTECTIVE_EXIT_ORDER_ACCEPTED_STATUSES:
            # Даём errorEvent короткое время дойти после смены статуса.
            if role_value in {"stop-loss", "stop_loss"}:
                await asyncio.sleep(0.50)
                held_until_rth_error = order_service.monitor.find_error(order_id, code=399)

                if held_until_rth_error is not None:
                    raise RuntimeError(
                        f"protective {role} order is held until RTH by broker: "
                        f"order_id={order_id}, order_ref={order_ref}, status={status}, "
                        f"ib_error={describe_ib_error(held_until_rth_error)}"
                    )

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


def is_ib_api_connected(order_service: OrderService) -> bool:
    try:
        return bool(order_service.ib.isConnected())
    except Exception:
        return False


def get_trade_intent_execution_timeout_seconds(intent) -> float:
    order_type = str(getattr(intent, "order_type", "") or "").upper()

    if order_type == "LIMIT":
        ttl_seconds = getattr(intent, "ttl_seconds", None)

        if ttl_seconds is not None and int(ttl_seconds) > 0:
            return float(int(ttl_seconds) + LIMIT_INTENT_EXECUTION_TIMEOUT_EXTRA_SECONDS)

        return DEFAULT_LIMIT_INTENT_EXECUTION_TIMEOUT_SECONDS

    return MARKET_INTENT_EXECUTION_TIMEOUT_SECONDS


async def emergency_close_position_after_protective_failure(
        *,
        order_service: OrderService,
        instrument_code: str,
        reason: str,
) -> None:
    # Fail-safe market close для protective watchdog.
    #
    # Важное правило: если IB сейчас disconnected, не делаем reqPositionsAsync/market order.
    # Иначе можно подвесить execution-loop или засыпать лог ConnectionError-ами.
    # Следующая итерация watchdog повторит попытку после reconnect.
    if not is_ib_api_connected(order_service):
        log_warning(
            logger,
            (
                f"{instrument_code}: protective safety market close deferred because IB API is disconnected; "
                f"reason={reason}"
            ),
            to_telegram=False,
        )
        return

    try:
        snapshots = await asyncio.wait_for(
            sync_broker_positions_once(
                    order_service.ib,
                    expected_account_id=order_service.account_id,
                    force_refresh=True,
                ),
            timeout=float(PROTECTIVE_EMERGENCY_SYNC_TIMEOUT_SECONDS),
        )

    except Exception as exc:
        log_warning(
            logger,
            (
                f"{instrument_code}: protective safety market close deferred because broker position sync failed: "
                f"{type(exc).__name__}: {exc}; reason={reason}"
            ),
            to_telegram=False,
        )
        return

    snapshot = find_snapshot(snapshots, instrument_code=str(instrument_code))

    if snapshot is None or str(snapshot.side).upper() not in {"LONG", "SHORT"} or float(snapshot.quantity) <= 0.0:
        log_warning(
            logger,
            f"{instrument_code}: protective safety market close skipped because broker position is already FLAT; reason={reason}",
            to_telegram=True,
        )
        return

    if not is_ib_api_connected(order_service):
        log_warning(
            logger,
            (
                f"{instrument_code}: protective safety market close deferred because IB disconnected after "
                f"position sync; reason={reason}"
            ),
            to_telegram=False,
        )
        return

    try:
        close_events = await asyncio.wait_for(
            close_market_safely(
                order_service=order_service,
                snapshot=snapshot,
                source_signal_id=PROTECTIVE_ORDER_SAFETY_SOURCE_SIGNAL_ID,
                intent_source=PROTECTIVE_ORDER_SAFETY_INTENT_SOURCE,
                reason=reason,
                now_ts=int(time.time()),
            ),
            timeout=float(PROTECTIVE_EMERGENCY_CLOSE_TIMEOUT_SECONDS),
        )

    except Exception as exc:
        log_warning(
            logger,
            (
                f"{instrument_code}: protective safety market close failed/deferred: "
                f"{type(exc).__name__}: {exc}; reason={reason}"
            ),
            to_telegram=True,
        )
        return

    await log_safety_close_events(close_events)


async def run_protective_order_price_watchdog_once(*, order_service: OrderService) -> None:
    if not is_protective_order_price_watchdog_enabled():
        return

    # Если IB disconnected, watchdog не должен лезть в emergency close.
    # Проверку цены повторим после reconnect.
    if not is_ib_api_connected(order_service):
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
        and str(order.get("status", "")).upper() == "ACTIVE"
        and order.get("stop_price") is not None
    ]

    if not stop_orders:
        return

    for stop_order in stop_orders:
        instrument_code = str(stop_order["instrument_code"])
        order_ref = str(stop_order.get("order_ref", "") or "")

        # Legacy auxiliary exit rows may remain in an old trade DB. The rolling-only
        # robot never creates them, and the ordinary stop watchdog must ignore them.
        if order_ref.endswith(("_EXT_SL", "_EXT_TP")):
            continue

        side = side_closed_by_exit_order_action(str(stop_order.get("order_action", "")))

        if side is None:
            continue

        start_ts = int(stop_order.get("created_at_ts") or now_ts)
        stale_max_seconds = get_protective_order_price_stale_max_seconds()
        latest_start_ts = max(0, int(now_ts) - stale_max_seconds * 2)

        latest_bar = read_latest_price_bar(
            instrument_code=instrument_code,
            start_ts=latest_start_ts,
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
            apply_protective_order_safety_flags(
                stop_order,
                instrument_code=instrument_code,
                role=PROTECTIVE_ORDER_ROLE_STOP_LOSS,
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
            apply_protective_order_safety_flags(
                take_profit_order,
                instrument_code=instrument_code,
                role=PROTECTIVE_ORDER_ROLE_TAKE_PROFIT,
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
            except Exception as cleanup_exc:
                log_warning(
                    logger,
                    (
                        f"{instrument_code}: failed to cancel partially placed protective order: "
                        f"order_id={order_id}, {type(cleanup_exc).__name__}: {cleanup_exc}"
                    ),
                    to_telegram=True,
                )

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


def read_executed_entries_missing_protective_stop() -> list[dict[str, Any]]:
    conn = get_trade_db_connection()

    try:
        initialize_execution_db(conn)
        initialize_protective_order_db(conn)
        rows = conn.execute(
            f"""
            SELECT
                ti.trade_intent_id,
                ti.instrument_code,
                ti.target_side,
                ti.target_qty
            FROM {TRADE_INTENTS_TABLE_NAME} AS ti
            WHERE ti.status = 'EXECUTED'
              AND ti.action IN ('OPEN_POSITION', 'REVERSE_POSITION')
              AND ti.target_side IN ('LONG', 'SHORT')
              AND ti.target_qty > 0
              AND ti.trade_intent_id = (
                  SELECT ti2.trade_intent_id
                  FROM {TRADE_INTENTS_TABLE_NAME} AS ti2
                  WHERE ti2.instrument_code = ti.instrument_code
                    AND ti2.status = 'EXECUTED'
                    AND ti2.action IN (
                        'OPEN_POSITION',
                        'REVERSE_POSITION',
                        'CLOSE_POSITION'
                    )
                  ORDER BY
                    COALESCE(
                        ti2.finished_at_ts,
                        ti2.sent_at_ts,
                        ti2.updated_at_ts,
                        ti2.created_at_ts
                    ) DESC,
                    ti2.trade_intent_id DESC
                  LIMIT 1
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM {PROTECTIVE_ORDERS_TABLE_NAME} AS po
                  WHERE po.parent_trade_intent_id = ti.trade_intent_id
                    AND po.role = 'STOP_LOSS'
                    AND po.status IN ('ACTIVE', 'UNPROTECTED')
              )
            ORDER BY ti.trade_intent_id
            """
        ).fetchall()

        return [
            {
                "trade_intent_id": int(row[0]),
                "instrument_code": str(row[1]),
                "target_side": str(row[2]).upper(),
                "target_qty": float(row[3]),
            }
            for row in rows
        ]

    finally:
        conn.close()


async def adopt_live_protective_orders_for_intent(
        *,
        conn,
        order_service: OrderService,
        intent,
) -> bool:
    """Adopt a live protective set only when a valid broker STOP exists.

    A TP-only remainder is cancelled and confirmed before a fresh pair is placed;
    otherwise a second TP could later open an unintended reverse position.
    """
    refresh_ok, _ = await refresh_ib_open_orders_if_possible(order_service)
    if not refresh_ok:
        return False

    expected_refs = {
        f"{intent.order_ref}{STOP_LOSS_ORDER_REF_SUFFIX}": PROTECTIVE_ORDER_ROLE_STOP_LOSS,
        f"{intent.order_ref}{TAKE_PROFIT_ORDER_REF_SUFFIX}": PROTECTIVE_ORDER_ROLE_TAKE_PROFIT,
    }
    expected_action = "SELL" if str(intent.target_side).upper() == "LONG" else "BUY"
    expected_quantity = int(float(intent.target_qty))

    open_trades = list(order_service.ib.openTrades() or [])

    matching_orders: list[dict[str, Any]] = []

    for trade in open_trades:
        order = getattr(trade, "order", None)
        if order is None:
            continue

        order_ref = str(getattr(order, "orderRef", "") or "")
        role = expected_refs.get(order_ref)
        if role is None:
            continue

        status = str(
            getattr(getattr(trade, "orderStatus", None), "status", "") or ""
        )
        if status not in PROTECTIVE_EXIT_ORDER_ACCEPTED_STATUSES:
            continue

        order_id = int(getattr(order, "orderId", 0) or 0)
        quantity = int(float(getattr(order, "totalQuantity", 0) or 0))
        order_action = str(getattr(order, "action", "") or "").upper()
        if order_id <= 0 or quantity <= 0:
            continue

        matching_orders.append({
            "trade": trade,
            "role": role,
            "order": order,
            "order_ref": order_ref,
            "order_id": order_id,
            "quantity": quantity,
            "order_action": order_action,
            "is_valid": (
                quantity == expected_quantity
                and order_action == expected_action
            ),
        })

    valid_stop_exists = any(
        item["role"] == PROTECTIVE_ORDER_ROLE_STOP_LOSS
        and bool(item["is_valid"])
        for item in matching_orders
    )

    if not valid_stop_exists:
        for item in matching_orders:
            order_id = int(item["order_id"])
            await order_service.cancel_order_id(order_id)
            done = await order_service.monitor.wait_for_done(
                item["trade"],
                timeout=5.0,
                poll_interval=0.10,
            )
            if not done.done or str(done.status) not in {"Cancelled", "ApiCancelled"}:
                raise RuntimeError(
                    f"could not confirm cancellation of leftover protective order: "
                    f"order_id={order_id}, status={done.status}, timed_out={done.timed_out}"
                )
        return False

    for item in matching_orders:
        if not bool(item["is_valid"]):
            continue

        order = item["order"]
        role = str(item["role"])
        order_type = str(getattr(order, "orderType", "") or "").upper()
        limit_price = getattr(order, "lmtPrice", None)
        stop_price = getattr(order, "auxPrice", None)
        oca_group = str(getattr(order, "ocaGroup", "") or "") or None

        record_protective_order(
            conn,
            instrument_code=intent.instrument_code,
            parent_trade_intent_id=int(intent.trade_intent_id),
            role=role,
            order_ref=str(item["order_ref"]),
            order_id=int(item["order_id"]),
            order_action=str(item["order_action"]),
            order_quantity=int(item["quantity"]),
            order_type=order_type or (
                "STP" if role == PROTECTIVE_ORDER_ROLE_STOP_LOSS else "LIMIT"
            ),
            limit_price=(
                None
                if role == PROTECTIVE_ORDER_ROLE_STOP_LOSS
                else float(limit_price)
            ),
            stop_price=(
                float(stop_price)
                if role == PROTECTIVE_ORDER_ROLE_STOP_LOSS
                else None
            ),
            oca_group=oca_group,
        )

    conn.commit()
    return True


async def restore_executed_entries_missing_protection(
        *,
        order_service: OrderService,
) -> None:
    candidates = read_executed_entries_missing_protective_stop()
    if not candidates:
        return

    snapshots = await asyncio.wait_for(
        sync_broker_positions_once(
                    order_service.ib,
                    expected_account_id=order_service.account_id,
                    force_refresh=True,
                ),
        timeout=float(PROTECTIVE_EMERGENCY_SYNC_TIMEOUT_SECONDS),
    )

    for candidate in candidates:
        snapshot = find_snapshot(
            snapshots,
            instrument_code=candidate["instrument_code"],
        )
        snapshot_side = (
            "FLAT"
            if snapshot is None
            else str(snapshot.side).upper()
        )
        snapshot_qty = (
            0.0
            if snapshot is None
            else float(snapshot.quantity)
        )

        if (
                snapshot_side != candidate["target_side"]
                or abs(snapshot_qty - candidate["target_qty"]) > 1e-9
        ):
            continue

        intent, result = read_executed_trade_intent_and_result_for_notification(
            trade_intent_id=candidate["trade_intent_id"],
        )
        if intent is None or result is None:
            continue

        conn = get_trade_db_connection()
        try:
            initialize_execution_db(conn)

            if has_active_protective_stop_for_parent(
                    conn,
                    parent_trade_intent_id=intent.trade_intent_id,
            ):
                continue

            try:
                adopted_stop = await adopt_live_protective_orders_for_intent(
                    conn=conn,
                    order_service=order_service,
                    intent=intent,
                )
            except Exception as exc:
                await emergency_close_position_after_protective_failure(
                    order_service=order_service,
                    instrument_code=intent.instrument_code,
                    reason=(
                        "missing_protection_recovery_could_not_clean_leftover_orders; "
                        f"trade_intent_id={intent.trade_intent_id}; "
                        f"{type(exc).__name__}: {exc}"
                    ),
                )
                continue

            if adopted_stop:
                log_info(
                    logger,
                    (
                        f"{intent.instrument_code}: adopted live protective STOP after execution restart: "
                        f"trade_intent_id={intent.trade_intent_id}"
                    ),
                    to_telegram=False,
                )
                continue

            if result.avg_fill_price is None:
                await emergency_close_position_after_protective_failure(
                    order_service=order_service,
                    instrument_code=intent.instrument_code,
                    reason=(
                        "executed_entry_missing_protection_and_avg_fill_market_close; "
                        f"trade_intent_id={intent.trade_intent_id}; "
                        f"order_id={result.order_id}"
                    ),
                )
                continue

            await place_protective_orders_after_entry(
                conn=conn,
                order_service=order_service,
                intent=intent,
                result=result,
            )
            conn.commit()
            log_warning(
                logger,
                (
                    f"{intent.instrument_code}: restored missing protective orders after execution restart: "
                    f"trade_intent_id={intent.trade_intent_id}, order_id={result.order_id}"
                ),
                to_telegram=True,
            )

        finally:
            conn.close()

async def reconcile_uncertain_executions_and_restore_protection(
        *,
        order_service: OrderService,
) -> None:
    events = await reconcile_uncertain_trade_intents_once(
        order_service=order_service,
    )

    for event in events:
        if str(event.log_level).upper() == "WARNING":
            log_warning(logger, event.message, to_telegram=True)
        else:
            log_info(logger, event.message, to_telegram=False)

        if (
                event.result is None
                or event.result.status != ExecutionStatus.EXECUTED
                or not event.needs_protection
        ):
            continue

        conn = get_trade_db_connection()
        try:
            initialize_execution_db(conn)

            snapshots = await asyncio.wait_for(
                sync_broker_positions_once(
                    order_service.ib,
                    expected_account_id=order_service.account_id,
                    force_refresh=True,
                ),
                timeout=float(PROTECTIVE_EMERGENCY_SYNC_TIMEOUT_SECONDS),
            )
            snapshot = find_snapshot(
                snapshots,
                instrument_code=event.intent.instrument_code,
            )
            snapshot_side = (
                "FLAT"
                if snapshot is None
                else str(snapshot.side).upper()
            )
            snapshot_qty = (
                0.0
                if snapshot is None
                else float(snapshot.quantity)
            )

            if (
                    snapshot_side != str(event.intent.target_side).upper()
                    or abs(snapshot_qty - float(event.intent.target_qty)) > 1e-9
            ):
                log_warning(
                    logger,
                    (
                        f"{event.intent.instrument_code}: recovered execution will not receive "
                        f"protective orders because broker position no longer matches target: "
                        f"trade_intent_id={event.intent.trade_intent_id}, "
                        f"target={event.intent.target_side}/{event.intent.target_qty:g}, "
                        f"broker={snapshot_side}/{snapshot_qty:g}"
                    ),
                    to_telegram=True,
                )
                continue

            if has_active_protective_stop_for_parent(
                    conn,
                    parent_trade_intent_id=event.intent.trade_intent_id,
            ):
                continue

            if event.result.avg_fill_price is None:
                await emergency_close_position_after_protective_failure(
                    order_service=order_service,
                    instrument_code=event.intent.instrument_code,
                    reason=(
                        "recovered_execution_missing_avg_fill_price_market_close; "
                        f"trade_intent_id={event.intent.trade_intent_id}; "
                        f"order_id={event.result.order_id}"
                    ),
                )
                continue

            await place_protective_orders_after_entry(
                conn=conn,
                order_service=order_service,
                intent=event.intent,
                result=event.result,
            )
            conn.commit()

        finally:
            conn.close()

    # Covers the hard-kill window after EXECUTED commit but before TP/SL persistence.
    await restore_executed_entries_missing_protection(
        order_service=order_service,
    )


async def reconcile_and_notify_protective_orders(
        *,
        order_service: OrderService,
        deal_telegram_sender=None,
        deal_message_thread_id=None,
) -> None:
    # Broker/DB reconciliation не должен зависеть от Telegram. Ограничиваем только IB-часть.
    reconciled_protective_orders = await asyncio.wait_for(
        reconcile_protective_orders_once(
            order_service=order_service,
        ),
        timeout=float(PROTECTIVE_RECONCILE_BROKER_TIMEOUT_SECONDS),
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

        if bool(reconciled_protective_order.get("requires_market_close")):
            await emergency_close_position_after_protective_failure(
                order_service=order_service,
                instrument_code=str(reconciled_protective_order["instrument_code"]),
                reason=(
                    str(reconciled_protective_order.get("reason") or "protective order unprotected")
                    + f"; protective_order_id={reconciled_protective_order.get('order_id')}"
                ),
            )
            continue

        if str(reconciled_protective_order.get("event", "")).upper() != "FILLED":
            continue

        synthetic_trade_intent_id = reconciled_protective_order.get("synthetic_trade_intent_id")

        if synthetic_trade_intent_id is None:
            continue

        try:
            if deal_telegram_sender is None:
                continue

            synthetic_intent, synthetic_result = read_executed_trade_intent_and_result_for_notification(
                trade_intent_id=int(synthetic_trade_intent_id),
            )

            if synthetic_intent is None or synthetic_result is None:
                continue

            await asyncio.wait_for(
                send_executed_deal_notification(
                    telegram_sender=deal_telegram_sender,
                    message_thread_id=deal_message_thread_id,
                    intent=synthetic_intent,
                    result=synthetic_result,
                ),
                timeout=float(PROTECTIVE_NOTIFICATION_TIMEOUT_SECONDS),
            )

        except Exception as notification_exc:
            # Не пытаемся отправлять warning в Telegram, если сам Telegram уже подвис/сломался.
            log_warning(
                logger,
                (
                    f"protective-order deal notification failed "
                    f"synthetic_trade_intent_id={synthetic_trade_intent_id}: "
                    f"{type(notification_exc).__name__}: {notification_exc}"
                ),
                to_telegram=False,
            )
