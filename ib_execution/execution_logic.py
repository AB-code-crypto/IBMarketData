import asyncio
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

from ib_execution.execution_models import ExecutionResult, ExecutionStatus, TradeIntent
from contracts import Instrument
from core.time_utils import CT_TIMEZONE
from ib_signal.signal_config import DEFAULT_SIGNAL_CONFIG
from ib_execution.contract_resolver import build_execution_contract
from ib_execution.order_service import (
    OrderRejectedError,
    OrderService,
    OrderTimeoutError,
)
from ib_execution.execution_store import (
    get_trade_db_connection,
    initialize_execution_db,
    read_trade_intent_cancel_request,
)


OrderSubmittedCallback = Callable[[int, str, int], Awaitable[None]]


class SubmittedOrderPersistenceError(RuntimeError):
    def __init__(
            self,
            *,
            order_id: int,
            order_action: str,
            order_quantity: int,
            cause: BaseException,
    ) -> None:
        super().__init__(
            f"order was submitted to IB, but local submission persistence failed: "
            f"order_id={order_id}, action={order_action}, qty={order_quantity}, "
            f"{type(cause).__name__}: {cause}"
        )
        self.order_id = int(order_id)
        self.order_action = str(order_action)
        self.order_quantity = int(order_quantity)


class OrderExecutionUncertainError(RuntimeError):
    def __init__(
            self,
            *,
            order_id: int,
            order_action: str,
            order_quantity: int,
            broker_status: str,
            filled_qty: float,
    ) -> None:
        super().__init__(
            f"market order final result is uncertain: order_id={order_id}, "
            f"action={order_action}, qty={order_quantity}, "
            f"broker_status={broker_status}, filled_qty={filled_qty:g}"
        )
        self.order_id = int(order_id)
        self.order_action = str(order_action)
        self.order_quantity = int(order_quantity)


async def persist_submitted_order(
        callback: OrderSubmittedCallback | None,
        *,
        order_id: int,
        order_action: str,
        order_quantity: int,
) -> None:
    if callback is None:
        return

    try:
        await callback(
            int(order_id),
            str(order_action),
            int(order_quantity),
        )
    except Exception as exc:
        raise SubmittedOrderPersistenceError(
            order_id=int(order_id),
            order_action=str(order_action),
            order_quantity=int(order_quantity),
            cause=exc,
        ) from exc


LIMIT_DONE_TIMEOUT_EXTRA_SECONDS = 10
DEFAULT_LIMIT_DONE_TIMEOUT_SECONDS = 600
LIMIT_CANCEL_CHECK_INTERVAL_SECONDS = 0.5
LIMIT_CANCEL_CONFIRM_TIMEOUT_SECONDS = 5.0
FUTURES_NO_NEW_TRADES_HOUR_CT = 15


def signed_qty(side: str, qty: float) -> float:
    side_value = str(side).upper()
    qty_value = float(qty)

    if side_value == "LONG":
        return qty_value

    if side_value == "SHORT":
        return -qty_value

    if side_value == "FLAT":
        return 0.0

    raise ValueError(f"Unknown side: {side!r}")


def calculate_order_delta(intent: TradeIntent) -> tuple[str, int]:
    before = signed_qty(intent.position_before_side, intent.position_before_qty)
    target = signed_qty(intent.target_side, intent.target_qty)
    delta = target - before

    if delta == 0.0:
        raise ValueError(f"TradeIntent has zero delta: id={intent.trade_intent_id}")

    abs_qty = abs(delta)

    if abs_qty != int(abs_qty):
        raise ValueError(
            f"Execution supports integer quantities only for now: id={intent.trade_intent_id}, qty={abs_qty}"
        )

    return ("BUY" if delta > 0 else "SELL"), int(abs_qty)


COMMISSION_REPORT_WAIT_TIMEOUT_SECONDS = 3.0
COMMISSION_REPORT_POLL_INTERVAL_SECONDS = 0.20

# Для protective TP/SL: если ордер уже filled, но IB долго не отдаёт commissionReport,
# не держим локальный protective order в ACTIVE бесконечно. Создаём synthetic close
# с NULL stats, а execution-stats reconciliation дозаполнит их позже.
COMMISSION_REPORT_PENDING_FINALIZE_GRACE_SECONDS = 60


def should_finalize_with_pending_commission(
        filled_at_ts: int | None,
        *,
        now_ts: int | None = None,
        grace_seconds: int = COMMISSION_REPORT_PENDING_FINALIZE_GRACE_SECONDS,
) -> bool:
    if filled_at_ts is None:
        return False

    try:
        filled_ts = int(filled_at_ts)
    except (TypeError, ValueError):
        return False

    now_value = int(time.time() if now_ts is None else now_ts)
    return now_value - filled_ts >= int(grace_seconds)


def _fill_execution(fill):
    return getattr(fill, "execution", None)


def _fill_size(fill) -> float:
    execution = _fill_execution(fill)

    if execution is None:
        return 0.0

    try:
        return abs(float(getattr(execution, "shares", 0.0) or 0.0))
    except (TypeError, ValueError):
        return 0.0


def collect_trade_filled_qty(trade) -> float:
    total_qty = 0.0

    for fill in list(getattr(trade, "fills", []) or []):
        total_qty += _fill_size(fill)

    return total_qty


def is_fill_commission_report_final(fill) -> bool:
    """True только когда IB commissionReport реально относится к execution.

    У ib_async/ib_insync commissionReport может существовать как default-объект:
    execId='', commission=0.0, realizedPNL=0.0. Такой объект нельзя считать
    финальной комиссией/PNL.
    """
    execution = _fill_execution(fill)

    if execution is None:
        return False

    execution_exec_id = str(getattr(execution, "execId", "") or "").strip()

    if not execution_exec_id:
        return False

    commission_report = getattr(fill, "commissionReport", None)

    if commission_report is None:
        return False

    report_exec_id = str(getattr(commission_report, "execId", "") or "").strip()

    if not report_exec_id:
        return False

    if report_exec_id != execution_exec_id:
        return False

    if getattr(commission_report, "commission", None) is None:
        return False

    if getattr(commission_report, "realizedPNL", None) is None:
        return False

    return True


def are_commission_reports_final_for_fills(fills) -> bool:
    checked_fills = 0

    for fill in list(fills or []):
        if _fill_size(fill) <= 0.0:
            continue

        checked_fills += 1

        if not is_fill_commission_report_final(fill):
            return False

    return checked_fills > 0


def are_commission_reports_final_for_trade(trade) -> bool:
    return are_commission_reports_final_for_fills(
        list(getattr(trade, "fills", []) or []),
    )


async def refresh_ib_executions_if_possible(ib) -> bool:
    try:
        req_async = getattr(ib, "reqExecutionsAsync", None)

        if req_async is not None:
            await req_async()
            return True

        req_sync = getattr(ib, "reqExecutions", None)

        if req_sync is not None:
            maybe_awaitable = req_sync()

            if asyncio.iscoroutine(maybe_awaitable):
                await maybe_awaitable

            return True

    except Exception:
        return False

    return False


async def wait_for_commission_reports(
        order_service,
        trade,
        *,
        timeout_seconds: float = COMMISSION_REPORT_WAIT_TIMEOUT_SECONDS,
        poll_interval_seconds: float = COMMISSION_REPORT_POLL_INTERVAL_SECONDS,
) -> bool:
    """Ждёт, пока для всех фактических fills появятся финальные commissionReport.

    Возвращает False по timeout. Это не ломает исполнение ордера: результат всё равно
    будет записан, но commission/realized_pnl останутся NULL вместо ложных 0.0.
    """
    if collect_trade_filled_qty(trade) <= 0.0:
        return True

    loop_time = asyncio.get_running_loop().time
    deadline = loop_time() + float(timeout_seconds)

    while True:
        if are_commission_reports_final_for_trade(trade):
            return True

        if loop_time() >= deadline:
            return False

        await refresh_ib_executions_if_possible(order_service.ib)
        await asyncio.sleep(float(poll_interval_seconds))


def collect_trade_fill_statistics(trade) -> tuple[float | None, float | None, float | None, float]:
    """Что делает: собирает avg_fill_price/commission/pnl/filled_qty из trade.fills.
    Зачем нужна: LIMIT теперь ждёт терминального состояния и должен записать факт fill, если он был."""
    fills = list(getattr(trade, "fills", []) or [])

    total_qty = 0.0
    total_notional = 0.0
    total_commission = 0.0
    total_realized_pnl = 0.0
    has_commission = False
    has_pnl = False

    for fill in fills:
        execution = getattr(fill, "execution", None)

        if execution is None:
            continue

        size = abs(float(getattr(execution, "shares", 0.0) or 0.0))
        price = float(getattr(execution, "price", 0.0) or 0.0)

        if size <= 0.0:
            continue

        total_qty += size
        total_notional += price * size

        commission_report = getattr(fill, "commissionReport", None)

        if is_fill_commission_report_final(fill):
            commission = getattr(commission_report, "commission", None)
            realized_pnl = getattr(commission_report, "realizedPNL", None)

            if commission is not None:
                total_commission += float(commission)
                has_commission = True

            if realized_pnl is not None:
                total_realized_pnl += float(realized_pnl)
                has_pnl = True

    avg_fill_price = total_notional / total_qty if total_qty > 0.0 else None

    return (
        avg_fill_price,
        total_commission if has_commission else None,
        total_realized_pnl if has_pnl else None,
        total_qty,
    )


def build_execution_result(
        *,
        intent: TradeIntent,
        order_id: int | None,
        order_action: str | None,
        order_quantity: int | None,
        status: ExecutionStatus,
        trade=None,
        error_text: str | None = None,
) -> ExecutionResult:
    avg_fill_price = None
    total_commission = None
    realized_pnl = None

    if trade is not None:
        avg_fill_price, total_commission, realized_pnl, _ = collect_trade_fill_statistics(trade)

    return ExecutionResult(
        trade_intent_id=intent.trade_intent_id,
        order_id=order_id,
        order_action=order_action,
        order_quantity=order_quantity,
        status=status,
        avg_fill_price=avg_fill_price,
        total_commission=total_commission,
        realized_pnl=realized_pnl,
        error_text=error_text,
    )


def get_limit_done_timeout_seconds(intent: TradeIntent) -> float:
    if intent.ttl_seconds is not None and int(intent.ttl_seconds) > 0:
        return float(int(intent.ttl_seconds) + LIMIT_DONE_TIMEOUT_EXTRA_SECONDS)

    return float(DEFAULT_LIMIT_DONE_TIMEOUT_SECONDS)


def classify_limit_terminal_status(
        *,
        intent: TradeIntent,
        ib_status: str,
        timed_out: bool,
        filled_qty: float,
        expected_qty: int,
) -> tuple[ExecutionStatus, str | None]:
    """Что делает: переводит финальный IB status лимитника в статус trade_intent.
    Зачем нужна: ACCEPTED — не финал; после него нужен EXECUTED/EXPIRED/CANCELLED/FAILED."""
    status_text = str(ib_status or "")

    if filled_qty >= float(expected_qty):
        return ExecutionStatus.EXECUTED, None

    if filled_qty > 0.0:
        return (
            ExecutionStatus.EXECUTED,
            (
                f"partial fill detected: filled_qty={filled_qty}, "
                f"expected_qty={expected_qty}, ib_status={status_text}"
            ),
        )

    if timed_out:
        return (
            ExecutionStatus.EXPIRED,
            (
                f"limit order wait timed out before terminal IB status; "
                f"timeout_seconds={get_limit_done_timeout_seconds(intent)}"
            ),
        )

    if status_text in {"Cancelled", "ApiCancelled"}:
        if intent.ttl_seconds is not None:
            return ExecutionStatus.EXPIRED, f"limit order expired/cancelled by IB: status={status_text}"

        return ExecutionStatus.CANCELLED, f"limit order cancelled by IB: status={status_text}"

    if status_text in {"Inactive", "Rejected"}:
        return ExecutionStatus.FAILED, f"limit order failed: status={status_text}"

    return ExecutionStatus.FAILED, f"unexpected limit terminal status: {status_text}"


async def execute_market_intent(
        *,
        order_service: OrderService,
        intent: TradeIntent,
        contract,
        order_action: str,
        quantity: int,
        order_ref: str,
        order_submitted_callback: OrderSubmittedCallback | None = None,
) -> ExecutionResult:
    if order_action == "BUY":
        placement = await order_service.buy_market(
            contract=contract,
            quantity=quantity,
            order_ref=order_ref,
            wait="none",
        )
    else:
        placement = await order_service.sell_market(
            contract=contract,
            quantity=quantity,
            order_ref=order_ref,
            wait="none",
        )

    order_id = int(placement.receipt.order_id)
    trade = placement.receipt.trade

    await persist_submitted_order(
        order_submitted_callback,
        order_id=order_id,
        order_action=order_action,
        order_quantity=quantity,
    )

    done = await order_service.monitor.wait_for_done(
        trade,
        timeout=60.0,
        poll_interval=0.10,
    )

    if not done.done:
        raise OrderTimeoutError(
            order_id=order_id,
            stage="done",
            status=done.status,
        )

    filled_qty = collect_trade_filled_qty(trade)
    broker_status = str(done.status or "")

    if broker_status in {"Cancelled", "ApiCancelled", "Inactive", "Rejected"} and filled_qty <= 0.0:
        raise OrderRejectedError(
            order_id=order_id,
            status=broker_status,
            error=order_service.monitor.last_error(order_id),
        )

    if broker_status != "Filled" or filled_qty < float(quantity):
        raise OrderExecutionUncertainError(
            order_id=order_id,
            order_action=order_action,
            order_quantity=quantity,
            broker_status=broker_status,
            filled_qty=filled_qty,
        )

    await wait_for_commission_reports(
        order_service,
        trade,
    )

    return build_execution_result(
        intent=intent,
        order_id=order_id,
        order_action=order_action,
        order_quantity=quantity,
        status=ExecutionStatus.EXECUTED,
        trade=trade,
        error_text=None,
    )



def get_ct_day_ts(*, now_ts: int, hour: int, minute: int = 0, second: int = 0) -> int:
    now_ct = datetime.fromtimestamp(int(now_ts), tz=timezone.utc).astimezone(CT_TIMEZONE)
    target_ct = now_ct.replace(
        hour=int(hour),
        minute=int(minute),
        second=int(second),
        microsecond=0,
    )
    return int(target_ct.astimezone(timezone.utc).timestamp())


def get_futures_limit_cutoff_ts(*, instrument_code: str, now_ts: int) -> int | None:
    """Что делает: возвращает 14:59:50 CT для futures, после чего лимитники жить не должны.
    Зачем нужна: лимитные заявки не должны переноситься в последний час перед клирингом."""
    instrument_row = Instrument.get(str(instrument_code))

    if instrument_row is None:
        return None

    if str(instrument_row.get("secType", "")).upper() != "FUT":
        return None

    no_new_trades_ts = get_ct_day_ts(
        now_ts=now_ts,
        hour=FUTURES_NO_NEW_TRADES_HOUR_CT,
    )
    return int(no_new_trades_ts) - int(DEFAULT_SIGNAL_CONFIG.slot_close_before_end_seconds)


def clamp_limit_ttl_seconds_for_futures_cutoff(intent: TradeIntent) -> int | None:
    """Что делает: режет TTL лимитника так, чтобы он не жил после 14:59:50 CT.
    Зачем нужна: к началу последнего часа все лимитники по futures должны быть удалены."""
    ttl_seconds = (
        int(intent.ttl_seconds)
        if intent.ttl_seconds is not None and int(intent.ttl_seconds) > 0
        else None
    )

    now_ts = int(time.time())
    cutoff_ts = get_futures_limit_cutoff_ts(
        instrument_code=intent.instrument_code,
        now_ts=now_ts,
    )

    if cutoff_ts is None:
        return ttl_seconds

    seconds_to_cutoff = int(cutoff_ts) - now_ts

    if seconds_to_cutoff <= 0:
        return 1

    if ttl_seconds is None:
        return seconds_to_cutoff

    return max(1, min(ttl_seconds, seconds_to_cutoff))


def read_cancel_request_for_trade_intent(trade_intent_id: int) -> dict | None:
    conn = get_trade_db_connection()

    try:
        initialize_execution_db(conn)
        return read_trade_intent_cancel_request(
            conn,
            trade_intent_id=int(trade_intent_id),
        )

    finally:
        conn.close()


async def wait_for_limit_done_or_cancel(
        *,
        order_service: OrderService,
        intent: TradeIntent,
        trade,
        order_id: int,
        timeout_seconds: float,
):
    loop_time = __import__("asyncio").get_running_loop().time
    deadline = loop_time() + float(timeout_seconds)
    cancel_request = None
    last_done = None

    while True:
        remaining_seconds = max(0.0, deadline - loop_time())
        poll_timeout = min(float(LIMIT_CANCEL_CHECK_INTERVAL_SECONDS), remaining_seconds)

        if poll_timeout <= 0.0:
            done = await order_service.monitor.wait_for_done(
                trade,
                timeout=0.0,
                poll_interval=0.10,
            )
            return done, cancel_request

        done = await order_service.monitor.wait_for_done(
            trade,
            timeout=poll_timeout,
            poll_interval=0.10,
        )
        last_done = done

        if done.done:
            return done, cancel_request

        cancel_request = read_cancel_request_for_trade_intent(intent.trade_intent_id)

        if cancel_request is None:
            continue

        try:
            await order_service.cancel_order_id(order_id)
        except Exception:
            # Даже если cancel_order_id упал, ещё раз проверим терминальный статус ниже.
            pass

        done = await order_service.monitor.wait_for_done(
            trade,
            timeout=float(LIMIT_CANCEL_CONFIRM_TIMEOUT_SECONDS),
            poll_interval=0.10,
        )
        return done, cancel_request


async def execute_limit_intent(
        *,
        order_service: OrderService,
        intent: TradeIntent,
        contract,
        order_action: str,
        quantity: int,
        order_ref: str,
        order_submitted_callback: OrderSubmittedCallback | None = None,
) -> ExecutionResult:
    """Что делает: ставит LIMIT и ждёт финал до ttl_seconds + запас.
    Зачем нужна: trade_intents не должны навечно зависать в ACCEPTED после отмены/expiry."""
    if intent.limit_price is None:
        raise ValueError(f"LIMIT intent without limit_price: id={intent.trade_intent_id}")

    effective_ttl_seconds = clamp_limit_ttl_seconds_for_futures_cutoff(intent)

    if order_action == "BUY":
        placement = await order_service.buy_limit(
            contract=contract,
            quantity=quantity,
            limit_price=float(intent.limit_price),
            order_ref=order_ref,
            ttl_seconds=effective_ttl_seconds,
            wait="none",
        )
    else:
        placement = await order_service.sell_limit(
            contract=contract,
            quantity=quantity,
            limit_price=float(intent.limit_price),
            order_ref=order_ref,
            ttl_seconds=effective_ttl_seconds,
            wait="none",
        )

    order_id = placement.receipt.order_id
    trade = placement.receipt.trade

    await persist_submitted_order(
        order_submitted_callback,
        order_id=order_id,
        order_action=order_action,
        order_quantity=quantity,
    )
    done, cancel_request = await wait_for_limit_done_or_cancel(
        order_service=order_service,
        intent=intent,
        trade=trade,
        order_id=order_id,
        timeout_seconds=float((effective_ttl_seconds or DEFAULT_LIMIT_DONE_TIMEOUT_SECONDS) + LIMIT_DONE_TIMEOUT_EXTRA_SECONDS),
    )

    if collect_trade_filled_qty(trade) > 0.0:
        await wait_for_commission_reports(order_service, trade)

    avg_fill_price, total_commission, realized_pnl, filled_qty = collect_trade_fill_statistics(trade)

    if cancel_request is not None and filled_qty <= 0.0 and str(done.status) in {"Cancelled", "ApiCancelled"}:
        status = ExecutionStatus.CANCELLED
        error_text = cancel_request.get("cancel_reason") or "limit order cancelled by cancel request"
    else:
        status, error_text = classify_limit_terminal_status(
            intent=intent,
            ib_status=done.status,
            timed_out=done.timed_out,
            filled_qty=filled_qty,
            expected_qty=quantity,
        )

        if cancel_request is not None:
            cancel_text = cancel_request.get("cancel_reason") or "cancel requested"
            error_text = f"{error_text}; {cancel_text}" if error_text else cancel_text

    if done.timed_out:
        try:
            await order_service.cancel_order_id(order_id)
        except Exception as exc:
            cancel_error = f"cancel after timeout failed: {type(exc).__name__}: {exc}"
            error_text = f"{error_text}; {cancel_error}" if error_text else cancel_error

    return ExecutionResult(
        trade_intent_id=intent.trade_intent_id,
        order_id=order_id,
        order_action=order_action,
        order_quantity=quantity,
        status=status,
        avg_fill_price=avg_fill_price,
        total_commission=total_commission,
        realized_pnl=realized_pnl,
        error_text=error_text,
    )


async def execute_trade_intent(
        *,
        order_service: OrderService,
        intent: TradeIntent,
        order_submitted_callback: OrderSubmittedCallback | None = None,
) -> ExecutionResult:
    order_action, quantity = calculate_order_delta(intent)
    contract = build_execution_contract(instrument_code=intent.instrument_code)

    if not intent.order_ref:
        raise ValueError(f"TradeIntent without order_ref: id={intent.trade_intent_id}")

    order_ref = intent.order_ref
    order_type = str(intent.order_type).upper()

    if order_type == "MARKET":
        return await execute_market_intent(
            order_service=order_service,
            intent=intent,
            contract=contract,
            order_action=order_action,
            quantity=quantity,
            order_ref=order_ref,
            order_submitted_callback=order_submitted_callback,
        )

    if order_type == "LIMIT":
        return await execute_limit_intent(
            order_service=order_service,
            intent=intent,
            contract=contract,
            order_action=order_action,
            quantity=quantity,
            order_ref=order_ref,
            order_submitted_callback=order_submitted_callback,
        )

    raise ValueError(f"Unknown order_type: {intent.order_type!r}")


# Совместимость со старым runner/import, если где-то ещё осталось имя.
execute_trade_intent_market = execute_trade_intent
