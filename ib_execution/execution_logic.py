from ib_execution.execution_models import ExecutionResult, ExecutionStatus, TradeIntent
from ib_execution.contract_resolver import build_execution_contract
from ib_execution.order_service import OrderService


LIMIT_DONE_TIMEOUT_EXTRA_SECONDS = 10
DEFAULT_LIMIT_DONE_TIMEOUT_SECONDS = 600


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

        if commission_report is not None:
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
) -> ExecutionResult:
    if order_action == "BUY":
        placement = await order_service.buy_market(
            contract=contract,
            quantity=quantity,
            order_ref=order_ref,
            wait="done",
            done_timeout=60.0,
        )
    else:
        placement = await order_service.sell_market(
            contract=contract,
            quantity=quantity,
            order_ref=order_ref,
            wait="done",
            done_timeout=60.0,
        )

    return build_execution_result(
        intent=intent,
        order_id=placement.receipt.order_id,
        order_action=order_action,
        order_quantity=quantity,
        status=ExecutionStatus.EXECUTED,
        trade=placement.receipt.trade,
        error_text=None,
    )


async def execute_limit_intent(
        *,
        order_service: OrderService,
        intent: TradeIntent,
        contract,
        order_action: str,
        quantity: int,
        order_ref: str,
) -> ExecutionResult:
    """Что делает: ставит LIMIT и ждёт финал до ttl_seconds + запас.
    Зачем нужна: trade_intents не должны навечно зависать в ACCEPTED после отмены/expiry."""
    if intent.limit_price is None:
        raise ValueError(f"LIMIT intent without limit_price: id={intent.trade_intent_id}")

    if order_action == "BUY":
        placement = await order_service.buy_limit(
            contract=contract,
            quantity=quantity,
            limit_price=float(intent.limit_price),
            order_ref=order_ref,
            ttl_seconds=intent.ttl_seconds,
            wait="none",
        )
    else:
        placement = await order_service.sell_limit(
            contract=contract,
            quantity=quantity,
            limit_price=float(intent.limit_price),
            order_ref=order_ref,
            ttl_seconds=intent.ttl_seconds,
            wait="none",
        )

    order_id = placement.receipt.order_id
    trade = placement.receipt.trade
    done = await order_service.monitor.wait_for_done(
        trade,
        timeout=get_limit_done_timeout_seconds(intent),
        poll_interval=0.10,
    )

    avg_fill_price, total_commission, realized_pnl, filled_qty = collect_trade_fill_statistics(trade)
    status, error_text = classify_limit_terminal_status(
        intent=intent,
        ib_status=done.status,
        timed_out=done.timed_out,
        filled_qty=filled_qty,
        expected_qty=quantity,
    )

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
) -> ExecutionResult:
    order_action, quantity = calculate_order_delta(intent)
    contract = build_execution_contract(instrument_code=intent.instrument_code)
    order_ref = f"IBMD_INTENT_{intent.trade_intent_id}_{intent.instrument_code}"
    order_type = str(intent.order_type).upper()

    if order_type == "MARKET":
        return await execute_market_intent(
            order_service=order_service,
            intent=intent,
            contract=contract,
            order_action=order_action,
            quantity=quantity,
            order_ref=order_ref,
        )

    if order_type == "LIMIT":
        return await execute_limit_intent(
            order_service=order_service,
            intent=intent,
            contract=contract,
            order_action=order_action,
            quantity=quantity,
            order_ref=order_ref,
        )

    raise ValueError(f"Unknown order_type: {intent.order_type!r}")


# Совместимость со старым runner/import, если где-то ещё осталось имя.
execute_trade_intent_market = execute_trade_intent
