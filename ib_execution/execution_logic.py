from ib_execution.execution_models import ExecutionResult, ExecutionStatus, TradeIntent
from ib_execution.contract_resolver import build_execution_contract
from ib_execution.order_service import OrderService


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

        return ExecutionResult(
            trade_intent_id=intent.trade_intent_id,
            order_id=placement.receipt.order_id,
            order_action=order_action,
            order_quantity=quantity,
            status=ExecutionStatus.EXECUTED,
            avg_fill_price=placement.avg_fill_price,
            total_commission=float(placement.total_commission),
            realized_pnl=float(placement.realized_pnl),
            error_text=None,
        )

    if order_type == "LIMIT":
        if intent.limit_price is None:
            raise ValueError(f"LIMIT intent without limit_price: id={intent.trade_intent_id}")

        if order_action == "BUY":
            placement = await order_service.buy_limit(
                contract=contract,
                quantity=quantity,
                limit_price=float(intent.limit_price),
                order_ref=order_ref,
                ttl_seconds=intent.ttl_seconds,
                wait="accept",
            )
        else:
            placement = await order_service.sell_limit(
                contract=contract,
                quantity=quantity,
                limit_price=float(intent.limit_price),
                order_ref=order_ref,
                ttl_seconds=intent.ttl_seconds,
                wait="accept",
            )

        return ExecutionResult(
            trade_intent_id=intent.trade_intent_id,
            order_id=placement.receipt.order_id,
            order_action=order_action,
            order_quantity=quantity,
            status=ExecutionStatus.ACCEPTED,
            avg_fill_price=None,
            total_commission=None,
            realized_pnl=None,
            error_text=None,
        )

    raise ValueError(f"Unknown order_type: {intent.order_type!r}")


# Совместимость со старым runner/import, если где-то ещё осталось имя.
execute_trade_intent_market = execute_trade_intent
