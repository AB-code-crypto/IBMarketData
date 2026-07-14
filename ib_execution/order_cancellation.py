from __future__ import annotations

import asyncio
from dataclasses import dataclass

from ib_execution.order_service import OrderService


ORDER_CANCEL_CONFIRM_TIMEOUT_SECONDS = 5.0
ORDER_CANCEL_POLL_INTERVAL_SECONDS = 0.10

ORDER_CANCELLED_STATUSES = {
    "Cancelled",
    "ApiCancelled",
    "Inactive",
    "Rejected",
}
ORDER_FILLED_STATUSES = {"Filled"}
ORDER_LIVE_STATUSES = {
    "ApiPending",
    "PendingSubmit",
    "PreSubmitted",
    "Submitted",
    "PendingCancel",
}


class OrderCancellationUncertainError(RuntimeError):
    pass


class OrderFilledDuringCancellationError(RuntimeError):
    pass


@dataclass(frozen=True)
class OrderCancellationResult:
    order_id: int
    terminal_status: str
    cancel_requested: bool


def read_known_order_status(
        order_service: OrderService,
        *,
        order_id: int,
) -> str | None:
    expected = int(order_id)

    # openTrades() is authoritative for live state.
    for trade in list(order_service.ib.openTrades() or []):
        order = getattr(trade, "order", None)
        if order is None:
            continue
        if int(getattr(order, "orderId", 0) or 0) != expected:
            continue
        return str(
            getattr(getattr(trade, "orderStatus", None), "status", "")
            or ""
        )

    # Historical cache is used only for terminal states; an old Submitted
    # value must never resurrect an order that disappeared from openTrades().
    for trade in list(order_service.ib.trades() or []):
        order = getattr(trade, "order", None)
        if order is None:
            continue
        if int(getattr(order, "orderId", 0) or 0) != expected:
            continue
        status = str(
            getattr(getattr(trade, "orderStatus", None), "status", "")
            or ""
        )
        if status in ORDER_CANCELLED_STATUSES | ORDER_FILLED_STATUSES:
            return status

    return None


async def cancel_order_and_require_terminal(
        order_service: OrderService,
        *,
        order_id: int,
        context: str,
        timeout_seconds: float = ORDER_CANCEL_CONFIRM_TIMEOUT_SECONDS,
) -> OrderCancellationResult:
    order_id = int(order_id)
    timeout_value = float(timeout_seconds)
    if timeout_value <= 0.0:
        raise ValueError(
            f"cancel confirmation timeout must be positive: {timeout_value}"
        )

    refreshed, refresh_error = (
        await order_service.broker_state.refresh_open_orders(force=True)
    )
    if not refreshed:
        raise OrderCancellationUncertainError(
            f"cannot refresh broker open orders before cancellation: "
            f"order_id={order_id}, context={context}, error={refresh_error}"
        )

    status = read_known_order_status(
        order_service,
        order_id=order_id,
    )
    if status in ORDER_FILLED_STATUSES:
        raise OrderFilledDuringCancellationError(
            f"order filled before cancellation completed: "
            f"order_id={order_id}, context={context}, status={status}"
        )
    if status in ORDER_CANCELLED_STATUSES:
        return OrderCancellationResult(
            order_id=order_id,
            terminal_status=status,
            cancel_requested=False,
        )
    if status is None:
        raise OrderCancellationUncertainError(
            f"order is absent from refreshed broker caches before cancellation: "
            f"order_id={order_id}, context={context}"
        )

    cancel_requested = status != "PendingCancel"
    if cancel_requested:
        try:
            await order_service.cancel_order_id(order_id)
        except Exception as exc:
            raise OrderCancellationUncertainError(
                f"cancel request failed: order_id={order_id}, context={context}, "
                f"{type(exc).__name__}: {exc}"
            ) from exc

    loop_time = asyncio.get_running_loop().time
    deadline = loop_time() + timeout_value
    last_status = status

    while loop_time() < deadline:
        status = read_known_order_status(
            order_service,
            order_id=order_id,
        )
        if status is not None:
            last_status = status

        if status in ORDER_FILLED_STATUSES:
            raise OrderFilledDuringCancellationError(
                f"order filled while cancellation was pending: "
                f"order_id={order_id}, context={context}, status={status}"
            )
        if status in ORDER_CANCELLED_STATUSES:
            return OrderCancellationResult(
                order_id=order_id,
                terminal_status=status,
                cancel_requested=cancel_requested,
            )

        await asyncio.sleep(ORDER_CANCEL_POLL_INTERVAL_SECONDS)

    # One final forced refresh prevents a stale in-memory PendingCancel from
    # being treated as a live order forever.
    refreshed, refresh_error = (
        await order_service.broker_state.refresh_open_orders(force=True)
    )
    if refreshed:
        status = read_known_order_status(
            order_service,
            order_id=order_id,
        )
        if status in ORDER_FILLED_STATUSES:
            raise OrderFilledDuringCancellationError(
                f"order filled while cancellation confirmation timed out: "
                f"order_id={order_id}, context={context}, status={status}"
            )
        if status in ORDER_CANCELLED_STATUSES:
            return OrderCancellationResult(
                order_id=order_id,
                terminal_status=status,
                cancel_requested=cancel_requested,
            )
        if status is not None:
            last_status = status
    else:
        last_status = f"refresh_failed:{refresh_error}"

    raise OrderCancellationUncertainError(
        f"broker did not confirm terminal cancellation: order_id={order_id}, "
        f"context={context}, last_status={last_status}, "
        f"timeout_seconds={timeout_value:g}"
    )


__all__ = [
    "OrderCancellationResult",
    "OrderCancellationUncertainError",
    "OrderFilledDuringCancellationError",
    "cancel_order_and_require_terminal",
    "read_known_order_status",
]
