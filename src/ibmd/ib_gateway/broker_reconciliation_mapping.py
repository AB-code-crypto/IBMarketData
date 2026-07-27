from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Iterable

from ibmd.foundation.time import ensure_utc, format_utc, parse_utc
from ibmd.public_contracts.broker_execution import BrokerOrderSide
from ibmd.public_contracts.broker_reconciliation import (
    BrokerCommissionFactV1,
    BrokerFillFactV1,
    BrokerOrderFactV1,
    BrokerOrderSource,
)

from .broker_reconciliation import BrokerReconciliationReadError

_TERMINAL_ORDER_STATUSES = {
    "APICANCELLED",
    "CANCELLED",
    "CANCELED",
    "INACTIVE",
    "REJECTED",
    "FAILED",
}


def _required_text(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise BrokerReconciliationReadError(f"IB {field_name} is required")
    return text


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise BrokerReconciliationReadError(f"IB {field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise BrokerReconciliationReadError(
            f"IB {field_name} must be an integer: {value!r}"
        ) from exc
    if parsed <= 0 or exact != float(parsed):
        raise BrokerReconciliationReadError(
            f"IB {field_name} must be a positive integer: {value!r}"
        )
    return parsed


def _non_negative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise BrokerReconciliationReadError(f"IB {field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise BrokerReconciliationReadError(
            f"IB {field_name} must be an integer: {value!r}"
        ) from exc
    if parsed < 0 or exact != float(parsed):
        raise BrokerReconciliationReadError(
            f"IB {field_name} must be a non-negative integer: {value!r}"
        )
    return parsed


def _positive_float(value: object, *, field_name: str) -> float:
    if isinstance(value, bool):
        raise BrokerReconciliationReadError(f"IB {field_name} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise BrokerReconciliationReadError(
            f"IB {field_name} must be numeric: {value!r}"
        ) from exc
    if not math.isfinite(parsed) or parsed <= 0.0:
        raise BrokerReconciliationReadError(
            f"IB {field_name} must be finite and positive: {value!r}"
        )
    return parsed


def _finite(value: object, *, field_name: str) -> float:
    if isinstance(value, bool):
        raise BrokerReconciliationReadError(f"IB {field_name} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise BrokerReconciliationReadError(
            f"IB {field_name} must be numeric: {value!r}"
        ) from exc
    if not math.isfinite(parsed):
        raise BrokerReconciliationReadError(
            f"IB {field_name} must be finite: {value!r}"
        )
    return parsed


def _side(value: object, *, field_name: str) -> BrokerOrderSide:
    text = str(value or "").strip().upper()
    if text in {"BUY", "BOT"}:
        return BrokerOrderSide.BUY
    if text in {"SELL", "SLD"}:
        return BrokerOrderSide.SELL
    raise BrokerReconciliationReadError(
        f"unsupported IB {field_name}: {value!r}"
    )


def _order_type(value: object) -> str:
    text = _required_text(value, field_name="order type").upper()
    return "MARKET" if text in {"MKT", "MARKET"} else text


def _utc_text(value: object, *, field_name: str) -> str:
    if isinstance(value, datetime):
        return format_utc(ensure_utc(value))
    try:
        return format_utc(parse_utc(str(value)))
    except (TypeError, ValueError) as exc:
        raise BrokerReconciliationReadError(
            f"IB {field_name} has no supported UTC timestamp: {value!r}"
        ) from exc


def _status_key(value: object | None) -> str:
    return "".join(
        character
        for character in str(value or "").upper()
        if character.isalnum()
    )


def _trade_warning_text(trade: Any, order_state: Any) -> str | None:
    values: list[str] = []
    order_warning = (
        ""
        if order_state is None
        else str(getattr(order_state, "warningText", "") or "").strip()
    )
    if order_warning:
        values.append(order_warning)
    for entry in tuple(getattr(trade, "log", ()) or ()):
        message = str(getattr(entry, "message", "") or "").strip()
        error_code = int(getattr(entry, "errorCode", 0) or 0)
        if not message and error_code:
            message = f"IB error {error_code}"
        if message and message not in values:
            values.append(message)
    return "; ".join(values) or None


def order_fact_from_ib_trade(
    trade: Any,
    *,
    expected_account_id: str,
    source: BrokerOrderSource,
    observed_at_utc: str,
) -> BrokerOrderFactV1 | None:
    order = getattr(trade, "order", None)
    order_status = getattr(trade, "orderStatus", None)
    contract = getattr(trade, "contract", None)
    order_state = getattr(trade, "orderState", None)
    if order is None or order_status is None or contract is None:
        raise BrokerReconciliationReadError(
            "IB trade is missing order, orderStatus or contract"
        )
    account = str(getattr(order, "account", "") or "").strip()
    if not account:
        raise BrokerReconciliationReadError(
            "IB order fact has no account identity"
        )
    if account != str(expected_account_id or "").strip():
        return None

    order_id = int(
        getattr(order, "orderId", 0)
        or getattr(order_status, "orderId", 0)
        or 0
    )
    perm_id_value = int(
        getattr(order, "permId", 0)
        or getattr(order_status, "permId", 0)
        or 0
    )
    status = _required_text(
        getattr(order_status, "status", ""),
        field_name="order status",
    )
    completed_status = (
        None
        if order_state is None
        else str(getattr(order_state, "completedStatus", "") or "").strip()
        or None
    )
    requested = _positive_int(
        getattr(order, "totalQuantity", 0),
        field_name="totalQuantity",
    )
    filled = _non_negative_int(
        getattr(order_status, "filled", 0),
        field_name="orderStatus.filled",
    )
    if filled > requested:
        raise BrokerReconciliationReadError(
            "IB order filled quantity exceeds requested quantity: "
            f"requested={requested}, filled={filled}"
        )
    raw_remaining = _non_negative_int(
        getattr(order_status, "remaining", requested - filled),
        field_name="orderStatus.remaining",
    )
    remaining = raw_remaining
    if filled + remaining != requested:
        terminal = bool(
            {
                _status_key(status),
                _status_key(completed_status),
            }
            & _TERMINAL_ORDER_STATUSES
        )
        if terminal:
            # TWS can report remaining=0 for a cancelled/rejected validation
            # result even though the unfilled economic quantity is still the
            # requested quantity minus fills. Internal retry/reconciliation
            # semantics need that unfilled quantity, not the inactive-order
            # display value.
            remaining = requested - filled
        else:
            raise BrokerReconciliationReadError(
                "IB order quantities disagree: "
                f"requested={requested}, filled={filled}, "
                f"remaining={raw_remaining}, status={status!r}, "
                f"completed_status={completed_status!r}"
            )

    return BrokerOrderFactV1(
        account_id=account,
        order_ref=(
            str(getattr(order, "orderRef", "") or "").strip() or None
        ),
        broker_order_id=(
            None
            if order_id <= 0
            else _positive_int(order_id, field_name="orderId")
        ),
        broker_perm_id=(
            None
            if perm_id_value <= 0
            else _positive_int(perm_id_value, field_name="permId")
        ),
        client_id=_non_negative_int(
            getattr(order, "clientId", 0),
            field_name="clientId",
        ),
        con_id=_positive_int(
            getattr(contract, "conId", 0),
            field_name="contract.conId",
        ),
        local_symbol=_required_text(
            getattr(contract, "localSymbol", ""),
            field_name="contract.localSymbol",
        ),
        side=_side(getattr(order, "action", ""), field_name="order action"),
        order_type=_order_type(getattr(order, "orderType", "")),
        requested_qty=requested,
        filled_qty=filled,
        remaining_qty=remaining,
        status=status,
        source=source,
        observed_at_utc=observed_at_utc,
        completed_status=completed_status,
        warning_text=_trade_warning_text(trade, order_state),
    )


def _commission_from_fill(
    fill: Any,
    *,
    exec_id: str,
    reported_at_utc: str,
) -> BrokerCommissionFactV1 | None:
    report = getattr(fill, "commissionReport", None)
    if report is None:
        return None
    report_exec_id = str(getattr(report, "execId", "") or "").strip()
    if report_exec_id != exec_id:
        return None
    currency = str(getattr(report, "currency", "") or "").strip()
    if not currency:
        return None
    realized_raw = getattr(report, "realizedPNL", None)
    realized = (
        None
        if realized_raw is None
        else _finite(realized_raw, field_name="commission realizedPNL")
    )
    return BrokerCommissionFactV1(
        exec_id=exec_id,
        commission=_finite(
            getattr(report, "commission", 0.0),
            field_name="commission",
        ),
        currency=currency,
        realized_pnl=realized,
        reported_at_utc=reported_at_utc,
    )


def fill_fact_from_ib(
    fill: Any,
    *,
    expected_account_id: str,
    observed_at_utc: str,
) -> BrokerFillFactV1 | None:
    execution = getattr(fill, "execution", None)
    contract = getattr(fill, "contract", None)
    if execution is None or contract is None:
        raise BrokerReconciliationReadError(
            "IB fill is missing execution or contract"
        )
    account = str(getattr(execution, "acctNumber", "") or "").strip()
    if not account:
        raise BrokerReconciliationReadError(
            "IB execution has no account identity"
        )
    if account != str(expected_account_id or "").strip():
        return None
    exec_id = _required_text(
        getattr(execution, "execId", ""),
        field_name="execution.execId",
    )
    executed_at = _utc_text(
        getattr(execution, "time", None),
        field_name="execution.time",
    )
    return BrokerFillFactV1(
        exec_id=exec_id,
        account_id=account,
        order_ref=(
            str(getattr(execution, "orderRef", "") or "").strip() or None
        ),
        broker_order_id=_positive_int(
            getattr(execution, "orderId", 0),
            field_name="execution.orderId",
        ),
        broker_perm_id=(
            None
            if int(getattr(execution, "permId", 0) or 0) <= 0
            else _positive_int(
                getattr(execution, "permId", 0),
                field_name="execution.permId",
            )
        ),
        client_id=_non_negative_int(
            getattr(execution, "clientId", 0),
            field_name="execution.clientId",
        ),
        con_id=_positive_int(
            getattr(contract, "conId", 0),
            field_name="contract.conId",
        ),
        local_symbol=_required_text(
            getattr(contract, "localSymbol", ""),
            field_name="contract.localSymbol",
        ),
        side=_side(getattr(execution, "side", ""), field_name="execution side"),
        shares=_positive_int(
            getattr(execution, "shares", 0),
            field_name="execution.shares",
        ),
        price=_positive_float(
            getattr(execution, "price", 0.0),
            field_name="execution.price",
        ),
        cumulative_qty=_positive_int(
            getattr(execution, "cumQty", 0),
            field_name="execution.cumQty",
        ),
        average_price=_positive_float(
            getattr(execution, "avgPrice", 0.0),
            field_name="execution.avgPrice",
        ),
        exchange=(
            str(getattr(execution, "exchange", "") or "").strip() or None
        ),
        executed_at_utc=executed_at,
        observed_at_utc=observed_at_utc,
        commission=_commission_from_fill(
            fill,
            exec_id=exec_id,
            reported_at_utc=observed_at_utc,
        ),
    )


def _dedupe_orders(
    values: Iterable[BrokerOrderFactV1],
) -> tuple[BrokerOrderFactV1, ...]:
    by_identity: dict[tuple[object, ...], BrokerOrderFactV1] = {}
    for value in values:
        key = value.broker_identity
        previous = by_identity.get(key)
        if previous is None:
            by_identity[key] = value
        elif previous.to_dict() != value.to_dict():
            raise BrokerReconciliationReadError(
                "IB returned conflicting facts for one broker order identity: "
                f"identity={key}"
            )
    return tuple(by_identity.values())


def _dedupe_fill_objects(values: Iterable[Any]) -> tuple[Any, ...]:
    by_exec_id: dict[str, Any] = {}
    for value in values:
        execution = getattr(value, "execution", None)
        exec_id = str(getattr(execution, "execId", "") or "").strip()
        if not exec_id:
            raise BrokerReconciliationReadError(
                "IB returned a fill without execution.execId"
            )
        previous = by_exec_id.get(exec_id)
        if previous is None:
            by_exec_id[exec_id] = value
            continue
        previous_report = getattr(previous, "commissionReport", None)
        current_report = getattr(value, "commissionReport", None)
        previous_complete = (
            str(getattr(previous_report, "execId", "") or "").strip()
            == exec_id
        )
        current_complete = (
            str(getattr(current_report, "execId", "") or "").strip()
            == exec_id
        )
        if current_complete and not previous_complete:
            by_exec_id[exec_id] = value
    return tuple(by_exec_id[key] for key in sorted(by_exec_id))
