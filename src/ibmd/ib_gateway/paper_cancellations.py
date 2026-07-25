from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ibmd.foundation.time import format_utc, parse_utc


class BrokerOrderCancelError(RuntimeError):
    pass


def _required_text(value: object, *, field_name: str) -> str:
    parsed = str(value or "").strip()
    if not parsed:
        raise BrokerOrderCancelError(f"{field_name} is required")
    return parsed


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise BrokerOrderCancelError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise BrokerOrderCancelError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed <= 0 or exact != float(parsed):
        raise BrokerOrderCancelError(
            f"{field_name} must be a positive integer: {value!r}"
        )
    return parsed


@dataclass(frozen=True)
class PaperOrderCancelRequest:
    account_id: str
    broker_order_id: int
    order_ref: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "account_id",
            _required_text(self.account_id, field_name="account_id"),
        )
        object.__setattr__(
            self,
            "broker_order_id",
            _positive_int(self.broker_order_id, field_name="broker_order_id"),
        )
        order_ref = _required_text(self.order_ref, field_name="order_ref")
        if len(order_ref) > 64:
            raise BrokerOrderCancelError("order_ref must not exceed 64 characters")
        object.__setattr__(self, "order_ref", order_ref)


@dataclass(frozen=True)
class PaperOrderCancelReceipt:
    broker_order_id: int
    order_ref: str
    cancel_requested_at_utc: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "broker_order_id",
            _positive_int(self.broker_order_id, field_name="broker_order_id"),
        )
        object.__setattr__(
            self,
            "order_ref",
            _required_text(self.order_ref, field_name="order_ref"),
        )
        object.__setattr__(
            self,
            "cancel_requested_at_utc",
            format_utc(parse_utc(self.cancel_requested_at_utc)),
        )


class PaperOrderCancellationGateway(Protocol):
    async def cancel_order(
        self,
        request: PaperOrderCancelRequest,
    ) -> PaperOrderCancelReceipt: ...

    async def close(self) -> None: ...
