from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Protocol

from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.broker_execution import BrokerOrderSide

_ORDER_REF_RE = re.compile(r"^[A-Za-z0-9:._-]{1,64}$")


class BrokerOrderSubmitError(RuntimeError):
    pass


def _required_text(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise BrokerOrderSubmitError(f"{field_name} is required")
    return text


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise BrokerOrderSubmitError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise BrokerOrderSubmitError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed <= 0 or exact != float(parsed):
        raise BrokerOrderSubmitError(
            f"{field_name} must be a positive integer: {value!r}"
        )
    return parsed


@dataclass(frozen=True)
class PaperOrderRoute:
    instrument_id: str
    con_id: int
    local_symbol: str
    last_trade_date: str
    sec_type: str
    exchange: str
    currency: str
    trading_class: str
    multiplier: float

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "instrument_id",
            _required_text(self.instrument_id, field_name="instrument_id"),
        )
        object.__setattr__(
            self,
            "con_id",
            _positive_int(self.con_id, field_name="con_id"),
        )
        for field_name in (
            "local_symbol",
            "last_trade_date",
            "sec_type",
            "exchange",
            "currency",
            "trading_class",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name=field_name),
            )
        object.__setattr__(self, "sec_type", self.sec_type.upper())
        multiplier = float(self.multiplier)
        if not math.isfinite(multiplier) or multiplier <= 0.0:
            raise BrokerOrderSubmitError(
                f"multiplier must be finite and positive: {self.multiplier!r}"
            )
        object.__setattr__(self, "multiplier", multiplier)


@dataclass(frozen=True)
class PaperMarketOrderRequest:
    account_id: str
    broker_order_id: int
    order_ref: str
    side: BrokerOrderSide
    quantity: int
    route: PaperOrderRoute

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
        order_ref = str(self.order_ref or "").strip()
        if not _ORDER_REF_RE.fullmatch(order_ref):
            raise BrokerOrderSubmitError(
                f"order_ref must contain 1..64 safe characters: {self.order_ref!r}"
            )
        object.__setattr__(self, "order_ref", order_ref)
        if not isinstance(self.side, BrokerOrderSide):
            raise BrokerOrderSubmitError(f"invalid broker order side: {self.side!r}")
        object.__setattr__(
            self,
            "quantity",
            _positive_int(self.quantity, field_name="quantity"),
        )
        if not isinstance(self.route, PaperOrderRoute):
            raise BrokerOrderSubmitError("route must be PaperOrderRoute")


@dataclass(frozen=True)
class PaperOrderSubmissionReceipt:
    broker_order_id: int
    order_ref: str
    submitted_at_utc: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "broker_order_id",
            _positive_int(self.broker_order_id, field_name="broker_order_id"),
        )
        order_ref = str(self.order_ref or "").strip()
        if not _ORDER_REF_RE.fullmatch(order_ref):
            raise BrokerOrderSubmitError(
                f"invalid receipt order_ref: {self.order_ref!r}"
            )
        object.__setattr__(self, "order_ref", order_ref)
        object.__setattr__(
            self,
            "submitted_at_utc",
            format_utc(parse_utc(self.submitted_at_utc)),
        )


class PaperOrderGateway(Protocol):
    async def allocate_order_id(self, *, account_id: str) -> int: ...

    async def submit_market_order(
        self,
        request: PaperMarketOrderRequest,
    ) -> PaperOrderSubmissionReceipt: ...

    async def close(self) -> None: ...
