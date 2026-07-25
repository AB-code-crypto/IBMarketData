from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Protocol

from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.broker_execution import BrokerOrderSide
from ibmd.public_contracts.protection import (
    ProtectiveOrderKind,
    ProtectiveOrderType,
)

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


def _positive_float(value: object, *, field_name: str) -> float:
    if isinstance(value, bool):
        raise BrokerOrderSubmitError(f"{field_name} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise BrokerOrderSubmitError(
            f"{field_name} must be numeric: {value!r}"
        ) from exc
    if not math.isfinite(parsed) or parsed <= 0.0:
        raise BrokerOrderSubmitError(
            f"{field_name} must be finite and positive: {value!r}"
        )
    return parsed


def _order_ref(value: object) -> str:
    parsed = str(value or "").strip()
    if not _ORDER_REF_RE.fullmatch(parsed):
        raise BrokerOrderSubmitError(
            f"order_ref must contain 1..64 safe characters: {value!r}"
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
        object.__setattr__(self, "order_ref", _order_ref(self.order_ref))
        if not isinstance(self.side, BrokerOrderSide):
            raise BrokerOrderSubmitError(
                f"invalid broker order side: {self.side!r}"
            )
        object.__setattr__(
            self,
            "quantity",
            _positive_int(self.quantity, field_name="quantity"),
        )
        if not isinstance(self.route, PaperOrderRoute):
            raise BrokerOrderSubmitError("route must be PaperOrderRoute")


@dataclass(frozen=True)
class PaperProtectiveOrderRequest:
    account_id: str
    broker_order_id: int
    order_ref: str
    kind: ProtectiveOrderKind
    side: BrokerOrderSide
    order_type: ProtectiveOrderType
    quantity: int
    route: PaperOrderRoute
    stop_price: float | None
    limit_price: float | None
    time_in_force: str
    outside_rth: bool
    oca_group: str | None

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
        object.__setattr__(self, "order_ref", _order_ref(self.order_ref))
        if not isinstance(self.kind, ProtectiveOrderKind):
            raise BrokerOrderSubmitError(
                f"invalid protective order kind: {self.kind!r}"
            )
        if not isinstance(self.side, BrokerOrderSide):
            raise BrokerOrderSubmitError(
                f"invalid broker order side: {self.side!r}"
            )
        if not isinstance(self.order_type, ProtectiveOrderType):
            raise BrokerOrderSubmitError(
                f"invalid protective order type: {self.order_type!r}"
            )
        object.__setattr__(
            self,
            "quantity",
            _positive_int(self.quantity, field_name="quantity"),
        )
        if not isinstance(self.route, PaperOrderRoute):
            raise BrokerOrderSubmitError("route must be PaperOrderRoute")
        tif = _required_text(
            self.time_in_force,
            field_name="time_in_force",
        ).upper()
        if tif not in {"DAY", "GTC"}:
            raise BrokerOrderSubmitError(
                f"unsupported protective TIF: {tif!r}"
            )
        object.__setattr__(self, "time_in_force", tif)
        if not isinstance(self.outside_rth, bool):
            raise BrokerOrderSubmitError("outside_rth must be boolean")
        group = str(self.oca_group or "").strip() or None
        if group is not None and len(group) > 64:
            raise BrokerOrderSubmitError("oca_group must not exceed 64 characters")
        object.__setattr__(self, "oca_group", group)

        stop_price = (
            None
            if self.stop_price is None
            else _positive_float(self.stop_price, field_name="stop_price")
        )
        limit_price = (
            None
            if self.limit_price is None
            else _positive_float(self.limit_price, field_name="limit_price")
        )
        if self.kind == ProtectiveOrderKind.STOP_LOSS:
            if (
                self.order_type != ProtectiveOrderType.STOP
                or stop_price is None
                or limit_price is not None
            ):
                raise BrokerOrderSubmitError(
                    "STOP_LOSS requires STOP with only stop_price"
                )
        else:
            if (
                self.order_type != ProtectiveOrderType.LIMIT
                or limit_price is None
                or stop_price is not None
            ):
                raise BrokerOrderSubmitError(
                    "TAKE_PROFIT requires LIMIT with only limit_price"
                )
        object.__setattr__(self, "stop_price", stop_price)
        object.__setattr__(self, "limit_price", limit_price)


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
        object.__setattr__(self, "order_ref", _order_ref(self.order_ref))
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

    async def submit_protective_order(
        self,
        request: PaperProtectiveOrderRequest,
    ) -> PaperOrderSubmissionReceipt: ...

    async def close(self) -> None: ...
