from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar, Mapping

from ibmd.foundation.identity import validate_id
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.broker_execution import BrokerOrderSide


class BrokerReconciliationContractError(ValueError):
    pass


class BrokerOrderSource(str, Enum):
    OPEN = "OPEN"
    COMPLETED = "COMPLETED"


def _keys(value: Mapping[str, Any], expected: set[str], context: str) -> None:
    actual = set(value)
    if actual != expected:
        raise BrokerReconciliationContractError(
            f"{context} fields mismatch: "
            f"missing={sorted(expected - actual)}, "
            f"unknown={sorted(actual - expected)}"
        )


def _text(value: object, field: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    parsed = str(value or "").strip()
    if not parsed and not optional:
        raise BrokerReconciliationContractError(f"{field} is required")
    return parsed or None


def _int(value: object, field: str, minimum: int = 0) -> int:
    if isinstance(value, bool):
        raise BrokerReconciliationContractError(f"{field} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise BrokerReconciliationContractError(
            f"{field} must be an integer: {value!r}"
        ) from exc
    if parsed < minimum or exact != float(parsed):
        raise BrokerReconciliationContractError(
            f"{field} must be an integer >= {minimum}: {value!r}"
        )
    return parsed


def _opt_positive_int(value: object | None, field: str) -> int | None:
    return None if value is None else _int(value, field, 1)


def _float(value: object, field: str, *, positive: bool = False) -> float:
    if isinstance(value, bool):
        raise BrokerReconciliationContractError(f"{field} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise BrokerReconciliationContractError(
            f"{field} must be numeric: {value!r}"
        ) from exc
    if not math.isfinite(parsed) or (positive and parsed <= 0.0):
        qualifier = "positive and finite" if positive else "finite"
        raise BrokerReconciliationContractError(f"{field} must be {qualifier}")
    return parsed


def _utc(value: object, field: str) -> str:
    try:
        return format_utc(parse_utc(str(value)))
    except (TypeError, ValueError) as exc:
        raise BrokerReconciliationContractError(
            f"invalid {field}: {value!r}"
        ) from exc


def _bool(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise BrokerReconciliationContractError(f"{field} must be a boolean")
    return value


@dataclass(frozen=True)
class BrokerOrderFactV1:
    account_id: str
    order_ref: str | None
    broker_order_id: int
    broker_perm_id: int | None
    client_id: int
    con_id: int
    local_symbol: str
    side: BrokerOrderSide
    order_type: str
    requested_qty: int
    filled_qty: int
    remaining_qty: int
    status: str
    source: BrokerOrderSource
    observed_at_utc: str
    completed_status: str | None = None
    warning_text: str | None = None

    SCHEMA_NAME: ClassVar[str] = "BrokerOrderFact"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name", "schema_version", "account_id", "order_ref",
        "broker_order_id", "broker_perm_id", "client_id", "con_id",
        "local_symbol", "side", "order_type", "requested_qty",
        "filled_qty", "remaining_qty", "status", "source",
        "observed_at_utc", "completed_status", "warning_text",
    }

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_id", _text(self.account_id, "account_id"))
        object.__setattr__(self, "order_ref", _text(self.order_ref, "order_ref", optional=True))
        object.__setattr__(self, "broker_order_id", _int(self.broker_order_id, "broker_order_id", 1))
        object.__setattr__(self, "broker_perm_id", _opt_positive_int(self.broker_perm_id, "broker_perm_id"))
        object.__setattr__(self, "client_id", _int(self.client_id, "client_id"))
        object.__setattr__(self, "con_id", _int(self.con_id, "con_id", 1))
        object.__setattr__(self, "local_symbol", _text(self.local_symbol, "local_symbol"))
        if not isinstance(self.side, BrokerOrderSide):
            raise BrokerReconciliationContractError(f"invalid order side: {self.side!r}")
        object.__setattr__(self, "order_type", str(_text(self.order_type, "order_type")).upper())
        requested = _int(self.requested_qty, "requested_qty", 1)
        filled = _int(self.filled_qty, "filled_qty")
        remaining = _int(self.remaining_qty, "remaining_qty")
        if filled + remaining != requested:
            raise BrokerReconciliationContractError(
                "filled_qty + remaining_qty must equal requested_qty"
            )
        object.__setattr__(self, "requested_qty", requested)
        object.__setattr__(self, "filled_qty", filled)
        object.__setattr__(self, "remaining_qty", remaining)
        object.__setattr__(self, "status", _text(self.status, "status"))
        if not isinstance(self.source, BrokerOrderSource):
            raise BrokerReconciliationContractError(f"invalid order source: {self.source!r}")
        object.__setattr__(self, "observed_at_utc", _utc(self.observed_at_utc, "observed_at_utc"))
        object.__setattr__(self, "completed_status", _text(self.completed_status, "completed_status", optional=True))
        object.__setattr__(self, "warning_text", _text(self.warning_text, "warning_text", optional=True))

    @property
    def broker_identity(self) -> tuple[int, int | None]:
        return self.broker_order_id, self.broker_perm_id

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME, "schema_version": self.SCHEMA_VERSION,
            "account_id": self.account_id, "order_ref": self.order_ref,
            "broker_order_id": self.broker_order_id, "broker_perm_id": self.broker_perm_id,
            "client_id": self.client_id, "con_id": self.con_id,
            "local_symbol": self.local_symbol, "side": self.side.value,
            "order_type": self.order_type, "requested_qty": self.requested_qty,
            "filled_qty": self.filled_qty, "remaining_qty": self.remaining_qty,
            "status": self.status, "source": self.source.value,
            "observed_at_utc": self.observed_at_utc,
            "completed_status": self.completed_status, "warning_text": self.warning_text,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> BrokerOrderFactV1:
        _keys(value, cls.KEYS, "broker order fact")
        if value["schema_name"] != cls.SCHEMA_NAME or value["schema_version"] != 1:
            raise BrokerReconciliationContractError("unsupported broker-order-fact schema")
        try:
            side = BrokerOrderSide(str(value["side"]))
            source = BrokerOrderSource(str(value["source"]))
        except ValueError as exc:
            raise BrokerReconciliationContractError("invalid broker-order-fact enum") from exc
        return cls(
            account_id=str(value["account_id"]), order_ref=value["order_ref"],
            broker_order_id=value["broker_order_id"], broker_perm_id=value["broker_perm_id"],
            client_id=value["client_id"], con_id=value["con_id"],
            local_symbol=str(value["local_symbol"]), side=side,
            order_type=str(value["order_type"]), requested_qty=value["requested_qty"],
            filled_qty=value["filled_qty"], remaining_qty=value["remaining_qty"],
            status=str(value["status"]), source=source,
            observed_at_utc=str(value["observed_at_utc"]),
            completed_status=value["completed_status"], warning_text=value["warning_text"],
        )


@dataclass(frozen=True)
class BrokerCommissionFactV1:
    exec_id: str
    commission: float
    currency: str
    realized_pnl: float | None
    reported_at_utc: str

    SCHEMA_NAME: ClassVar[str] = "BrokerCommissionFact"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name", "schema_version", "exec_id", "commission",
        "currency", "realized_pnl", "reported_at_utc",
    }

    def __post_init__(self) -> None:
        object.__setattr__(self, "exec_id", _text(self.exec_id, "exec_id"))
        object.__setattr__(self, "commission", _float(self.commission, "commission"))
        object.__setattr__(self, "currency", str(_text(self.currency, "currency")).upper())
        if self.realized_pnl is not None:
            object.__setattr__(self, "realized_pnl", _float(self.realized_pnl, "realized_pnl"))
        object.__setattr__(self, "reported_at_utc", _utc(self.reported_at_utc, "reported_at_utc"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME, "schema_version": self.SCHEMA_VERSION,
            "exec_id": self.exec_id, "commission": self.commission,
            "currency": self.currency, "realized_pnl": self.realized_pnl,
            "reported_at_utc": self.reported_at_utc,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> BrokerCommissionFactV1:
        _keys(value, cls.KEYS, "broker commission fact")
        if value["schema_name"] != cls.SCHEMA_NAME or value["schema_version"] != 1:
            raise BrokerReconciliationContractError("unsupported broker-commission-fact schema")
        return cls(
            exec_id=str(value["exec_id"]), commission=value["commission"],
            currency=str(value["currency"]), realized_pnl=value["realized_pnl"],
            reported_at_utc=str(value["reported_at_utc"]),
        )


@dataclass(frozen=True)
class BrokerFillFactV1:
    exec_id: str
    account_id: str
    order_ref: str | None
    broker_order_id: int
    broker_perm_id: int | None
    client_id: int
    con_id: int
    local_symbol: str
    side: BrokerOrderSide
    shares: int
    price: float
    cumulative_qty: int
    average_price: float
    exchange: str | None
    executed_at_utc: str
    observed_at_utc: str
    commission: BrokerCommissionFactV1 | None = None

    SCHEMA_NAME: ClassVar[str] = "BrokerFillFact"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name", "schema_version", "exec_id", "account_id", "order_ref",
        "broker_order_id", "broker_perm_id", "client_id", "con_id",
        "local_symbol", "side", "shares", "price", "cumulative_qty",
        "average_price", "exchange", "executed_at_utc", "observed_at_utc",
        "commission",
    }

    def __post_init__(self) -> None:
        object.__setattr__(self, "exec_id", _text(self.exec_id, "exec_id"))
        object.__setattr__(self, "account_id", _text(self.account_id, "account_id"))
        object.__setattr__(self, "order_ref", _text(self.order_ref, "order_ref", optional=True))
        object.__setattr__(self, "broker_order_id", _int(self.broker_order_id, "broker_order_id", 1))
        object.__setattr__(self, "broker_perm_id", _opt_positive_int(self.broker_perm_id, "broker_perm_id"))
        object.__setattr__(self, "client_id", _int(self.client_id, "client_id"))
        object.__setattr__(self, "con_id", _int(self.con_id, "con_id", 1))
        object.__setattr__(self, "local_symbol", _text(self.local_symbol, "local_symbol"))
        if not isinstance(self.side, BrokerOrderSide):
            raise BrokerReconciliationContractError(f"invalid fill side: {self.side!r}")
        shares = _int(self.shares, "shares", 1)
        cumulative = _int(self.cumulative_qty, "cumulative_qty", 1)
        if cumulative < shares:
            raise BrokerReconciliationContractError("cumulative_qty cannot be below shares")
        object.__setattr__(self, "shares", shares)
        object.__setattr__(self, "cumulative_qty", cumulative)
        object.__setattr__(self, "price", _float(self.price, "price", positive=True))
        object.__setattr__(self, "average_price", _float(self.average_price, "average_price", positive=True))
        object.__setattr__(self, "exchange", _text(self.exchange, "exchange", optional=True))
        object.__setattr__(self, "executed_at_utc", _utc(self.executed_at_utc, "executed_at_utc"))
        object.__setattr__(self, "observed_at_utc", _utc(self.observed_at_utc, "observed_at_utc"))
        if parse_utc(self.observed_at_utc) < parse_utc(self.executed_at_utc):
            raise BrokerReconciliationContractError("fill observation precedes execution")
        if self.commission is not None:
            if not isinstance(self.commission, BrokerCommissionFactV1):
                raise BrokerReconciliationContractError("invalid commission fact")
            if self.commission.exec_id != self.exec_id:
                raise BrokerReconciliationContractError("commission exec_id differs from fill")

    @property
    def commission_complete(self) -> bool:
        return self.commission is not None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME, "schema_version": self.SCHEMA_VERSION,
            "exec_id": self.exec_id, "account_id": self.account_id,
            "order_ref": self.order_ref, "broker_order_id": self.broker_order_id,
            "broker_perm_id": self.broker_perm_id, "client_id": self.client_id,
            "con_id": self.con_id, "local_symbol": self.local_symbol,
            "side": self.side.value, "shares": self.shares, "price": self.price,
            "cumulative_qty": self.cumulative_qty, "average_price": self.average_price,
            "exchange": self.exchange, "executed_at_utc": self.executed_at_utc,
            "observed_at_utc": self.observed_at_utc,
            "commission": None if self.commission is None else self.commission.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> BrokerFillFactV1:
        _keys(value, cls.KEYS, "broker fill fact")
        if value["schema_name"] != cls.SCHEMA_NAME or value["schema_version"] != 1:
            raise BrokerReconciliationContractError("unsupported broker-fill-fact schema")
        try:
            side = BrokerOrderSide(str(value["side"]))
        except ValueError as exc:
            raise BrokerReconciliationContractError("invalid broker fill side") from exc
        raw = value["commission"]
        if raw is not None and not isinstance(raw, Mapping):
            raise BrokerReconciliationContractError("commission must be object or null")
        return cls(
            exec_id=str(value["exec_id"]), account_id=str(value["account_id"]),
            order_ref=value["order_ref"], broker_order_id=value["broker_order_id"],
            broker_perm_id=value["broker_perm_id"], client_id=value["client_id"],
            con_id=value["con_id"], local_symbol=str(value["local_symbol"]),
            side=side, shares=value["shares"], price=value["price"],
            cumulative_qty=value["cumulative_qty"], average_price=value["average_price"],
            exchange=value["exchange"], executed_at_utc=str(value["executed_at_utc"]),
            observed_at_utc=str(value["observed_at_utc"]),
            commission=None if raw is None else BrokerCommissionFactV1.from_dict(raw),
        )


@dataclass(frozen=True)
class BrokerReconciliationSnapshotV1:
    source_session_id: str
    account_id: str
    captured_at_utc: str
    open_orders: tuple[BrokerOrderFactV1, ...]
    completed_orders: tuple[BrokerOrderFactV1, ...]
    fills: tuple[BrokerFillFactV1, ...]
    requests_complete: bool

    SCHEMA_NAME: ClassVar[str] = "BrokerReconciliationSnapshot"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name", "schema_version", "source_session_id", "account_id",
        "captured_at_utc", "open_orders", "completed_orders", "fills",
        "requests_complete",
    }

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_session_id", validate_id(self.source_session_id, expected_kind="ib_session"))
        account = str(_text(self.account_id, "account_id"))
        object.__setattr__(self, "account_id", account)
        object.__setattr__(self, "captured_at_utc", _utc(self.captured_at_utc, "captured_at_utc"))
        object.__setattr__(self, "requests_complete", _bool(self.requests_complete, "requests_complete"))
        for field, source in (("open_orders", BrokerOrderSource.OPEN), ("completed_orders", BrokerOrderSource.COMPLETED)):
            values = tuple(getattr(self, field))
            if any(not isinstance(item, BrokerOrderFactV1) for item in values):
                raise BrokerReconciliationContractError(f"invalid {field}")
            if any(item.account_id != account or item.source != source for item in values):
                raise BrokerReconciliationContractError(f"{field} scope/source mismatch")
            identities = [item.broker_identity for item in values]
            if len(identities) != len(set(identities)):
                raise BrokerReconciliationContractError(f"duplicate identities in {field}")
            object.__setattr__(self, field, tuple(sorted(values, key=lambda item: (item.order_ref or "", item.broker_order_id))))
        fills = tuple(self.fills)
        if any(not isinstance(item, BrokerFillFactV1) or item.account_id != account for item in fills):
            raise BrokerReconciliationContractError("invalid fill scope")
        ids = [item.exec_id for item in fills]
        if len(ids) != len(set(ids)):
            raise BrokerReconciliationContractError("duplicate fill exec_id")
        object.__setattr__(self, "fills", tuple(sorted(fills, key=lambda item: item.exec_id)))

    @property
    def commission_complete_count(self) -> int:
        return sum(item.commission_complete for item in self.fills)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME, "schema_version": self.SCHEMA_VERSION,
            "source_session_id": self.source_session_id, "account_id": self.account_id,
            "captured_at_utc": self.captured_at_utc,
            "open_orders": [item.to_dict() for item in self.open_orders],
            "completed_orders": [item.to_dict() for item in self.completed_orders],
            "fills": [item.to_dict() for item in self.fills],
            "requests_complete": self.requests_complete,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> BrokerReconciliationSnapshotV1:
        _keys(value, cls.KEYS, "broker reconciliation snapshot")
        if value["schema_name"] != cls.SCHEMA_NAME or value["schema_version"] != 1:
            raise BrokerReconciliationContractError("unsupported reconciliation snapshot schema")
        for field in ("open_orders", "completed_orders", "fills"):
            if not isinstance(value[field], list):
                raise BrokerReconciliationContractError(f"{field} must be a list")
        return cls(
            source_session_id=str(value["source_session_id"]), account_id=str(value["account_id"]),
            captured_at_utc=str(value["captured_at_utc"]),
            open_orders=tuple(BrokerOrderFactV1.from_dict(item) for item in value["open_orders"]),
            completed_orders=tuple(BrokerOrderFactV1.from_dict(item) for item in value["completed_orders"]),
            fills=tuple(BrokerFillFactV1.from_dict(item) for item in value["fills"]),
            requests_complete=value["requests_complete"],
        )
