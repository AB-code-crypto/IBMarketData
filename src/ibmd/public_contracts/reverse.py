from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, ClassVar, Mapping

from ibmd.foundation.identity import validate_id
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.broker_execution import BrokerOrderSide


class ReverseContractError(ValueError):
    pass


def _keys(value: Mapping[str, Any], expected: set[str]) -> None:
    actual = set(value)
    missing = sorted(expected - actual)
    unknown = sorted(actual - expected)
    if missing or unknown:
        raise ReverseContractError(
            f"reverse allocation fields mismatch: missing={missing}, unknown={unknown}"
        )


def _text(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ReverseContractError(f"{field_name} is required")
    return text


def _integer(value: object, *, field_name: str, minimum: int = 0) -> int:
    if isinstance(value, bool):
        raise ReverseContractError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise ReverseContractError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed < minimum or exact != float(parsed):
        raise ReverseContractError(
            f"{field_name} must be an integer >= {minimum}: {value!r}"
        )
    return parsed


def _positive_float(value: object, *, field_name: str) -> float:
    if isinstance(value, bool):
        raise ReverseContractError(f"{field_name} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ReverseContractError(
            f"{field_name} must be numeric: {value!r}"
        ) from exc
    if not math.isfinite(parsed) or parsed <= 0.0:
        raise ReverseContractError(
            f"{field_name} must be finite and positive: {value!r}"
        )
    return parsed


@dataclass(frozen=True)
class ReverseFillAllocationV1:
    reverse_allocation_id: str
    source_operation_id: str
    source_attempt_id: str
    exec_id: str
    sequence_no: int
    closing_position_episode_id: str
    opening_position_episode_id: str
    side: BrokerOrderSide
    close_quantity: int
    open_quantity: int
    price: float
    executed_at_utc: str
    commission_complete: bool

    SCHEMA_NAME: ClassVar[str] = "ReverseFillAllocation"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "reverse_allocation_id",
        "source_operation_id",
        "source_attempt_id",
        "exec_id",
        "sequence_no",
        "closing_position_episode_id",
        "opening_position_episode_id",
        "side",
        "close_quantity",
        "open_quantity",
        "price",
        "executed_at_utc",
        "commission_complete",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "reverse_allocation_id",
            validate_id(
                self.reverse_allocation_id,
                expected_kind="reverse_allocation",
            ),
        )
        object.__setattr__(
            self,
            "source_operation_id",
            validate_id(
                self.source_operation_id,
                expected_kind="broker_operation",
            ),
        )
        object.__setattr__(
            self,
            "source_attempt_id",
            validate_id(
                self.source_attempt_id,
                expected_kind="broker_attempt",
            ),
        )
        object.__setattr__(
            self,
            "closing_position_episode_id",
            validate_id(
                self.closing_position_episode_id,
                expected_kind="position_episode",
            ),
        )
        object.__setattr__(
            self,
            "opening_position_episode_id",
            validate_id(
                self.opening_position_episode_id,
                expected_kind="position_episode",
            ),
        )
        if (
            self.closing_position_episode_id
            == self.opening_position_episode_id
        ):
            raise ReverseContractError(
                "reverse allocation requires distinct closing/opening episodes"
            )
        object.__setattr__(
            self,
            "exec_id",
            _text(self.exec_id, field_name="exec_id"),
        )
        object.__setattr__(
            self,
            "sequence_no",
            _integer(
                self.sequence_no,
                field_name="sequence_no",
                minimum=1,
            ),
        )
        if not isinstance(self.side, BrokerOrderSide):
            raise ReverseContractError(f"invalid broker side: {self.side!r}")
        close = _integer(
            self.close_quantity,
            field_name="close_quantity",
            minimum=0,
        )
        opened = _integer(
            self.open_quantity,
            field_name="open_quantity",
            minimum=0,
        )
        if close + opened <= 0:
            raise ReverseContractError(
                "reverse allocation must allocate positive fill quantity"
            )
        object.__setattr__(self, "close_quantity", close)
        object.__setattr__(self, "open_quantity", opened)
        object.__setattr__(
            self,
            "price",
            _positive_float(self.price, field_name="price"),
        )
        object.__setattr__(
            self,
            "executed_at_utc",
            format_utc(parse_utc(self.executed_at_utc)),
        )
        if not isinstance(self.commission_complete, bool):
            raise ReverseContractError(
                "commission_complete must be boolean"
            )

    @property
    def allocated_quantity(self) -> int:
        return self.close_quantity + self.open_quantity

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "reverse_allocation_id": self.reverse_allocation_id,
            "source_operation_id": self.source_operation_id,
            "source_attempt_id": self.source_attempt_id,
            "exec_id": self.exec_id,
            "sequence_no": self.sequence_no,
            "closing_position_episode_id": (
                self.closing_position_episode_id
            ),
            "opening_position_episode_id": (
                self.opening_position_episode_id
            ),
            "side": self.side.value,
            "close_quantity": self.close_quantity,
            "open_quantity": self.open_quantity,
            "price": self.price,
            "executed_at_utc": self.executed_at_utc,
            "commission_complete": self.commission_complete,
        }

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
    ) -> "ReverseFillAllocationV1":
        _keys(value, cls.KEYS)
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise ReverseContractError(
                "unsupported reverse-fill-allocation schema"
            )
        try:
            side = BrokerOrderSide(str(value["side"]))
        except ValueError as exc:
            raise ReverseContractError(
                "invalid reverse allocation side"
            ) from exc
        return cls(
            reverse_allocation_id=str(value["reverse_allocation_id"]),
            source_operation_id=str(value["source_operation_id"]),
            source_attempt_id=str(value["source_attempt_id"]),
            exec_id=str(value["exec_id"]),
            sequence_no=value["sequence_no"],
            closing_position_episode_id=str(
                value["closing_position_episode_id"]
            ),
            opening_position_episode_id=str(
                value["opening_position_episode_id"]
            ),
            side=side,
            close_quantity=value["close_quantity"],
            open_quantity=value["open_quantity"],
            price=value["price"],
            executed_at_utc=str(value["executed_at_utc"]),
            commission_complete=value["commission_complete"],
        )
