from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar, Mapping

from ibmd.foundation.identity import validate_id
from ibmd.foundation.time import format_utc, parse_utc


class BrokerPositionContractError(ValueError):
    pass


class BrokerPositionSnapshotStatus(str, Enum):
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


def _required_text(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise BrokerPositionContractError(f"{field_name} is required")
    return text


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _finite_number(value: object, *, field_name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise BrokerPositionContractError(
            f"{field_name} must be numeric: {value!r}"
        ) from exc
    if not math.isfinite(number):
        raise BrokerPositionContractError(
            f"{field_name} must be finite: {value!r}"
        )
    return number


@dataclass(frozen=True)
class BrokerPositionRowV1:
    con_id: int
    local_symbol: str | None
    symbol: str
    sec_type: str
    exchange: str | None
    currency: str | None
    signed_quantity: float
    average_cost: float | None

    KEYS: ClassVar[set[str]] = {
        "con_id",
        "local_symbol",
        "symbol",
        "sec_type",
        "exchange",
        "currency",
        "signed_quantity",
        "average_cost",
    }

    def __post_init__(self) -> None:
        try:
            con_id = int(self.con_id)
        except (TypeError, ValueError) as exc:
            raise BrokerPositionContractError(
                f"con_id must be an integer: {self.con_id!r}"
            ) from exc
        if con_id <= 0:
            raise BrokerPositionContractError(
                f"con_id must be positive: {con_id}"
            )
        object.__setattr__(self, "con_id", con_id)
        object.__setattr__(
            self,
            "local_symbol",
            _optional_text(self.local_symbol),
        )
        object.__setattr__(
            self,
            "symbol",
            _required_text(self.symbol, field_name="symbol"),
        )
        object.__setattr__(
            self,
            "sec_type",
            _required_text(self.sec_type, field_name="sec_type").upper(),
        )
        object.__setattr__(self, "exchange", _optional_text(self.exchange))
        object.__setattr__(self, "currency", _optional_text(self.currency))

        quantity = _finite_number(
            self.signed_quantity,
            field_name="signed_quantity",
        )
        if abs(quantity) <= 1e-12:
            raise BrokerPositionContractError(
                "zero signed_quantity must be represented by absence from a "
                "COMPLETE snapshot"
            )
        object.__setattr__(self, "signed_quantity", quantity)

        if self.average_cost is not None:
            object.__setattr__(
                self,
                "average_cost",
                _finite_number(self.average_cost, field_name="average_cost"),
            )

    @property
    def sort_key(self) -> tuple[int, str, str]:
        return (
            self.con_id,
            self.local_symbol or "",
            self.symbol,
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "BrokerPositionRowV1":
        keys = set(value)
        if keys != cls.KEYS:
            raise BrokerPositionContractError(
                "invalid broker-position row keys: "
                f"missing={sorted(cls.KEYS - keys)}, "
                f"unknown={sorted(keys - cls.KEYS)}"
            )
        return cls(**{key: value[key] for key in cls.KEYS})

    def to_dict(self) -> dict[str, Any]:
        return {
            "con_id": self.con_id,
            "local_symbol": self.local_symbol,
            "symbol": self.symbol,
            "sec_type": self.sec_type,
            "exchange": self.exchange,
            "currency": self.currency,
            "signed_quantity": self.signed_quantity,
            "average_cost": self.average_cost,
        }


@dataclass(frozen=True)
class BrokerPositionSnapshotFreshnessV1:
    snapshot_id: str
    captured_at_utc: str
    observed_at_utc: str
    age_seconds: float
    max_age_seconds: float
    is_fresh: bool

    def __post_init__(self) -> None:
        validate_id(self.snapshot_id, expected_kind="position_snapshot")
        captured = parse_utc(self.captured_at_utc)
        observed = parse_utc(self.observed_at_utc)
        expected_age = max(0.0, (observed - captured).total_seconds())
        age = _finite_number(self.age_seconds, field_name="age_seconds")
        maximum = _finite_number(
            self.max_age_seconds,
            field_name="max_age_seconds",
        )
        if maximum < 0.0:
            raise BrokerPositionContractError(
                f"max_age_seconds must be non-negative: {maximum}"
            )
        if abs(age - expected_age) > 1e-6:
            raise BrokerPositionContractError(
                "age_seconds does not match captured/observed timestamps: "
                f"expected={expected_age}, actual={age}"
            )
        expected_fresh = age <= maximum
        if bool(self.is_fresh) != expected_fresh:
            raise BrokerPositionContractError(
                "is_fresh does not match age/max-age policy"
            )
        object.__setattr__(self, "age_seconds", age)
        object.__setattr__(self, "max_age_seconds", maximum)
        object.__setattr__(self, "is_fresh", expected_fresh)


@dataclass(frozen=True)
class BrokerPositionSnapshotV1:
    snapshot_id: str
    account_id: str
    captured_at_utc: str
    published_at_utc: str
    status: BrokerPositionSnapshotStatus
    row_count: int
    source_session_id: str
    rows: tuple[BrokerPositionRowV1, ...] = ()
    error_text: str | None = None

    SCHEMA_NAME: ClassVar[str] = "BrokerPositionSnapshot"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "snapshot_id",
        "account_id",
        "captured_at_utc",
        "published_at_utc",
        "status",
        "row_count",
        "source_session_id",
        "rows",
        "error_text",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "snapshot_id",
            validate_id(
                self.snapshot_id,
                expected_kind="position_snapshot",
            ),
        )
        object.__setattr__(
            self,
            "source_session_id",
            validate_id(
                self.source_session_id,
                expected_kind="ib_session",
            ),
        )
        object.__setattr__(
            self,
            "account_id",
            _required_text(self.account_id, field_name="account_id"),
        )
        captured = parse_utc(self.captured_at_utc)
        published = parse_utc(self.published_at_utc)
        if published < captured:
            raise BrokerPositionContractError(
                "published_at_utc cannot precede captured_at_utc"
            )
        if not isinstance(self.status, BrokerPositionSnapshotStatus):
            raise BrokerPositionContractError(
                f"invalid snapshot status: {self.status!r}"
            )

        canonical_rows = tuple(
            sorted(tuple(self.rows), key=lambda item: item.sort_key)
        )
        if not all(
            isinstance(item, BrokerPositionRowV1)
            for item in canonical_rows
        ):
            raise BrokerPositionContractError(
                "rows must contain BrokerPositionRowV1 values"
            )
        con_ids = [item.con_id for item in canonical_rows]
        if len(con_ids) != len(set(con_ids)):
            raise BrokerPositionContractError(
                f"duplicate con_id values in snapshot: {con_ids}"
            )
        object.__setattr__(self, "rows", canonical_rows)

        try:
            count = int(self.row_count)
        except (TypeError, ValueError) as exc:
            raise BrokerPositionContractError(
                f"row_count must be an integer: {self.row_count!r}"
            ) from exc
        if count != len(canonical_rows):
            raise BrokerPositionContractError(
                "row_count does not match rows: "
                f"row_count={count}, rows={len(canonical_rows)}"
            )
        object.__setattr__(self, "row_count", count)

        error_text = _optional_text(self.error_text)
        object.__setattr__(self, "error_text", error_text)
        if self.status == BrokerPositionSnapshotStatus.COMPLETE:
            if error_text is not None:
                raise BrokerPositionContractError(
                    "COMPLETE snapshot cannot contain error_text"
                )
        else:
            if canonical_rows:
                raise BrokerPositionContractError(
                    "FAILED snapshot cannot contain position rows"
                )
            if error_text is None:
                raise BrokerPositionContractError(
                    "FAILED snapshot requires error_text"
                )

    @classmethod
    def complete(
        cls,
        *,
        snapshot_id: str,
        account_id: str,
        captured_at_utc: str,
        published_at_utc: str,
        source_session_id: str,
        rows: tuple[BrokerPositionRowV1, ...],
    ) -> "BrokerPositionSnapshotV1":
        return cls(
            snapshot_id=snapshot_id,
            account_id=account_id,
            captured_at_utc=captured_at_utc,
            published_at_utc=published_at_utc,
            status=BrokerPositionSnapshotStatus.COMPLETE,
            row_count=len(rows),
            source_session_id=source_session_id,
            rows=rows,
            error_text=None,
        )

    @classmethod
    def failed(
        cls,
        *,
        snapshot_id: str,
        account_id: str,
        captured_at_utc: str,
        published_at_utc: str,
        source_session_id: str,
        error_text: str,
    ) -> "BrokerPositionSnapshotV1":
        return cls(
            snapshot_id=snapshot_id,
            account_id=account_id,
            captured_at_utc=captured_at_utc,
            published_at_utc=published_at_utc,
            status=BrokerPositionSnapshotStatus.FAILED,
            row_count=0,
            source_session_id=source_session_id,
            rows=(),
            error_text=error_text,
        )

    def freshness(
        self,
        *,
        observed_at_utc: str,
        max_age_seconds: float,
    ) -> BrokerPositionSnapshotFreshnessV1:
        captured = parse_utc(self.captured_at_utc)
        observed = parse_utc(observed_at_utc)
        age = max(0.0, (observed - captured).total_seconds())
        return BrokerPositionSnapshotFreshnessV1(
            snapshot_id=self.snapshot_id,
            captured_at_utc=format_utc(captured),
            observed_at_utc=format_utc(observed),
            age_seconds=age,
            max_age_seconds=float(max_age_seconds),
            is_fresh=age <= float(max_age_seconds),
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "BrokerPositionSnapshotV1":
        keys = set(value)
        if keys != cls.KEYS:
            raise BrokerPositionContractError(
                "invalid broker-position snapshot keys: "
                f"missing={sorted(cls.KEYS - keys)}, "
                f"unknown={sorted(keys - cls.KEYS)}"
            )
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise BrokerPositionContractError(
                "unsupported broker-position snapshot schema"
            )
        rows = value["rows"]
        if not isinstance(rows, list):
            raise BrokerPositionContractError("rows must be a list")
        try:
            status = BrokerPositionSnapshotStatus(str(value["status"]))
        except ValueError as exc:
            raise BrokerPositionContractError(
                f"invalid snapshot status: {value['status']!r}"
            ) from exc
        return cls(
            snapshot_id=str(value["snapshot_id"]),
            account_id=str(value["account_id"]),
            captured_at_utc=str(value["captured_at_utc"]),
            published_at_utc=str(value["published_at_utc"]),
            status=status,
            row_count=int(value["row_count"]),
            source_session_id=str(value["source_session_id"]),
            rows=tuple(BrokerPositionRowV1.from_dict(item) for item in rows),
            error_text=(
                None
                if value["error_text"] is None
                else str(value["error_text"])
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "snapshot_id": self.snapshot_id,
            "account_id": self.account_id,
            "captured_at_utc": self.captured_at_utc,
            "published_at_utc": self.published_at_utc,
            "status": self.status.value,
            "row_count": self.row_count,
            "source_session_id": self.source_session_id,
            "rows": [item.to_dict() for item in self.rows],
            "error_text": self.error_text,
        }
