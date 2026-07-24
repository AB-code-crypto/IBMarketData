from __future__ import annotations

import math
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar, Mapping

from ibmd.foundation.identity import validate_id
from ibmd.foundation.time import format_utc, parse_utc

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


class DecisionContractError(ValueError):
    pass


class DecisionOutcome(str, Enum):
    COMMAND = "COMMAND"
    NO_ACTION = "NO_ACTION"
    REJECTED = "REJECTED"


class StrategyCommandKind(str, Enum):
    OPEN = "OPEN"
    REVERSE = "REVERSE"


class DesiredTargetSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


def _exact_keys(
    value: Mapping[str, Any],
    expected: set[str],
    *,
    context: str,
) -> None:
    actual = set(value)
    missing = sorted(expected - actual)
    unknown = sorted(actual - expected)
    if missing or unknown:
        raise DecisionContractError(
            f"{context} fields mismatch: missing={missing}, unknown={unknown}"
        )


def _identifier(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(text):
        raise DecisionContractError(f"invalid {field_name}: {value!r}")
    return text


def _required_text(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise DecisionContractError(f"{field_name} is required")
    return text


def _optional_text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _hash(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not _HASH_RE.fullmatch(text):
        raise DecisionContractError(
            f"{field_name} must be lowercase SHA-256 hex: {value!r}"
        )
    return text


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise DecisionContractError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise DecisionContractError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed <= 0 or exact != float(parsed):
        raise DecisionContractError(
            f"{field_name} must be a positive integer: {value!r}"
        )
    return parsed


def _non_negative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise DecisionContractError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise DecisionContractError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed < 0 or exact != float(parsed):
        raise DecisionContractError(
            f"{field_name} must be a non-negative integer: {value!r}"
        )
    return parsed


@dataclass(frozen=True)
class StrategyCommandRequestV1:
    command_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    source_signal_id: str
    desired_target_side: DesiredTargetSide
    desired_target_quantity: int
    command_kind: StrategyCommandKind
    reason: str
    created_at_utc: str
    expires_at_utc: str
    policy_hash: str

    SCHEMA_NAME: ClassVar[str] = "StrategyCommandRequest"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "command_id",
        "strategy_id",
        "strategy_version",
        "deployment_id",
        "instrument_id",
        "source_signal_id",
        "desired_target_side",
        "desired_target_quantity",
        "command_kind",
        "reason",
        "created_at_utc",
        "expires_at_utc",
        "policy_hash",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "command_id",
            validate_id(self.command_id, expected_kind="strategy_command"),
        )
        object.__setattr__(
            self,
            "strategy_id",
            _identifier(self.strategy_id, field_name="strategy_id"),
        )
        object.__setattr__(
            self,
            "strategy_version",
            _positive_int(self.strategy_version, field_name="strategy_version"),
        )
        object.__setattr__(
            self,
            "deployment_id",
            _identifier(self.deployment_id, field_name="deployment_id"),
        )
        object.__setattr__(
            self,
            "instrument_id",
            _identifier(self.instrument_id, field_name="instrument_id"),
        )
        object.__setattr__(
            self,
            "source_signal_id",
            validate_id(self.source_signal_id, expected_kind="signal_event"),
        )
        if not isinstance(self.desired_target_side, DesiredTargetSide):
            raise DecisionContractError(
                f"invalid desired_target_side: {self.desired_target_side!r}"
            )
        object.__setattr__(
            self,
            "desired_target_quantity",
            _positive_int(
                self.desired_target_quantity,
                field_name="desired_target_quantity",
            ),
        )
        if not isinstance(self.command_kind, StrategyCommandKind):
            raise DecisionContractError(
                f"invalid command_kind: {self.command_kind!r}"
            )
        object.__setattr__(
            self,
            "reason",
            _required_text(self.reason, field_name="reason"),
        )
        created = parse_utc(self.created_at_utc)
        expires = parse_utc(self.expires_at_utc)
        if expires <= created:
            raise DecisionContractError(
                "expires_at_utc must be later than created_at_utc"
            )
        object.__setattr__(self, "created_at_utc", format_utc(created))
        object.__setattr__(self, "expires_at_utc", format_utc(expires))
        object.__setattr__(
            self,
            "policy_hash",
            _hash(self.policy_hash, field_name="policy_hash"),
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "StrategyCommandRequestV1":
        _exact_keys(value, cls.KEYS, context="strategy command request")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise DecisionContractError("unsupported strategy-command schema")
        try:
            target_side = DesiredTargetSide(str(value["desired_target_side"]))
            command_kind = StrategyCommandKind(str(value["command_kind"]))
        except ValueError as exc:
            raise DecisionContractError(
                "invalid strategy-command enum value"
            ) from exc
        return cls(
            command_id=str(value["command_id"]),
            strategy_id=str(value["strategy_id"]),
            strategy_version=value["strategy_version"],
            deployment_id=str(value["deployment_id"]),
            instrument_id=str(value["instrument_id"]),
            source_signal_id=str(value["source_signal_id"]),
            desired_target_side=target_side,
            desired_target_quantity=value["desired_target_quantity"],
            command_kind=command_kind,
            reason=str(value["reason"]),
            created_at_utc=str(value["created_at_utc"]),
            expires_at_utc=str(value["expires_at_utc"]),
            policy_hash=str(value["policy_hash"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "command_id": self.command_id,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "deployment_id": self.deployment_id,
            "instrument_id": self.instrument_id,
            "source_signal_id": self.source_signal_id,
            "desired_target_side": self.desired_target_side.value,
            "desired_target_quantity": self.desired_target_quantity,
            "command_kind": self.command_kind.value,
            "reason": self.reason,
            "created_at_utc": self.created_at_utc,
            "expires_at_utc": self.expires_at_utc,
            "policy_hash": self.policy_hash,
        }


@dataclass(frozen=True)
class DecisionRecordV1:
    decision_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    source_signal_id: str
    evaluated_at_utc: str
    outcome: DecisionOutcome
    reason_code: str
    reason_detail: str | None
    input_hash: str
    policy_hash: str
    position_status: str
    position_side: str
    position_quantity: int
    command_id: str | None = None
    command_kind: StrategyCommandKind | None = None

    SCHEMA_NAME: ClassVar[str] = "DecisionRecord"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "decision_id",
        "strategy_id",
        "strategy_version",
        "deployment_id",
        "instrument_id",
        "source_signal_id",
        "evaluated_at_utc",
        "outcome",
        "reason_code",
        "reason_detail",
        "input_hash",
        "policy_hash",
        "position_status",
        "position_side",
        "position_quantity",
        "command_id",
        "command_kind",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "decision_id",
            validate_id(self.decision_id, expected_kind="decision_record"),
        )
        object.__setattr__(
            self,
            "strategy_id",
            _identifier(self.strategy_id, field_name="strategy_id"),
        )
        object.__setattr__(
            self,
            "strategy_version",
            _positive_int(self.strategy_version, field_name="strategy_version"),
        )
        object.__setattr__(
            self,
            "deployment_id",
            _identifier(self.deployment_id, field_name="deployment_id"),
        )
        object.__setattr__(
            self,
            "instrument_id",
            _identifier(self.instrument_id, field_name="instrument_id"),
        )
        object.__setattr__(
            self,
            "source_signal_id",
            validate_id(self.source_signal_id, expected_kind="signal_event"),
        )
        object.__setattr__(
            self,
            "evaluated_at_utc",
            format_utc(parse_utc(self.evaluated_at_utc)),
        )
        if not isinstance(self.outcome, DecisionOutcome):
            raise DecisionContractError(f"invalid outcome: {self.outcome!r}")
        object.__setattr__(
            self,
            "reason_code",
            _identifier(self.reason_code, field_name="reason_code"),
        )
        object.__setattr__(self, "reason_detail", _optional_text(self.reason_detail))
        object.__setattr__(
            self,
            "input_hash",
            _hash(self.input_hash, field_name="input_hash"),
        )
        object.__setattr__(
            self,
            "policy_hash",
            _hash(self.policy_hash, field_name="policy_hash"),
        )
        object.__setattr__(
            self,
            "position_status",
            _identifier(self.position_status, field_name="position_status"),
        )
        object.__setattr__(
            self,
            "position_side",
            _identifier(self.position_side, field_name="position_side"),
        )
        object.__setattr__(
            self,
            "position_quantity",
            _non_negative_int(
                self.position_quantity,
                field_name="position_quantity",
            ),
        )

        if self.outcome == DecisionOutcome.COMMAND:
            if self.command_id is None or self.command_kind is None:
                raise DecisionContractError(
                    "COMMAND decision requires command_id and command_kind"
                )
            object.__setattr__(
                self,
                "command_id",
                validate_id(self.command_id, expected_kind="strategy_command"),
            )
            if not isinstance(self.command_kind, StrategyCommandKind):
                raise DecisionContractError(
                    f"invalid command_kind: {self.command_kind!r}"
                )
        elif self.command_id is not None or self.command_kind is not None:
            raise DecisionContractError(
                "non-COMMAND decision cannot reference a strategy command"
            )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "DecisionRecordV1":
        _exact_keys(value, cls.KEYS, context="decision record")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise DecisionContractError("unsupported decision-record schema")
        try:
            outcome = DecisionOutcome(str(value["outcome"]))
            command_kind = (
                None
                if value["command_kind"] is None
                else StrategyCommandKind(str(value["command_kind"]))
            )
        except ValueError as exc:
            raise DecisionContractError("invalid decision enum value") from exc
        return cls(
            decision_id=str(value["decision_id"]),
            strategy_id=str(value["strategy_id"]),
            strategy_version=value["strategy_version"],
            deployment_id=str(value["deployment_id"]),
            instrument_id=str(value["instrument_id"]),
            source_signal_id=str(value["source_signal_id"]),
            evaluated_at_utc=str(value["evaluated_at_utc"]),
            outcome=outcome,
            reason_code=str(value["reason_code"]),
            reason_detail=(
                None
                if value["reason_detail"] is None
                else str(value["reason_detail"])
            ),
            input_hash=str(value["input_hash"]),
            policy_hash=str(value["policy_hash"]),
            position_status=str(value["position_status"]),
            position_side=str(value["position_side"]),
            position_quantity=value["position_quantity"],
            command_id=(
                None if value["command_id"] is None else str(value["command_id"])
            ),
            command_kind=command_kind,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "decision_id": self.decision_id,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "deployment_id": self.deployment_id,
            "instrument_id": self.instrument_id,
            "source_signal_id": self.source_signal_id,
            "evaluated_at_utc": self.evaluated_at_utc,
            "outcome": self.outcome.value,
            "reason_code": self.reason_code,
            "reason_detail": self.reason_detail,
            "input_hash": self.input_hash,
            "policy_hash": self.policy_hash,
            "position_status": self.position_status,
            "position_side": self.position_side,
            "position_quantity": self.position_quantity,
            "command_id": self.command_id,
            "command_kind": (
                None if self.command_kind is None else self.command_kind.value
            ),
        }
