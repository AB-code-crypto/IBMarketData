from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Any, ClassVar, Mapping

from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.decision import (
    DecisionOutcome,
    DecisionRecordV1,
    DesiredTargetSide,
    StrategyCommandKind,
    StrategyCommandRequestV1,
)
from ibmd.public_contracts.signal import SignalDirection, SignalEventV1

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


class DecisionDomainError(ValueError):
    pass


class PositionSide(str, Enum):
    FLAT = "FLAT"
    LONG = "LONG"
    SHORT = "SHORT"
    UNKNOWN = "UNKNOWN"


class PositionProjectionStatus(str, Enum):
    FLAT = "FLAT"
    OPEN = "OPEN"
    UNKNOWN = "UNKNOWN"
    STALE = "STALE"
    MULTI_CONTRACT_INCIDENT = "MULTI_CONTRACT_INCIDENT"
    OWNERSHIP_UNPROVEN = "OWNERSHIP_UNPROVEN"


class DailyRiskStatus(str, Enum):
    NOT_READY = "NOT_READY"
    MONITORING = "MONITORING"
    TRIGGERED = "TRIGGERED"
    CLOSING = "CLOSING"
    HALTED = "HALTED"


def _identifier(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(text):
        raise DecisionDomainError(f"invalid {field_name}: {value!r}")
    return text


def _required_text(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise DecisionDomainError(f"{field_name} is required")
    return text


def _optional_text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise DecisionDomainError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise DecisionDomainError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed <= 0 or exact != float(parsed):
        raise DecisionDomainError(
            f"{field_name} must be a positive integer: {value!r}"
        )
    return parsed


def _non_negative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise DecisionDomainError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise DecisionDomainError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed < 0 or exact != float(parsed):
        raise DecisionDomainError(
            f"{field_name} must be a non-negative integer: {value!r}"
        )
    return parsed


def _strict_bool(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise DecisionDomainError(f"{field_name} must be a boolean")
    return value


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
        raise DecisionDomainError(
            f"{context} fields mismatch: missing={missing}, unknown={unknown}"
        )


def _stable_id(kind: str, payload: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()[:32]
    return f"{kind}_{digest}"


@dataclass(frozen=True)
class StrategyPositionFixtureV1:
    account_id: str
    strategy_id: str
    deployment_id: str
    instrument_id: str
    projection_status: PositionProjectionStatus
    side: PositionSide
    quantity: int
    contract_is_active: bool | None

    KEYS: ClassVar[set[str]] = {
        "account_id",
        "strategy_id",
        "deployment_id",
        "instrument_id",
        "projection_status",
        "side",
        "quantity",
        "contract_is_active",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "account_id",
            _required_text(self.account_id, field_name="account_id"),
        )
        object.__setattr__(
            self,
            "strategy_id",
            _identifier(self.strategy_id, field_name="strategy_id"),
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
        if not isinstance(self.projection_status, PositionProjectionStatus):
            raise DecisionDomainError(
                f"invalid projection_status: {self.projection_status!r}"
            )
        if not isinstance(self.side, PositionSide):
            raise DecisionDomainError(f"invalid position side: {self.side!r}")
        object.__setattr__(
            self,
            "quantity",
            _non_negative_int(self.quantity, field_name="quantity"),
        )
        if self.contract_is_active is not None:
            object.__setattr__(
                self,
                "contract_is_active",
                _strict_bool(
                    self.contract_is_active,
                    field_name="contract_is_active",
                ),
            )

        if self.projection_status == PositionProjectionStatus.FLAT:
            if self.side != PositionSide.FLAT or self.quantity != 0:
                raise DecisionDomainError(
                    "FLAT projection requires side=FLAT and quantity=0"
                )
            if self.contract_is_active is not None:
                raise DecisionDomainError(
                    "FLAT projection cannot have contract_is_active"
                )
        elif self.projection_status == PositionProjectionStatus.OPEN:
            if self.side not in {PositionSide.LONG, PositionSide.SHORT}:
                raise DecisionDomainError(
                    "OPEN projection requires LONG or SHORT side"
                )
            if self.quantity <= 0:
                raise DecisionDomainError(
                    "OPEN projection requires positive quantity"
                )
            if self.contract_is_active is None:
                raise DecisionDomainError(
                    "OPEN projection requires contract_is_active proof"
                )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "StrategyPositionFixtureV1":
        _exact_keys(value, cls.KEYS, context="strategy position fixture")
        try:
            status = PositionProjectionStatus(str(value["projection_status"]))
            side = PositionSide(str(value["side"]))
        except ValueError as exc:
            raise DecisionDomainError(
                "invalid strategy-position fixture enum"
            ) from exc
        return cls(
            account_id=str(value["account_id"]),
            strategy_id=str(value["strategy_id"]),
            deployment_id=str(value["deployment_id"]),
            instrument_id=str(value["instrument_id"]),
            projection_status=status,
            side=side,
            quantity=value["quantity"],
            contract_is_active=value["contract_is_active"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "strategy_id": self.strategy_id,
            "deployment_id": self.deployment_id,
            "instrument_id": self.instrument_id,
            "projection_status": self.projection_status.value,
            "side": self.side.value,
            "quantity": self.quantity,
            "contract_is_active": self.contract_is_active,
        }


@dataclass(frozen=True)
class ExecutionDecisionFixtureV1:
    observed_at_utc: str
    execution_ready: bool
    execution_clock_healthy: bool
    pnl_reconciliation_ready: bool
    unresolved_command: bool
    daily_risk_status: DailyRiskStatus
    blocking_reason: str | None
    position: StrategyPositionFixtureV1

    SCHEMA_NAME: ClassVar[str] = "ExecutionDecisionFixture"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "observed_at_utc",
        "execution_ready",
        "execution_clock_healthy",
        "pnl_reconciliation_ready",
        "unresolved_command",
        "daily_risk_status",
        "blocking_reason",
        "position",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "observed_at_utc",
            format_utc(parse_utc(self.observed_at_utc)),
        )
        for field_name in (
            "execution_ready",
            "execution_clock_healthy",
            "pnl_reconciliation_ready",
            "unresolved_command",
        ):
            object.__setattr__(
                self,
                field_name,
                _strict_bool(getattr(self, field_name), field_name=field_name),
            )
        if not isinstance(self.daily_risk_status, DailyRiskStatus):
            raise DecisionDomainError(
                f"invalid daily_risk_status: {self.daily_risk_status!r}"
            )
        object.__setattr__(
            self,
            "blocking_reason",
            _optional_text(self.blocking_reason),
        )
        if not isinstance(self.position, StrategyPositionFixtureV1):
            raise DecisionDomainError("position must be StrategyPositionFixtureV1")

    @property
    def input_hash(self) -> str:
        return hashlib.sha256(
            canonical_json_text(self.to_dict()).encode("utf-8")
        ).hexdigest()

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ExecutionDecisionFixtureV1":
        _exact_keys(value, cls.KEYS, context="execution decision fixture")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise DecisionDomainError("unsupported execution fixture schema")
        raw_position = value["position"]
        if not isinstance(raw_position, Mapping):
            raise DecisionDomainError("fixture position must be an object")
        try:
            risk_status = DailyRiskStatus(str(value["daily_risk_status"]))
        except ValueError as exc:
            raise DecisionDomainError(
                f"invalid daily_risk_status: {value['daily_risk_status']!r}"
            ) from exc
        return cls(
            observed_at_utc=str(value["observed_at_utc"]),
            execution_ready=value["execution_ready"],
            execution_clock_healthy=value["execution_clock_healthy"],
            pnl_reconciliation_ready=value["pnl_reconciliation_ready"],
            unresolved_command=value["unresolved_command"],
            daily_risk_status=risk_status,
            blocking_reason=(
                None
                if value["blocking_reason"] is None
                else str(value["blocking_reason"])
            ),
            position=StrategyPositionFixtureV1.from_dict(raw_position),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "observed_at_utc": self.observed_at_utc,
            "execution_ready": self.execution_ready,
            "execution_clock_healthy": self.execution_clock_healthy,
            "pnl_reconciliation_ready": self.pnl_reconciliation_ready,
            "unresolved_command": self.unresolved_command,
            "daily_risk_status": self.daily_risk_status.value,
            "blocking_reason": self.blocking_reason,
            "position": self.position.to_dict(),
        }


@dataclass(frozen=True)
class DecisionPolicyV1:
    account_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    target_quantity: int
    max_signal_age_seconds: int
    policy_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "account_id",
            _required_text(self.account_id, field_name="account_id"),
        )
        for field_name in (
            "strategy_id",
            "deployment_id",
            "instrument_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _identifier(getattr(self, field_name), field_name=field_name),
            )
        object.__setattr__(
            self,
            "strategy_version",
            _positive_int(self.strategy_version, field_name="strategy_version"),
        )
        object.__setattr__(
            self,
            "target_quantity",
            _positive_int(self.target_quantity, field_name="target_quantity"),
        )
        object.__setattr__(
            self,
            "max_signal_age_seconds",
            _positive_int(
                self.max_signal_age_seconds,
                field_name="max_signal_age_seconds",
            ),
        )
        policy_hash = str(self.policy_hash or "").strip()
        if not _HASH_RE.fullmatch(policy_hash):
            raise DecisionDomainError(
                f"policy_hash must be lowercase SHA-256 hex: {self.policy_hash!r}"
            )
        object.__setattr__(self, "policy_hash", policy_hash)


@dataclass(frozen=True)
class DecisionEvaluation:
    record: DecisionRecordV1
    command: StrategyCommandRequestV1 | None
    fixture_payload: dict[str, Any]


def _validate_scope(
    signal: SignalEventV1,
    fixture: ExecutionDecisionFixtureV1,
    policy: DecisionPolicyV1,
) -> None:
    expected = {
        "account_id": policy.account_id,
        "strategy_id": policy.strategy_id,
        "deployment_id": policy.deployment_id,
        "instrument_id": policy.instrument_id,
    }
    actual = {
        "account_id": fixture.position.account_id,
        "strategy_id": fixture.position.strategy_id,
        "deployment_id": fixture.position.deployment_id,
        "instrument_id": fixture.position.instrument_id,
    }
    if actual != expected:
        raise DecisionDomainError(
            f"fixture scope mismatch: expected={expected}, actual={actual}"
        )
    if signal.strategy_id != policy.strategy_id:
        raise DecisionDomainError(
            "signal strategy_id does not match decision policy"
        )
    if signal.strategy_version != policy.strategy_version:
        raise DecisionDomainError(
            "signal strategy_version does not match decision policy"
        )
    if signal.instrument_id != policy.instrument_id:
        raise DecisionDomainError(
            "signal instrument_id does not match decision policy"
        )


def evaluate_signal(
    *,
    signal: SignalEventV1,
    fixture: ExecutionDecisionFixtureV1,
    policy: DecisionPolicyV1,
) -> DecisionEvaluation:
    _validate_scope(signal, fixture, policy)
    observed = parse_utc(fixture.observed_at_utc)
    created = parse_utc(signal.created_at_utc)
    if observed < created:
        raise DecisionDomainError(
            "execution fixture observed_at_utc cannot precede signal creation"
        )

    fixture_payload = fixture.to_dict()
    input_hash = fixture.input_hash
    decision_id = _stable_id(
        "decision_record",
        {
            "source_signal_id": signal.event_id,
            "deployment_id": policy.deployment_id,
            "policy_hash": policy.policy_hash,
            "input_hash": input_hash,
        },
    )

    def finish(
        *,
        outcome: DecisionOutcome,
        reason_code: str,
        reason_detail: str | None = None,
        command: StrategyCommandRequestV1 | None = None,
    ) -> DecisionEvaluation:
        record = DecisionRecordV1(
            decision_id=decision_id,
            strategy_id=policy.strategy_id,
            strategy_version=policy.strategy_version,
            deployment_id=policy.deployment_id,
            instrument_id=policy.instrument_id,
            source_signal_id=signal.event_id,
            evaluated_at_utc=fixture.observed_at_utc,
            outcome=outcome,
            reason_code=reason_code,
            reason_detail=reason_detail,
            input_hash=input_hash,
            policy_hash=policy.policy_hash,
            position_status=fixture.position.projection_status.value,
            position_side=fixture.position.side.value,
            position_quantity=fixture.position.quantity,
            command_id=None if command is None else command.command_id,
            command_kind=None if command is None else command.command_kind,
        )
        return DecisionEvaluation(
            record=record,
            command=command,
            fixture_payload=fixture_payload,
        )

    expires = created + timedelta(seconds=policy.max_signal_age_seconds)
    if observed >= expires:
        return finish(
            outcome=DecisionOutcome.REJECTED,
            reason_code="signal_expired",
            reason_detail=(
                f"observed_at={format_utc(observed)}; "
                f"expires_at={format_utc(expires)}"
            ),
        )

    status = fixture.position.projection_status
    if status not in {
        PositionProjectionStatus.FLAT,
        PositionProjectionStatus.OPEN,
    }:
        return finish(
            outcome=DecisionOutcome.REJECTED,
            reason_code=f"position_{status.value.lower()}",
            reason_detail=fixture.blocking_reason,
        )

    if fixture.unresolved_command:
        return finish(
            outcome=DecisionOutcome.NO_ACTION,
            reason_code="unresolved_command_exists",
            reason_detail=fixture.blocking_reason,
        )

    signal_side = DesiredTargetSide(signal.direction.value)
    if status == PositionProjectionStatus.FLAT:
        command_kind = StrategyCommandKind.OPEN
        command_reason = "flat_position_open_by_rolling_signal"
    else:
        position_side = fixture.position.side
        if position_side.value == signal_side.value:
            return finish(
                outcome=DecisionOutcome.NO_ACTION,
                reason_code="already_at_target_side",
            )
        if fixture.position.contract_is_active is not True:
            return finish(
                outcome=DecisionOutcome.REJECTED,
                reason_code="non_active_contract_requires_execution_liquidation",
                reason_detail=fixture.blocking_reason,
            )
        command_kind = StrategyCommandKind.REVERSE
        command_reason = "opposite_rolling_signal_reverse_position"

    if not fixture.execution_ready:
        return finish(
            outcome=DecisionOutcome.REJECTED,
            reason_code="execution_not_ready",
            reason_detail=fixture.blocking_reason,
        )
    if not fixture.execution_clock_healthy:
        return finish(
            outcome=DecisionOutcome.REJECTED,
            reason_code="execution_clock_unhealthy",
            reason_detail=fixture.blocking_reason,
        )
    if not fixture.pnl_reconciliation_ready:
        return finish(
            outcome=DecisionOutcome.REJECTED,
            reason_code="pnl_reconciliation_not_ready",
            reason_detail=fixture.blocking_reason,
        )
    if fixture.daily_risk_status != DailyRiskStatus.MONITORING:
        return finish(
            outcome=DecisionOutcome.REJECTED,
            reason_code=(
                f"daily_risk_{fixture.daily_risk_status.value.lower()}"
            ),
            reason_detail=fixture.blocking_reason,
        )

    command_id = _stable_id(
        "strategy_command",
        {
            "strategy_id": policy.strategy_id,
            "strategy_version": policy.strategy_version,
            "deployment_id": policy.deployment_id,
            "instrument_id": policy.instrument_id,
            "source_signal_id": signal.event_id,
            "policy_hash": policy.policy_hash,
        },
    )
    command = StrategyCommandRequestV1(
        command_id=command_id,
        strategy_id=policy.strategy_id,
        strategy_version=policy.strategy_version,
        deployment_id=policy.deployment_id,
        instrument_id=policy.instrument_id,
        source_signal_id=signal.event_id,
        desired_target_side=signal_side,
        desired_target_quantity=policy.target_quantity,
        command_kind=command_kind,
        reason=command_reason,
        created_at_utc=fixture.observed_at_utc,
        expires_at_utc=format_utc(expires),
        policy_hash=policy.policy_hash,
    )
    return finish(
        outcome=DecisionOutcome.COMMAND,
        reason_code=command_reason,
        command=command,
    )
