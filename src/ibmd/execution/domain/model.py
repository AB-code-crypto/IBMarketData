from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, ClassVar, Mapping

from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.decision import (
    DesiredTargetSide,
    StrategyCommandKind,
    StrategyCommandRequestV1,
)
from ibmd.public_contracts.execution import (
    DailyRiskStateV1,
    DailyRiskStatus,
    ExecutionCommandState,
    ExecutionCommandStateV1,
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
    StrategyPositionSide,
    StrategyPositionStatus,
    StrategyPositionV1,
)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


class ExecutionDomainError(ValueError):
    pass


def _identifier(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(text):
        raise ExecutionDomainError(f"invalid {field_name}: {value!r}")
    return text


def _required_text(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ExecutionDomainError(f"{field_name} is required")
    return text


def _hash(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not _HASH_RE.fullmatch(text):
        raise ExecutionDomainError(
            f"{field_name} must be lowercase SHA-256 hex: {value!r}"
        )
    return text


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise ExecutionDomainError(f"{field_name} must be an integer")
    try:
        parsed = int(value)
        exact = float(value)
    except (TypeError, ValueError) as exc:
        raise ExecutionDomainError(
            f"{field_name} must be an integer: {value!r}"
        ) from exc
    if parsed <= 0 or exact != float(parsed):
        raise ExecutionDomainError(
            f"{field_name} must be a positive integer: {value!r}"
        )
    return parsed


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
        raise ExecutionDomainError(
            f"{context} fields mismatch: missing={missing}, unknown={unknown}"
        )


@dataclass(frozen=True)
class ExecutionFoundationPolicyV1:
    account_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    policy_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "account_id",
            _required_text(self.account_id, field_name="account_id"),
        )
        for field_name in ("strategy_id", "deployment_id", "instrument_id"):
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
            "policy_hash",
            _hash(self.policy_hash, field_name="policy_hash"),
        )


@dataclass(frozen=True)
class ExecutionFoundationFixtureV1:
    observed_at_utc: str
    readiness: ExecutionReadinessV1
    position: StrategyPositionV1
    daily_risk: DailyRiskStateV1

    SCHEMA_NAME: ClassVar[str] = "ExecutionFoundationFixture"
    SCHEMA_VERSION: ClassVar[int] = 1
    KEYS: ClassVar[set[str]] = {
        "schema_name",
        "schema_version",
        "observed_at_utc",
        "readiness",
        "position",
        "daily_risk",
    }

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "observed_at_utc",
            format_utc(parse_utc(self.observed_at_utc)),
        )
        if not isinstance(self.readiness, ExecutionReadinessV1):
            raise ExecutionDomainError(
                "readiness must be ExecutionReadinessV1"
            )
        if not isinstance(self.position, StrategyPositionV1):
            raise ExecutionDomainError("position must be StrategyPositionV1")
        if not isinstance(self.daily_risk, DailyRiskStateV1):
            raise ExecutionDomainError("daily_risk must be DailyRiskStateV1")

        scopes = {
            (
                self.readiness.account_id,
                self.readiness.strategy_id,
                self.readiness.deployment_id,
            ),
            (
                self.position.account_id,
                self.position.strategy_id,
                self.position.deployment_id,
            ),
            (
                self.daily_risk.account_id,
                self.daily_risk.strategy_id,
                self.daily_risk.deployment_id,
            ),
        }
        if len(scopes) != 1:
            raise ExecutionDomainError(
                "execution fixture readiness/position/risk scopes differ"
            )
        if self.readiness.instrument_id != self.position.instrument_id:
            raise ExecutionDomainError(
                "execution fixture readiness/position instruments differ"
            )

    @property
    def input_hash(self) -> str:
        return hashlib.sha256(
            canonical_json_text(self.to_dict()).encode("utf-8")
        ).hexdigest()

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ExecutionFoundationFixtureV1":
        _exact_keys(value, cls.KEYS, context="execution foundation fixture")
        if (
            value["schema_name"] != cls.SCHEMA_NAME
            or value["schema_version"] != cls.SCHEMA_VERSION
        ):
            raise ExecutionDomainError("unsupported execution fixture schema")
        readiness = value["readiness"]
        position = value["position"]
        daily_risk = value["daily_risk"]
        if not isinstance(readiness, Mapping):
            raise ExecutionDomainError("fixture readiness must be an object")
        if not isinstance(position, Mapping):
            raise ExecutionDomainError("fixture position must be an object")
        if not isinstance(daily_risk, Mapping):
            raise ExecutionDomainError("fixture daily_risk must be an object")
        return cls(
            observed_at_utc=str(value["observed_at_utc"]),
            readiness=ExecutionReadinessV1.from_dict(readiness),
            position=StrategyPositionV1.from_dict(position),
            daily_risk=DailyRiskStateV1.from_dict(daily_risk),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_name": self.SCHEMA_NAME,
            "schema_version": self.SCHEMA_VERSION,
            "observed_at_utc": self.observed_at_utc,
            "readiness": self.readiness.to_dict(),
            "position": self.position.to_dict(),
            "daily_risk": self.daily_risk.to_dict(),
        }


@dataclass(frozen=True)
class ExecutionAdmission:
    command_state: ExecutionCommandStateV1
    fixture_payload: dict[str, Any]

    def __post_init__(self) -> None:
        if not isinstance(self.command_state, ExecutionCommandStateV1):
            raise ExecutionDomainError(
                "command_state must be ExecutionCommandStateV1"
            )
        if not isinstance(self.fixture_payload, dict):
            raise ExecutionDomainError("fixture_payload must be a dict")


def _scope_error(
    *,
    command: StrategyCommandRequestV1,
    policy: ExecutionFoundationPolicyV1,
    fixture: ExecutionFoundationFixtureV1,
) -> str | None:
    expected = (
        policy.strategy_id,
        policy.strategy_version,
        policy.deployment_id,
        policy.instrument_id,
    )
    actual = (
        command.strategy_id,
        command.strategy_version,
        command.deployment_id,
        command.instrument_id,
    )
    if actual != expected:
        return f"command_routing_mismatch: expected={expected}, actual={actual}"
    if command.policy_hash != policy.policy_hash:
        return (
            "command_policy_hash_mismatch: "
            f"expected={policy.policy_hash}, actual={command.policy_hash}"
        )
    fixture_scope = (
        fixture.position.account_id,
        fixture.position.strategy_id,
        fixture.position.deployment_id,
        fixture.position.instrument_id,
    )
    expected_fixture = (
        policy.account_id,
        policy.strategy_id,
        policy.deployment_id,
        policy.instrument_id,
    )
    if fixture_scope != expected_fixture:
        return (
            "fixture_routing_mismatch: "
            f"expected={expected_fixture}, actual={fixture_scope}"
        )
    return None


def _rejection_reason(
    *,
    command: StrategyCommandRequestV1,
    policy: ExecutionFoundationPolicyV1,
    fixture: ExecutionFoundationFixtureV1,
) -> str | None:
    scope = _scope_error(command=command, policy=policy, fixture=fixture)
    if scope is not None:
        return scope

    observed = parse_utc(fixture.observed_at_utc)
    if observed >= parse_utc(command.expires_at_utc):
        return "command_expired"

    readiness = fixture.readiness
    if readiness.status != ExecutionReadinessStatus.READY:
        return f"execution_{readiness.status.value.lower()}"
    if not readiness.command_intake_enabled:
        return "command_intake_disabled"
    if not readiness.reconciliation_complete:
        return "reconciliation_incomplete"
    if not readiness.clock_healthy:
        return "execution_clock_unhealthy"

    risk = fixture.daily_risk
    if not risk.pnl_ready:
        return "daily_risk_pnl_not_ready"
    if risk.status != DailyRiskStatus.MONITORING:
        return f"daily_risk_{risk.status.value.lower()}"

    position = fixture.position
    if position.projection_status not in {
        StrategyPositionStatus.FLAT,
        StrategyPositionStatus.OPEN,
    }:
        return f"position_{position.projection_status.value.lower()}"

    if command.command_kind == StrategyCommandKind.OPEN:
        if position.projection_status != StrategyPositionStatus.FLAT:
            return "open_requires_flat_position"
        return None

    if command.command_kind == StrategyCommandKind.REVERSE:
        if position.projection_status != StrategyPositionStatus.OPEN:
            return "reverse_requires_open_position"
        expected_current = (
            StrategyPositionSide.SHORT
            if command.desired_target_side == DesiredTargetSide.LONG
            else StrategyPositionSide.LONG
        )
        if position.side != expected_current:
            return "reverse_requires_opposite_position"
        if not position.contracts or any(
            not contract.contract_is_active for contract in position.contracts
        ):
            return "non_active_contract_requires_liquidation"
        return None

    return f"unsupported_command_kind:{command.command_kind.value}"


def admit_strategy_command(
    *,
    command: StrategyCommandRequestV1,
    policy: ExecutionFoundationPolicyV1,
    fixture: ExecutionFoundationFixtureV1,
) -> ExecutionAdmission:
    if not isinstance(command, StrategyCommandRequestV1):
        raise ExecutionDomainError(
            "command must be StrategyCommandRequestV1"
        )
    if not isinstance(policy, ExecutionFoundationPolicyV1):
        raise ExecutionDomainError(
            "policy must be ExecutionFoundationPolicyV1"
        )
    if not isinstance(fixture, ExecutionFoundationFixtureV1):
        raise ExecutionDomainError(
            "fixture must be ExecutionFoundationFixtureV1"
        )

    reason = _rejection_reason(
        command=command,
        policy=policy,
        fixture=fixture,
    )
    observed = fixture.observed_at_utc
    state = (
        ExecutionCommandState.ADMITTED
        if reason is None
        else ExecutionCommandState.REJECTED
    )
    command_state = ExecutionCommandStateV1(
        command_id=command.command_id,
        strategy_id=command.strategy_id,
        strategy_version=command.strategy_version,
        deployment_id=command.deployment_id,
        instrument_id=command.instrument_id,
        command_kind=command.command_kind,
        desired_target_side=command.desired_target_side,
        desired_target_quantity=command.desired_target_quantity,
        state=state,
        requested_qty=command.desired_target_quantity,
        filled_qty=0,
        remaining_qty=command.desired_target_quantity,
        latest_attempt_id=None,
        blocking_reason=reason,
        received_at_utc=observed,
        updated_at_utc=observed,
        terminal_at_utc=(
            observed if state == ExecutionCommandState.REJECTED else None
        ),
    )
    return ExecutionAdmission(
        command_state=command_state,
        fixture_payload=fixture.to_dict(),
    )
