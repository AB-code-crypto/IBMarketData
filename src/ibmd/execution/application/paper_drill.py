from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ibmd.decision.domain import DecisionEvaluation
from ibmd.execution.domain import (
    BrokerOperationSnapshot,
    ExecutionFoundationFixtureV1,
    ExecutionFoundationPolicyV1,
    PositionProjectionPolicyV1,
    RegisteredFuturesContractV1,
    admit_strategy_command,
    project_strategy_position,
)
from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.decision import (
    DecisionOutcome,
    DecisionRecordV1,
    DesiredTargetSide,
    StrategyCommandKind,
    StrategyCommandRequestV1,
)
from ibmd.public_contracts.execution import (
    DailyRiskCleanupStatus,
    DailyRiskStateV1,
    DailyRiskStatus,
    ExecutionCommandState,
    ExecutionCommandStateV1,
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
    StrategyPositionStatus,
    StrategyPositionV1,
)
from ibmd.public_contracts.positions import BrokerPositionSnapshotV1

_DRILL_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class PaperDrillPreparationError(ValueError):
    pass


class DecisionDrillRepository(Protocol):
    def read_record(self, decision_id: str) -> DecisionRecordV1 | None: ...

    def read_command(self, command_id: str) -> StrategyCommandRequestV1 | None: ...

    def publish(self, evaluation: DecisionEvaluation) -> DecisionEvaluation: ...


class ExecutionDrillRepository(Protocol):
    def read_command_state(
        self,
        command_id: str,
    ) -> ExecutionCommandStateV1 | None: ...

    def publish_fixture(
        self,
        fixture: ExecutionFoundationFixtureV1,
    ) -> tuple[ExecutionReadinessV1, StrategyPositionV1, DailyRiskStateV1]: ...

    def publish_admission(self, admission) -> ExecutionCommandStateV1: ...


class ExecutionDrillStateSource(Protocol):
    def read_position(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> StrategyPositionV1 | None: ...


class PositionSnapshotSource(Protocol):
    def read_latest_complete(self) -> BrokerPositionSnapshotV1 | None: ...


class BrokerAttemptDrillSource(Protocol):
    def read_unresolved(self) -> tuple[BrokerOperationSnapshot, ...]: ...


@dataclass(frozen=True)
class PaperDrillPolicy:
    drill_id: str
    account_id: str
    environment: str
    confirmed_paper_account_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    policy_hash: str
    target_side: DesiredTargetSide
    target_quantity: int
    command_ttl_seconds: int
    position_max_age_seconds: float
    daily_risk_timezone: str
    daily_risk_target: float
    active_contract: RegisteredFuturesContractV1 | None

    def __post_init__(self) -> None:
        drill_id = str(self.drill_id or "").strip()
        if not _DRILL_ID_RE.fullmatch(drill_id):
            raise PaperDrillPreparationError(
                "drill_id must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}"
            )
        object.__setattr__(self, "drill_id", drill_id)
        environment = str(self.environment or "").strip().lower()
        object.__setattr__(self, "environment", environment)
        for field_name in (
            "account_id",
            "confirmed_paper_account_id",
            "strategy_id",
            "deployment_id",
            "instrument_id",
            "policy_hash",
            "daily_risk_timezone",
        ):
            value = str(getattr(self, field_name) or "").strip()
            if not value:
                raise PaperDrillPreparationError(f"{field_name} is required")
            object.__setattr__(self, field_name, value)
        if "paper-drill" not in self.deployment_id.lower():
            raise PaperDrillPreparationError(
                "paper drill preparation requires a dedicated deployment_id "
                "containing 'paper-drill'"
            )
        try:
            strategy_version = int(self.strategy_version)
            target_quantity = int(self.target_quantity)
            ttl = int(self.command_ttl_seconds)
        except (TypeError, ValueError) as exc:
            raise PaperDrillPreparationError(
                "strategy_version, target_quantity and command_ttl_seconds "
                "must be integers"
            ) from exc
        if strategy_version <= 0 or target_quantity <= 0:
            raise PaperDrillPreparationError(
                "strategy_version and target_quantity must be positive"
            )
        if ttl < 60 or ttl > 900:
            raise PaperDrillPreparationError(
                "command_ttl_seconds must be between 60 and 900"
            )
        object.__setattr__(self, "strategy_version", strategy_version)
        object.__setattr__(self, "target_quantity", target_quantity)
        object.__setattr__(self, "command_ttl_seconds", ttl)
        maximum_age = float(self.position_max_age_seconds)
        if maximum_age <= 0.0:
            raise PaperDrillPreparationError(
                "position_max_age_seconds must be positive"
            )
        object.__setattr__(self, "position_max_age_seconds", maximum_age)
        target = float(self.daily_risk_target)
        if target <= 0.0:
            raise PaperDrillPreparationError(
                "daily_risk_target must be positive"
            )
        object.__setattr__(self, "daily_risk_target", target)
        if not isinstance(self.target_side, DesiredTargetSide):
            raise PaperDrillPreparationError(
                "target_side must be DesiredTargetSide"
            )
        try:
            ZoneInfo(self.daily_risk_timezone)
        except ZoneInfoNotFoundError as exc:
            raise PaperDrillPreparationError(
                f"unknown daily risk timezone: {self.daily_risk_timezone!r}"
            ) from exc
        if self.active_contract is None:
            raise PaperDrillPreparationError(
                "one active registered contract is required"
            )
        if not isinstance(self.active_contract, RegisteredFuturesContractV1):
            raise PaperDrillPreparationError(
                "active_contract must be RegisteredFuturesContractV1"
            )
        if not self.active_contract.contract_is_active:
            raise PaperDrillPreparationError(
                "paper drill active contract is not active"
            )


@dataclass(frozen=True)
class PaperDrillPreparation:
    drill_id: str
    command: StrategyCommandRequestV1
    decision: DecisionRecordV1
    command_state: ExecutionCommandStateV1
    fixture: ExecutionFoundationFixtureV1
    active_contract: RegisteredFuturesContractV1
    position_proof_expires_at_utc: str
    reused_existing_command: bool

    @property
    def ready_for_submit(self) -> bool:
        return (
            self.command_state.state == ExecutionCommandState.ADMITTED
            and self.fixture.position.projection_status
            == StrategyPositionStatus.FLAT
            and parse_utc(self.fixture.observed_at_utc)
            < parse_utc(self.command.expires_at_utc)
        )

    def to_dict(self) -> dict:
        return {
            "drill_id": self.drill_id,
            "ready_for_submit": self.ready_for_submit,
            "reused_existing_command": self.reused_existing_command,
            "broker_mutations_performed": False,
            "position_proof_expires_at_utc": self.position_proof_expires_at_utc,
            "submit_before_utc": min(
                self.command.expires_at_utc,
                self.position_proof_expires_at_utc,
            ),
            "command": self.command.to_dict(),
            "decision": self.decision.to_dict(),
            "command_state": self.command_state.to_dict(),
            "execution_fixture": self.fixture.to_dict(),
            "active_contract": {
                "con_id": self.active_contract.con_id,
                "local_symbol": self.active_contract.local_symbol,
                "contract_is_active": self.active_contract.contract_is_active,
            },
        }


def _stable_id(kind: str, payload: dict[str, object]) -> str:
    digest = hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()[:32]
    return f"{kind}_{digest}"


def require_paper_drill_gate(policy: PaperDrillPolicy) -> None:
    if policy.environment != "paper":
        raise PaperDrillPreparationError(
            "paper drill preparation requires IBMD_ENVIRONMENT=paper"
        )
    if policy.confirmed_paper_account_id != policy.account_id:
        raise PaperDrillPreparationError(
            "paper account confirmation does not match configured account"
        )
    if not policy.account_id.upper().startswith("D"):
        raise PaperDrillPreparationError(
            "configured account does not look like an IB paper account"
        )


class PaperExecutionDrillPreparer:
    def __init__(
        self,
        *,
        policy: PaperDrillPolicy,
        decision_repository: DecisionDrillRepository,
        execution_repository: ExecutionDrillRepository,
        execution_state_source: ExecutionDrillStateSource,
        position_snapshot_source: PositionSnapshotSource,
        broker_attempt_source: BrokerAttemptDrillSource,
        contract_registry: tuple[RegisteredFuturesContractV1, ...],
    ) -> None:
        self.policy = policy
        self.decision_repository = decision_repository
        self.execution_repository = execution_repository
        self.execution_state_source = execution_state_source
        self.position_snapshot_source = position_snapshot_source
        self.broker_attempt_source = broker_attempt_source
        self.contract_registry = tuple(contract_registry)
        if not self.contract_registry:
            raise PaperDrillPreparationError(
                "contract_registry cannot be empty"
            )

    def _identity_payload(self) -> dict[str, object]:
        return {
            "drill_id": self.policy.drill_id,
            "account_id": self.policy.account_id,
            "strategy_id": self.policy.strategy_id,
            "strategy_version": self.policy.strategy_version,
            "deployment_id": self.policy.deployment_id,
            "instrument_id": self.policy.instrument_id,
        }

    def _ids(self) -> tuple[str, str, str]:
        payload = self._identity_payload()
        return (
            _stable_id("signal_event", payload),
            _stable_id("decision_record", payload),
            _stable_id("strategy_command", payload),
        )

    def _assert_no_other_unresolved(self, command_id: str) -> None:
        conflicts = [
            item.operation.operation_id
            for item in self.broker_attempt_source.read_unresolved()
            if item.operation.command_id != command_id
            and (
                item.operation.account_id,
                item.operation.strategy_id,
                item.operation.deployment_id,
                item.operation.instrument_id,
            )
            == (
                self.policy.account_id,
                self.policy.strategy_id,
                self.policy.deployment_id,
                self.policy.instrument_id,
            )
        ]
        if conflicts:
            raise PaperDrillPreparationError(
                "another unresolved broker operation already owns the paper "
                f"drill scope: {conflicts}"
            )

    def _project_flat(self, observed_at_utc: str) -> StrategyPositionV1:
        previous = self.execution_state_source.read_position(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
            instrument_id=self.policy.instrument_id,
        )
        if (
            previous is not None
            and previous.projection_status != StrategyPositionStatus.FLAT
        ):
            raise PaperDrillPreparationError(
                "dedicated paper drill execution store already contains a "
                f"non-FLAT position: {previous.projection_status.value}"
            )
        snapshot = self.position_snapshot_source.read_latest_complete()
        projection = project_strategy_position(
            snapshot=snapshot,
            previous=previous,
            policy=PositionProjectionPolicyV1(
                account_id=self.policy.account_id,
                strategy_id=self.policy.strategy_id,
                deployment_id=self.policy.deployment_id,
                instrument_id=self.policy.instrument_id,
                max_snapshot_age_seconds=self.policy.position_max_age_seconds,
            ),
            registry=self.contract_registry,
            observed_at_utc=observed_at_utc,
            active_contract_available=True,
        )
        if projection.position.projection_status != StrategyPositionStatus.FLAT:
            raise PaperDrillPreparationError(
                "paper execution drill requires a fresh broker-proven FLAT "
                "position; projected="
                f"{projection.position.projection_status.value}, "
                f"reasons={projection.blocking_reasons}"
            )
        if projection.blocking_reasons:
            raise PaperDrillPreparationError(
                "paper execution drill position projection is blocked: "
                f"{projection.blocking_reasons}"
            )
        return projection.position

    def _build_fixture(
        self,
        *,
        observed_at_utc: str,
        position: StrategyPositionV1,
    ) -> ExecutionFoundationFixtureV1:
        trading_day = (
            parse_utc(observed_at_utc)
            .astimezone(ZoneInfo(self.policy.daily_risk_timezone))
            .date()
            .isoformat()
        )
        readiness = ExecutionReadinessV1(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
            instrument_id=self.policy.instrument_id,
            status=ExecutionReadinessStatus.READY,
            command_intake_enabled=True,
            broker_actions_enabled=True,
            reconciliation_complete=True,
            clock_healthy=True,
            blocking_reasons=(),
            updated_at_utc=observed_at_utc,
        )
        risk = DailyRiskStateV1(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
            trading_day=trading_day,
            status=DailyRiskStatus.MONITORING,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            total_pnl=0.0,
            target_pnl=self.policy.daily_risk_target,
            pnl_ready=True,
            cleanup_status=DailyRiskCleanupStatus.NOT_REQUIRED,
            updated_at_utc=observed_at_utc,
        )
        return ExecutionFoundationFixtureV1(
            observed_at_utc=observed_at_utc,
            readiness=readiness,
            position=position,
            daily_risk=risk,
        )

    def _verify_existing_command(
        self,
        *,
        command: StrategyCommandRequestV1,
        command_id: str,
    ) -> None:
        expected = (
            command_id,
            self.policy.strategy_id,
            self.policy.strategy_version,
            self.policy.deployment_id,
            self.policy.instrument_id,
            self.policy.target_side,
            self.policy.target_quantity,
            StrategyCommandKind.OPEN,
            self.policy.policy_hash,
        )
        actual = (
            command.command_id,
            command.strategy_id,
            command.strategy_version,
            command.deployment_id,
            command.instrument_id,
            command.desired_target_side,
            command.desired_target_quantity,
            command.command_kind,
            command.policy_hash,
        )
        if actual != expected:
            raise PaperDrillPreparationError(
                "existing paper drill command conflicts with the requested drill"
            )

    def prepare(self, *, observed_at_utc: str) -> PaperDrillPreparation:
        require_paper_drill_gate(self.policy)
        observed = format_utc(parse_utc(observed_at_utc))
        signal_id, decision_id, command_id = self._ids()
        self._assert_no_other_unresolved(command_id)

        existing_command = self.decision_repository.read_command(command_id)
        existing_state = self.execution_repository.read_command_state(command_id)
        if existing_state is not None and existing_command is None:
            raise PaperDrillPreparationError(
                "execution command exists without its decision command source"
            )

        if existing_command is not None:
            self._verify_existing_command(
                command=existing_command,
                command_id=command_id,
            )
            if parse_utc(observed) >= parse_utc(existing_command.expires_at_utc):
                raise PaperDrillPreparationError(
                    "existing paper drill command expired; use a new drill_id"
                )

        position = self._project_flat(observed)
        fixture = self._build_fixture(
            observed_at_utc=observed,
            position=position,
        )
        if position.source_freshness_seconds is None:
            raise PaperDrillPreparationError(
                "paper drill FLAT projection has no source freshness proof"
            )
        remaining_freshness = (
            self.policy.position_max_age_seconds
            - position.source_freshness_seconds
        )
        if remaining_freshness <= 0.0:
            raise PaperDrillPreparationError(
                "paper drill position proof is already stale"
            )
        position_proof_expires_at = format_utc(
            parse_utc(observed) + timedelta(seconds=remaining_freshness)
        )

        if existing_command is None:
            expires = parse_utc(observed) + timedelta(
                seconds=self.policy.command_ttl_seconds
            )
            command = StrategyCommandRequestV1(
                command_id=command_id,
                strategy_id=self.policy.strategy_id,
                strategy_version=self.policy.strategy_version,
                deployment_id=self.policy.deployment_id,
                instrument_id=self.policy.instrument_id,
                source_signal_id=signal_id,
                desired_target_side=self.policy.target_side,
                desired_target_quantity=self.policy.target_quantity,
                command_kind=StrategyCommandKind.OPEN,
                reason="paper_execution_drill",
                created_at_utc=observed,
                expires_at_utc=format_utc(expires),
                policy_hash=self.policy.policy_hash,
            )
            decision_fixture = {
                "schema_name": "PaperExecutionDrillFixture",
                "schema_version": 1,
                "drill_id": self.policy.drill_id,
                "prepared_at_utc": observed,
                "position_snapshot_id": position.broker_snapshot_id,
                "target_side": self.policy.target_side.value,
                "target_quantity": self.policy.target_quantity,
                "active_contract": {
                    "con_id": self.policy.active_contract.con_id,
                    "local_symbol": self.policy.active_contract.local_symbol,
                },
            }
            input_hash = hashlib.sha256(
                canonical_json_text(decision_fixture).encode("utf-8")
            ).hexdigest()
            decision = DecisionRecordV1(
                decision_id=decision_id,
                strategy_id=self.policy.strategy_id,
                strategy_version=self.policy.strategy_version,
                deployment_id=self.policy.deployment_id,
                instrument_id=self.policy.instrument_id,
                source_signal_id=signal_id,
                evaluated_at_utc=observed,
                outcome=DecisionOutcome.COMMAND,
                reason_code="paper_execution_drill",
                reason_detail=(
                    "operator-staged paper-only execution drill; no trading "
                    "signal was evaluated"
                ),
                input_hash=input_hash,
                policy_hash=self.policy.policy_hash,
                position_status=position.projection_status.value,
                position_side=position.side.value,
                position_quantity=position.quantity,
                command_id=command.command_id,
                command_kind=command.command_kind,
            )
            evaluation = DecisionEvaluation(
                record=decision,
                command=command,
                fixture_payload=decision_fixture,
            )
            stored = self.decision_repository.publish(evaluation)
            command = stored.command
            decision = stored.record
            if command is None:
                raise PaperDrillPreparationError(
                    "paper drill decision did not persist its command"
                )
        else:
            command = existing_command
            decision = self.decision_repository.read_record(decision_id)
            if decision is None:
                raise PaperDrillPreparationError(
                    "paper drill command exists without its decision record"
                )
            if decision.command_id != command.command_id:
                raise PaperDrillPreparationError(
                    "paper drill decision record references another command"
                )

        if existing_state is None:
            admission = admit_strategy_command(
                command=command,
                policy=ExecutionFoundationPolicyV1(
                    account_id=self.policy.account_id,
                    strategy_id=self.policy.strategy_id,
                    strategy_version=self.policy.strategy_version,
                    deployment_id=self.policy.deployment_id,
                    instrument_id=self.policy.instrument_id,
                    policy_hash=self.policy.policy_hash,
                ),
                fixture=fixture,
            )
            state = self.execution_repository.publish_admission(admission)
        else:
            state = existing_state
            if state.state != ExecutionCommandState.ADMITTED:
                raise PaperDrillPreparationError(
                    "existing paper drill execution command is not ADMITTED: "
                    f"{state.state.value}"
                )
            state_material = (
                state.command_id,
                state.strategy_id,
                state.strategy_version,
                state.deployment_id,
                state.instrument_id,
                state.command_kind,
                state.desired_target_side,
                state.desired_target_quantity,
            )
            command_material = (
                command.command_id,
                command.strategy_id,
                command.strategy_version,
                command.deployment_id,
                command.instrument_id,
                command.command_kind,
                command.desired_target_side,
                command.desired_target_quantity,
            )
            if state_material != command_material:
                raise PaperDrillPreparationError(
                    "existing execution command conflicts with the decision command"
                )
            self.execution_repository.publish_fixture(fixture)

        return PaperDrillPreparation(
            drill_id=self.policy.drill_id,
            command=command,
            decision=decision,
            command_state=state,
            fixture=fixture,
            active_contract=self.policy.active_contract,
            position_proof_expires_at_utc=position_proof_expires_at,
            reused_existing_command=existing_command is not None,
        )
