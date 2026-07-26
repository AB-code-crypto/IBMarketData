from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol

from ibmd.execution.application.paper_drill import (
    PaperDrillDecisionEvaluation,
)
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
from ibmd.public_contracts.positions import BrokerPositionSnapshotV1
from ibmd.public_contracts.protection import (
    PositionEpisodeStatus,
    PositionEpisodeV1,
    ProtectionStateV1,
    ProtectiveOrderState,
)

_DRILL_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class PaperReverseDrillPreparationError(ValueError):
    pass


class DecisionReverseDrillRepository(Protocol):
    def read_record(self, decision_id: str) -> DecisionRecordV1 | None: ...

    def read_command(
        self,
        command_id: str,
    ) -> StrategyCommandRequestV1 | None: ...

    def publish(
        self,
        evaluation: PaperDrillDecisionEvaluation,
    ) -> PaperDrillDecisionEvaluation: ...


class ExecutionReverseDrillRepository(Protocol):
    def read_command_state(
        self,
        command_id: str,
    ) -> ExecutionCommandStateV1 | None: ...

    def publish_fixture(
        self,
        fixture: ExecutionFoundationFixtureV1,
    ) -> tuple[ExecutionReadinessV1, StrategyPositionV1, DailyRiskStateV1]: ...

    def publish_admission(self, admission) -> ExecutionCommandStateV1: ...


class ExecutionReverseDrillStateSource(Protocol):
    def read_position(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> StrategyPositionV1 | None: ...

    def read_readiness(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> ExecutionReadinessV1 | None: ...

    def read_latest_daily_risk(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
    ) -> DailyRiskStateV1 | None: ...


class ReverseDrillProtectionSource(Protocol):
    def read_episode(
        self,
        position_episode_id: str,
    ) -> PositionEpisodeV1 | None: ...

    def read_protection_by_episode(
        self,
        position_episode_id: str,
    ) -> ProtectionStateV1 | None: ...


class ReverseDrillPositionSnapshotSource(Protocol):
    def read_latest_complete(self) -> BrokerPositionSnapshotV1 | None: ...


class ReverseDrillBrokerAttemptSource(Protocol):
    def read_unresolved(self) -> tuple[BrokerOperationSnapshot, ...]: ...


class ReverseDrillLiquidationSource(Protocol):
    def read_snapshot_by_episode(self, position_episode_id: str): ...


@dataclass(frozen=True)
class PaperReverseDrillPolicyV1:
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
    active_contract: RegisteredFuturesContractV1

    def __post_init__(self) -> None:
        drill_id = str(self.drill_id or "").strip()
        if not _DRILL_ID_RE.fullmatch(drill_id):
            raise PaperReverseDrillPreparationError(
                "drill_id must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}"
            )
        object.__setattr__(self, "drill_id", drill_id)
        object.__setattr__(
            self,
            "environment",
            str(self.environment or "").strip().lower(),
        )
        for field_name in (
            "account_id",
            "confirmed_paper_account_id",
            "strategy_id",
            "deployment_id",
            "instrument_id",
            "policy_hash",
        ):
            value = str(getattr(self, field_name) or "").strip()
            if not value:
                raise PaperReverseDrillPreparationError(
                    f"{field_name} is required"
                )
            object.__setattr__(self, field_name, value)
        if "paper-drill" not in self.deployment_id.lower():
            raise PaperReverseDrillPreparationError(
                "paper reverse drill requires a dedicated deployment_id "
                "containing 'paper-drill'"
            )
        version = int(self.strategy_version)
        quantity = int(self.target_quantity)
        ttl = int(self.command_ttl_seconds)
        if version <= 0 or quantity <= 0:
            raise PaperReverseDrillPreparationError(
                "strategy_version and target_quantity must be positive"
            )
        if ttl < 60 or ttl > 900:
            raise PaperReverseDrillPreparationError(
                "command_ttl_seconds must be between 60 and 900"
            )
        object.__setattr__(self, "strategy_version", version)
        object.__setattr__(self, "target_quantity", quantity)
        object.__setattr__(self, "command_ttl_seconds", ttl)
        max_age = float(self.position_max_age_seconds)
        if max_age <= 0.0:
            raise PaperReverseDrillPreparationError(
                "position_max_age_seconds must be positive"
            )
        object.__setattr__(self, "position_max_age_seconds", max_age)
        if not isinstance(self.target_side, DesiredTargetSide):
            raise PaperReverseDrillPreparationError(
                "target_side must be DesiredTargetSide"
            )
        if not isinstance(self.active_contract, RegisteredFuturesContractV1):
            raise PaperReverseDrillPreparationError(
                "one active registered contract is required"
            )
        if not self.active_contract.contract_is_active:
            raise PaperReverseDrillPreparationError(
                "paper reverse drill contract is not active"
            )


@dataclass(frozen=True)
class PaperReverseDrillPreparationV1:
    drill_id: str
    command: StrategyCommandRequestV1
    decision: DecisionRecordV1
    command_state: ExecutionCommandStateV1
    fixture: ExecutionFoundationFixtureV1
    source_episode: PositionEpisodeV1
    source_protection: ProtectionStateV1
    active_contract: RegisteredFuturesContractV1
    position_proof_expires_at_utc: str
    reverse_order_quantity: int
    reused_existing_command: bool

    @property
    def ready_for_handoff(self) -> bool:
        position = self.fixture.position
        expected_source = (
            StrategyPositionSide.SHORT
            if self.command.desired_target_side == DesiredTargetSide.LONG
            else StrategyPositionSide.LONG
        )
        return (
            self.command_state.state == ExecutionCommandState.ADMITTED
            and self.command.command_kind == StrategyCommandKind.REVERSE
            and position.projection_status == StrategyPositionStatus.OPEN
            and position.side == expected_source
            and position.position_episode_id
            == self.source_episode.position_episode_id
            and parse_utc(self.fixture.observed_at_utc)
            < parse_utc(self.command.expires_at_utc)
            and parse_utc(self.fixture.observed_at_utc)
            < parse_utc(self.position_proof_expires_at_utc)
        )

    def to_dict(self) -> dict[str, object]:
        submit_before = min(
            parse_utc(self.command.expires_at_utc),
            parse_utc(self.position_proof_expires_at_utc),
        )
        return {
            "drill_id": self.drill_id,
            "ready_for_handoff": self.ready_for_handoff,
            "reused_existing_command": self.reused_existing_command,
            "broker_mutations_performed": False,
            "position_proof_expires_at_utc": (
                self.position_proof_expires_at_utc
            ),
            "submit_before_utc": format_utc(submit_before),
            "reverse_order_quantity": self.reverse_order_quantity,
            "command": self.command.to_dict(),
            "decision": self.decision.to_dict(),
            "command_state": self.command_state.to_dict(),
            "execution_fixture": self.fixture.to_dict(),
            "source_episode": self.source_episode.to_dict(),
            "source_protection": self.source_protection.to_dict(),
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


def require_paper_reverse_drill_gate(
    policy: PaperReverseDrillPolicyV1,
) -> None:
    if policy.environment != "paper":
        raise PaperReverseDrillPreparationError(
            "paper reverse drill requires IBMD_ENVIRONMENT=paper"
        )
    if policy.confirmed_paper_account_id != policy.account_id:
        raise PaperReverseDrillPreparationError(
            "paper account confirmation does not match configured account"
        )
    if not policy.account_id.upper().startswith("D"):
        raise PaperReverseDrillPreparationError(
            "configured account does not look like an IB paper account"
        )


class PaperReverseExecutionDrillPreparer:
    def __init__(
        self,
        *,
        policy: PaperReverseDrillPolicyV1,
        decision_repository: DecisionReverseDrillRepository,
        execution_repository: ExecutionReverseDrillRepository,
        execution_state_source: ExecutionReverseDrillStateSource,
        protection_source: ReverseDrillProtectionSource,
        position_snapshot_source: ReverseDrillPositionSnapshotSource,
        broker_attempt_source: ReverseDrillBrokerAttemptSource,
        liquidation_source: ReverseDrillLiquidationSource,
        contract_registry: tuple[RegisteredFuturesContractV1, ...],
    ) -> None:
        self.policy = policy
        self.decision_repository = decision_repository
        self.execution_repository = execution_repository
        self.execution_state_source = execution_state_source
        self.protection_source = protection_source
        self.position_snapshot_source = position_snapshot_source
        self.broker_attempt_source = broker_attempt_source
        self.liquidation_source = liquidation_source
        self.contract_registry = tuple(contract_registry)
        if not self.contract_registry:
            raise PaperReverseDrillPreparationError(
                "contract_registry cannot be empty"
            )

    def _ids(self) -> tuple[str, str, str]:
        identity = {
            "drill_id": self.policy.drill_id,
            "account_id": self.policy.account_id,
            "strategy_id": self.policy.strategy_id,
            "strategy_version": self.policy.strategy_version,
            "deployment_id": self.policy.deployment_id,
            "instrument_id": self.policy.instrument_id,
            "command_kind": "REVERSE",
        }
        return (
            _stable_id("signal_event", identity),
            _stable_id("decision_record", identity),
            _stable_id("strategy_command", identity),
        )

    def _assert_no_other_unresolved(self, command_id: str) -> None:
        scope = (
            self.policy.account_id,
            self.policy.strategy_id,
            self.policy.deployment_id,
            self.policy.instrument_id,
        )
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
            == scope
        ]
        if conflicts:
            raise PaperReverseDrillPreparationError(
                "another unresolved broker operation owns the reverse drill "
                f"scope: {conflicts}"
            )

    def _scope(self) -> tuple[str, str, str, str]:
        return (
            self.policy.account_id,
            self.policy.strategy_id,
            self.policy.deployment_id,
            self.policy.instrument_id,
        )

    def _load_fresh_source(
        self,
        observed_at_utc: str,
    ) -> tuple[
        StrategyPositionV1,
        ExecutionReadinessV1,
        DailyRiskStateV1,
        PositionEpisodeV1,
        ProtectionStateV1,
    ]:
        position = self.execution_state_source.read_position(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
            instrument_id=self.policy.instrument_id,
        )
        readiness = self.execution_state_source.read_readiness(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
            instrument_id=self.policy.instrument_id,
        )
        risk = self.execution_state_source.read_latest_daily_risk(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
        )
        if position is None or readiness is None or risk is None:
            raise PaperReverseDrillPreparationError(
                "execution position/readiness/daily-risk state is incomplete"
            )
        if position.projection_status != StrategyPositionStatus.OPEN:
            raise PaperReverseDrillPreparationError(
                "paper reverse drill requires an OPEN strategy position"
            )
        expected_source = (
            StrategyPositionSide.SHORT
            if self.policy.target_side == DesiredTargetSide.LONG
            else StrategyPositionSide.LONG
        )
        if position.side != expected_source:
            raise PaperReverseDrillPreparationError(
                "paper reverse target is not opposite to the current position"
            )
        if position.position_episode_id is None:
            raise PaperReverseDrillPreparationError(
                "paper reverse source position has no position_episode_id"
            )
        if (
            readiness.status != ExecutionReadinessStatus.READY
            or not readiness.command_intake_enabled
            or not readiness.broker_actions_enabled
            or not readiness.reconciliation_complete
            or not readiness.clock_healthy
        ):
            raise PaperReverseDrillPreparationError(
                "paper reverse drill requires fully READY execution state"
            )
        if not risk.pnl_ready or risk.status != DailyRiskStatus.MONITORING:
            raise PaperReverseDrillPreparationError(
                "paper reverse drill requires MONITORING and ready daily risk"
            )
        episode = self.protection_source.read_episode(
            position.position_episode_id
        )
        protection = self.protection_source.read_protection_by_episode(
            position.position_episode_id
        )
        if episode is None or protection is None:
            raise PaperReverseDrillPreparationError(
                "paper reverse source episode/protection is missing"
            )
        if episode.status != PositionEpisodeStatus.OPEN:
            raise PaperReverseDrillPreparationError(
                "paper reverse source episode is not OPEN"
            )
        stop = protection.stop_order
        take_profit = protection.take_profit_order
        if stop.state != ProtectiveOrderState.LIVE:
            raise PaperReverseDrillPreparationError(
                "paper reverse source STOP is not LIVE"
            )
        if take_profit is not None and take_profit.state not in {
            ProtectiveOrderState.LIVE,
            ProtectiveOrderState.NOT_REQUIRED,
        }:
            raise PaperReverseDrillPreparationError(
                "paper reverse source TAKE PROFIT is not LIVE/NOT_REQUIRED"
            )
        if self.liquidation_source.read_snapshot_by_episode(
            episode.position_episode_id
        ) is not None:
            raise PaperReverseDrillPreparationError(
                "paper reverse source episode already has liquidation ownership"
            )
        projection = project_strategy_position(
            snapshot=self.position_snapshot_source.read_latest_complete(),
            previous=position,
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
        fresh = projection.position
        if projection.blocking_reasons:
            raise PaperReverseDrillPreparationError(
                "paper reverse broker position projection is blocked: "
                f"{projection.blocking_reasons}"
            )
        if (
            fresh.projection_status != StrategyPositionStatus.OPEN
            or fresh.position_episode_id != episode.position_episode_id
            or fresh.side != position.side
            or fresh.quantity != position.quantity
            or len(fresh.contracts) != 1
        ):
            raise PaperReverseDrillPreparationError(
                "fresh broker position does not prove the source episode"
            )
        held = fresh.contracts[0]
        if (
            not held.contract_is_active
            or held.con_id != self.policy.active_contract.con_id
            or held.local_symbol != self.policy.active_contract.local_symbol
        ):
            raise PaperReverseDrillPreparationError(
                "paper reverse requires the actively traded held contract"
            )
        return fresh, readiness, risk, episode, protection

    def _verify_existing_command(
        self,
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
            StrategyCommandKind.REVERSE,
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
            raise PaperReverseDrillPreparationError(
                "existing paper reverse command conflicts with the drill"
            )

    def _new_evaluation(
        self,
        *,
        signal_id: str,
        decision_id: str,
        command_id: str,
        observed_at_utc: str,
        position: StrategyPositionV1,
        episode: PositionEpisodeV1,
    ) -> PaperDrillDecisionEvaluation:
        expires_at = format_utc(
            parse_utc(observed_at_utc)
            + timedelta(seconds=self.policy.command_ttl_seconds)
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
            command_kind=StrategyCommandKind.REVERSE,
            reason="paper_reverse_execution_drill",
            created_at_utc=observed_at_utc,
            expires_at_utc=expires_at,
            policy_hash=self.policy.policy_hash,
        )
        fixture_payload: dict[str, object] = {
            "schema_name": "PaperReverseExecutionDrillFixture",
            "schema_version": 1,
            "drill_id": self.policy.drill_id,
            "prepared_at_utc": observed_at_utc,
            "position_snapshot_id": position.broker_snapshot_id,
            "source_position_episode_id": episode.position_episode_id,
            "source_side": position.side.value,
            "source_quantity": position.quantity,
            "target_side": self.policy.target_side.value,
            "target_quantity": self.policy.target_quantity,
            "reverse_order_quantity": (
                position.quantity + self.policy.target_quantity
            ),
            "held_contract": {
                "con_id": self.policy.active_contract.con_id,
                "local_symbol": self.policy.active_contract.local_symbol,
            },
        }
        input_hash = hashlib.sha256(
            canonical_json_text(fixture_payload).encode("utf-8")
        ).hexdigest()
        record = DecisionRecordV1(
            decision_id=decision_id,
            strategy_id=self.policy.strategy_id,
            strategy_version=self.policy.strategy_version,
            deployment_id=self.policy.deployment_id,
            instrument_id=self.policy.instrument_id,
            source_signal_id=signal_id,
            evaluated_at_utc=observed_at_utc,
            outcome=DecisionOutcome.COMMAND,
            reason_code="paper_reverse_execution_drill",
            reason_detail=(
                "operator-staged paper-only REVERSE drill; no trading signal "
                "was evaluated"
            ),
            input_hash=input_hash,
            policy_hash=self.policy.policy_hash,
            position_status=position.projection_status.value,
            position_side=position.side.value,
            position_quantity=position.quantity,
            command_id=command.command_id,
            command_kind=command.command_kind,
        )
        return PaperDrillDecisionEvaluation(
            record=record,
            command=command,
            fixture_payload=fixture_payload,
        )

    def prepare(
        self,
        *,
        observed_at_utc: str,
    ) -> PaperReverseDrillPreparationV1:
        require_paper_reverse_drill_gate(self.policy)
        observed = format_utc(parse_utc(observed_at_utc))
        signal_id, decision_id, command_id = self._ids()
        self._assert_no_other_unresolved(command_id)
        existing_command = self.decision_repository.read_command(command_id)
        existing_state = self.execution_repository.read_command_state(command_id)
        if existing_state is not None and existing_command is None:
            raise PaperReverseDrillPreparationError(
                "execution reverse command exists without decision source"
            )
        if existing_command is not None:
            self._verify_existing_command(existing_command, command_id)
            if parse_utc(observed) >= parse_utc(existing_command.expires_at_utc):
                raise PaperReverseDrillPreparationError(
                    "existing paper reverse command expired; use a new drill_id"
                )
        position, readiness, risk, episode, protection = (
            self._load_fresh_source(observed)
        )
        fixture = ExecutionFoundationFixtureV1(
            observed_at_utc=observed,
            readiness=readiness,
            position=position,
            daily_risk=risk,
        )
        freshness = position.source_freshness_seconds
        if freshness is None:
            raise PaperReverseDrillPreparationError(
                "paper reverse position has no freshness proof"
            )
        remaining = self.policy.position_max_age_seconds - freshness
        if remaining <= 0.0:
            raise PaperReverseDrillPreparationError(
                "paper reverse position proof is already stale"
            )
        proof_expires_at = format_utc(
            parse_utc(observed) + timedelta(seconds=remaining)
        )
        if existing_command is None:
            stored = self.decision_repository.publish(
                self._new_evaluation(
                    signal_id=signal_id,
                    decision_id=decision_id,
                    command_id=command_id,
                    observed_at_utc=observed,
                    position=position,
                    episode=episode,
                )
            )
            command = stored.command
            decision = stored.record
            if command is None:
                raise PaperReverseDrillPreparationError(
                    "paper reverse decision did not persist its command"
                )
        else:
            command = existing_command
            decision = self.decision_repository.read_record(decision_id)
            if decision is None or decision.command_id != command.command_id:
                raise PaperReverseDrillPreparationError(
                    "paper reverse command has no matching decision record"
                )
        self.execution_repository.publish_fixture(fixture)
        if existing_state is None:
            state = self.execution_repository.publish_admission(
                admit_strategy_command(
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
            )
        else:
            state = existing_state
            if state.state != ExecutionCommandState.ADMITTED:
                raise PaperReverseDrillPreparationError(
                    "existing paper reverse execution command is not ADMITTED"
                )
        result = PaperReverseDrillPreparationV1(
            drill_id=self.policy.drill_id,
            command=command,
            decision=decision,
            command_state=state,
            fixture=fixture,
            source_episode=episode,
            source_protection=protection,
            active_contract=self.policy.active_contract,
            position_proof_expires_at_utc=proof_expires_at,
            reverse_order_quantity=(position.quantity + self.policy.target_quantity),
            reused_existing_command=existing_command is not None,
        )
        if not result.ready_for_handoff:
            raise PaperReverseDrillPreparationError(
                "paper reverse drill did not become ready_for_handoff"
            )
        return result
