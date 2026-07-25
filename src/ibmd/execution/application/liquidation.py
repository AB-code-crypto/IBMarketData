from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ibmd.execution.domain.liquidation import (
    LiquidationRequestResult,
    LiquidationSnapshot,
    assess_next_action,
    liquidation_readiness,
    mark_broker_flat,
    plan_close_attempt,
    request_liquidation,
)
from ibmd.execution.domain.liquidation_completion import (
    LiquidationCompletion,
    complete_liquidation_after_flat,
)
from ibmd.execution.domain.liquidation_position import (
    LiquidationBrokerPositionProof,
    prove_liquidation_broker_position,
)
from ibmd.public_contracts.execution import (
    ExecutionReadinessV1,
    StrategyPositionV1,
)
from ibmd.public_contracts.liquidation import (
    LiquidationNextAction,
    LiquidationOperationState,
    LiquidationReason,
)
from ibmd.public_contracts.positions import BrokerPositionSnapshotV1
from ibmd.public_contracts.protection import PositionEpisodeV1, ProtectionStateV1


class LiquidationServiceError(RuntimeError):
    pass


class ProtectionStateSource(Protocol):
    def read_episode(
        self,
        position_episode_id: str,
    ) -> PositionEpisodeV1 | None: ...

    def read_protection_by_episode(
        self,
        position_episode_id: str,
    ) -> ProtectionStateV1 | None: ...


class ExecutionStateSource(Protocol):
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


class PositionSnapshotSource(Protocol):
    def read_latest_complete(self) -> BrokerPositionSnapshotV1 | None: ...


class LiquidationRepository(Protocol):
    def read_snapshot_by_episode(
        self,
        position_episode_id: str,
    ) -> LiquidationSnapshot | None: ...

    def publish_request(
        self,
        *,
        current: LiquidationSnapshot | None,
        result: LiquidationRequestResult,
    ) -> LiquidationSnapshot: ...

    def publish_state(
        self,
        *,
        current: LiquidationSnapshot,
        updated: LiquidationSnapshot,
        readiness: ExecutionReadinessV1,
        current_protection: ProtectionStateV1 | None = None,
        updated_protection: ProtectionStateV1 | None = None,
        episode: PositionEpisodeV1 | None = None,
        strategy_position: StrategyPositionV1 | None = None,
    ) -> LiquidationSnapshot: ...


@dataclass(frozen=True)
class LiquidationPolicyV1:
    account_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    position_max_age_seconds: float = 10.0

    def __post_init__(self) -> None:
        for field_name in (
            "account_id",
            "strategy_id",
            "deployment_id",
            "instrument_id",
        ):
            parsed = str(getattr(self, field_name) or "").strip()
            if not parsed:
                raise LiquidationServiceError(f"{field_name} is required")
            object.__setattr__(self, field_name, parsed)
        version = int(self.strategy_version)
        if version <= 0:
            raise LiquidationServiceError("strategy_version must be positive")
        object.__setattr__(self, "strategy_version", version)
        max_age = float(self.position_max_age_seconds)
        if max_age <= 0.0:
            raise LiquidationServiceError(
                "position_max_age_seconds must be positive"
            )
        object.__setattr__(self, "position_max_age_seconds", max_age)


@dataclass(frozen=True)
class LiquidationFoundationRun:
    snapshot: LiquidationSnapshot
    broker_position_proof: LiquidationBrokerPositionProof | None
    execution_readiness: ExecutionReadinessV1
    completion: LiquidationCompletion | None
    operation_created: bool
    trigger_created: bool
    broker_mutations_performed: bool = False


class LiquidationFoundationService:
    def __init__(
        self,
        *,
        policy: LiquidationPolicyV1,
        protection_source: ProtectionStateSource,
        execution_state_source: ExecutionStateSource,
        position_snapshot_source: PositionSnapshotSource,
        repository: LiquidationRepository,
    ) -> None:
        self.policy = policy
        self.protection_source = protection_source
        self.execution_state_source = execution_state_source
        self.position_snapshot_source = position_snapshot_source
        self.repository = repository

    def _load(
        self,
        position_episode_id: str,
    ) -> tuple[
        PositionEpisodeV1,
        ProtectionStateV1,
        StrategyPositionV1,
        ExecutionReadinessV1,
    ]:
        episode_id = str(position_episode_id or "").strip()
        if not episode_id:
            raise LiquidationServiceError("position_episode_id is required")
        episode = self.protection_source.read_episode(episode_id)
        if episode is None:
            raise LiquidationServiceError(
                f"position episode does not exist: {episode_id}"
            )
        expected = (
            self.policy.account_id,
            self.policy.strategy_id,
            self.policy.strategy_version,
            self.policy.deployment_id,
            self.policy.instrument_id,
        )
        actual = (
            episode.account_id,
            episode.strategy_id,
            episode.strategy_version,
            episode.deployment_id,
            episode.instrument_id,
        )
        if actual != expected:
            raise LiquidationServiceError(
                "position episode belongs to another liquidation policy scope"
            )
        protection = self.protection_source.read_protection_by_episode(episode_id)
        if protection is None:
            raise LiquidationServiceError(
                "position episode has no protection state"
            )
        position = self.execution_state_source.read_position(
            account_id=episode.account_id,
            strategy_id=episode.strategy_id,
            deployment_id=episode.deployment_id,
            instrument_id=episode.instrument_id,
        )
        readiness = self.execution_state_source.read_readiness(
            account_id=episode.account_id,
            strategy_id=episode.strategy_id,
            deployment_id=episode.deployment_id,
            instrument_id=episode.instrument_id,
        )
        if position is None or readiness is None:
            raise LiquidationServiceError(
                "execution position/readiness is incomplete for liquidation"
            )
        return episode, protection, position, readiness

    def _position_proof(
        self,
        *,
        episode: PositionEpisodeV1,
        observed_at_utc: str,
    ) -> LiquidationBrokerPositionProof:
        snapshot = self.position_snapshot_source.read_latest_complete()
        if snapshot is None:
            raise LiquidationServiceError(
                "no COMPLETE broker position snapshot is available"
            )
        return prove_liquidation_broker_position(
            snapshot=snapshot,
            episode=episode,
            observed_at_utc=observed_at_utc,
            max_age_seconds=self.policy.position_max_age_seconds,
        )

    def request(
        self,
        *,
        position_episode_id: str,
        reason: LiquidationReason,
        source_ref: str,
        observed_at_utc: str,
    ) -> LiquidationFoundationRun:
        episode, _protection, position, readiness = self._load(
            position_episode_id
        )
        current = self.repository.read_snapshot_by_episode(
            episode.position_episode_id
        )
        result = request_liquidation(
            episode=episode,
            position=position,
            readiness=readiness,
            reason=reason,
            source_ref=source_ref,
            observed_at_utc=observed_at_utc,
            existing=current,
        )
        persisted = self.repository.publish_request(
            current=current,
            result=result,
        )
        return LiquidationFoundationRun(
            snapshot=persisted,
            broker_position_proof=None,
            execution_readiness=result.execution_readiness,
            completion=None,
            operation_created=result.operation_created,
            trigger_created=result.trigger_created,
        )

    def advance(
        self,
        *,
        position_episode_id: str,
        observed_at_utc: str,
        allow_proven_retry: bool = False,
    ) -> LiquidationFoundationRun:
        episode, protection, position, readiness = self._load(
            position_episode_id
        )
        current = self.repository.read_snapshot_by_episode(
            episode.position_episode_id
        )
        if current is None:
            raise LiquidationServiceError(
                "liquidation operation has not been requested"
            )
        proof = self._position_proof(
            episode=episode,
            observed_at_utc=observed_at_utc,
        )
        updated = assess_next_action(
            snapshot=current,
            protection=protection,
            broker_position_state=proof.state,
            observed_at_utc=observed_at_utc,
        )
        completion = None
        close_outcome_unresolved = (
            updated.attempt is not None
            and updated.attempt.state.value
            in {"SUBMITTING", "LIVE", "UNKNOWN_OUTCOME"}
        )
        unresolved_protection = any(
            item.state.value
            in {"SUBMITTING", "LIVE", "CANCEL_REQUESTED", "UNKNOWN_OUTCOME"}
            for item in protection.orders
        )
        if (
            proof.state == "FLAT"
            and not close_outcome_unresolved
            and not unresolved_protection
        ):
            updated = mark_broker_flat(
                updated,
                observed_at_utc=observed_at_utc,
            )
            completion = complete_liquidation_after_flat(
                liquidation=updated,
                episode=episode,
                protection=protection,
                current_position=position,
                current_readiness=readiness,
                position_proof=proof,
                observed_at_utc=observed_at_utc,
            )
            updated_readiness = completion.execution_readiness
        else:
            retryable = (
                updated.operation.state
                == LiquidationOperationState.FAILED_RETRYABLE
            )
            if (
                proof.state == "OPEN"
                and updated.operation.next_action
                == LiquidationNextAction.SUBMIT_MARKET_CLOSE
                and updated.attempt is None
            ) or (
                proof.state == "OPEN"
                and retryable
                and allow_proven_retry
            ):
                updated = plan_close_attempt(
                    updated,
                    broker_quantity=proof.quantity,
                    observed_at_utc=observed_at_utc,
                )
            updated_readiness = liquidation_readiness(
                readiness,
                operation=updated.operation,
                observed_at_utc=observed_at_utc,
            )
        persisted = self.repository.publish_state(
            current=current,
            updated=updated,
            readiness=updated_readiness,
            current_protection=(protection if completion is not None else None),
            updated_protection=(
                completion.protection if completion is not None else None
            ),
            episode=(completion.episode if completion is not None else None),
            strategy_position=(
                completion.strategy_position if completion is not None else None
            ),
        )
        return LiquidationFoundationRun(
            snapshot=persisted,
            broker_position_proof=proof,
            execution_readiness=updated_readiness,
            completion=completion,
            operation_created=False,
            trigger_created=False,
        )


def liquidation_foundation_payload(run: LiquidationFoundationRun) -> dict:
    return {
        "liquidation_operation": run.snapshot.operation.to_dict(),
        "liquidation_attempt": (
            None if run.snapshot.attempt is None else run.snapshot.attempt.to_dict()
        ),
        "triggers": [item.to_dict() for item in run.snapshot.triggers],
        "broker_position_proof": (
            None
            if run.broker_position_proof is None
            else {
                "state": run.broker_position_proof.state,
                "snapshot_id": run.broker_position_proof.snapshot_id,
                "freshness_seconds": run.broker_position_proof.freshness_seconds,
                "quantity": run.broker_position_proof.quantity,
                "side": (
                    None
                    if run.broker_position_proof.side is None
                    else run.broker_position_proof.side.value
                ),
                "reason": run.broker_position_proof.reason,
            }
        ),
        "execution_readiness": run.execution_readiness.to_dict(),
        "episode_closed": run.completion is not None,
        "operation_created": run.operation_created,
        "trigger_created": run.trigger_created,
        "broker_mutations_performed": run.broker_mutations_performed,
        "automatic_retry_enabled": False,
        "legacy_database_compatibility_required": False,
    }
