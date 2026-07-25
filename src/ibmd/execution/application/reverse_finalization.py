from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Protocol

from ibmd.execution.domain.broker_attempt import BrokerOperationSnapshot
from ibmd.execution.domain.reverse_finalization import (
    ReverseFinalizationPolicyV1,
    ReversePositionFinalizationV1,
    finalize_reverse_position,
)
from ibmd.public_contracts.broker_reconciliation import BrokerFillFactV1
from ibmd.public_contracts.execution import (
    ExecutionCommandStateV1,
    ExecutionReadinessV1,
    StrategyPositionV1,
)
from ibmd.public_contracts.positions import BrokerPositionSnapshotV1
from ibmd.public_contracts.protection import PositionEpisodeV1, ProtectionStateV1


class ReverseFinalizationServiceError(RuntimeError):
    pass


class OperationSource(Protocol):
    def read_snapshot(
        self,
        operation_id: str,
    ) -> BrokerOperationSnapshot | None: ...


class CommandStateSource(Protocol):
    def read_command_state(
        self,
        command_id: str,
    ) -> ExecutionCommandStateV1 | None: ...


class FillSource(Protocol):
    def read_fills(
        self,
        attempt_id: str,
    ) -> tuple[BrokerFillFactV1, ...]: ...


class PositionSnapshotSource(Protocol):
    def read_latest_complete(self) -> BrokerPositionSnapshotV1 | None: ...


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


class ProtectionStateSource(Protocol):
    def read_episode(
        self,
        position_episode_id: str,
    ) -> PositionEpisodeV1 | None: ...

    def read_protection_by_episode(
        self,
        position_episode_id: str,
    ) -> ProtectionStateV1 | None: ...


class LiquidationStateSource(Protocol):
    def read_snapshot_by_episode(self, position_episode_id: str): ...


class ReverseFinalizationRepository(Protocol):
    def read_by_operation(
        self,
        operation_id: str,
    ) -> ReversePositionFinalizationV1 | None: ...

    def publish_finalization(
        self,
        *,
        current_episode: PositionEpisodeV1,
        current_protection: ProtectionStateV1,
        current_position: StrategyPositionV1,
        current_readiness: ExecutionReadinessV1,
        result: ReversePositionFinalizationV1,
    ) -> ReversePositionFinalizationV1: ...

    def refresh_commission_completion(
        self,
        *,
        current: ReversePositionFinalizationV1,
        updated: ReversePositionFinalizationV1,
    ) -> ReversePositionFinalizationV1: ...


@dataclass(frozen=True)
class ReverseFinalizationRunV1:
    finalization: ReversePositionFinalizationV1
    finalization_created: bool
    commission_completion_refreshed: bool
    broker_mutations_performed: bool = False


def _refreshed_commission_completion(
    existing: ReversePositionFinalizationV1,
    fills: tuple[BrokerFillFactV1, ...],
) -> ReversePositionFinalizationV1:
    by_exec_id = {item.exec_id: item for item in fills}
    if len(by_exec_id) != len(fills):
        raise ReverseFinalizationServiceError(
            "reverse operation fill source contains duplicate execId values"
        )
    expected = {item.exec_id for item in existing.allocations}
    if set(by_exec_id) != expected:
        raise ReverseFinalizationServiceError(
            "reverse commission refresh fill set differs from persisted allocations: "
            f"expected={sorted(expected)}, actual={sorted(by_exec_id)}"
        )
    allocations = tuple(
        replace(
            item,
            commission_complete=by_exec_id[item.exec_id].commission_complete,
        )
        for item in existing.allocations
    )
    return replace(
        existing,
        allocations=allocations,
        commission_complete=all(
            item.commission_complete for item in allocations
        ),
    )


class ReverseFinalizationService:
    def __init__(
        self,
        *,
        policy: ReverseFinalizationPolicyV1,
        operation_source: OperationSource,
        command_state_source: CommandStateSource,
        fill_source: FillSource,
        position_snapshot_source: PositionSnapshotSource,
        execution_state_source: ExecutionStateSource,
        protection_state_source: ProtectionStateSource,
        liquidation_state_source: LiquidationStateSource,
        repository: ReverseFinalizationRepository,
    ) -> None:
        self.policy = policy
        self.operation_source = operation_source
        self.command_state_source = command_state_source
        self.fill_source = fill_source
        self.position_snapshot_source = position_snapshot_source
        self.execution_state_source = execution_state_source
        self.protection_state_source = protection_state_source
        self.liquidation_state_source = liquidation_state_source
        self.repository = repository

    def _operation(self, operation_id: str) -> BrokerOperationSnapshot:
        operation = self.operation_source.read_snapshot(operation_id)
        if operation is None:
            raise ReverseFinalizationServiceError(
                f"reverse broker operation does not exist: {operation_id}"
            )
        return operation

    def _execution_state(
        self,
    ) -> tuple[StrategyPositionV1, ExecutionReadinessV1]:
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
        if position is None or readiness is None:
            raise ReverseFinalizationServiceError(
                "execution position/readiness is incomplete for reverse finalization"
            )
        return position, readiness

    def _refresh_existing(
        self,
        *,
        existing: ReversePositionFinalizationV1,
        operation: BrokerOperationSnapshot,
    ) -> ReverseFinalizationRunV1:
        fills = self.fill_source.read_fills(operation.attempt.attempt_id)
        updated = _refreshed_commission_completion(existing, fills)
        if updated == existing:
            return ReverseFinalizationRunV1(
                finalization=existing,
                finalization_created=False,
                commission_completion_refreshed=False,
            )
        persisted = self.repository.refresh_commission_completion(
            current=existing,
            updated=updated,
        )
        return ReverseFinalizationRunV1(
            finalization=persisted,
            finalization_created=False,
            commission_completion_refreshed=True,
        )

    def finalize_from_operation(
        self,
        *,
        operation_id: str,
        observed_at_utc: str,
    ) -> ReverseFinalizationRunV1:
        operation_id = str(operation_id or "").strip()
        if not operation_id:
            raise ReverseFinalizationServiceError("operation_id is required")
        operation = self._operation(operation_id)
        existing = self.repository.read_by_operation(operation_id)
        if existing is not None:
            return self._refresh_existing(
                existing=existing,
                operation=operation,
            )

        command = self.command_state_source.read_command_state(
            operation.operation.command_id
        )
        if command is None:
            raise ReverseFinalizationServiceError(
                "reverse operation source command state does not exist: "
                f"{operation.operation.command_id}"
            )
        position, readiness = self._execution_state()
        episode_id = str(position.position_episode_id or "").strip()
        if not episode_id:
            raise ReverseFinalizationServiceError(
                "reverse source execution position has no position_episode_id"
            )
        episode = self.protection_state_source.read_episode(episode_id)
        protection = self.protection_state_source.read_protection_by_episode(
            episode_id
        )
        if episode is None or protection is None:
            raise ReverseFinalizationServiceError(
                "reverse source position episode/protection is missing"
            )
        if self.liquidation_state_source.read_snapshot_by_episode(episode_id) is not None:
            raise ReverseFinalizationServiceError(
                "reverse source episode already has a liquidation operation"
            )
        broker_snapshot = self.position_snapshot_source.read_latest_complete()
        if broker_snapshot is None:
            raise ReverseFinalizationServiceError(
                "no COMPLETE broker position snapshot is available"
            )
        fills = self.fill_source.read_fills(operation.attempt.attempt_id)
        result = finalize_reverse_position(
            operation=operation,
            command=command,
            fills=fills,
            broker_snapshot=broker_snapshot,
            old_episode=episode,
            old_protection=protection,
            old_position=position,
            current_readiness=readiness,
            policy=self.policy,
            observed_at_utc=observed_at_utc,
        )
        persisted = self.repository.publish_finalization(
            current_episode=episode,
            current_protection=protection,
            current_position=position,
            current_readiness=readiness,
            result=result,
        )
        return ReverseFinalizationRunV1(
            finalization=persisted,
            finalization_created=True,
            commission_completion_refreshed=False,
        )


def reverse_finalization_payload(run: ReverseFinalizationRunV1) -> dict:
    value = run.finalization
    return {
        "source_operation_id": value.new_plan.episode.source_operation_id,
        "source_attempt_id": value.new_plan.episode.source_attempt_id,
        "closing_position_episode_id": value.closed_episode.position_episode_id,
        "opening_position_episode_id": value.new_plan.episode.position_episode_id,
        "closing_completed_at_utc": value.closing_completed_at_utc,
        "opening_started_at_utc": value.opening_started_at_utc,
        "opening_side": value.new_plan.episode.side.value,
        "opening_quantity": value.new_plan.episode.quantity,
        "opening_entry_average_price": value.new_plan.episode.entry_average_price,
        "opening_exec_ids": list(value.new_plan.episode.source_exec_ids),
        "allocations": [item.to_dict() for item in value.allocations],
        "commission_complete": value.commission_complete,
        "finalization_created": run.finalization_created,
        "commission_completion_refreshed": (
            run.commission_completion_refreshed
        ),
        "execution_readiness": value.new_plan.execution_readiness.to_dict(),
        "protection": value.new_plan.protection.to_dict(),
        "broker_mutations_performed": run.broker_mutations_performed,
        "automatic_retry_enabled": False,
        "legacy_database_compatibility_required": False,
    }
