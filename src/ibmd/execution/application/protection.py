from __future__ import annotations

from typing import Protocol

from ibmd.execution.domain.broker_attempt import BrokerOperationSnapshot
from ibmd.execution.domain.protection import (
    PositionEpisodeProtectionPlan,
    ProtectionPlanningPolicyV1,
    create_position_episode_protection_plan,
)
from ibmd.public_contracts.broker_reconciliation import BrokerFillFactV1
from ibmd.public_contracts.execution import (
    ExecutionCommandStateV1,
    ExecutionReadinessV1,
    StrategyPositionV1,
)
from ibmd.public_contracts.positions import BrokerPositionSnapshotV1
from ibmd.public_contracts.protection import PositionEpisodeV1, ProtectionStateV1


class ProtectionPlanningServiceError(RuntimeError):
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


class ProtectionRepository(Protocol):
    def read_episode_by_operation(
        self,
        operation_id: str,
    ) -> PositionEpisodeV1 | None: ...

    def read_protection_by_episode(
        self,
        position_episode_id: str,
    ) -> ProtectionStateV1 | None: ...

    def publish_plan(
        self,
        plan: PositionEpisodeProtectionPlan,
    ) -> PositionEpisodeProtectionPlan: ...


class PositionEpisodeProtectionService:
    def __init__(
        self,
        *,
        policy: ProtectionPlanningPolicyV1,
        operation_source: OperationSource,
        command_source: CommandStateSource,
        fill_source: FillSource,
        position_snapshot_source: PositionSnapshotSource,
        execution_state_source: ExecutionStateSource,
        protection_repository: ProtectionRepository,
    ) -> None:
        self.policy = policy
        self.operation_source = operation_source
        self.command_source = command_source
        self.fill_source = fill_source
        self.position_snapshot_source = position_snapshot_source
        self.execution_state_source = execution_state_source
        self.protection_repository = protection_repository

    def _execution_state(
        self,
    ) -> tuple[StrategyPositionV1 | None, ExecutionReadinessV1]:
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
        if readiness is None:
            raise ProtectionPlanningServiceError(
                "execution readiness does not exist before protection planning"
            )
        return position, readiness

    def plan_from_operation(
        self,
        *,
        operation_id: str,
        observed_at_utc: str,
    ) -> PositionEpisodeProtectionPlan:
        operation_id = str(operation_id or "").strip()
        if not operation_id:
            raise ProtectionPlanningServiceError(
                "operation_id is required"
            )
        previous_position, readiness = self._execution_state()
        existing_episode = self.protection_repository.read_episode_by_operation(
            operation_id
        )
        if existing_episode is not None:
            protection = self.protection_repository.read_protection_by_episode(
                existing_episode.position_episode_id
            )
            current_position = self.execution_state_source.read_position(
                account_id=self.policy.account_id,
                strategy_id=self.policy.strategy_id,
                deployment_id=self.policy.deployment_id,
                instrument_id=self.policy.instrument_id,
            )
            current_readiness = self.execution_state_source.read_readiness(
                account_id=self.policy.account_id,
                strategy_id=self.policy.strategy_id,
                deployment_id=self.policy.deployment_id,
                instrument_id=self.policy.instrument_id,
            )
            if (
                protection is None
                or current_position is None
                or current_readiness is None
                or current_position.position_episode_id
                != existing_episode.position_episode_id
            ):
                raise ProtectionPlanningServiceError(
                    "persisted position episode has incomplete public "
                    "execution state"
                )
            return PositionEpisodeProtectionPlan(
                episode=existing_episode,
                strategy_position=current_position,
                execution_readiness=current_readiness,
                protection=protection,
            )

        operation = self.operation_source.read_snapshot(operation_id)
        if operation is None:
            raise ProtectionPlanningServiceError(
                f"broker operation does not exist: {operation_id}"
            )
        command = self.command_source.read_command_state(
            operation.operation.command_id
        )
        if command is None:
            raise ProtectionPlanningServiceError(
                "broker operation source command state does not exist: "
                f"{operation.operation.command_id}"
            )
        broker_snapshot = self.position_snapshot_source.read_latest_complete()
        if broker_snapshot is None:
            raise ProtectionPlanningServiceError(
                "no COMPLETE broker position snapshot is available"
            )
        fills = self.fill_source.read_fills(operation.attempt.attempt_id)
        plan = create_position_episode_protection_plan(
            operation=operation,
            command=command,
            fills=fills,
            broker_snapshot=broker_snapshot,
            previous_position=previous_position,
            current_readiness=readiness,
            policy=self.policy,
            observed_at_utc=observed_at_utc,
        )
        return self.protection_repository.publish_plan(plan)


def protection_plan_payload(
    plan: PositionEpisodeProtectionPlan,
) -> dict:
    return {
        "position_episode": plan.episode.to_dict(),
        "strategy_position": plan.strategy_position.to_dict(),
        "execution_readiness": plan.execution_readiness.to_dict(),
        "protection": plan.protection.to_dict(),
        "broker_mutations_performed": False,
        "stop_submission_enabled": False,
        "take_profit_submission_enabled": False,
        "liquidation_enabled": False,
    }
