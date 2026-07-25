from __future__ import annotations

from typing import Protocol

from ibmd.execution.domain.protective_lifecycle import (
    ProtectiveLifecyclePolicyV1,
    ProtectiveLifecycleUpdate,
    reconcile_protective_lifecycle,
)
from ibmd.public_contracts.broker_reconciliation import (
    BrokerFillFactV1,
    BrokerReconciliationSnapshotV1,
)
from ibmd.public_contracts.execution import (
    ExecutionReadinessV1,
    StrategyPositionV1,
)
from ibmd.public_contracts.positions import BrokerPositionSnapshotV1
from ibmd.public_contracts.protection import PositionEpisodeV1, ProtectionStateV1


class ProtectiveLifecycleServiceError(RuntimeError):
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


class BrokerSnapshotSource(Protocol):
    async def read_snapshot(
        self,
        *,
        account_id: str,
    ) -> BrokerReconciliationSnapshotV1: ...


class ProtectiveLifecycleRepository(Protocol):
    def publish_lifecycle(
        self,
        *,
        current_episode: PositionEpisodeV1,
        current_protection: ProtectionStateV1,
        current_position: StrategyPositionV1,
        current_readiness: ExecutionReadinessV1,
        update: ProtectiveLifecycleUpdate,
    ) -> ProtectiveLifecycleUpdate: ...

    def read_fills(
        self,
        position_episode_id: str,
    ) -> tuple[BrokerFillFactV1, ...]: ...

    def read_commission_pending_exec_ids(
        self,
        position_episode_id: str,
    ) -> tuple[str, ...]: ...


class ProtectiveLifecycleService:
    def __init__(
        self,
        *,
        policy: ProtectiveLifecyclePolicyV1,
        protection_source: ProtectionStateSource,
        execution_state_source: ExecutionStateSource,
        position_snapshot_source: PositionSnapshotSource,
        broker_snapshot_source: BrokerSnapshotSource,
        repository: ProtectiveLifecycleRepository,
    ) -> None:
        self.policy = policy
        self.protection_source = protection_source
        self.execution_state_source = execution_state_source
        self.position_snapshot_source = position_snapshot_source
        self.broker_snapshot_source = broker_snapshot_source
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
            raise ProtectiveLifecycleServiceError(
                "position_episode_id is required"
            )
        episode = self.protection_source.read_episode(episode_id)
        if episode is None:
            raise ProtectiveLifecycleServiceError(
                f"position episode does not exist: {episode_id}"
            )
        protection = self.protection_source.read_protection_by_episode(episode_id)
        if protection is None:
            raise ProtectiveLifecycleServiceError(
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
            raise ProtectiveLifecycleServiceError(
                "execution position/readiness is incomplete for position episode"
            )
        return episode, protection, position, readiness

    async def run_once(
        self,
        *,
        position_episode_id: str,
        observed_at_utc: str,
    ) -> ProtectiveLifecycleUpdate:
        episode, protection, position, readiness = self._load(
            position_episode_id
        )
        position_snapshot = self.position_snapshot_source.read_latest_complete()
        if position_snapshot is None:
            raise ProtectiveLifecycleServiceError(
                "no COMPLETE broker position snapshot is available"
            )
        broker_snapshot = await self.broker_snapshot_source.read_snapshot(
            account_id=episode.account_id
        )
        update = reconcile_protective_lifecycle(
            episode=episode,
            protection=protection,
            strategy_position=position,
            execution_readiness=readiness,
            broker_snapshot=broker_snapshot,
            position_snapshot=position_snapshot,
            policy=self.policy,
            observed_at_utc=observed_at_utc,
        )
        return self.repository.publish_lifecycle(
            current_episode=episode,
            current_protection=protection,
            current_position=position,
            current_readiness=readiness,
            update=update,
        )


def protective_lifecycle_payload(
    update: ProtectiveLifecycleUpdate,
    *,
    fills: tuple[BrokerFillFactV1, ...],
    commission_pending_exec_ids: tuple[str, ...],
) -> dict:
    return {
        "position_episode": update.episode.to_dict(),
        "protection": update.protection.to_dict(),
        "strategy_position": update.strategy_position.to_dict(),
        "execution_readiness": update.execution_readiness.to_dict(),
        "broker_position_state": update.broker_position_state,
        "episode_closed": update.episode_closed,
        "observed_reconciliation": [
            {
                "kind": item.kind.value,
                "observation": item.result.observation.to_dict(),
                "source_session_id": item.result.source_session_id,
                "captured_at_utc": item.result.captured_at_utc,
                "exec_ids": [fill.exec_id for fill in item.result.fills],
                "commission_complete": item.result.commission_complete,
            }
            for item in update.evidence
        ],
        "persisted_protective_fills": [item.to_dict() for item in fills],
        "commission_pending_exec_ids": list(commission_pending_exec_ids),
        "commission_complete": not commission_pending_exec_ids,
        "broker_mutations_performed": False,
        "cancel_enabled": False,
        "automatic_retry_enabled": False,
        "liquidation_enabled": False,
        "legacy_database_compatibility_required": False,
    }
