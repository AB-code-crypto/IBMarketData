from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ibmd.execution.domain.daily_risk import (
    DailyRiskMarketMarkV1,
    DailyRiskOwnedFillV1,
    DailyRiskPolicyV1,
    DailyRiskUpdateV1,
    calculate_daily_risk,
)
from ibmd.public_contracts.execution import (
    DailyRiskStateV1,
    ExecutionReadinessV1,
    StrategyPositionStatus,
    StrategyPositionV1,
)
from ibmd.public_contracts.liquidation import LiquidationOperationV1
from ibmd.public_contracts.protection import PositionEpisodeV1


class DailyRiskServiceError(RuntimeError):
    pass


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


class PositionEpisodeSource(Protocol):
    def read_episode(
        self,
        position_episode_id: str,
    ) -> PositionEpisodeV1 | None: ...


class OwnedFillSource(Protocol):
    def read_owned_fills(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> tuple[DailyRiskOwnedFillV1, ...]: ...

    def read_liquidation_operation(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
        position_episode_id: str | None,
    ) -> LiquidationOperationV1 | None: ...


class MarketMarkSource(Protocol):
    def read_latest_mark(
        self,
        *,
        observed_at_utc: str,
    ) -> DailyRiskMarketMarkV1 | None: ...


class DailyRiskRepository(Protocol):
    def read_latest_state(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
    ) -> DailyRiskStateV1 | None: ...

    def publish(
        self,
        *,
        current_state: DailyRiskStateV1 | None,
        current_readiness: ExecutionReadinessV1,
        update: DailyRiskUpdateV1,
    ) -> DailyRiskUpdateV1: ...


@dataclass(frozen=True)
class DailyRiskRunV1:
    update: DailyRiskUpdateV1
    owned_fill_count: int
    broker_mutations_performed: bool = False


class DailyRiskService:
    def __init__(
        self,
        *,
        policy: DailyRiskPolicyV1,
        execution_state_source: ExecutionStateSource,
        episode_source: PositionEpisodeSource,
        owned_fill_source: OwnedFillSource,
        market_mark_source: MarketMarkSource,
        repository: DailyRiskRepository,
    ) -> None:
        self.policy = policy
        self.execution_state_source = execution_state_source
        self.episode_source = episode_source
        self.owned_fill_source = owned_fill_source
        self.market_mark_source = market_mark_source
        self.repository = repository

    def run_once(self, *, observed_at_utc: str) -> DailyRiskRunV1:
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
            raise DailyRiskServiceError(
                "execution position/readiness is incomplete for daily-risk evaluation"
            )
        episode = None
        if position.projection_status == StrategyPositionStatus.OPEN:
            episode_id = str(position.position_episode_id or "").strip()
            if not episode_id:
                raise DailyRiskServiceError(
                    "OPEN execution position has no position_episode_id"
                )
            episode = self.episode_source.read_episode(episode_id)
            if episode is None:
                raise DailyRiskServiceError(
                    f"position episode does not exist: {episode_id}"
                )
        current_state = self.repository.read_latest_state(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
        )
        fills = self.owned_fill_source.read_owned_fills(
            account_id=self.policy.account_id,
            strategy_id=self.policy.strategy_id,
            deployment_id=self.policy.deployment_id,
            instrument_id=self.policy.instrument_id,
        )
        mark = (
            self.market_mark_source.read_latest_mark(
                observed_at_utc=observed_at_utc
            )
            if position.projection_status == StrategyPositionStatus.OPEN
            else None
        )
        needs_cleanup_state = (
            current_state is not None
            and current_state.status.value in {"TRIGGERED", "CLOSING"}
        )
        liquidation = (
            self.owned_fill_source.read_liquidation_operation(
                account_id=self.policy.account_id,
                strategy_id=self.policy.strategy_id,
                deployment_id=self.policy.deployment_id,
                instrument_id=self.policy.instrument_id,
                position_episode_id=(
                    None if episode is None else episode.position_episode_id
                ),
            )
            if episode is not None or needs_cleanup_state
            else None
        )
        update = calculate_daily_risk(
            policy=self.policy,
            owned_fills=fills,
            position=position,
            episode=episode,
            market_mark=mark,
            current_state=current_state,
            current_readiness=readiness,
            liquidation=liquidation,
            observed_at_utc=observed_at_utc,
        )
        persisted = self.repository.publish(
            current_state=current_state,
            current_readiness=readiness,
            update=update,
        )
        return DailyRiskRunV1(
            update=persisted,
            owned_fill_count=len(fills),
        )


def daily_risk_payload(run: DailyRiskRunV1) -> dict:
    return {
        "calculation": run.update.calculation.to_dict(),
        "daily_risk_state": run.update.state.to_dict(),
        "execution_readiness": run.update.execution_readiness.to_dict(),
        "owned_fill_count": run.owned_fill_count,
        "broker_mutations_performed": run.broker_mutations_performed,
        "automatic_liquidation_submission_enabled": False,
        "legacy_database_compatibility_required": False,
    }
