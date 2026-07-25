from __future__ import annotations

from dataclasses import dataclass, replace

from ibmd.execution.domain.liquidation import LiquidationSnapshot
from ibmd.execution.domain.liquidation_position import (
    LiquidationBrokerPositionProof,
)
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.execution import (
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
    StrategyPositionSide,
    StrategyPositionStatus,
    StrategyPositionV1,
)
from ibmd.public_contracts.liquidation import LiquidationOperationState
from ibmd.public_contracts.protection import (
    PositionEpisodeStatus,
    PositionEpisodeV1,
    ProtectionSetStatus,
    ProtectionStateV1,
    ProtectiveOrderState,
)


class LiquidationCompletionError(ValueError):
    pass


@dataclass(frozen=True)
class LiquidationCompletion:
    episode: PositionEpisodeV1
    protection: ProtectionStateV1
    strategy_position: StrategyPositionV1
    execution_readiness: ExecutionReadinessV1


_UNRESOLVED_ORDER_STATES = {
    ProtectiveOrderState.SUBMITTING,
    ProtectiveOrderState.LIVE,
    ProtectiveOrderState.CANCEL_REQUESTED,
    ProtectiveOrderState.UNKNOWN_OUTCOME,
}


def complete_liquidation_after_flat(
    *,
    liquidation: LiquidationSnapshot,
    episode: PositionEpisodeV1,
    protection: ProtectionStateV1,
    current_position: StrategyPositionV1,
    current_readiness: ExecutionReadinessV1,
    position_proof: LiquidationBrokerPositionProof,
    observed_at_utc: str,
) -> LiquidationCompletion:
    if liquidation.operation.state not in {
        LiquidationOperationState.SUCCEEDED,
        LiquidationOperationState.CANCELLED_AS_ALREADY_FLAT,
    }:
        raise LiquidationCompletionError(
            "liquidation operation is not terminal-successful"
        )
    if position_proof.state != "FLAT":
        raise LiquidationCompletionError(
            "position episode cannot close without broker-proven FLAT"
        )
    if liquidation.operation.position_episode_id != episode.position_episode_id:
        raise LiquidationCompletionError(
            "liquidation operation belongs to another position episode"
        )
    if protection.position_episode_id != episode.position_episode_id:
        raise LiquidationCompletionError(
            "protection belongs to another position episode"
        )
    unresolved = [
        item
        for item in protection.orders
        if item.state in _UNRESOLVED_ORDER_STATES
    ]
    if unresolved:
        raise LiquidationCompletionError(
            "position cannot close while protective orders are unresolved: "
            + ",".join(item.state.value for item in unresolved)
        )
    observed = format_utc(parse_utc(observed_at_utc))
    orders = tuple(
        replace(
            item,
            state=ProtectiveOrderState.NOT_REQUIRED,
            updated_at_utc=observed,
            terminal_at_utc=observed,
            failure_reason="liquidation_closed_before_order_submission",
        )
        if item.state == ProtectiveOrderState.PLANNED
        else item
        for item in protection.orders
    )
    updated_protection = ProtectionStateV1(
        protection_set_id=protection.protection_set_id,
        position_episode_id=protection.position_episode_id,
        account_id=protection.account_id,
        strategy_id=protection.strategy_id,
        strategy_version=protection.strategy_version,
        deployment_id=protection.deployment_id,
        instrument_id=protection.instrument_id,
        status=ProtectionSetStatus.CLOSED,
        orders=orders,
        created_at_utc=protection.created_at_utc,
        updated_at_utc=observed,
        terminal_at_utc=observed,
        blocking_reason=None,
    )
    updated_episode = replace(
        episode,
        status=PositionEpisodeStatus.CLOSED,
        closed_at_utc=observed,
        closing_operation_id=(
            liquidation.operation.liquidation_operation_id
        ),
    )
    updated_position = StrategyPositionV1(
        account_id=episode.account_id,
        strategy_id=episode.strategy_id,
        deployment_id=episode.deployment_id,
        instrument_id=episode.instrument_id,
        position_episode_id=None,
        side=StrategyPositionSide.FLAT,
        quantity=0,
        contracts=(),
        projection_status=StrategyPositionStatus.FLAT,
        broker_snapshot_id=position_proof.snapshot_id,
        updated_at_utc=observed,
        source_freshness_seconds=position_proof.freshness_seconds,
    )
    scope = (
        episode.account_id,
        episode.strategy_id,
        episode.deployment_id,
        episode.instrument_id,
    )
    readiness_scope = (
        current_readiness.account_id,
        current_readiness.strategy_id,
        current_readiness.deployment_id,
        current_readiness.instrument_id,
    )
    position_scope = (
        current_position.account_id,
        current_position.strategy_id,
        current_position.deployment_id,
        current_position.instrument_id,
    )
    if readiness_scope != scope or position_scope != scope:
        raise LiquidationCompletionError(
            "execution state belongs to another liquidation scope"
        )
    reasons = tuple(
        item
        for item in current_readiness.blocking_reasons
        if not item.startswith("liquidation:")
        and not item.startswith("protection:")
    )
    if reasons:
        readiness_status = ExecutionReadinessStatus.BLOCKED
        intake = False
    elif current_readiness.reconciliation_complete and current_readiness.clock_healthy:
        readiness_status = ExecutionReadinessStatus.READY
        intake = True
    else:
        readiness_status = ExecutionReadinessStatus.NOT_READY
        intake = False
    updated_readiness = ExecutionReadinessV1(
        account_id=current_readiness.account_id,
        strategy_id=current_readiness.strategy_id,
        deployment_id=current_readiness.deployment_id,
        instrument_id=current_readiness.instrument_id,
        status=readiness_status,
        command_intake_enabled=intake,
        broker_actions_enabled=current_readiness.broker_actions_enabled,
        reconciliation_complete=current_readiness.reconciliation_complete,
        clock_healthy=current_readiness.clock_healthy,
        blocking_reasons=reasons,
        updated_at_utc=observed,
    )
    return LiquidationCompletion(
        episode=updated_episode,
        protection=updated_protection,
        strategy_position=updated_position,
        execution_readiness=updated_readiness,
    )
