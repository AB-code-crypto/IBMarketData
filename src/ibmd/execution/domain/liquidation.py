from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from typing import Any, Mapping

from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.broker_execution import (
    BrokerObservationOutcome,
    BrokerOrderObservationV1,
    BrokerOrderSide,
)
from ibmd.public_contracts.execution import (
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
    StrategyPositionSide,
    StrategyPositionStatus,
    StrategyPositionV1,
)
from ibmd.public_contracts.liquidation import (
    LiquidationAttemptState,
    LiquidationAttemptV1,
    LiquidationNextAction,
    LiquidationOperationState,
    LiquidationOperationV1,
    LiquidationReason,
    LiquidationTriggerV1,
)
from ibmd.public_contracts.protection import (
    PositionEpisodeStatus,
    PositionEpisodeV1,
    ProtectionSetStatus,
    ProtectionStateV1,
    ProtectiveOrderKind,
    ProtectiveOrderState,
)


class LiquidationDomainError(ValueError):
    pass


@dataclass(frozen=True)
class LiquidationSnapshot:
    operation: LiquidationOperationV1
    attempt: LiquidationAttemptV1 | None
    triggers: tuple[LiquidationTriggerV1, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.operation, LiquidationOperationV1):
            raise LiquidationDomainError(
                "operation must be LiquidationOperationV1"
            )
        if self.attempt is None:
            if self.operation.current_attempt_id is not None:
                raise LiquidationDomainError(
                    "operation references an absent liquidation attempt"
                )
        else:
            if not isinstance(self.attempt, LiquidationAttemptV1):
                raise LiquidationDomainError(
                    "attempt must be LiquidationAttemptV1 or None"
                )
            if (
                self.operation.liquidation_operation_id
                != self.attempt.liquidation_operation_id
                or self.operation.current_attempt_id
                != self.attempt.liquidation_attempt_id
                or self.operation.current_attempt_no != self.attempt.attempt_no
            ):
                raise LiquidationDomainError(
                    "liquidation operation and attempt identities disagree"
                )
        triggers = tuple(self.triggers)
        if any(not isinstance(item, LiquidationTriggerV1) for item in triggers):
            raise LiquidationDomainError(
                "triggers must contain LiquidationTriggerV1 values"
            )
        if any(
            item.liquidation_operation_id
            != self.operation.liquidation_operation_id
            for item in triggers
        ):
            raise LiquidationDomainError(
                "liquidation trigger belongs to another operation"
            )
        identities = [item.trigger_id for item in triggers]
        if len(identities) != len(set(identities)):
            raise LiquidationDomainError("liquidation trigger identities must be unique")
        object.__setattr__(self, "triggers", triggers)


@dataclass(frozen=True)
class LiquidationRequestResult:
    snapshot: LiquidationSnapshot
    trigger: LiquidationTriggerV1
    execution_readiness: ExecutionReadinessV1
    operation_created: bool
    trigger_created: bool


def _stable_id(kind: str, payload: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()[:32]
    return f"{kind}_{digest}"


def liquidation_operation_id(episode: PositionEpisodeV1) -> str:
    if not isinstance(episode, PositionEpisodeV1):
        raise LiquidationDomainError("episode must be PositionEpisodeV1")
    return _stable_id(
        "liquidation_operation",
        {
            "account_id": episode.account_id,
            "strategy_id": episode.strategy_id,
            "deployment_id": episode.deployment_id,
            "instrument_id": episode.instrument_id,
            "position_episode_id": episode.position_episode_id,
            "target_state": "FLAT",
        },
    )


def liquidation_attempt_id(operation_id: str, attempt_no: int) -> str:
    return _stable_id(
        "liquidation_attempt",
        {
            "liquidation_operation_id": str(operation_id),
            "attempt_no": int(attempt_no),
        },
    )


def liquidation_trigger_id(
    operation_id: str,
    reason: LiquidationReason,
    source_ref: str,
) -> str:
    return _stable_id(
        "liquidation_trigger",
        {
            "liquidation_operation_id": str(operation_id),
            "reason": reason.value,
            "source_ref": str(source_ref),
        },
    )


def build_liquidation_order_ref(operation_id: str, attempt_no: int) -> str:
    value = f"IBMD:{operation_id}:{int(attempt_no)}"
    if len(value) > 64:
        raise LiquidationDomainError(
            f"liquidation order_ref exceeds 64 characters: {value!r}"
        )
    return value


def _scope_episode(episode: PositionEpisodeV1) -> tuple[str, str, int, str, str]:
    return (
        episode.account_id,
        episode.strategy_id,
        episode.strategy_version,
        episode.deployment_id,
        episode.instrument_id,
    )


def _validate_request_state(
    *,
    episode: PositionEpisodeV1,
    position: StrategyPositionV1,
    readiness: ExecutionReadinessV1,
) -> None:
    if episode.status != PositionEpisodeStatus.OPEN:
        raise LiquidationDomainError(
            f"liquidation requires OPEN position episode: {episode.status.value}"
        )
    expected_state_scope = (
        episode.account_id,
        episode.strategy_id,
        episode.deployment_id,
        episode.instrument_id,
    )
    position_scope = (
        position.account_id,
        position.strategy_id,
        position.deployment_id,
        position.instrument_id,
    )
    readiness_scope = (
        readiness.account_id,
        readiness.strategy_id,
        readiness.deployment_id,
        readiness.instrument_id,
    )
    if position_scope != expected_state_scope or readiness_scope != expected_state_scope:
        raise LiquidationDomainError(
            "episode, position and readiness scopes differ"
        )
    if position.projection_status not in {
        StrategyPositionStatus.OPEN,
        StrategyPositionStatus.UNKNOWN,
        StrategyPositionStatus.STALE,
    }:
        raise LiquidationDomainError(
            "liquidation request requires OPEN/UNKNOWN/STALE execution position"
        )
    if (
        position.position_episode_id is not None
        and position.position_episode_id != episode.position_episode_id
    ):
        raise LiquidationDomainError(
            "execution position belongs to another position episode"
        )
    # A liquidation request is a durable safety fact, not a broker call.
    # It must be recorded even while broker actions are temporarily disabled.


def liquidation_readiness(
    current: ExecutionReadinessV1,
    *,
    operation: LiquidationOperationV1,
    observed_at_utc: str,
) -> ExecutionReadinessV1:
    if not isinstance(current, ExecutionReadinessV1):
        raise LiquidationDomainError(
            "current readiness must be ExecutionReadinessV1"
        )
    expected = (
        operation.account_id,
        operation.strategy_id,
        operation.deployment_id,
        operation.instrument_id,
    )
    actual = (
        current.account_id,
        current.strategy_id,
        current.deployment_id,
        current.instrument_id,
    )
    if actual != expected:
        raise LiquidationDomainError(
            "execution readiness belongs to another liquidation scope"
        )
    other = tuple(
        item
        for item in current.blocking_reasons
        if not item.startswith("liquidation:")
    )
    resolved = operation.state in {
        LiquidationOperationState.SUCCEEDED,
        LiquidationOperationState.CANCELLED_AS_ALREADY_FLAT,
    }
    if resolved:
        reasons = other
        if reasons:
            status = ExecutionReadinessStatus.BLOCKED
            intake = False
        elif current.reconciliation_complete and current.clock_healthy:
            status = ExecutionReadinessStatus.READY
            intake = True
        else:
            status = ExecutionReadinessStatus.NOT_READY
            intake = False
    else:
        detail = operation.blocking_reason or operation.state.value.lower()
        reasons = other + (
            f"liquidation:{operation.liquidation_operation_id}:{detail}",
        )
        status = ExecutionReadinessStatus.BLOCKED
        intake = False
    return ExecutionReadinessV1(
        account_id=current.account_id,
        strategy_id=current.strategy_id,
        deployment_id=current.deployment_id,
        instrument_id=current.instrument_id,
        status=status,
        command_intake_enabled=intake,
        broker_actions_enabled=current.broker_actions_enabled,
        reconciliation_complete=current.reconciliation_complete,
        clock_healthy=current.clock_healthy,
        blocking_reasons=reasons,
        updated_at_utc=format_utc(parse_utc(observed_at_utc)),
    )


def request_liquidation(
    *,
    episode: PositionEpisodeV1,
    position: StrategyPositionV1,
    readiness: ExecutionReadinessV1,
    reason: LiquidationReason,
    source_ref: str,
    observed_at_utc: str,
    existing: LiquidationSnapshot | None = None,
) -> LiquidationRequestResult:
    if not isinstance(reason, LiquidationReason):
        raise LiquidationDomainError("reason must be LiquidationReason")
    _validate_request_state(
        episode=episode,
        position=position,
        readiness=readiness,
    )
    observed = format_utc(parse_utc(observed_at_utc))
    operation_id = liquidation_operation_id(episode)
    trigger = LiquidationTriggerV1(
        trigger_id=liquidation_trigger_id(operation_id, reason, source_ref),
        liquidation_operation_id=operation_id,
        reason=reason,
        source_ref=source_ref,
        triggered_at_utc=observed,
    )
    if existing is None:
        operation = LiquidationOperationV1(
            liquidation_operation_id=operation_id,
            account_id=episode.account_id,
            strategy_id=episode.strategy_id,
            strategy_version=episode.strategy_version,
            deployment_id=episode.deployment_id,
            instrument_id=episode.instrument_id,
            position_episode_id=episode.position_episode_id,
            target_state="FLAT",
            initial_side=episode.side,
            initial_quantity=episode.quantity,
            con_id=episode.con_id,
            local_symbol=episode.local_symbol,
            broker_remaining_quantity=episode.quantity,
            liquidation_filled_quantity=0,
            state=LiquidationOperationState.REQUESTED,
            trigger_reasons=(reason,),
            current_attempt_id=None,
            current_attempt_no=0,
            next_action=LiquidationNextAction.RECONCILE_EXITS,
            created_at_utc=observed,
            updated_at_utc=observed,
            terminal_at_utc=None,
            blocking_reason="liquidation_requested",
        )
        snapshot = LiquidationSnapshot(
            operation=operation,
            attempt=None,
            triggers=(trigger,),
        )
        operation_created = True
        trigger_created = True
    else:
        if existing.operation.liquidation_operation_id != operation_id:
            raise LiquidationDomainError(
                "existing liquidation operation belongs to another episode"
            )
        trigger_created = all(
            item.trigger_id != trigger.trigger_id for item in existing.triggers
        )
        triggers = (
            existing.triggers + (trigger,)
            if trigger_created
            else existing.triggers
        )
        reasons = tuple(
            sorted(
                {*existing.operation.trigger_reasons, reason},
                key=lambda item: item.value,
            )
        )
        operation = replace(
            existing.operation,
            trigger_reasons=reasons,
            updated_at_utc=observed,
        )
        snapshot = LiquidationSnapshot(
            operation=operation,
            attempt=existing.attempt,
            triggers=triggers,
        )
        operation_created = False
    updated_readiness = liquidation_readiness(
        readiness,
        operation=snapshot.operation,
        observed_at_utc=observed,
    )
    return LiquidationRequestResult(
        snapshot=snapshot,
        trigger=trigger,
        execution_readiness=updated_readiness,
        operation_created=operation_created,
        trigger_created=trigger_created,
    )


_EXPOSED_PROTECTIVE_STATES = {
    ProtectiveOrderState.SUBMITTING,
    ProtectiveOrderState.CANCEL_REQUESTED,
    ProtectiveOrderState.UNKNOWN_OUTCOME,
}
_TERMINAL_PROTECTIVE_STATES = {
    ProtectiveOrderState.CANCELLED,
    ProtectiveOrderState.REJECTED,
    ProtectiveOrderState.FAILED,
    ProtectiveOrderState.NOT_REQUIRED,
}


def assess_next_action(
    *,
    snapshot: LiquidationSnapshot,
    protection: ProtectionStateV1,
    broker_position_state: str,
    observed_at_utc: str,
) -> LiquidationSnapshot:
    if protection.position_episode_id != snapshot.operation.position_episode_id:
        raise LiquidationDomainError(
            "protection belongs to another position episode"
        )
    position_state = str(broker_position_state or "").strip().upper()
    if position_state not in {"OPEN", "FLAT", "INCIDENT"}:
        raise LiquidationDomainError(
            f"invalid broker_position_state: {broker_position_state!r}"
        )
    operation = snapshot.operation
    if operation.state in {
        LiquidationOperationState.SUCCEEDED,
        LiquidationOperationState.CANCELLED_AS_ALREADY_FLAT,
        LiquidationOperationState.FAILED_OPERATOR_REQUIRED,
    }:
        return snapshot
    observed = format_utc(parse_utc(observed_at_utc))
    attempt = snapshot.attempt
    if attempt is not None:
        if attempt.state in {
            LiquidationAttemptState.SUBMITTING,
            LiquidationAttemptState.LIVE,
            LiquidationAttemptState.UNKNOWN_OUTCOME,
        }:
            updated = replace(
                operation,
                state=LiquidationOperationState.RECONCILING,
                next_action=LiquidationNextAction.RECONCILE_MARKET_CLOSE,
                updated_at_utc=observed,
                blocking_reason=(
                    attempt.failure_reason or "liquidation_close_reconciliation_required"
                ),
            )
            return replace(snapshot, operation=updated)
        if attempt.state == LiquidationAttemptState.FILLED:
            updated = replace(
                operation,
                state=LiquidationOperationState.RECONCILING,
                next_action=LiquidationNextAction.WAIT_FOR_FLAT,
                updated_at_utc=observed,
                blocking_reason="liquidation_fill_waiting_for_flat_position",
            )
            return replace(snapshot, operation=updated)
        if operation.state == LiquidationOperationState.FAILED_RETRYABLE:
            updated = replace(
                operation,
                next_action=LiquidationNextAction.OPERATOR_REQUIRED,
                updated_at_utc=observed,
            )
            return replace(snapshot, operation=updated)

    orders = protection.orders
    unresolved = [item for item in orders if item.state in _EXPOSED_PROTECTIVE_STATES]
    if unresolved:
        updated = replace(
            operation,
            state=LiquidationOperationState.CANCELING_EXITS,
            next_action=LiquidationNextAction.RECONCILE_EXITS,
            updated_at_utc=observed,
            blocking_reason="protective_exit_reconciliation_required",
        )
        return replace(snapshot, operation=updated)

    take_profit = protection.take_profit_order
    if take_profit is not None and take_profit.state == ProtectiveOrderState.LIVE:
        updated = replace(
            operation,
            state=LiquidationOperationState.CANCELING_EXITS,
            next_action=LiquidationNextAction.CANCEL_TAKE_PROFIT,
            updated_at_utc=observed,
            blocking_reason="take_profit_cancel_required",
        )
        return replace(snapshot, operation=updated)
    if protection.stop_order.state == ProtectiveOrderState.LIVE:
        updated = replace(
            operation,
            state=LiquidationOperationState.CANCELING_EXITS,
            next_action=LiquidationNextAction.CANCEL_STOP,
            updated_at_utc=observed,
            blocking_reason="stop_cancel_required",
        )
        return replace(snapshot, operation=updated)

    unsafe_terminal = [
        item
        for item in orders
        if item.state == ProtectiveOrderState.FILLED
    ]
    if unsafe_terminal:
        updated = replace(
            operation,
            state=LiquidationOperationState.RECONCILING,
            next_action=LiquidationNextAction.WAIT_FOR_FLAT,
            updated_at_utc=observed,
            blocking_reason="protective_exit_fill_waiting_for_flat_position",
        )
        return replace(snapshot, operation=updated)

    if position_state == "INCIDENT":
        return mark_operator_required(
            snapshot,
            reason="broker_position_incident_during_liquidation",
            observed_at_utc=observed,
        )
    if position_state == "FLAT":
        return snapshot

    nonterminal = [
        item
        for item in orders
        if item.state
        not in _TERMINAL_PROTECTIVE_STATES | {ProtectiveOrderState.PLANNED}
    ]
    if nonterminal:
        return mark_operator_required(
            snapshot,
            reason="unsupported_protective_state_before_liquidation_close:"
            + ",".join(item.state.value for item in nonterminal),
            observed_at_utc=observed,
        )
    updated = replace(
        operation,
        state=LiquidationOperationState.PREPARING,
        next_action=LiquidationNextAction.SUBMIT_MARKET_CLOSE,
        updated_at_utc=observed,
        blocking_reason="market_close_preparation_required",
    )
    return replace(snapshot, operation=updated)


def mark_protective_cancel_requested(
    protection: ProtectionStateV1,
    *,
    kind: ProtectiveOrderKind,
    observed_at_utc: str,
) -> ProtectionStateV1:
    if not isinstance(protection, ProtectionStateV1):
        raise LiquidationDomainError("protection must be ProtectionStateV1")
    if not isinstance(kind, ProtectiveOrderKind):
        raise LiquidationDomainError("kind must be ProtectiveOrderKind")
    values = [item for item in protection.orders if item.kind == kind]
    if len(values) != 1:
        raise LiquidationDomainError(
            f"protection has no unique {kind.value} order"
        )
    order = values[0]
    if order.state != ProtectiveOrderState.LIVE:
        raise LiquidationDomainError(
            f"only LIVE protective order can be cancel-requested: {order.state.value}"
        )
    observed = format_utc(parse_utc(observed_at_utc))
    updated_order = replace(
        order,
        state=ProtectiveOrderState.CANCEL_REQUESTED,
        updated_at_utc=observed,
        failure_reason="liquidation_cancel_requested",
    )
    orders = tuple(
        updated_order if item.protective_order_id == order.protective_order_id else item
        for item in protection.orders
    )
    return ProtectionStateV1(
        protection_set_id=protection.protection_set_id,
        position_episode_id=protection.position_episode_id,
        account_id=protection.account_id,
        strategy_id=protection.strategy_id,
        strategy_version=protection.strategy_version,
        deployment_id=protection.deployment_id,
        instrument_id=protection.instrument_id,
        status=ProtectionSetStatus.UNPROTECTED,
        orders=orders,
        created_at_utc=protection.created_at_utc,
        updated_at_utc=observed,
        terminal_at_utc=None,
        blocking_reason=f"liquidation_cancel_requested:{kind.value}",
    )


def plan_close_attempt(
    snapshot: LiquidationSnapshot,
    *,
    broker_quantity: int,
    observed_at_utc: str,
) -> LiquidationSnapshot:
    operation = snapshot.operation
    if operation.state not in {
        LiquidationOperationState.REQUESTED,
        LiquidationOperationState.PREPARING,
        LiquidationOperationState.CANCELING_EXITS,
        LiquidationOperationState.FAILED_RETRYABLE,
    }:
        raise LiquidationDomainError(
            f"cannot plan liquidation close from {operation.state.value}"
        )
    quantity = int(broker_quantity)
    if quantity <= 0:
        raise LiquidationDomainError("broker_quantity must be positive")
    if snapshot.attempt is None:
        attempt_no = 1
    else:
        prior = snapshot.attempt
        if operation.state != LiquidationOperationState.FAILED_RETRYABLE:
            raise LiquidationDomainError(
                "existing liquidation attempt prevents a new close attempt"
            )
        if prior.state not in {
            LiquidationAttemptState.CANCELLED,
            LiquidationAttemptState.REJECTED,
            LiquidationAttemptState.FAILED,
        } or not prior.broker_terminal_proven:
            raise LiquidationDomainError(
                "retry requires terminal proof for previous no-fill/failed attempt"
            )
        attempt_no = prior.attempt_no + 1
    observed = format_utc(parse_utc(observed_at_utc))
    operation_id = operation.liquidation_operation_id
    attempt_id = liquidation_attempt_id(operation_id, attempt_no)
    side = (
        BrokerOrderSide.SELL
        if operation.initial_side == StrategyPositionSide.LONG
        else BrokerOrderSide.BUY
    )
    attempt = LiquidationAttemptV1(
        liquidation_attempt_id=attempt_id,
        liquidation_operation_id=operation_id,
        attempt_no=attempt_no,
        order_ref=build_liquidation_order_ref(operation_id, attempt_no),
        side=side,
        order_type="MARKET",
        con_id=operation.con_id,
        local_symbol=operation.local_symbol,
        requested_qty=quantity,
        filled_qty=0,
        remaining_qty=quantity,
        state=LiquidationAttemptState.PREPARING,
        broker_order_id=None,
        broker_perm_id=None,
        broker_status=None,
        broker_terminal_proven=False,
        created_at_utc=observed,
        updated_at_utc=observed,
        terminal_at_utc=None,
        last_broker_proof_at_utc=None,
        failure_reason=None,
    )
    updated = replace(
        operation,
        broker_remaining_quantity=quantity,
        state=LiquidationOperationState.PREPARING,
        current_attempt_id=attempt_id,
        current_attempt_no=attempt_no,
        next_action=LiquidationNextAction.SUBMIT_MARKET_CLOSE,
        updated_at_utc=observed,
        terminal_at_utc=None,
        blocking_reason="market_close_prepared",
    )
    return LiquidationSnapshot(
        operation=updated,
        attempt=attempt,
        triggers=snapshot.triggers,
    )


def mark_close_submitting(
    snapshot: LiquidationSnapshot,
    *,
    broker_order_id: int,
    observed_at_utc: str,
) -> LiquidationSnapshot:
    if snapshot.attempt is None:
        raise LiquidationDomainError("liquidation close attempt is absent")
    if (
        snapshot.operation.state != LiquidationOperationState.PREPARING
        or snapshot.attempt.state != LiquidationAttemptState.PREPARING
    ):
        raise LiquidationDomainError(
            "only PREPARING liquidation can enter SUBMITTING"
        )
    order_id = int(broker_order_id)
    if order_id <= 0:
        raise LiquidationDomainError("broker_order_id must be positive")
    observed = format_utc(parse_utc(observed_at_utc))
    attempt = replace(
        snapshot.attempt,
        state=LiquidationAttemptState.SUBMITTING,
        broker_order_id=order_id,
        updated_at_utc=observed,
    )
    operation = replace(
        snapshot.operation,
        state=LiquidationOperationState.SUBMITTING,
        next_action=LiquidationNextAction.RECONCILE_MARKET_CLOSE,
        updated_at_utc=observed,
        blocking_reason="market_close_submission_in_progress",
    )
    return LiquidationSnapshot(
        operation=operation,
        attempt=attempt,
        triggers=snapshot.triggers,
    )


def apply_close_observation(
    snapshot: LiquidationSnapshot,
    *,
    observation: BrokerOrderObservationV1,
) -> LiquidationSnapshot:
    attempt = snapshot.attempt
    if attempt is None or attempt.state == LiquidationAttemptState.PREPARING:
        raise LiquidationDomainError(
            "liquidation close has no broker exposure to reconcile"
        )
    if observation.order_ref != attempt.order_ref:
        raise LiquidationDomainError(
            "broker observation order_ref differs from liquidation attempt"
        )
    observed = observation.observed_at_utc
    if observation.outcome in {
        BrokerObservationOutcome.NOT_FOUND,
        BrokerObservationOutcome.AMBIGUOUS,
    }:
        reason = (
            f"liquidation_broker_{observation.outcome.value.lower()}:"
            f"{observation.detail}"
        )
        unknown_attempt = replace(
            attempt,
            state=LiquidationAttemptState.UNKNOWN_OUTCOME,
            updated_at_utc=observed,
            broker_terminal_proven=False,
            failure_reason=reason,
        )
        operation = replace(
            snapshot.operation,
            state=LiquidationOperationState.RECONCILING,
            next_action=LiquidationNextAction.RECONCILE_MARKET_CLOSE,
            updated_at_utc=observed,
            blocking_reason=reason,
        )
        return LiquidationSnapshot(operation, unknown_attempt, snapshot.triggers)
    if observation.requested_qty != attempt.requested_qty:
        raise LiquidationDomainError(
            "broker observation quantity differs from liquidation attempt"
        )
    if observation.broker_order_id != attempt.broker_order_id:
        raise LiquidationDomainError(
            "broker observation order id differs from liquidation attempt"
        )
    cumulative_filled = (
        snapshot.operation.liquidation_filled_quantity
        - attempt.filled_qty
        + int(observation.filled_qty)
    )
    if cumulative_filled < 0:
        raise LiquidationDomainError(
            "liquidation cumulative fill quantity became negative"
        )
    if observation.outcome == BrokerObservationOutcome.LIVE:
        live_attempt = replace(
            attempt,
            state=LiquidationAttemptState.LIVE,
            filled_qty=int(observation.filled_qty),
            remaining_qty=int(observation.remaining_qty),
            broker_perm_id=observation.broker_perm_id,
            broker_status=observation.broker_status,
            broker_terminal_proven=False,
            updated_at_utc=observed,
            last_broker_proof_at_utc=observed,
            failure_reason=None,
        )
        operation = replace(
            snapshot.operation,
            state=LiquidationOperationState.LIVE,
            broker_remaining_quantity=int(observation.remaining_qty),
            liquidation_filled_quantity=cumulative_filled,
            next_action=LiquidationNextAction.RECONCILE_MARKET_CLOSE,
            updated_at_utc=observed,
            blocking_reason="market_close_live",
        )
        return LiquidationSnapshot(operation, live_attempt, snapshot.triggers)

    state_by_outcome = {
        BrokerObservationOutcome.FILLED: LiquidationAttemptState.FILLED,
        BrokerObservationOutcome.CANCELLED: LiquidationAttemptState.CANCELLED,
        BrokerObservationOutcome.REJECTED: LiquidationAttemptState.REJECTED,
        BrokerObservationOutcome.FAILED: LiquidationAttemptState.FAILED,
    }
    next_state = state_by_outcome[observation.outcome]
    terminal_attempt = replace(
        attempt,
        state=next_state,
        filled_qty=int(observation.filled_qty),
        remaining_qty=int(observation.remaining_qty),
        broker_perm_id=observation.broker_perm_id,
        broker_status=observation.broker_status,
        broker_terminal_proven=True,
        updated_at_utc=observed,
        terminal_at_utc=observed,
        last_broker_proof_at_utc=observed,
        failure_reason=(
            None
            if next_state == LiquidationAttemptState.FILLED
            else observation.detail or observation.outcome.value.lower()
        ),
    )
    if next_state == LiquidationAttemptState.FILLED:
        operation = replace(
            snapshot.operation,
            state=LiquidationOperationState.RECONCILING,
            liquidation_filled_quantity=cumulative_filled,
            next_action=LiquidationNextAction.WAIT_FOR_FLAT,
            updated_at_utc=observed,
            blocking_reason="liquidation_fill_waiting_for_flat_position",
        )
    else:
        operation = replace(
            snapshot.operation,
            state=LiquidationOperationState.FAILED_RETRYABLE,
            broker_remaining_quantity=terminal_attempt.remaining_qty,
            liquidation_filled_quantity=cumulative_filled,
            next_action=LiquidationNextAction.OPERATOR_REQUIRED,
            updated_at_utc=observed,
            blocking_reason=(
                terminal_attempt.failure_reason
                or f"market_close_{next_state.value.lower()}"
            ),
        )
    return LiquidationSnapshot(operation, terminal_attempt, snapshot.triggers)


def mark_broker_flat(
    snapshot: LiquidationSnapshot,
    *,
    observed_at_utc: str,
) -> LiquidationSnapshot:
    observed = format_utc(parse_utc(observed_at_utc))
    attempt = snapshot.attempt
    if attempt is None:
        state = LiquidationOperationState.CANCELLED_AS_ALREADY_FLAT
    elif attempt.state == LiquidationAttemptState.FILLED:
        state = LiquidationOperationState.SUCCEEDED
    elif attempt.state in {
        LiquidationAttemptState.SUBMITTING,
        LiquidationAttemptState.LIVE,
        LiquidationAttemptState.UNKNOWN_OUTCOME,
    }:
        raise LiquidationDomainError(
            "broker FLAT cannot close liquidation while close order outcome is unresolved"
        )
    else:
        state = LiquidationOperationState.CANCELLED_AS_ALREADY_FLAT
    operation = replace(
        snapshot.operation,
        state=state,
        broker_remaining_quantity=0,
        next_action=LiquidationNextAction.NONE,
        updated_at_utc=observed,
        terminal_at_utc=observed,
        blocking_reason=None,
    )
    return LiquidationSnapshot(operation, attempt, snapshot.triggers)


def mark_operator_required(
    snapshot: LiquidationSnapshot,
    *,
    reason: str,
    observed_at_utc: str,
) -> LiquidationSnapshot:
    detail = str(reason or "").strip()
    if not detail:
        raise LiquidationDomainError("operator-required liquidation needs a reason")
    observed = format_utc(parse_utc(observed_at_utc))
    operation = replace(
        snapshot.operation,
        state=LiquidationOperationState.FAILED_OPERATOR_REQUIRED,
        next_action=LiquidationNextAction.NONE,
        updated_at_utc=observed,
        terminal_at_utc=observed,
        blocking_reason=detail,
    )
    return LiquidationSnapshot(operation, snapshot.attempt, snapshot.triggers)
