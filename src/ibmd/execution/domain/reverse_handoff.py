from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum

from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.decision import StrategyCommandKind
from ibmd.public_contracts.execution import (
    ExecutionCommandState,
    ExecutionCommandStateV1,
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
    StrategyPositionStatus,
    StrategyPositionV1,
)
from ibmd.public_contracts.protection import (
    PositionEpisodeStatus,
    PositionEpisodeV1,
    ProtectionSetStatus,
    ProtectionStateV1,
    ProtectiveOrderKind,
    ProtectiveOrderState,
)


class ReverseHandoffError(ValueError):
    pass


class ReverseHandoffAction(str, Enum):
    RECONCILE_EXITS = "RECONCILE_EXITS"
    CANCEL_TAKE_PROFIT = "CANCEL_TAKE_PROFIT"
    CANCEL_STOP = "CANCEL_STOP"
    READY_TO_SUBMIT = "READY_TO_SUBMIT"
    OPERATOR_REQUIRED = "OPERATOR_REQUIRED"


@dataclass(frozen=True)
class ReverseHandoffAssessmentV1:
    action: ReverseHandoffAction
    blocking_reason: str | None


_EXPOSED_STATES = {
    ProtectiveOrderState.SUBMITTING,
    ProtectiveOrderState.CANCEL_REQUESTED,
    ProtectiveOrderState.UNKNOWN_OUTCOME,
}
_SAFE_TERMINAL_STATES = {
    ProtectiveOrderState.PLANNED,
    ProtectiveOrderState.CANCELLED,
    ProtectiveOrderState.REJECTED,
    ProtectiveOrderState.FAILED,
    ProtectiveOrderState.NOT_REQUIRED,
}


def _scope(value) -> tuple[str, str, str, str]:
    return (
        value.account_id,
        value.strategy_id,
        value.deployment_id,
        value.instrument_id,
    )


def validate_reverse_handoff_scope(
    *,
    command: ExecutionCommandStateV1,
    position: StrategyPositionV1,
    episode: PositionEpisodeV1,
    protection: ProtectionStateV1,
) -> None:
    if command.state != ExecutionCommandState.ADMITTED:
        raise ReverseHandoffError(
            "reverse handoff requires an ADMITTED command"
        )
    if command.command_kind != StrategyCommandKind.REVERSE:
        raise ReverseHandoffError(
            "reverse handoff requires command_kind=REVERSE"
        )
    if position.projection_status != StrategyPositionStatus.OPEN:
        raise ReverseHandoffError(
            "reverse handoff requires an OPEN execution position"
        )
    if episode.status != PositionEpisodeStatus.OPEN:
        raise ReverseHandoffError(
            "reverse handoff requires an OPEN position episode"
        )
    if position.position_episode_id != episode.position_episode_id:
        raise ReverseHandoffError(
            "execution position does not reference the source position episode"
        )
    if protection.position_episode_id != episode.position_episode_id:
        raise ReverseHandoffError(
            "protection belongs to another position episode"
        )
    expected = (
        episode.account_id,
        episode.strategy_id,
        episode.deployment_id,
        episode.instrument_id,
    )
    if _scope(position) != expected or _scope(protection) != expected:
        raise ReverseHandoffError(
            "position/protection scope differs from source episode"
        )
    command_scope = (
        command.strategy_id,
        command.deployment_id,
        command.instrument_id,
    )
    episode_scope = (
        episode.strategy_id,
        episode.deployment_id,
        episode.instrument_id,
    )
    if command_scope != episode_scope or command.strategy_version != episode.strategy_version:
        raise ReverseHandoffError(
            "reverse command scope differs from source episode"
        )
    if command.desired_target_side.value == episode.side.value:
        raise ReverseHandoffError(
            "reverse command target side is not opposite the source episode"
        )


def assess_reverse_handoff(
    protection: ProtectionStateV1,
) -> ReverseHandoffAssessmentV1:
    if not isinstance(protection, ProtectionStateV1):
        raise ReverseHandoffError(
            "protection must be ProtectionStateV1"
        )
    filled = [
        item for item in protection.orders if item.state == ProtectiveOrderState.FILLED
    ]
    if filled:
        return ReverseHandoffAssessmentV1(
            action=ReverseHandoffAction.OPERATOR_REQUIRED,
            blocking_reason=(
                "protective_fill_observed_before_reverse_handoff:"
                + ",".join(item.kind.value for item in filled)
            ),
        )
    exposed = [
        item for item in protection.orders if item.state in _EXPOSED_STATES
    ]
    if exposed:
        return ReverseHandoffAssessmentV1(
            action=ReverseHandoffAction.RECONCILE_EXITS,
            blocking_reason=(
                "protective_exit_reconciliation_required:"
                + ",".join(
                    f"{item.kind.value}={item.state.value}"
                    for item in exposed
                )
            ),
        )
    take_profit = protection.take_profit_order
    if (
        take_profit is not None
        and take_profit.state == ProtectiveOrderState.LIVE
    ):
        return ReverseHandoffAssessmentV1(
            action=ReverseHandoffAction.CANCEL_TAKE_PROFIT,
            blocking_reason="take_profit_cancel_required_before_reverse",
        )
    stop = protection.stop_order
    if stop.state == ProtectiveOrderState.LIVE:
        return ReverseHandoffAssessmentV1(
            action=ReverseHandoffAction.CANCEL_STOP,
            blocking_reason="stop_cancel_required_before_reverse",
        )
    unsafe = [
        item
        for item in protection.orders
        if item.state not in _SAFE_TERMINAL_STATES
    ]
    if unsafe:
        return ReverseHandoffAssessmentV1(
            action=ReverseHandoffAction.OPERATOR_REQUIRED,
            blocking_reason=(
                "unsupported_protective_state_before_reverse:"
                + ",".join(
                    f"{item.kind.value}={item.state.value}"
                    for item in unsafe
                )
            ),
        )
    return ReverseHandoffAssessmentV1(
        action=ReverseHandoffAction.READY_TO_SUBMIT,
        blocking_reason=None,
    )


def mark_reverse_cancel_requested(
    protection: ProtectionStateV1,
    *,
    kind: ProtectiveOrderKind,
    command_id: str,
    observed_at_utc: str,
) -> ProtectionStateV1:
    if not isinstance(kind, ProtectiveOrderKind):
        raise ReverseHandoffError("kind must be ProtectiveOrderKind")
    command = str(command_id or "").strip()
    if not command:
        raise ReverseHandoffError("command_id is required")
    values = [item for item in protection.orders if item.kind == kind]
    if len(values) != 1:
        raise ReverseHandoffError(
            f"protection has no unique {kind.value} order"
        )
    order = values[0]
    if order.state != ProtectiveOrderState.LIVE:
        raise ReverseHandoffError(
            f"only LIVE protective order can be cancel-requested: {order.state.value}"
        )
    observed = format_utc(parse_utc(observed_at_utc))
    updated_order = replace(
        order,
        state=ProtectiveOrderState.CANCEL_REQUESTED,
        updated_at_utc=observed,
        failure_reason=f"reverse_handoff_cancel_requested:{command}",
    )
    orders = tuple(
        updated_order
        if item.protective_order_id == order.protective_order_id
        else item
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
        blocking_reason=(
            f"reverse_handoff_cancel_requested:{kind.value}:{command}"
        ),
    )


def reverse_handoff_readiness(
    current: ExecutionReadinessV1,
    *,
    command_id: str,
    assessment: ReverseHandoffAssessmentV1,
    observed_at_utc: str,
) -> ExecutionReadinessV1:
    if not isinstance(current, ExecutionReadinessV1):
        raise ReverseHandoffError(
            "current readiness must be ExecutionReadinessV1"
        )
    command = str(command_id or "").strip()
    if not command:
        raise ReverseHandoffError("command_id is required")
    other = tuple(
        item
        for item in current.blocking_reasons
        if not item.startswith("reverse_handoff:")
    )
    if assessment.action == ReverseHandoffAction.READY_TO_SUBMIT:
        reasons = other
        if reasons:
            status = ExecutionReadinessStatus.BLOCKED
        elif current.reconciliation_complete and current.clock_healthy:
            status = ExecutionReadinessStatus.READY
        else:
            status = ExecutionReadinessStatus.NOT_READY
    else:
        detail = assessment.blocking_reason or assessment.action.value.lower()
        reasons = other + (f"reverse_handoff:{command}:{detail}",)
        status = ExecutionReadinessStatus.BLOCKED
    return ExecutionReadinessV1(
        account_id=current.account_id,
        strategy_id=current.strategy_id,
        deployment_id=current.deployment_id,
        instrument_id=current.instrument_id,
        status=status,
        command_intake_enabled=False,
        broker_actions_enabled=current.broker_actions_enabled,
        reconciliation_complete=current.reconciliation_complete,
        clock_healthy=current.clock_healthy,
        blocking_reasons=reasons,
        updated_at_utc=format_utc(parse_utc(observed_at_utc)),
    )


def require_reverse_ready_for_submit(
    *,
    command: ExecutionCommandStateV1,
    position: StrategyPositionV1,
    episode: PositionEpisodeV1,
    protection: ProtectionStateV1,
) -> None:
    validate_reverse_handoff_scope(
        command=command,
        position=position,
        episode=episode,
        protection=protection,
    )
    assessment = assess_reverse_handoff(protection)
    if assessment.action != ReverseHandoffAction.READY_TO_SUBMIT:
        raise ReverseHandoffError(
            "reverse protective handoff is incomplete: "
            f"action={assessment.action.value}, "
            f"reason={assessment.blocking_reason}"
        )
