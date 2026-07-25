from __future__ import annotations

from dataclasses import replace

from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.execution import (
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
)
from ibmd.public_contracts.protection import (
    ProtectionSetStatus,
    ProtectionStateV1,
    ProtectiveOrderKind,
    ProtectiveOrderState,
)

from .protective_submission import ProtectiveSubmissionDomainError


_TP_UNRESOLVED_STATES = {
    ProtectiveOrderState.SUBMITTING,
    ProtectiveOrderState.UNKNOWN_OUTCOME,
}


def _order_for_kind(
    protection: ProtectionStateV1,
    kind: ProtectiveOrderKind,
):
    values = [item for item in protection.orders if item.kind == kind]
    if len(values) != 1:
        raise ProtectiveSubmissionDomainError(
            f"protection does not contain exactly one {kind.value} order"
        )
    return values[0]


def mark_protective_order_unknown(
    protection: ProtectionStateV1,
    *,
    kind: ProtectiveOrderKind,
    observed_at_utc: str,
    reason: str,
) -> ProtectionStateV1:
    order = _order_for_kind(protection, kind)
    if order.state not in {
        ProtectiveOrderState.SUBMITTING,
        ProtectiveOrderState.LIVE,
        ProtectiveOrderState.UNKNOWN_OUTCOME,
    }:
        raise ProtectiveSubmissionDomainError(
            "only exposed protective orders can become UNKNOWN_OUTCOME: "
            f"{order.state.value}"
        )
    parsed_reason = str(reason or "").strip()
    if not parsed_reason:
        raise ProtectiveSubmissionDomainError(
            "UNKNOWN_OUTCOME requires a reason"
        )
    observed = format_utc(parse_utc(observed_at_utc))
    unknown = replace(
        order,
        state=ProtectiveOrderState.UNKNOWN_OUTCOME,
        broker_terminal_proven=False,
        updated_at_utc=observed,
        terminal_at_utc=None,
        failure_reason=parsed_reason,
    )
    orders = tuple(
        unknown if item.kind == kind else item
        for item in protection.orders
    )
    stop = next(
        item for item in orders if item.kind == ProtectiveOrderKind.STOP_LOSS
    )
    if stop.state == ProtectiveOrderState.LIVE:
        status = ProtectionSetStatus.STOP_LIVE
        blocking_reason = "take_profit_outcome_unproven_stop_live"
    else:
        status = ProtectionSetStatus.UNPROTECTED
        blocking_reason = parsed_reason
    return ProtectionStateV1(
        protection_set_id=protection.protection_set_id,
        position_episode_id=protection.position_episode_id,
        account_id=protection.account_id,
        strategy_id=protection.strategy_id,
        strategy_version=protection.strategy_version,
        deployment_id=protection.deployment_id,
        instrument_id=protection.instrument_id,
        status=status,
        orders=orders,
        created_at_utc=protection.created_at_utc,
        updated_at_utc=observed,
        terminal_at_utc=None,
        blocking_reason=blocking_reason,
    )


def readiness_for_protection(
    current: ExecutionReadinessV1,
    *,
    protection: ProtectionStateV1,
    observed_at_utc: str,
) -> ExecutionReadinessV1:
    if not isinstance(current, ExecutionReadinessV1):
        raise ProtectiveSubmissionDomainError(
            "current readiness must be ExecutionReadinessV1"
        )
    expected_scope = (
        protection.account_id,
        protection.strategy_id,
        protection.deployment_id,
        protection.instrument_id,
    )
    actual_scope = (
        current.account_id,
        current.strategy_id,
        current.deployment_id,
        current.instrument_id,
    )
    if actual_scope != expected_scope:
        raise ProtectiveSubmissionDomainError(
            "execution readiness belongs to another protection scope"
        )
    observed = format_utc(parse_utc(observed_at_utc))
    other_reasons = tuple(
        item
        for item in current.blocking_reasons
        if not item.startswith("protection:")
    )
    stop_safe = protection.stop_order.state == ProtectiveOrderState.LIVE
    tp = protection.take_profit_order
    tp_unresolved = (
        tp is not None and tp.state in _TP_UNRESOLVED_STATES
    )

    if stop_safe and tp_unresolved:
        reasons = other_reasons + (
            "protection:take_profit_outcome_unproven",
        )
        status = ExecutionReadinessStatus.BLOCKED
        intake = False
    elif stop_safe:
        reasons = other_reasons
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
        detail = protection.blocking_reason or protection.status.value.lower()
        reasons = other_reasons + (f"protection:{detail}",)
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
        updated_at_utc=observed,
    )
