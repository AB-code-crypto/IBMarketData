from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from typing import Any, Mapping

from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.broker_execution import (
    BrokerAttemptState,
    BrokerObservationOutcome,
    BrokerOperationState,
    BrokerOrderAttemptV1,
    BrokerOrderObservationV1,
    BrokerOrderOperationV1,
    BrokerOrderSide,
)
from ibmd.public_contracts.decision import DesiredTargetSide, StrategyCommandKind
from ibmd.public_contracts.execution import (
    ExecutionCommandState,
    ExecutionCommandStateV1,
    StrategyPositionSide,
    StrategyPositionStatus,
    StrategyPositionV1,
)

from .position_projection import RegisteredFuturesContractV1


class BrokerAttemptDomainError(ValueError):
    pass


@dataclass(frozen=True)
class BrokerOperationSnapshot:
    operation: BrokerOrderOperationV1
    attempt: BrokerOrderAttemptV1

    def __post_init__(self) -> None:
        if not isinstance(self.operation, BrokerOrderOperationV1):
            raise BrokerAttemptDomainError(
                "operation must be BrokerOrderOperationV1"
            )
        if not isinstance(self.attempt, BrokerOrderAttemptV1):
            raise BrokerAttemptDomainError("attempt must be BrokerOrderAttemptV1")
        if self.operation.operation_id != self.attempt.operation_id:
            raise BrokerAttemptDomainError(
                "operation and attempt identities do not match"
            )
        if self.operation.current_attempt_id != self.attempt.attempt_id:
            raise BrokerAttemptDomainError(
                "operation does not reference the supplied current attempt"
            )
        if self.operation.current_attempt_no != self.attempt.attempt_no:
            raise BrokerAttemptDomainError(
                "operation and attempt numbers do not match"
            )
        if (
            self.operation.side != self.attempt.side
            or self.operation.order_type != self.attempt.order_type
            or self.operation.con_id != self.attempt.con_id
            or self.operation.local_symbol != self.attempt.local_symbol
        ):
            raise BrokerAttemptDomainError(
                "operation and attempt broker routing differ"
            )
        if self.operation.remaining_qty != self.attempt.remaining_qty:
            raise BrokerAttemptDomainError(
                "operation and current attempt remaining quantities differ"
            )
        prior_filled = self.operation.filled_qty - self.attempt.filled_qty
        if prior_filled < 0:
            raise BrokerAttemptDomainError(
                "attempt filled quantity exceeds operation cumulative fill"
            )


def _stable_id(kind: str, payload: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()[:32]
    return f"{kind}_{digest}"


def _operation_id(command_id: str) -> str:
    return _stable_id("broker_operation", {"command_id": command_id})


def _attempt_id(operation_id: str, attempt_no: int) -> str:
    return _stable_id(
        "broker_attempt",
        {"operation_id": operation_id, "attempt_no": int(attempt_no)},
    )


def build_order_ref(operation_id: str, attempt_no: int) -> str:
    value = f"IBMD:{operation_id}:{int(attempt_no)}"
    if len(value) > 64:
        raise BrokerAttemptDomainError(
            f"stable broker order_ref exceeds 64 characters: {value!r}"
        )
    return value


def _broker_side(target: DesiredTargetSide) -> BrokerOrderSide:
    return (
        BrokerOrderSide.BUY
        if target == DesiredTargetSide.LONG
        else BrokerOrderSide.SELL
    )


def _scope_error(
    command: ExecutionCommandStateV1,
    position: StrategyPositionV1,
) -> str | None:
    command_scope = (
        command.strategy_id,
        command.deployment_id,
        command.instrument_id,
    )
    position_scope = (
        position.strategy_id,
        position.deployment_id,
        position.instrument_id,
    )
    if command_scope != position_scope:
        return (
            "command and strategy position scopes differ: "
            f"command={command_scope}, position={position_scope}"
        )
    return None


def plan_broker_operation(
    *,
    command: ExecutionCommandStateV1,
    position: StrategyPositionV1,
    active_contract: RegisteredFuturesContractV1,
    account_id: str,
    observed_at_utc: str,
) -> BrokerOperationSnapshot:
    if not isinstance(command, ExecutionCommandStateV1):
        raise BrokerAttemptDomainError(
            "command must be ExecutionCommandStateV1"
        )
    if command.state != ExecutionCommandState.ADMITTED:
        raise BrokerAttemptDomainError(
            "broker operation can be planned only from ADMITTED command"
        )
    if not isinstance(position, StrategyPositionV1):
        raise BrokerAttemptDomainError("position must be StrategyPositionV1")
    if not isinstance(active_contract, RegisteredFuturesContractV1):
        raise BrokerAttemptDomainError(
            "active_contract must be RegisteredFuturesContractV1"
        )
    if not active_contract.contract_is_active:
        raise BrokerAttemptDomainError(
            "broker operation requires one active registered contract"
        )
    scope_error = _scope_error(command, position)
    if scope_error is not None:
        raise BrokerAttemptDomainError(scope_error)

    if command.command_kind == StrategyCommandKind.OPEN:
        if position.projection_status != StrategyPositionStatus.FLAT:
            raise BrokerAttemptDomainError(
                "OPEN broker operation requires proven FLAT position"
            )
        quantity = command.desired_target_quantity
        con_id = active_contract.con_id
        local_symbol = active_contract.local_symbol
    elif command.command_kind == StrategyCommandKind.REVERSE:
        if position.projection_status != StrategyPositionStatus.OPEN:
            raise BrokerAttemptDomainError(
                "REVERSE broker operation requires proven OPEN position"
            )
        expected_current = (
            StrategyPositionSide.SHORT
            if command.desired_target_side == DesiredTargetSide.LONG
            else StrategyPositionSide.LONG
        )
        if position.side != expected_current:
            raise BrokerAttemptDomainError(
                "REVERSE broker operation requires opposite current side"
            )
        if len(position.contracts) != 1:
            raise BrokerAttemptDomainError(
                "REVERSE broker operation requires exactly one held contract"
            )
        held = position.contracts[0]
        if not held.contract_is_active:
            raise BrokerAttemptDomainError(
                "REVERSE cannot use a non-active held contract"
            )
        if (
            held.con_id != active_contract.con_id
            or held.local_symbol != active_contract.local_symbol
        ):
            raise BrokerAttemptDomainError(
                "held contract does not match current active contract"
            )
        quantity = position.quantity + command.desired_target_quantity
        con_id = held.con_id
        local_symbol = held.local_symbol
    else:
        raise BrokerAttemptDomainError(
            f"unsupported strategy command kind: {command.command_kind.value}"
        )

    observed = format_utc(parse_utc(observed_at_utc))
    operation_id = _operation_id(command.command_id)
    attempt_no = 1
    attempt_id = _attempt_id(operation_id, attempt_no)
    side = _broker_side(command.desired_target_side)
    attempt = BrokerOrderAttemptV1(
        attempt_id=attempt_id,
        operation_id=operation_id,
        attempt_no=attempt_no,
        order_ref=build_order_ref(operation_id, attempt_no),
        side=side,
        order_type="MARKET",
        con_id=con_id,
        local_symbol=local_symbol,
        requested_qty=quantity,
        filled_qty=0,
        remaining_qty=quantity,
        state=BrokerAttemptState.PREPARING,
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
    operation = BrokerOrderOperationV1(
        operation_id=operation_id,
        command_id=command.command_id,
        account_id=str(account_id),
        strategy_id=command.strategy_id,
        strategy_version=command.strategy_version,
        deployment_id=command.deployment_id,
        instrument_id=command.instrument_id,
        side=side,
        order_type="MARKET",
        con_id=con_id,
        local_symbol=local_symbol,
        requested_qty=quantity,
        filled_qty=0,
        remaining_qty=quantity,
        state=BrokerOperationState.PREPARING,
        current_attempt_id=attempt_id,
        current_attempt_no=attempt_no,
        created_at_utc=observed,
        updated_at_utc=observed,
        terminal_at_utc=None,
        blocking_reason=None,
    )
    return BrokerOperationSnapshot(operation=operation, attempt=attempt)


def mark_attempt_submitting(
    snapshot: BrokerOperationSnapshot,
    *,
    observed_at_utc: str,
    broker_order_id: int | None = None,
) -> BrokerOperationSnapshot:
    if snapshot.operation.state != BrokerOperationState.PREPARING:
        raise BrokerAttemptDomainError(
            "only PREPARING operation can enter SUBMITTING"
        )
    if snapshot.attempt.state != BrokerAttemptState.PREPARING:
        raise BrokerAttemptDomainError(
            "only PREPARING attempt can enter SUBMITTING"
        )
    observed = format_utc(parse_utc(observed_at_utc))
    attempt = replace(
        snapshot.attempt,
        state=BrokerAttemptState.SUBMITTING,
        broker_order_id=broker_order_id,
        updated_at_utc=observed,
    )
    operation = replace(
        snapshot.operation,
        state=BrokerOperationState.SUBMITTING,
        updated_at_utc=observed,
    )
    return BrokerOperationSnapshot(operation=operation, attempt=attempt)


def begin_reconciliation(
    snapshot: BrokerOperationSnapshot,
    *,
    observed_at_utc: str,
) -> BrokerOperationSnapshot:
    if snapshot.operation.state not in {
        BrokerOperationState.SUBMITTING,
        BrokerOperationState.LIVE,
        BrokerOperationState.UNKNOWN_OUTCOME,
    }:
        raise BrokerAttemptDomainError(
            "reconciliation requires SUBMITTING, LIVE or UNKNOWN_OUTCOME operation"
        )
    if snapshot.attempt.state not in {
        BrokerAttemptState.SUBMITTING,
        BrokerAttemptState.LIVE,
        BrokerAttemptState.UNKNOWN_OUTCOME,
    }:
        raise BrokerAttemptDomainError(
            "current attempt is not eligible for broker reconciliation"
        )
    observed = format_utc(parse_utc(observed_at_utc))
    operation = replace(
        snapshot.operation,
        state=BrokerOperationState.RECONCILING,
        updated_at_utc=observed,
        blocking_reason="broker_reconciliation_in_progress",
    )
    return BrokerOperationSnapshot(operation=operation, attempt=snapshot.attempt)


def mark_unknown_outcome(
    snapshot: BrokerOperationSnapshot,
    *,
    observed_at_utc: str,
    reason: str,
) -> BrokerOperationSnapshot:
    if snapshot.attempt.state == BrokerAttemptState.PREPARING:
        raise BrokerAttemptDomainError(
            "PREPARING attempt has no broker exposure and cannot be UNKNOWN_OUTCOME"
        )
    detail = str(reason or "").strip()
    if not detail:
        raise BrokerAttemptDomainError("unknown outcome requires a reason")
    observed = format_utc(parse_utc(observed_at_utc))
    attempt = replace(
        snapshot.attempt,
        state=BrokerAttemptState.UNKNOWN_OUTCOME,
        updated_at_utc=observed,
        terminal_at_utc=None,
        broker_terminal_proven=False,
        failure_reason=detail,
    )
    operation = replace(
        snapshot.operation,
        state=BrokerOperationState.UNKNOWN_OUTCOME,
        updated_at_utc=observed,
        terminal_at_utc=None,
        blocking_reason=detail,
    )
    return BrokerOperationSnapshot(operation=operation, attempt=attempt)


def apply_broker_observation(
    snapshot: BrokerOperationSnapshot,
    *,
    observation: BrokerOrderObservationV1,
) -> BrokerOperationSnapshot:
    if not isinstance(observation, BrokerOrderObservationV1):
        raise BrokerAttemptDomainError(
            "observation must be BrokerOrderObservationV1"
        )
    if snapshot.attempt.state == BrokerAttemptState.PREPARING:
        raise BrokerAttemptDomainError(
            "PREPARING attempt cannot be reconciled with broker facts"
        )
    if observation.order_ref != snapshot.attempt.order_ref:
        raise BrokerAttemptDomainError(
            "broker observation order_ref does not match current attempt"
        )
    if observation.outcome in {
        BrokerObservationOutcome.NOT_FOUND,
        BrokerObservationOutcome.AMBIGUOUS,
    }:
        return mark_unknown_outcome(
            snapshot,
            observed_at_utc=observation.observed_at_utc,
            reason=(
                f"broker_reconciliation_{observation.outcome.value.lower()}: "
                f"{observation.detail}"
            ),
        )

    if observation.requested_qty != snapshot.attempt.requested_qty:
        raise BrokerAttemptDomainError(
            "broker observation requested quantity differs from persisted attempt"
        )
    prior_filled = snapshot.operation.filled_qty - snapshot.attempt.filled_qty
    if prior_filled < 0:
        raise BrokerAttemptDomainError(
            "persisted operation fill is lower than current attempt fill"
        )
    cumulative_filled = prior_filled + int(observation.filled_qty)
    operation_remaining = snapshot.operation.requested_qty - cumulative_filled
    if operation_remaining != int(observation.remaining_qty):
        raise BrokerAttemptDomainError(
            "broker observation remaining quantity conflicts with operation total"
        )

    terminal_attempt = observation.outcome in {
        BrokerObservationOutcome.FILLED,
        BrokerObservationOutcome.CANCELLED,
        BrokerObservationOutcome.REJECTED,
        BrokerObservationOutcome.FAILED,
    }
    attempt_state = BrokerAttemptState(observation.outcome.value)
    observed = observation.observed_at_utc
    attempt = replace(
        snapshot.attempt,
        state=attempt_state,
        filled_qty=int(observation.filled_qty),
        remaining_qty=int(observation.remaining_qty),
        broker_order_id=observation.broker_order_id,
        broker_perm_id=observation.broker_perm_id,
        broker_status=observation.broker_status,
        broker_terminal_proven=terminal_attempt,
        updated_at_utc=observed,
        terminal_at_utc=observed if terminal_attempt else None,
        last_broker_proof_at_utc=observed,
        failure_reason=(
            observation.detail
            if observation.outcome
            in {
                BrokerObservationOutcome.REJECTED,
                BrokerObservationOutcome.FAILED,
            }
            else None
        ),
    )

    if observation.outcome == BrokerObservationOutcome.LIVE:
        operation_state = BrokerOperationState.LIVE
        terminal_at = None
        reason = None
    elif operation_remaining == 0:
        operation_state = BrokerOperationState.SUCCEEDED
        terminal_at = observed
        reason = None
    else:
        operation_state = BrokerOperationState.FAILED_RETRYABLE
        terminal_at = None
        reason = (
            f"broker_attempt_{observation.outcome.value.lower()}_with_remaining"
        )
    operation = replace(
        snapshot.operation,
        state=operation_state,
        filled_qty=cumulative_filled,
        remaining_qty=operation_remaining,
        updated_at_utc=observed,
        terminal_at_utc=terminal_at,
        blocking_reason=reason,
    )
    return BrokerOperationSnapshot(operation=operation, attempt=attempt)


def prepare_next_attempt(
    snapshot: BrokerOperationSnapshot,
    *,
    observed_at_utc: str,
) -> BrokerOperationSnapshot:
    if snapshot.operation.state != BrokerOperationState.FAILED_RETRYABLE:
        raise BrokerAttemptDomainError(
            "next attempt requires FAILED_RETRYABLE operation"
        )
    if snapshot.attempt.state not in {
        BrokerAttemptState.CANCELLED,
        BrokerAttemptState.REJECTED,
        BrokerAttemptState.FAILED,
    }:
        raise BrokerAttemptDomainError(
            "previous attempt is not a terminal retryable outcome"
        )
    if not snapshot.attempt.broker_terminal_proven:
        raise BrokerAttemptDomainError(
            "next attempt requires proven terminal broker outcome"
        )
    if snapshot.operation.remaining_qty <= 0:
        raise BrokerAttemptDomainError(
            "next attempt requires positive fresh remaining quantity"
        )
    observed = format_utc(parse_utc(observed_at_utc))
    attempt_no = snapshot.attempt.attempt_no + 1
    attempt_id = _attempt_id(snapshot.operation.operation_id, attempt_no)
    attempt = BrokerOrderAttemptV1(
        attempt_id=attempt_id,
        operation_id=snapshot.operation.operation_id,
        attempt_no=attempt_no,
        order_ref=build_order_ref(snapshot.operation.operation_id, attempt_no),
        side=snapshot.operation.side,
        order_type=snapshot.operation.order_type,
        con_id=snapshot.operation.con_id,
        local_symbol=snapshot.operation.local_symbol,
        requested_qty=snapshot.operation.remaining_qty,
        filled_qty=0,
        remaining_qty=snapshot.operation.remaining_qty,
        state=BrokerAttemptState.PREPARING,
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
    operation = replace(
        snapshot.operation,
        state=BrokerOperationState.PREPARING,
        current_attempt_id=attempt_id,
        current_attempt_no=attempt_no,
        updated_at_utc=observed,
        blocking_reason=None,
    )
    return BrokerOperationSnapshot(operation=operation, attempt=attempt)


def require_operator_resolution(
    snapshot: BrokerOperationSnapshot,
    *,
    observed_at_utc: str,
    reason: str,
) -> BrokerOperationSnapshot:
    if snapshot.operation.state != BrokerOperationState.UNKNOWN_OUTCOME:
        raise BrokerAttemptDomainError(
            "operator-required terminal state is reserved for UNKNOWN_OUTCOME"
        )
    detail = str(reason or "").strip()
    if not detail:
        raise BrokerAttemptDomainError("operator resolution requires a reason")
    observed = format_utc(parse_utc(observed_at_utc))
    operation = replace(
        snapshot.operation,
        state=BrokerOperationState.FAILED_OPERATOR_REQUIRED,
        updated_at_utc=observed,
        terminal_at_utc=observed,
        blocking_reason=detail,
    )
    return BrokerOperationSnapshot(operation=operation, attempt=snapshot.attempt)
