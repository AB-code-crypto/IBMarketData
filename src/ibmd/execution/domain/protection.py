from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, replace
from decimal import Decimal, ROUND_HALF_UP

from ibmd.execution.domain.broker_attempt import BrokerOperationSnapshot
from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.broker_execution import (
    BrokerAttemptState,
    BrokerObservationOutcome,
    BrokerOperationState,
    BrokerOrderObservationV1,
    BrokerOrderSide,
)
from ibmd.public_contracts.broker_reconciliation import BrokerFillFactV1
from ibmd.public_contracts.decision import DesiredTargetSide, StrategyCommandKind
from ibmd.public_contracts.execution import (
    ExecutionCommandState,
    ExecutionCommandStateV1,
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
    PositionContractV1,
    StrategyPositionSide,
    StrategyPositionStatus,
    StrategyPositionV1,
)
from ibmd.public_contracts.positions import (
    BrokerPositionSnapshotStatus,
    BrokerPositionSnapshotV1,
)
from ibmd.public_contracts.protection import (
    PositionEpisodePolicyV1,
    PositionEpisodeStatus,
    PositionEpisodeV1,
    ProtectionSetStatus,
    ProtectionStateV1,
    ProtectiveOrderKind,
    ProtectiveOrderState,
    ProtectiveOrderType,
    ProtectiveOrderV1,
)


class ProtectionPlanningError(ValueError):
    pass


def _stable_id(kind: str, payload: dict[str, object]) -> str:
    digest = hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()[:32]
    return f"{kind}_{digest}"


def _positive_float(value: object, *, field_name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ProtectionPlanningError(
            f"{field_name} must be numeric: {value!r}"
        ) from exc
    if not math.isfinite(parsed) or parsed <= 0.0:
        raise ProtectionPlanningError(
            f"{field_name} must be finite and positive: {parsed}"
        )
    return parsed


@dataclass(frozen=True)
class ProtectionPlanningPolicyV1:
    account_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    strategy_policy_hash: str
    position_max_age_seconds: float
    protective_policy: PositionEpisodePolicyV1

    def __post_init__(self) -> None:
        for field_name in (
            "account_id",
            "strategy_id",
            "deployment_id",
            "instrument_id",
            "strategy_policy_hash",
        ):
            value = str(getattr(self, field_name) or "").strip()
            if not value:
                raise ProtectionPlanningError(f"{field_name} is required")
            object.__setattr__(self, field_name, value)
        version = int(self.strategy_version)
        if version <= 0:
            raise ProtectionPlanningError("strategy_version must be positive")
        object.__setattr__(self, "strategy_version", version)
        object.__setattr__(
            self,
            "position_max_age_seconds",
            _positive_float(
                self.position_max_age_seconds,
                field_name="position_max_age_seconds",
            ),
        )
        if not isinstance(self.protective_policy, PositionEpisodePolicyV1):
            raise ProtectionPlanningError(
                "protective_policy must be PositionEpisodePolicyV1"
            )


@dataclass(frozen=True)
class PositionEpisodeProtectionPlan:
    episode: PositionEpisodeV1
    strategy_position: StrategyPositionV1
    execution_readiness: ExecutionReadinessV1
    protection: ProtectionStateV1


def _tick_price(value: float, tick: float) -> float:
    tick_decimal = Decimal(str(tick))
    raw = Decimal(str(value))
    ticks = (raw / tick_decimal).quantize(
        Decimal("1"),
        rounding=ROUND_HALF_UP,
    )
    return float(ticks * tick_decimal)


def _weighted_entry_price(
    *,
    operation: BrokerOperationSnapshot,
    fills: tuple[BrokerFillFactV1, ...],
) -> tuple[float, tuple[str, ...], str]:
    if not fills:
        raise ProtectionPlanningError(
            "a succeeded operation requires immutable fill facts"
        )
    seen: set[str] = set()
    total_qty = 0
    total_value = 0.0
    executed_at = []
    for fill in fills:
        if fill.exec_id in seen:
            raise ProtectionPlanningError(
                f"duplicate fill execId: {fill.exec_id}"
            )
        seen.add(fill.exec_id)
        if fill.account_id != operation.operation.account_id:
            raise ProtectionPlanningError(
                "fill account differs from operation"
            )
        if (
            fill.order_ref != operation.attempt.order_ref
            or fill.con_id != operation.operation.con_id
            or fill.local_symbol != operation.operation.local_symbol
            or fill.side != operation.operation.side
        ):
            raise ProtectionPlanningError(
                f"fill does not belong to operation: exec_id={fill.exec_id}"
            )
        if (
            operation.attempt.broker_order_id is not None
            and fill.broker_order_id != operation.attempt.broker_order_id
        ):
            raise ProtectionPlanningError(
                f"fill broker order id differs: exec_id={fill.exec_id}"
            )
        total_qty += fill.shares
        total_value += fill.price * fill.shares
        executed_at.append(parse_utc(fill.executed_at_utc))
    if total_qty != operation.operation.requested_qty:
        raise ProtectionPlanningError(
            "fill quantity does not complete operation: "
            f"expected={operation.operation.requested_qty}, actual={total_qty}"
        )
    return (
        total_value / total_qty,
        tuple(sorted(seen)),
        format_utc(max(executed_at)),
    )


def _validate_source_operation(
    *,
    operation: BrokerOperationSnapshot,
    command: ExecutionCommandStateV1,
    policy: ProtectionPlanningPolicyV1,
) -> None:
    if (
        operation.operation.state != BrokerOperationState.SUCCEEDED
        or operation.attempt.state != BrokerAttemptState.FILLED
        or operation.operation.remaining_qty != 0
        or operation.attempt.remaining_qty != 0
    ):
        raise ProtectionPlanningError(
            "position episode requires a completely filled SUCCEEDED operation"
        )
    if command.state != ExecutionCommandState.ADMITTED:
        raise ProtectionPlanningError(
            "position episode source command must remain ADMITTED"
        )
    expected_scope = (
        policy.strategy_id,
        policy.strategy_version,
        policy.deployment_id,
        policy.instrument_id,
    )
    operation_scope = (
        operation.operation.strategy_id,
        operation.operation.strategy_version,
        operation.operation.deployment_id,
        operation.operation.instrument_id,
    )
    command_scope = (
        command.strategy_id,
        command.strategy_version,
        command.deployment_id,
        command.instrument_id,
    )
    if operation_scope != expected_scope or command_scope != expected_scope:
        raise ProtectionPlanningError(
            "operation/command scope differs from protection policy"
        )
    if (
        operation.operation.command_id != command.command_id
        or operation.operation.account_id != policy.account_id
    ):
        raise ProtectionPlanningError(
            "operation identity differs from command/account"
        )
    if command.command_kind not in {
        StrategyCommandKind.OPEN,
        StrategyCommandKind.REVERSE,
    }:
        raise ProtectionPlanningError(
            "unsupported episode source command: "
            f"{command.command_kind.value}"
        )


def _target_position_row(
    *,
    snapshot: BrokerPositionSnapshotV1,
    operation: BrokerOperationSnapshot,
    command: ExecutionCommandStateV1,
    policy: ProtectionPlanningPolicyV1,
    observed_at_utc: str,
):
    if snapshot.status != BrokerPositionSnapshotStatus.COMPLETE:
        raise ProtectionPlanningError(
            "broker position snapshot is not COMPLETE"
        )
    if snapshot.account_id != policy.account_id:
        raise ProtectionPlanningError(
            "broker position snapshot account mismatch"
        )
    freshness = snapshot.freshness(
        observed_at_utc=observed_at_utc,
        max_age_seconds=policy.position_max_age_seconds,
    )
    if not freshness.is_fresh:
        raise ProtectionPlanningError(
            "broker position snapshot is stale: "
            f"age={freshness.age_seconds:.6f}s"
        )
    rows = [
        row
        for row in snapshot.rows
        if row.con_id == operation.operation.con_id
        and str(row.local_symbol or "") == operation.operation.local_symbol
    ]
    if len(rows) != 1:
        raise ProtectionPlanningError(
            "broker snapshot does not contain exactly one operation contract row"
        )
    row = rows[0]
    if (
        row.symbol.upper() != policy.instrument_id.upper()
        or row.sec_type != "FUT"
    ):
        raise ProtectionPlanningError(
            "broker position row identity differs from protected instrument"
        )
    raw_quantity = float(row.signed_quantity)
    rounded = round(raw_quantity)
    if abs(raw_quantity - rounded) > 1e-9:
        raise ProtectionPlanningError(
            "broker position quantity is fractional"
        )
    expected_signed = (
        command.desired_target_quantity
        if command.desired_target_side == DesiredTargetSide.LONG
        else -command.desired_target_quantity
    )
    if int(rounded) != expected_signed:
        raise ProtectionPlanningError(
            "broker position does not prove command target: "
            f"expected={expected_signed}, actual={rounded}"
        )
    competing = [
        item
        for item in snapshot.rows
        if item is not row
        and (
            item.symbol.upper() == policy.instrument_id.upper()
            or str(item.local_symbol or "").upper().startswith(
                policy.instrument_id.upper()
            )
        )
    ]
    if competing:
        raise ProtectionPlanningError(
            "broker position snapshot contains another instrument contract"
        )
    return row, freshness


def create_position_episode_protection_plan(
    *,
    operation: BrokerOperationSnapshot,
    command: ExecutionCommandStateV1,
    fills: tuple[BrokerFillFactV1, ...],
    broker_snapshot: BrokerPositionSnapshotV1,
    previous_position: StrategyPositionV1 | None,
    current_readiness: ExecutionReadinessV1,
    policy: ProtectionPlanningPolicyV1,
    observed_at_utc: str,
) -> PositionEpisodeProtectionPlan:
    if not isinstance(operation, BrokerOperationSnapshot):
        raise ProtectionPlanningError(
            "operation must be BrokerOperationSnapshot"
        )
    if not isinstance(command, ExecutionCommandStateV1):
        raise ProtectionPlanningError(
            "command must be ExecutionCommandStateV1"
        )
    if not isinstance(current_readiness, ExecutionReadinessV1):
        raise ProtectionPlanningError(
            "current_readiness must be ExecutionReadinessV1"
        )
    _validate_source_operation(
        operation=operation,
        command=command,
        policy=policy,
    )
    observed = format_utc(parse_utc(observed_at_utc))
    _row, freshness = _target_position_row(
        snapshot=broker_snapshot,
        operation=operation,
        command=command,
        policy=policy,
        observed_at_utc=observed,
    )
    if previous_position is not None:
        expected_position_scope = (
            policy.account_id,
            policy.strategy_id,
            policy.deployment_id,
            policy.instrument_id,
        )
        actual_position_scope = (
            previous_position.account_id,
            previous_position.strategy_id,
            previous_position.deployment_id,
            previous_position.instrument_id,
        )
        if actual_position_scope != expected_position_scope:
            raise ProtectionPlanningError(
                "previous strategy position belongs to another scope"
            )
        if (
            command.command_kind == StrategyCommandKind.OPEN
            and previous_position.projection_status
            != StrategyPositionStatus.FLAT
        ):
            raise ProtectionPlanningError(
                "OPEN episode activation requires previous FLAT projection"
            )
        if command.command_kind == StrategyCommandKind.REVERSE and (
            previous_position.projection_status
            != StrategyPositionStatus.OPEN
            or previous_position.side.value
            == command.desired_target_side.value
        ):
            raise ProtectionPlanningError(
                "REVERSE episode activation requires opposite previous position"
            )
    readiness_scope = (
        current_readiness.account_id,
        current_readiness.strategy_id,
        current_readiness.deployment_id,
        current_readiness.instrument_id,
    )
    expected_readiness_scope = (
        policy.account_id,
        policy.strategy_id,
        policy.deployment_id,
        policy.instrument_id,
    )
    if readiness_scope != expected_readiness_scope:
        raise ProtectionPlanningError(
            "current execution readiness belongs to another scope"
        )

    entry_price, exec_ids, opened_at = _weighted_entry_price(
        operation=operation,
        fills=fills,
    )
    identity = {
        "source_operation_id": operation.operation.operation_id,
        "account_id": policy.account_id,
        "strategy_id": policy.strategy_id,
        "strategy_version": policy.strategy_version,
        "deployment_id": policy.deployment_id,
        "instrument_id": policy.instrument_id,
    }
    episode_id = _stable_id("position_episode", identity)
    protection_set_id = _stable_id(
        "protection_set",
        {"position_episode_id": episode_id},
    )
    stop_id = _stable_id(
        "protective_order",
        {
            "protection_set_id": protection_set_id,
            "kind": ProtectiveOrderKind.STOP_LOSS.value,
        },
    )
    tp_id = _stable_id(
        "protective_order",
        {
            "protection_set_id": protection_set_id,
            "kind": ProtectiveOrderKind.TAKE_PROFIT.value,
        },
    )
    opposite_side = (
        BrokerOrderSide.SELL
        if command.desired_target_side == DesiredTargetSide.LONG
        else BrokerOrderSide.BUY
    )
    position_side = StrategyPositionSide(
        command.desired_target_side.value
    )
    signed_quantity = (
        command.desired_target_quantity
        if position_side == StrategyPositionSide.LONG
        else -command.desired_target_quantity
    )
    protective = policy.protective_policy
    stop_raw = (
        entry_price - protective.stop_loss_points
        if position_side == StrategyPositionSide.LONG
        else entry_price + protective.stop_loss_points
    )
    tp_raw = (
        entry_price + protective.take_profit_points
        if position_side == StrategyPositionSide.LONG
        else entry_price - protective.take_profit_points
    )
    stop_price = _tick_price(stop_raw, protective.price_tick)
    take_profit_price = _tick_price(tp_raw, protective.price_tick)
    if stop_price <= 0.0 or take_profit_price <= 0.0:
        raise ProtectionPlanningError(
            "calculated protective price is not positive"
        )
    oca_group = (
        f"IBMD_OCA_{protection_set_id.rsplit('_', 1)[-1]}"
        if protective.take_profit_enabled
        else None
    )
    stop_order = ProtectiveOrderV1(
        protective_order_id=stop_id,
        protection_set_id=protection_set_id,
        position_episode_id=episode_id,
        kind=ProtectiveOrderKind.STOP_LOSS,
        state=ProtectiveOrderState.PLANNED,
        planned_sequence=1,
        order_ref=f"IBMD:{protection_set_id}:SL",
        side=opposite_side,
        order_type=ProtectiveOrderType.STOP,
        quantity=command.desired_target_quantity,
        con_id=operation.operation.con_id,
        local_symbol=operation.operation.local_symbol,
        stop_price=stop_price,
        limit_price=None,
        time_in_force=protective.time_in_force,
        outside_rth=protective.stop_outside_rth,
        oca_group=oca_group,
        filled_qty=0,
        remaining_qty=command.desired_target_quantity,
        broker_order_id=None,
        broker_perm_id=None,
        broker_status=None,
        broker_terminal_proven=False,
        created_at_utc=opened_at,
        updated_at_utc=opened_at,
        terminal_at_utc=None,
        last_broker_proof_at_utc=None,
        failure_reason=None,
    )
    orders = [stop_order]
    if protective.take_profit_enabled:
        orders.append(
            ProtectiveOrderV1(
                protective_order_id=tp_id,
                protection_set_id=protection_set_id,
                position_episode_id=episode_id,
                kind=ProtectiveOrderKind.TAKE_PROFIT,
                state=ProtectiveOrderState.PLANNED,
                planned_sequence=2,
                order_ref=f"IBMD:{protection_set_id}:TP",
                side=opposite_side,
                order_type=ProtectiveOrderType.LIMIT,
                quantity=command.desired_target_quantity,
                con_id=operation.operation.con_id,
                local_symbol=operation.operation.local_symbol,
                stop_price=None,
                limit_price=take_profit_price,
                time_in_force=protective.time_in_force,
                outside_rth=protective.take_profit_outside_rth,
                oca_group=oca_group,
                filled_qty=0,
                remaining_qty=command.desired_target_quantity,
                broker_order_id=None,
                broker_perm_id=None,
                broker_status=None,
                broker_terminal_proven=False,
                created_at_utc=opened_at,
                updated_at_utc=opened_at,
                terminal_at_utc=None,
                last_broker_proof_at_utc=None,
                failure_reason=None,
            )
        )

    episode = PositionEpisodeV1(
        position_episode_id=episode_id,
        account_id=policy.account_id,
        strategy_id=policy.strategy_id,
        strategy_version=policy.strategy_version,
        deployment_id=policy.deployment_id,
        instrument_id=policy.instrument_id,
        source_command_id=command.command_id,
        source_operation_id=operation.operation.operation_id,
        source_attempt_id=operation.attempt.attempt_id,
        source_exec_ids=exec_ids,
        side=position_side,
        quantity=command.desired_target_quantity,
        con_id=operation.operation.con_id,
        local_symbol=operation.operation.local_symbol,
        entry_average_price=entry_price,
        broker_snapshot_id=broker_snapshot.snapshot_id,
        opened_at_utc=opened_at,
        status=PositionEpisodeStatus.OPEN,
        strategy_policy_hash=policy.strategy_policy_hash,
        protective_policy_hash=protective.content_hash,
        protective_policy=protective,
    )
    strategy_position = StrategyPositionV1(
        account_id=policy.account_id,
        strategy_id=policy.strategy_id,
        deployment_id=policy.deployment_id,
        instrument_id=policy.instrument_id,
        position_episode_id=episode_id,
        side=position_side,
        quantity=command.desired_target_quantity,
        contracts=(
            PositionContractV1(
                con_id=operation.operation.con_id,
                local_symbol=operation.operation.local_symbol,
                signed_quantity=signed_quantity,
                contract_is_active=True,
            ),
        ),
        projection_status=StrategyPositionStatus.OPEN,
        broker_snapshot_id=broker_snapshot.snapshot_id,
        updated_at_utc=observed,
        source_freshness_seconds=freshness.age_seconds,
    )
    reasons = tuple(
        item
        for item in current_readiness.blocking_reasons
        if not item.startswith("protection:")
    ) + ("protection:stop_not_proven",)
    execution_readiness = ExecutionReadinessV1(
        account_id=current_readiness.account_id,
        strategy_id=current_readiness.strategy_id,
        deployment_id=current_readiness.deployment_id,
        instrument_id=current_readiness.instrument_id,
        status=ExecutionReadinessStatus.BLOCKED,
        command_intake_enabled=False,
        broker_actions_enabled=True,
        reconciliation_complete=current_readiness.reconciliation_complete,
        clock_healthy=current_readiness.clock_healthy,
        blocking_reasons=reasons,
        updated_at_utc=observed,
    )
    protection = ProtectionStateV1(
        protection_set_id=protection_set_id,
        position_episode_id=episode_id,
        account_id=policy.account_id,
        strategy_id=policy.strategy_id,
        strategy_version=policy.strategy_version,
        deployment_id=policy.deployment_id,
        instrument_id=policy.instrument_id,
        status=ProtectionSetStatus.PLANNED,
        orders=tuple(orders),
        created_at_utc=opened_at,
        updated_at_utc=opened_at,
        terminal_at_utc=None,
        blocking_reason="stop_not_submitted",
    )
    return PositionEpisodeProtectionPlan(
        episode=episode,
        strategy_position=strategy_position,
        execution_readiness=execution_readiness,
        protection=protection,
    )


def _observation_order_state(
    *,
    order: ProtectiveOrderV1,
    observation: BrokerOrderObservationV1,
) -> ProtectiveOrderV1:
    if observation.order_ref != order.order_ref:
        raise ProtectionPlanningError(
            "broker observation order_ref mismatch"
        )
    observed = observation.observed_at_utc
    detail = observation.detail
    if (
        order.kind == ProtectiveOrderKind.STOP_LOSS
        and detail is not None
        and "399" in detail
    ):
        return replace(
            order,
            state=ProtectiveOrderState.REJECTED,
            filled_qty=0,
            remaining_qty=order.quantity,
            broker_order_id=observation.broker_order_id,
            broker_perm_id=observation.broker_perm_id,
            broker_status=observation.broker_status or "HELD_399",
            broker_terminal_proven=True,
            updated_at_utc=observed,
            terminal_at_utc=observed,
            last_broker_proof_at_utc=observed,
            failure_reason=detail,
        )
    if observation.outcome in {
        BrokerObservationOutcome.NOT_FOUND,
        BrokerObservationOutcome.AMBIGUOUS,
    }:
        if order.state == ProtectiveOrderState.PLANNED:
            return order
        return replace(
            order,
            state=ProtectiveOrderState.UNKNOWN_OUTCOME,
            updated_at_utc=observed,
            failure_reason=detail or observation.outcome.value.lower(),
        )
    state = {
        BrokerObservationOutcome.LIVE: ProtectiveOrderState.LIVE,
        BrokerObservationOutcome.FILLED: ProtectiveOrderState.FILLED,
        BrokerObservationOutcome.CANCELLED: ProtectiveOrderState.CANCELLED,
        BrokerObservationOutcome.REJECTED: ProtectiveOrderState.REJECTED,
        BrokerObservationOutcome.FAILED: ProtectiveOrderState.FAILED,
    }[observation.outcome]
    terminal = state in {
        ProtectiveOrderState.FILLED,
        ProtectiveOrderState.CANCELLED,
        ProtectiveOrderState.REJECTED,
        ProtectiveOrderState.FAILED,
    }
    return replace(
        order,
        state=state,
        filled_qty=int(observation.filled_qty or 0),
        remaining_qty=int(observation.remaining_qty or 0),
        broker_order_id=observation.broker_order_id,
        broker_perm_id=observation.broker_perm_id,
        broker_status=observation.broker_status,
        broker_terminal_proven=terminal,
        updated_at_utc=observed,
        terminal_at_utc=observed if terminal else None,
        last_broker_proof_at_utc=observed,
        failure_reason=(
            detail
            if state in {
                ProtectiveOrderState.REJECTED,
                ProtectiveOrderState.FAILED,
            }
            else None
        ),
    )


def apply_protective_observation(
    *,
    protection: ProtectionStateV1,
    kind: ProtectiveOrderKind,
    observation: BrokerOrderObservationV1,
    position_open: bool,
) -> ProtectionStateV1:
    if not isinstance(protection, ProtectionStateV1):
        raise ProtectionPlanningError(
            "protection must be ProtectionStateV1"
        )
    if not isinstance(kind, ProtectiveOrderKind):
        raise ProtectionPlanningError(
            "kind must be ProtectiveOrderKind"
        )
    if not isinstance(observation, BrokerOrderObservationV1):
        raise ProtectionPlanningError(
            "observation must be BrokerOrderObservationV1"
        )
    if not isinstance(position_open, bool):
        raise ProtectionPlanningError(
            "position_open must be boolean"
        )
    matching = [
        item for item in protection.orders if item.kind == kind
    ]
    if len(matching) != 1:
        raise ProtectionPlanningError(
            f"protection does not contain exactly one {kind.value} order"
        )
    updated_order = _observation_order_state(
        order=matching[0],
        observation=observation,
    )
    orders = tuple(
        updated_order if item.kind == kind else item
        for item in protection.orders
    )
    stop = next(
        item
        for item in orders
        if item.kind == ProtectiveOrderKind.STOP_LOSS
    )
    tp = next(
        (
            item
            for item in orders
            if item.kind == ProtectiveOrderKind.TAKE_PROFIT
        ),
        None,
    )
    observed = observation.observed_at_utc
    reason = None
    terminal_at = None
    if any(
        item.state == ProtectiveOrderState.FILLED for item in orders
    ):
        status = ProtectionSetStatus.EXITED
        terminal_at = observed
    elif not position_open:
        status = ProtectionSetStatus.CLOSED
        terminal_at = observed
    elif stop.state == ProtectiveOrderState.LIVE:
        if tp is None or tp.state in {
            ProtectiveOrderState.LIVE,
            ProtectiveOrderState.FAILED,
            ProtectiveOrderState.REJECTED,
            ProtectiveOrderState.CANCELLED,
            ProtectiveOrderState.NOT_REQUIRED,
        }:
            status = ProtectionSetStatus.PROTECTED
            if tp is not None and tp.state != ProtectiveOrderState.LIVE:
                reason = "take_profit_unavailable_stop_live"
        else:
            status = ProtectionSetStatus.STOP_LIVE
    elif stop.state == ProtectiveOrderState.SUBMITTING:
        status = ProtectionSetStatus.STOP_SUBMITTING
    elif stop.state in {
        ProtectiveOrderState.CANCELLED,
        ProtectiveOrderState.REJECTED,
        ProtectiveOrderState.FAILED,
        ProtectiveOrderState.UNKNOWN_OUTCOME,
    }:
        status = ProtectionSetStatus.UNPROTECTED
        reason = stop.failure_reason or f"stop_{stop.state.value.lower()}"
    else:
        status = ProtectionSetStatus.PLANNED
        reason = "stop_not_submitted"
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
        terminal_at_utc=terminal_at,
        blocking_reason=reason,
    )
