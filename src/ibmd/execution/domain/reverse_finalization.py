from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, replace
from decimal import Decimal, ROUND_HALF_UP

from ibmd.execution.domain.broker_attempt import BrokerOperationSnapshot
from ibmd.execution.domain.protection import PositionEpisodeProtectionPlan
from ibmd.foundation.atomic_json import canonical_json_text
from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.broker_execution import (
    BrokerAttemptState,
    BrokerOperationState,
    BrokerOrderSide,
)
from ibmd.public_contracts.broker_reconciliation import BrokerFillFactV1
from ibmd.public_contracts.decision import (
    DesiredTargetSide,
    StrategyCommandKind,
)
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
from ibmd.public_contracts.reverse import ReverseFillAllocationV1


class ReverseFinalizationError(ValueError):
    pass


@dataclass(frozen=True)
class ReverseFinalizationPolicyV1:
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
                raise ReverseFinalizationError(f"{field_name} is required")
            object.__setattr__(self, field_name, value)
        version = int(self.strategy_version)
        if version <= 0:
            raise ReverseFinalizationError(
                "strategy_version must be positive"
            )
        object.__setattr__(self, "strategy_version", version)
        max_age = float(self.position_max_age_seconds)
        if not math.isfinite(max_age) or max_age <= 0.0:
            raise ReverseFinalizationError(
                "position_max_age_seconds must be finite and positive"
            )
        object.__setattr__(self, "position_max_age_seconds", max_age)
        if not isinstance(self.protective_policy, PositionEpisodePolicyV1):
            raise ReverseFinalizationError(
                "protective_policy must be PositionEpisodePolicyV1"
            )


@dataclass(frozen=True)
class ReversePositionFinalizationV1:
    closed_episode: PositionEpisodeV1
    closed_protection: ProtectionStateV1
    new_plan: PositionEpisodeProtectionPlan
    allocations: tuple[ReverseFillAllocationV1, ...]
    closing_completed_at_utc: str
    opening_started_at_utc: str
    commission_complete: bool

    def __post_init__(self) -> None:
        if self.closed_episode.status != PositionEpisodeStatus.CLOSED:
            raise ReverseFinalizationError(
                "reverse finalization requires a CLOSED source episode"
            )
        if self.closed_protection.status != ProtectionSetStatus.CLOSED:
            raise ReverseFinalizationError(
                "reverse finalization requires CLOSED source protection"
            )
        if (
            self.new_plan.episode.position_episode_id
            == self.closed_episode.position_episode_id
        ):
            raise ReverseFinalizationError(
                "reverse finalization requires a new position episode"
            )
        if not self.allocations:
            raise ReverseFinalizationError(
                "reverse finalization requires fill allocations"
            )
        if not isinstance(self.commission_complete, bool):
            raise ReverseFinalizationError(
                "commission_complete must be boolean"
            )
        object.__setattr__(
            self,
            "closing_completed_at_utc",
            format_utc(parse_utc(self.closing_completed_at_utc)),
        )
        object.__setattr__(
            self,
            "opening_started_at_utc",
            format_utc(parse_utc(self.opening_started_at_utc)),
        )


def _stable_id(kind: str, payload: dict[str, object]) -> str:
    digest = hashlib.sha256(
        canonical_json_text(payload).encode("utf-8")
    ).hexdigest()[:32]
    return f"{kind}_{digest}"


def _tick_price(value: float, tick: float) -> float:
    tick_decimal = Decimal(str(tick))
    raw = Decimal(str(value))
    ticks = (raw / tick_decimal).quantize(
        Decimal("1"),
        rounding=ROUND_HALF_UP,
    )
    return float(ticks * tick_decimal)


def _validate_scope(
    *,
    operation: BrokerOperationSnapshot,
    command: ExecutionCommandStateV1,
    old_episode: PositionEpisodeV1,
    old_protection: ProtectionStateV1,
    old_position: StrategyPositionV1,
    policy: ReverseFinalizationPolicyV1,
) -> None:
    expected = (
        policy.account_id,
        policy.strategy_id,
        policy.strategy_version,
        policy.deployment_id,
        policy.instrument_id,
    )
    operation_scope = (
        operation.operation.account_id,
        operation.operation.strategy_id,
        operation.operation.strategy_version,
        operation.operation.deployment_id,
        operation.operation.instrument_id,
    )
    command_scope = (
        policy.account_id,
        command.strategy_id,
        command.strategy_version,
        command.deployment_id,
        command.instrument_id,
    )
    episode_scope = (
        old_episode.account_id,
        old_episode.strategy_id,
        old_episode.strategy_version,
        old_episode.deployment_id,
        old_episode.instrument_id,
    )
    protection_scope = (
        old_protection.account_id,
        old_protection.strategy_id,
        old_protection.strategy_version,
        old_protection.deployment_id,
        old_protection.instrument_id,
    )
    position_scope = (
        old_position.account_id,
        old_position.strategy_id,
        old_episode.strategy_version,
        old_position.deployment_id,
        old_position.instrument_id,
    )
    if any(
        value != expected
        for value in (
            operation_scope,
            command_scope,
            episode_scope,
            protection_scope,
            position_scope,
        )
    ):
        raise ReverseFinalizationError(
            "operation/command/episode/protection/position scope mismatch"
        )
    if operation.operation.command_id != command.command_id:
        raise ReverseFinalizationError(
            "reverse operation belongs to another command"
        )
    if old_protection.position_episode_id != old_episode.position_episode_id:
        raise ReverseFinalizationError(
            "source protection belongs to another episode"
        )
    if old_position.position_episode_id != old_episode.position_episode_id:
        raise ReverseFinalizationError(
            "source execution position belongs to another episode"
        )


def _validate_material_state(
    *,
    operation: BrokerOperationSnapshot,
    command: ExecutionCommandStateV1,
    old_episode: PositionEpisodeV1,
    old_protection: ProtectionStateV1,
    old_position: StrategyPositionV1,
) -> None:
    if (
        operation.operation.state != BrokerOperationState.SUCCEEDED
        or operation.attempt.state != BrokerAttemptState.FILLED
        or operation.operation.remaining_qty != 0
        or operation.attempt.remaining_qty != 0
    ):
        raise ReverseFinalizationError(
            "reverse finalization requires a completely filled SUCCEEDED operation"
        )
    if (
        command.state != ExecutionCommandState.ADMITTED
        or command.command_kind != StrategyCommandKind.REVERSE
    ):
        raise ReverseFinalizationError(
            "reverse finalization requires an ADMITTED REVERSE command"
        )
    if old_episode.status != PositionEpisodeStatus.OPEN:
        raise ReverseFinalizationError(
            "source position episode is not OPEN"
        )
    if old_position.projection_status != StrategyPositionStatus.OPEN:
        raise ReverseFinalizationError(
            "source execution position is not OPEN"
        )
    if (
        old_position.side != old_episode.side
        or old_position.quantity != old_episode.quantity
    ):
        raise ReverseFinalizationError(
            "source execution position differs from position episode"
        )
    if command.desired_target_side.value == old_episode.side.value:
        raise ReverseFinalizationError(
            "reverse target side must be opposite the source episode"
        )
    expected_broker_side = (
        BrokerOrderSide.SELL
        if old_episode.side == StrategyPositionSide.LONG
        else BrokerOrderSide.BUY
    )
    if (
        operation.operation.side != expected_broker_side
        or operation.attempt.side != expected_broker_side
    ):
        raise ReverseFinalizationError(
            "reverse operation side does not close the source episode"
        )
    expected_quantity = old_episode.quantity + command.desired_target_quantity
    if (
        operation.operation.requested_qty != expected_quantity
        or operation.attempt.requested_qty != expected_quantity
    ):
        raise ReverseFinalizationError(
            "reverse operation quantity does not equal close + new target quantity"
        )
    if (
        operation.operation.con_id != old_episode.con_id
        or operation.operation.local_symbol != old_episode.local_symbol
    ):
        raise ReverseFinalizationError(
            "reverse operation route differs from source episode contract"
        )
    unsafe_states = {
        ProtectiveOrderState.SUBMITTING,
        ProtectiveOrderState.LIVE,
        ProtectiveOrderState.FILLED,
        ProtectiveOrderState.CANCEL_REQUESTED,
        ProtectiveOrderState.UNKNOWN_OUTCOME,
    }
    unsafe = [
        item for item in old_protection.orders if item.state in unsafe_states
    ]
    if unsafe:
        raise ReverseFinalizationError(
            "source protective orders remain exposed before reverse finalization: "
            + ",".join(
                f"{item.kind.value}={item.state.value}" for item in unsafe
            )
        )


def _allocation_id(
    *,
    operation_id: str,
    exec_id: str,
    sequence_no: int,
) -> str:
    return _stable_id(
        "reverse_allocation",
        {
            "operation_id": operation_id,
            "exec_id": exec_id,
            "sequence_no": sequence_no,
        },
    )


def _new_episode_id(
    *,
    operation: BrokerOperationSnapshot,
    policy: ReverseFinalizationPolicyV1,
) -> str:
    return _stable_id(
        "position_episode",
        {
            "source_operation_id": operation.operation.operation_id,
            "account_id": policy.account_id,
            "strategy_id": policy.strategy_id,
            "strategy_version": policy.strategy_version,
            "deployment_id": policy.deployment_id,
            "instrument_id": policy.instrument_id,
        },
    )


def allocate_reverse_fills(
    *,
    operation: BrokerOperationSnapshot,
    command: ExecutionCommandStateV1,
    old_episode: PositionEpisodeV1,
    fills: tuple[BrokerFillFactV1, ...],
    opening_episode_id: str,
) -> tuple[
    tuple[ReverseFillAllocationV1, ...],
    str,
    str,
    float,
    tuple[str, ...],
]:
    if not fills:
        raise ReverseFinalizationError(
            "reverse finalization requires immutable broker fills"
        )
    seen: set[str] = set()
    ordered = sorted(
        fills,
        key=lambda item: (
            item.cumulative_qty,
            parse_utc(item.executed_at_utc),
            item.exec_id,
        ),
    )
    expected_cumulative = 0
    remaining_close = old_episode.quantity
    opened_quantity = 0
    opening_value = 0.0
    close_completed_at = None
    opening_started_at = None
    opening_exec_ids: list[str] = []
    allocations: list[ReverseFillAllocationV1] = []
    for sequence_no, fill in enumerate(ordered, start=1):
        if fill.exec_id in seen:
            raise ReverseFinalizationError(
                f"duplicate reverse fill execId: {fill.exec_id}"
            )
        seen.add(fill.exec_id)
        if (
            fill.account_id != operation.operation.account_id
            or fill.order_ref != operation.attempt.order_ref
            or fill.broker_order_id != operation.attempt.broker_order_id
            or fill.con_id != operation.operation.con_id
            or fill.local_symbol != operation.operation.local_symbol
            or fill.side != operation.operation.side
        ):
            raise ReverseFinalizationError(
                f"fill does not belong to reverse operation: {fill.exec_id}"
            )
        expected_cumulative += fill.shares
        if fill.cumulative_qty != expected_cumulative:
            raise ReverseFinalizationError(
                "reverse fills have a non-contiguous cumulative quantity: "
                f"exec_id={fill.exec_id}, expected={expected_cumulative}, "
                f"actual={fill.cumulative_qty}"
            )
        close_quantity = min(fill.shares, remaining_close)
        open_quantity = fill.shares - close_quantity
        remaining_close -= close_quantity
        if close_quantity > 0 and remaining_close == 0:
            close_completed_at = fill.executed_at_utc
        if open_quantity > 0:
            if opening_started_at is None:
                opening_started_at = fill.executed_at_utc
            opened_quantity += open_quantity
            opening_value += fill.price * open_quantity
            opening_exec_ids.append(fill.exec_id)
        allocations.append(
            ReverseFillAllocationV1(
                reverse_allocation_id=_allocation_id(
                    operation_id=operation.operation.operation_id,
                    exec_id=fill.exec_id,
                    sequence_no=sequence_no,
                ),
                source_operation_id=operation.operation.operation_id,
                source_attempt_id=operation.attempt.attempt_id,
                exec_id=fill.exec_id,
                sequence_no=sequence_no,
                closing_position_episode_id=old_episode.position_episode_id,
                opening_position_episode_id=opening_episode_id,
                side=fill.side,
                close_quantity=close_quantity,
                open_quantity=open_quantity,
                price=fill.price,
                executed_at_utc=fill.executed_at_utc,
                commission_complete=fill.commission_complete,
            )
        )
    if expected_cumulative != operation.operation.requested_qty:
        raise ReverseFinalizationError(
            "reverse fill quantity does not complete broker operation"
        )
    if remaining_close != 0 or close_completed_at is None:
        raise ReverseFinalizationError(
            "reverse fills do not fully close the source episode"
        )
    if (
        opened_quantity != command.desired_target_quantity
        or opening_started_at is None
        or not opening_exec_ids
    ):
        raise ReverseFinalizationError(
            "reverse fills do not create the requested target position"
        )
    return (
        tuple(allocations),
        format_utc(parse_utc(close_completed_at)),
        format_utc(parse_utc(opening_started_at)),
        opening_value / opened_quantity,
        tuple(opening_exec_ids),
    )


def _validate_target_position(
    *,
    snapshot: BrokerPositionSnapshotV1,
    operation: BrokerOperationSnapshot,
    command: ExecutionCommandStateV1,
    policy: ReverseFinalizationPolicyV1,
    observed_at_utc: str,
):
    if snapshot.status != BrokerPositionSnapshotStatus.COMPLETE:
        raise ReverseFinalizationError(
            "broker position snapshot is not COMPLETE"
        )
    if snapshot.account_id != policy.account_id:
        raise ReverseFinalizationError(
            "broker position snapshot account mismatch"
        )
    freshness = snapshot.freshness(
        observed_at_utc=observed_at_utc,
        max_age_seconds=policy.position_max_age_seconds,
    )
    if not freshness.is_fresh:
        raise ReverseFinalizationError(
            "broker position snapshot is stale for reverse finalization: "
            f"age={freshness.age_seconds:.6f}s"
        )
    relevant = [
        row
        for row in snapshot.rows
        if row.symbol.upper() == policy.instrument_id.upper()
        or str(row.local_symbol or "").upper().startswith(
            policy.instrument_id.upper()
        )
    ]
    nonzero = [
        row for row in relevant if abs(float(row.signed_quantity)) > 1e-9
    ]
    exact = [
        row
        for row in nonzero
        if row.con_id == operation.operation.con_id
        and str(row.local_symbol or "") == operation.operation.local_symbol
    ]
    expected_signed = (
        command.desired_target_quantity
        if command.desired_target_side == DesiredTargetSide.LONG
        else -command.desired_target_quantity
    )
    if (
        len(nonzero) != 1
        or len(exact) != 1
        or exact[0].sec_type != "FUT"
        or abs(float(exact[0].signed_quantity) - expected_signed) > 1e-9
    ):
        raise ReverseFinalizationError(
            "broker position does not prove the reverse target: "
            f"expected={expected_signed}, rows="
            f"{[(row.con_id, row.local_symbol, row.signed_quantity) for row in nonzero]}"
        )
    return freshness


def _new_protection_plan(
    *,
    operation: BrokerOperationSnapshot,
    command: ExecutionCommandStateV1,
    broker_snapshot: BrokerPositionSnapshotV1,
    current_readiness: ExecutionReadinessV1,
    policy: ReverseFinalizationPolicyV1,
    episode_id: str,
    entry_price: float,
    opening_exec_ids: tuple[str, ...],
    opened_at_utc: str,
    freshness_seconds: float,
    observed_at_utc: str,
) -> PositionEpisodeProtectionPlan:
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
    position_side = StrategyPositionSide(
        command.desired_target_side.value
    )
    exit_side = (
        BrokerOrderSide.SELL
        if position_side == StrategyPositionSide.LONG
        else BrokerOrderSide.BUY
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
    tp_price = _tick_price(tp_raw, protective.price_tick)
    if stop_price <= 0.0 or tp_price <= 0.0:
        raise ReverseFinalizationError(
            "calculated reverse protective price is not positive"
        )
    oca_group = (
        f"IBMD_OCA_{protection_set_id.rsplit('_', 1)[-1]}"
        if protective.take_profit_enabled
        else None
    )
    stop = ProtectiveOrderV1(
        protective_order_id=stop_id,
        protection_set_id=protection_set_id,
        position_episode_id=episode_id,
        kind=ProtectiveOrderKind.STOP_LOSS,
        state=ProtectiveOrderState.PLANNED,
        planned_sequence=1,
        order_ref=f"IBMD:{protection_set_id}:SL",
        side=exit_side,
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
        created_at_utc=opened_at_utc,
        updated_at_utc=opened_at_utc,
        terminal_at_utc=None,
        last_broker_proof_at_utc=None,
        failure_reason=None,
    )
    orders = [stop]
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
                side=exit_side,
                order_type=ProtectiveOrderType.LIMIT,
                quantity=command.desired_target_quantity,
                con_id=operation.operation.con_id,
                local_symbol=operation.operation.local_symbol,
                stop_price=None,
                limit_price=tp_price,
                time_in_force=protective.time_in_force,
                outside_rth=protective.take_profit_outside_rth,
                oca_group=oca_group,
                filled_qty=0,
                remaining_qty=command.desired_target_quantity,
                broker_order_id=None,
                broker_perm_id=None,
                broker_status=None,
                broker_terminal_proven=False,
                created_at_utc=opened_at_utc,
                updated_at_utc=opened_at_utc,
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
        source_exec_ids=opening_exec_ids,
        side=position_side,
        quantity=command.desired_target_quantity,
        con_id=operation.operation.con_id,
        local_symbol=operation.operation.local_symbol,
        entry_average_price=entry_price,
        broker_snapshot_id=broker_snapshot.snapshot_id,
        opened_at_utc=opened_at_utc,
        status=PositionEpisodeStatus.OPEN,
        strategy_policy_hash=policy.strategy_policy_hash,
        protective_policy_hash=protective.content_hash,
        protective_policy=protective,
    )
    position = StrategyPositionV1(
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
        updated_at_utc=observed_at_utc,
        source_freshness_seconds=freshness_seconds,
    )
    preserved_reasons = tuple(
        item
        for item in current_readiness.blocking_reasons
        if not item.startswith("protection:")
        and not item.startswith("reverse_handoff:")
    )
    readiness = ExecutionReadinessV1(
        account_id=current_readiness.account_id,
        strategy_id=current_readiness.strategy_id,
        deployment_id=current_readiness.deployment_id,
        instrument_id=current_readiness.instrument_id,
        status=ExecutionReadinessStatus.BLOCKED,
        command_intake_enabled=False,
        broker_actions_enabled=current_readiness.broker_actions_enabled,
        reconciliation_complete=current_readiness.reconciliation_complete,
        clock_healthy=current_readiness.clock_healthy,
        blocking_reasons=preserved_reasons
        + ("protection:stop_not_proven",),
        updated_at_utc=observed_at_utc,
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
        created_at_utc=opened_at_utc,
        updated_at_utc=opened_at_utc,
        terminal_at_utc=None,
        blocking_reason="stop_not_submitted",
    )
    return PositionEpisodeProtectionPlan(
        episode=episode,
        strategy_position=position,
        execution_readiness=readiness,
        protection=protection,
    )


def finalize_reverse_position(
    *,
    operation: BrokerOperationSnapshot,
    command: ExecutionCommandStateV1,
    fills: tuple[BrokerFillFactV1, ...],
    broker_snapshot: BrokerPositionSnapshotV1,
    old_episode: PositionEpisodeV1,
    old_protection: ProtectionStateV1,
    old_position: StrategyPositionV1,
    current_readiness: ExecutionReadinessV1,
    policy: ReverseFinalizationPolicyV1,
    observed_at_utc: str,
) -> ReversePositionFinalizationV1:
    if not isinstance(operation, BrokerOperationSnapshot):
        raise ReverseFinalizationError(
            "operation must be BrokerOperationSnapshot"
        )
    if not isinstance(command, ExecutionCommandStateV1):
        raise ReverseFinalizationError(
            "command must be ExecutionCommandStateV1"
        )
    if not isinstance(current_readiness, ExecutionReadinessV1):
        raise ReverseFinalizationError(
            "current_readiness must be ExecutionReadinessV1"
        )
    observed = format_utc(parse_utc(observed_at_utc))
    _validate_scope(
        operation=operation,
        command=command,
        old_episode=old_episode,
        old_protection=old_protection,
        old_position=old_position,
        policy=policy,
    )
    _validate_material_state(
        operation=operation,
        command=command,
        old_episode=old_episode,
        old_protection=old_protection,
        old_position=old_position,
    )
    new_episode_id = _new_episode_id(
        operation=operation,
        policy=policy,
    )
    (
        allocations,
        close_completed_at,
        opening_started_at,
        opening_entry_price,
        opening_exec_ids,
    ) = allocate_reverse_fills(
        operation=operation,
        command=command,
        old_episode=old_episode,
        fills=fills,
        opening_episode_id=new_episode_id,
    )
    freshness = _validate_target_position(
        snapshot=broker_snapshot,
        operation=operation,
        command=command,
        policy=policy,
        observed_at_utc=observed,
    )
    closed_episode = replace(
        old_episode,
        status=PositionEpisodeStatus.CLOSED,
        closed_at_utc=close_completed_at,
        closing_operation_id=operation.operation.operation_id,
    )
    closed_protection = ProtectionStateV1(
        protection_set_id=old_protection.protection_set_id,
        position_episode_id=old_protection.position_episode_id,
        account_id=old_protection.account_id,
        strategy_id=old_protection.strategy_id,
        strategy_version=old_protection.strategy_version,
        deployment_id=old_protection.deployment_id,
        instrument_id=old_protection.instrument_id,
        status=ProtectionSetStatus.CLOSED,
        orders=old_protection.orders,
        created_at_utc=old_protection.created_at_utc,
        updated_at_utc=close_completed_at,
        terminal_at_utc=close_completed_at,
        blocking_reason=None,
    )
    plan = _new_protection_plan(
        operation=operation,
        command=command,
        broker_snapshot=broker_snapshot,
        current_readiness=current_readiness,
        policy=policy,
        episode_id=new_episode_id,
        entry_price=opening_entry_price,
        opening_exec_ids=opening_exec_ids,
        opened_at_utc=opening_started_at,
        freshness_seconds=freshness.age_seconds,
        observed_at_utc=observed,
    )
    return ReversePositionFinalizationV1(
        closed_episode=closed_episode,
        closed_protection=closed_protection,
        new_plan=plan,
        allocations=allocations,
        closing_completed_at_utc=close_completed_at,
        opening_started_at_utc=opening_started_at,
        commission_complete=all(
            item.commission_complete for item in allocations
        ),
    )
