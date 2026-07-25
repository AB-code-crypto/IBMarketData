from __future__ import annotations

from dataclasses import dataclass, replace

from ibmd.execution.domain.protection import apply_protective_observation
from ibmd.execution.domain.protective_submission import (
    ProtectiveOrderReconciliationResult,
    reconcile_protective_order_snapshot,
)
from ibmd.public_contracts.broker_execution import BrokerObservationOutcome
from ibmd.public_contracts.broker_reconciliation import (
    BrokerReconciliationSnapshotV1,
)
from ibmd.public_contracts.protection import (
    PositionEpisodeV1,
    ProtectionSetStatus,
    ProtectionStateV1,
    ProtectiveOrderKind,
    ProtectiveOrderState,
)


class LiquidationExitError(ValueError):
    pass


@dataclass(frozen=True)
class LiquidationExitReconciliation:
    protection: ProtectionStateV1
    evidence: tuple[ProtectiveOrderReconciliationResult, ...]


def _order_by_kind(
    protection: ProtectionStateV1,
    kind: ProtectiveOrderKind,
):
    values = [item for item in protection.orders if item.kind == kind]
    if not values:
        return None
    if len(values) != 1:
        raise LiquidationExitError(
            f"protection has duplicate {kind.value} orders"
        )
    return values[0]


def _cancel_requested_live(
    protection: ProtectionStateV1,
    *,
    kind: ProtectiveOrderKind,
    result: ProtectiveOrderReconciliationResult,
) -> ProtectionStateV1:
    order = _order_by_kind(protection, kind)
    if order is None:
        raise LiquidationExitError("cancel-requested order disappeared")
    observation = result.observation
    if order.state != ProtectiveOrderState.CANCEL_REQUESTED:
        raise LiquidationExitError(
            "cancel-requested LIVE preservation requires CANCEL_REQUESTED state"
        )
    updated_order = replace(
        order,
        broker_order_id=observation.broker_order_id or order.broker_order_id,
        broker_perm_id=observation.broker_perm_id or order.broker_perm_id,
        broker_status=observation.broker_status,
        broker_terminal_proven=False,
        updated_at_utc=observation.observed_at_utc,
        terminal_at_utc=None,
        last_broker_proof_at_utc=observation.observed_at_utc,
        failure_reason="liquidation_cancel_outcome_not_terminal",
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
        updated_at_utc=observation.observed_at_utc,
        terminal_at_utc=None,
        blocking_reason=f"liquidation_cancel_pending:{kind.value}",
    )


def reconcile_liquidation_exits(
    *,
    episode: PositionEpisodeV1,
    protection: ProtectionStateV1,
    broker_snapshot: BrokerReconciliationSnapshotV1,
    position_open: bool,
) -> LiquidationExitReconciliation:
    working = protection
    evidence: list[ProtectiveOrderReconciliationResult] = []
    for kind in (
        ProtectiveOrderKind.STOP_LOSS,
        ProtectiveOrderKind.TAKE_PROFIT,
    ):
        order = _order_by_kind(working, kind)
        if order is None or order.state in {
            ProtectiveOrderState.PLANNED,
            ProtectiveOrderState.NOT_REQUIRED,
        }:
            continue
        result = reconcile_protective_order_snapshot(
            broker_snapshot=broker_snapshot,
            episode=episode,
            protection=working,
            kind=kind,
        )
        evidence.append(result)
        current = _order_by_kind(working, kind)
        if current is None:
            raise LiquidationExitError("protective order disappeared")
        if (
            current.state == ProtectiveOrderState.CANCEL_REQUESTED
            and result.observation.outcome == BrokerObservationOutcome.LIVE
        ):
            working = _cancel_requested_live(
                working,
                kind=kind,
                result=result,
            )
            continue
        working = apply_protective_observation(
            protection=working,
            kind=kind,
            observation=result.observation,
            position_open=position_open,
        )
    return LiquidationExitReconciliation(
        protection=working,
        evidence=tuple(evidence),
    )
