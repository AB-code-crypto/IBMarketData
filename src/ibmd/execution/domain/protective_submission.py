from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Iterable

from ibmd.foundation.time import format_utc, parse_utc
from ibmd.public_contracts.broker_execution import (
    BrokerObservationOutcome,
    BrokerOrderObservationV1,
)
from ibmd.public_contracts.broker_reconciliation import (
    BrokerFillFactV1,
    BrokerOrderFactV1,
    BrokerOrderSource,
    BrokerReconciliationSnapshotV1,
)
from ibmd.public_contracts.execution import (
    ExecutionReadinessStatus,
    ExecutionReadinessV1,
)
from ibmd.public_contracts.protection import (
    PositionEpisodeV1,
    ProtectionSetStatus,
    ProtectionStateV1,
    ProtectiveOrderKind,
    ProtectiveOrderState,
    ProtectiveOrderType,
    ProtectiveOrderV1,
)


class ProtectiveSubmissionDomainError(ValueError):
    pass


@dataclass(frozen=True)
class ProtectiveOrderReconciliationResult:
    observation: BrokerOrderObservationV1
    fills: tuple[BrokerFillFactV1, ...]
    source_session_id: str
    captured_at_utc: str

    def __post_init__(self) -> None:
        if not isinstance(self.observation, BrokerOrderObservationV1):
            raise ProtectiveSubmissionDomainError(
                "observation must be BrokerOrderObservationV1"
            )
        if any(not isinstance(item, BrokerFillFactV1) for item in self.fills):
            raise ProtectiveSubmissionDomainError(
                "fills must contain BrokerFillFactV1 values"
            )
        exec_ids = [item.exec_id for item in self.fills]
        if len(exec_ids) != len(set(exec_ids)):
            raise ProtectiveSubmissionDomainError(
                "protective reconciliation fill execIds must be unique"
            )

    @property
    def commission_complete(self) -> bool:
        return all(item.commission_complete for item in self.fills)


_ACTIVE_STATUSES = {
    "APIPENDING",
    "PENDINGCANCEL",
    "PENDINGSUBMIT",
    "PRESUBMITTED",
    "SUBMITTED",
}
_FILLED_STATUSES = {"FILLED"}
_CANCELLED_STATUSES = {"APICANCELLED", "CANCELLED", "CANCELED"}
_REJECTED_STATUSES = {"REJECTED"}
_FAILED_STATUSES = {"FAILED"}


def _status_key(value: str | None) -> str:
    return "".join(
        character
        for character in str(value or "").upper()
        if character.isalnum()
    )


def _order_for_kind(
    protection: ProtectionStateV1,
    kind: ProtectiveOrderKind,
) -> ProtectiveOrderV1:
    values = [item for item in protection.orders if item.kind == kind]
    if len(values) != 1:
        raise ProtectiveSubmissionDomainError(
            f"protection does not contain exactly one {kind.value} order"
        )
    return values[0]


def mark_protective_order_submitting(
    protection: ProtectionStateV1,
    *,
    kind: ProtectiveOrderKind,
    broker_order_id: int,
    observed_at_utc: str,
) -> ProtectionStateV1:
    if not isinstance(protection, ProtectionStateV1):
        raise ProtectiveSubmissionDomainError(
            "protection must be ProtectionStateV1"
        )
    if not isinstance(kind, ProtectiveOrderKind):
        raise ProtectiveSubmissionDomainError(
            "kind must be ProtectiveOrderKind"
        )
    try:
        order_id = int(broker_order_id)
    except (TypeError, ValueError) as exc:
        raise ProtectiveSubmissionDomainError(
            "broker_order_id must be an integer"
        ) from exc
    if order_id <= 0:
        raise ProtectiveSubmissionDomainError(
            "broker_order_id must be positive"
        )
    observed = format_utc(parse_utc(observed_at_utc))
    order = _order_for_kind(protection, kind)
    stop = protection.stop_order
    if order.state != ProtectiveOrderState.PLANNED:
        raise ProtectiveSubmissionDomainError(
            f"{kind.value} is not PLANNED: {order.state.value}"
        )
    if kind == ProtectiveOrderKind.STOP_LOSS:
        if protection.status != ProtectionSetStatus.PLANNED:
            raise ProtectiveSubmissionDomainError(
                "STOP submission requires PLANNED protection"
            )
        next_status = ProtectionSetStatus.STOP_SUBMITTING
        reason = "stop_submission_in_progress"
    else:
        if stop.state != ProtectiveOrderState.LIVE:
            raise ProtectiveSubmissionDomainError(
                "TAKE_PROFIT cannot submit before STOP is proven LIVE"
            )
        if protection.status not in {
            ProtectionSetStatus.STOP_LIVE,
            ProtectionSetStatus.PROTECTED,
        }:
            raise ProtectiveSubmissionDomainError(
                "TAKE_PROFIT submission requires STOP_LIVE/PROTECTED state"
            )
        next_status = ProtectionSetStatus.STOP_LIVE
        reason = "take_profit_submission_in_progress_stop_live"
    submitting = replace(
        order,
        state=ProtectiveOrderState.SUBMITTING,
        broker_order_id=order_id,
        broker_perm_id=None,
        broker_status=None,
        broker_terminal_proven=False,
        updated_at_utc=observed,
        terminal_at_utc=None,
        last_broker_proof_at_utc=None,
        failure_reason=None,
    )
    orders = tuple(
        submitting if item.kind == kind else item
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
        status=next_status,
        orders=orders,
        created_at_utc=protection.created_at_utc,
        updated_at_utc=observed,
        terminal_at_utc=None,
        blocking_reason=reason,
    )


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
        status = ProtectionSetStatus.PROTECTED
        blocking_reason = "take_profit_unknown_stop_live"
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
    if stop_safe:
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


def _same_order_identity(
    left: BrokerOrderFactV1,
    right: BrokerOrderFactV1,
) -> bool:
    if left.broker_perm_id is not None and right.broker_perm_id is not None:
        return left.broker_perm_id == right.broker_perm_id
    if left.broker_order_id is not None and right.broker_order_id is not None:
        return left.broker_order_id == right.broker_order_id
    return (
        left.order_ref is not None
        and left.order_ref == right.order_ref
    )


def _same_fill_identity(
    fill: BrokerFillFactV1,
    order: BrokerOrderFactV1,
) -> bool:
    if fill.broker_perm_id is not None and order.broker_perm_id is not None:
        return fill.broker_perm_id == order.broker_perm_id
    return fill.broker_order_id == order.broker_order_id


def _matching_orders(
    snapshot: BrokerReconciliationSnapshotV1,
    *,
    order_ref: str,
) -> tuple[BrokerOrderFactV1, ...]:
    values = tuple(
        item
        for item in (*snapshot.open_orders, *snapshot.completed_orders)
        if item.order_ref == order_ref
    )
    if not values:
        return ()
    groups: list[list[BrokerOrderFactV1]] = []
    for item in values:
        for group in groups:
            if _same_order_identity(group[0], item):
                group.append(item)
                break
        else:
            groups.append([item])
    if len(groups) > 1:
        return values
    group = groups[0]
    immutable_routes = {
        (
            item.account_id,
            item.con_id,
            item.local_symbol,
            item.side,
            item.order_type,
            item.requested_qty,
        )
        for item in group
    }
    if len(immutable_routes) != 1:
        return values
    completed = [
        item for item in group if item.source == BrokerOrderSource.COMPLETED
    ]
    if completed:
        completed.sort(
            key=lambda item: (
                item.filled_qty,
                -item.remaining_qty,
                item.status,
            ),
            reverse=True,
        )
        return (completed[0],)
    return (group[0],)


def _matching_fills(
    snapshot: BrokerReconciliationSnapshotV1,
    *,
    order_ref: str,
) -> tuple[BrokerFillFactV1, ...]:
    return tuple(
        sorted(
            (item for item in snapshot.fills if item.order_ref == order_ref),
            key=lambda item: (item.executed_at_utc, item.exec_id),
        )
    )


def _fill_groups(
    fills: Iterable[BrokerFillFactV1],
) -> tuple[tuple[BrokerFillFactV1, ...], ...]:
    groups: list[list[BrokerFillFactV1]] = []
    for item in fills:
        for group in groups:
            first = group[0]
            same = (
                item.broker_perm_id is not None
                and first.broker_perm_id is not None
                and item.broker_perm_id == first.broker_perm_id
            ) or (
                (item.broker_perm_id is None or first.broker_perm_id is None)
                and item.broker_order_id == first.broker_order_id
            )
            if same:
                group.append(item)
                break
        else:
            groups.append([item])
    return tuple(tuple(group) for group in groups)


def _uncertain(
    *,
    order_ref: str,
    captured_at_utc: str,
    outcome: BrokerObservationOutcome,
    detail: str,
) -> BrokerOrderObservationV1:
    return BrokerOrderObservationV1(
        order_ref=order_ref,
        outcome=outcome,
        observed_at_utc=captured_at_utc,
        broker_order_id=None,
        broker_perm_id=None,
        broker_status=None,
        requested_qty=None,
        filled_qty=None,
        remaining_qty=None,
        detail=detail,
    )


def _expected_order_types(order: ProtectiveOrderV1) -> set[str]:
    if order.order_type == ProtectiveOrderType.STOP:
        return {"STP", "STOP"}
    return {"LMT", "LIMIT"}


def _route_errors(
    *,
    order_fact: BrokerOrderFactV1,
    expected: ProtectiveOrderV1,
    episode: PositionEpisodeV1,
) -> tuple[str, ...]:
    errors: list[str] = []
    if order_fact.account_id != episode.account_id:
        errors.append("account")
    if order_fact.con_id != expected.con_id:
        errors.append("con_id")
    if order_fact.local_symbol != expected.local_symbol:
        errors.append("local_symbol")
    if order_fact.side != expected.side:
        errors.append("side")
    if order_fact.order_type.upper() not in _expected_order_types(expected):
        errors.append("order_type")
    if order_fact.requested_qty != expected.quantity:
        errors.append("requested_qty")
    if (
        expected.broker_order_id is not None
        and order_fact.broker_order_id is not None
        and order_fact.broker_order_id != expected.broker_order_id
    ):
        errors.append("broker_order_id")
    if (
        expected.broker_perm_id is not None
        and order_fact.broker_perm_id is not None
        and order_fact.broker_perm_id != expected.broker_perm_id
    ):
        errors.append("broker_perm_id")
    return tuple(errors)


def _fill_route_errors(
    *,
    fill: BrokerFillFactV1,
    expected: ProtectiveOrderV1,
    episode: PositionEpisodeV1,
) -> tuple[str, ...]:
    errors: list[str] = []
    if fill.account_id != episode.account_id:
        errors.append("account")
    if fill.con_id != expected.con_id:
        errors.append("con_id")
    if fill.local_symbol != expected.local_symbol:
        errors.append("local_symbol")
    if fill.side != expected.side:
        errors.append("side")
    if (
        expected.broker_order_id is not None
        and fill.broker_order_id != expected.broker_order_id
    ):
        errors.append("broker_order_id")
    if (
        expected.broker_perm_id is not None
        and fill.broker_perm_id is not None
        and fill.broker_perm_id != expected.broker_perm_id
    ):
        errors.append("broker_perm_id")
    return tuple(errors)


def _order_outcome(order: BrokerOrderFactV1) -> BrokerObservationOutcome | None:
    if order.filled_qty == order.requested_qty and order.remaining_qty == 0:
        return BrokerObservationOutcome.FILLED
    status = _status_key(order.status)
    completed = _status_key(order.completed_status)
    keys = {status, completed} - {""}
    if keys & _FILLED_STATUSES:
        return BrokerObservationOutcome.FILLED
    if keys & _CANCELLED_STATUSES:
        return BrokerObservationOutcome.CANCELLED
    if keys & _REJECTED_STATUSES:
        return BrokerObservationOutcome.REJECTED
    if keys & _FAILED_STATUSES:
        return BrokerObservationOutcome.FAILED
    if status == "INACTIVE":
        return (
            BrokerObservationOutcome.REJECTED
            if order.filled_qty == 0
            else BrokerObservationOutcome.FAILED
        )
    if status in _ACTIVE_STATUSES:
        return BrokerObservationOutcome.LIVE
    return None


def _order_detail(order: BrokerOrderFactV1) -> str | None:
    values = [
        value
        for value in (order.completed_status, order.warning_text)
        if value
    ]
    return "; ".join(values) or None


def _broker_status(order: BrokerOrderFactV1) -> str:
    if order.completed_status:
        return f"{order.status}/{order.completed_status}"
    return order.status


def reconcile_protective_order_snapshot(
    *,
    broker_snapshot: BrokerReconciliationSnapshotV1,
    episode: PositionEpisodeV1,
    protection: ProtectionStateV1,
    kind: ProtectiveOrderKind,
) -> ProtectiveOrderReconciliationResult:
    if not isinstance(broker_snapshot, BrokerReconciliationSnapshotV1):
        raise ProtectiveSubmissionDomainError(
            "broker_snapshot must be BrokerReconciliationSnapshotV1"
        )
    if not broker_snapshot.requests_complete:
        raise ProtectiveSubmissionDomainError(
            "broker reconciliation snapshot is incomplete"
        )
    if broker_snapshot.account_id != episode.account_id:
        raise ProtectiveSubmissionDomainError(
            "broker snapshot account differs from position episode"
        )
    if protection.position_episode_id != episode.position_episode_id:
        raise ProtectiveSubmissionDomainError(
            "protection belongs to another position episode"
        )
    order = _order_for_kind(protection, kind)
    if order.state == ProtectiveOrderState.PLANNED:
        raise ProtectiveSubmissionDomainError(
            "PLANNED protective order has no broker exposure to reconcile"
        )
    order_ref = order.order_ref
    all_orders = tuple(
        item
        for item in (*broker_snapshot.open_orders, *broker_snapshot.completed_orders)
        if item.order_ref == order_ref
    )
    orders = _matching_orders(broker_snapshot, order_ref=order_ref)
    fills = _matching_fills(broker_snapshot, order_ref=order_ref)

    fill_errors = sorted(
        {
            error
            for fill in fills
            for error in _fill_route_errors(
                fill=fill,
                expected=order,
                episode=episode,
            )
        }
    )
    if fill_errors:
        observation = _uncertain(
            order_ref=order_ref,
            captured_at_utc=broker_snapshot.captured_at_utc,
            outcome=BrokerObservationOutcome.AMBIGUOUS,
            detail=(
                "matching protective execution route differs from plan: "
                + ",".join(fill_errors)
            ),
        )
        return ProtectiveOrderReconciliationResult(
            observation=observation,
            fills=fills,
            source_session_id=broker_snapshot.source_session_id,
            captured_at_utc=broker_snapshot.captured_at_utc,
        )

    if len(all_orders) > 1 and len(orders) != 1:
        observation = _uncertain(
            order_ref=order_ref,
            captured_at_utc=broker_snapshot.captured_at_utc,
            outcome=BrokerObservationOutcome.AMBIGUOUS,
            detail=(
                "multiple broker order identities matched protective order_ref"
            ),
        )
        return ProtectiveOrderReconciliationResult(
            observation=observation,
            fills=fills,
            source_session_id=broker_snapshot.source_session_id,
            captured_at_utc=broker_snapshot.captured_at_utc,
        )

    if not orders:
        groups = _fill_groups(fills)
        if not groups:
            observation = _uncertain(
                order_ref=order_ref,
                captured_at_utc=broker_snapshot.captured_at_utc,
                outcome=BrokerObservationOutcome.NOT_FOUND,
                detail=(
                    "no open/completed order or execution matched the persisted "
                    "protective order_ref"
                ),
            )
        elif len(groups) != 1:
            observation = _uncertain(
                order_ref=order_ref,
                captured_at_utc=broker_snapshot.captured_at_utc,
                outcome=BrokerObservationOutcome.AMBIGUOUS,
                detail=(
                    "protective executions with one order_ref map to multiple "
                    "broker identities"
                ),
            )
        else:
            group = groups[0]
            filled_qty = sum(item.shares for item in group)
            if filled_qty == order.quantity:
                first = group[0]
                observation = BrokerOrderObservationV1(
                    order_ref=order_ref,
                    outcome=BrokerObservationOutcome.FILLED,
                    observed_at_utc=broker_snapshot.captured_at_utc,
                    broker_order_id=first.broker_order_id,
                    broker_perm_id=first.broker_perm_id,
                    broker_status="FILLED_FROM_EXECUTIONS",
                    requested_qty=order.quantity,
                    filled_qty=filled_qty,
                    remaining_qty=0,
                    detail=(
                        "full protective quantity proven by immutable executions"
                    ),
                )
            else:
                observation = _uncertain(
                    order_ref=order_ref,
                    captured_at_utc=broker_snapshot.captured_at_utc,
                    outcome=BrokerObservationOutcome.AMBIGUOUS,
                    detail=(
                        "partial protective executions exist without an exact "
                        "order row proving remaining state"
                    ),
                )
        return ProtectiveOrderReconciliationResult(
            observation=observation,
            fills=fills,
            source_session_id=broker_snapshot.source_session_id,
            captured_at_utc=broker_snapshot.captured_at_utc,
        )

    order_fact = orders[0]
    route_errors = _route_errors(
        order_fact=order_fact,
        expected=order,
        episode=episode,
    )
    if route_errors:
        observation = _uncertain(
            order_ref=order_ref,
            captured_at_utc=broker_snapshot.captured_at_utc,
            outcome=BrokerObservationOutcome.AMBIGUOUS,
            detail=(
                "broker protective route differs from persisted plan: "
                + ",".join(route_errors)
            ),
        )
        return ProtectiveOrderReconciliationResult(
            observation=observation,
            fills=fills,
            source_session_id=broker_snapshot.source_session_id,
            captured_at_utc=broker_snapshot.captured_at_utc,
        )

    order_fills = tuple(
        item for item in fills if _same_fill_identity(item, order_fact)
    )
    unrelated = tuple(item for item in fills if item not in order_fills)
    if unrelated:
        observation = _uncertain(
            order_ref=order_ref,
            captured_at_utc=broker_snapshot.captured_at_utc,
            outcome=BrokerObservationOutcome.AMBIGUOUS,
            detail=(
                "protective executions disagree with exact broker order identity"
            ),
        )
        return ProtectiveOrderReconciliationResult(
            observation=observation,
            fills=fills,
            source_session_id=broker_snapshot.source_session_id,
            captured_at_utc=broker_snapshot.captured_at_utc,
        )

    executed_qty = sum(item.shares for item in order_fills)
    if executed_qty > order_fact.filled_qty or executed_qty > order_fact.requested_qty:
        observation = _uncertain(
            order_ref=order_ref,
            captured_at_utc=broker_snapshot.captured_at_utc,
            outcome=BrokerObservationOutcome.AMBIGUOUS,
            detail=(
                "protective execution quantity exceeds broker order fill quantity"
            ),
        )
        return ProtectiveOrderReconciliationResult(
            observation=observation,
            fills=order_fills,
            source_session_id=broker_snapshot.source_session_id,
            captured_at_utc=broker_snapshot.captured_at_utc,
        )

    outcome = _order_outcome(order_fact)
    if outcome is None:
        observation = _uncertain(
            order_ref=order_ref,
            captured_at_utc=broker_snapshot.captured_at_utc,
            outcome=BrokerObservationOutcome.AMBIGUOUS,
            detail=(
                "unsupported or conflicting protective broker status: "
                f"status={order_fact.status!r}, "
                f"completed_status={order_fact.completed_status!r}"
            ),
        )
    else:
        observation = BrokerOrderObservationV1(
            order_ref=order_ref,
            outcome=outcome,
            observed_at_utc=broker_snapshot.captured_at_utc,
            broker_order_id=order_fact.broker_order_id,
            broker_perm_id=order_fact.broker_perm_id,
            broker_status=_broker_status(order_fact),
            requested_qty=order_fact.requested_qty,
            filled_qty=order_fact.filled_qty,
            remaining_qty=order_fact.remaining_qty,
            detail=_order_detail(order_fact),
        )
    return ProtectiveOrderReconciliationResult(
        observation=observation,
        fills=order_fills,
        source_session_id=broker_snapshot.source_session_id,
        captured_at_utc=broker_snapshot.captured_at_utc,
    )
