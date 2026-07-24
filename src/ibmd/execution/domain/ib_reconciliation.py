from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ibmd.execution.domain.broker_attempt import BrokerOperationSnapshot
from ibmd.public_contracts.broker_execution import (
    BrokerAttemptState,
    BrokerObservationOutcome,
    BrokerOrderObservationV1,
)
from ibmd.public_contracts.broker_reconciliation import (
    BrokerFillFactV1,
    BrokerOrderFactV1,
    BrokerOrderSource,
    BrokerReconciliationSnapshotV1,
)


class BrokerReconciliationDomainError(ValueError):
    pass


@dataclass(frozen=True)
class BrokerAttemptReconciliationResult:
    observation: BrokerOrderObservationV1
    fills: tuple[BrokerFillFactV1, ...]
    source_session_id: str
    captured_at_utc: str

    def __post_init__(self) -> None:
        if not isinstance(self.observation, BrokerOrderObservationV1):
            raise BrokerReconciliationDomainError(
                "observation must be BrokerOrderObservationV1"
            )
        if any(not isinstance(item, BrokerFillFactV1) for item in self.fills):
            raise BrokerReconciliationDomainError(
                "fills must contain BrokerFillFactV1 values"
            )
        exec_ids = [item.exec_id for item in self.fills]
        if len(exec_ids) != len(set(exec_ids)):
            raise BrokerReconciliationDomainError(
                "reconciliation result fill execIds must be unique"
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
    return "".join(character for character in str(value or "").upper() if character.isalnum())


def _uncertain(
    *,
    attempt_order_ref: str,
    captured_at_utc: str,
    outcome: BrokerObservationOutcome,
    detail: str,
) -> BrokerOrderObservationV1:
    if outcome not in {
        BrokerObservationOutcome.NOT_FOUND,
        BrokerObservationOutcome.AMBIGUOUS,
    }:
        raise BrokerReconciliationDomainError(
            "uncertain observation must be NOT_FOUND or AMBIGUOUS"
        )
    return BrokerOrderObservationV1(
        order_ref=attempt_order_ref,
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


def _same_order_identity(
    left: BrokerOrderFactV1,
    right: BrokerOrderFactV1,
) -> bool:
    if left.broker_perm_id is not None and right.broker_perm_id is not None:
        return left.broker_perm_id == right.broker_perm_id
    return left.broker_order_id == right.broker_order_id


def _same_fill_identity(
    fill: BrokerFillFactV1,
    order: BrokerOrderFactV1,
) -> bool:
    if fill.broker_perm_id is not None and order.broker_perm_id is not None:
        return fill.broker_perm_id == order.broker_perm_id
    return fill.broker_order_id == order.broker_order_id


def _route_errors(
    *,
    order: BrokerOrderFactV1,
    current: BrokerOperationSnapshot,
) -> tuple[str, ...]:
    attempt = current.attempt
    errors: list[str] = []
    if order.account_id != current.operation.account_id:
        errors.append("account")
    if order.con_id != attempt.con_id:
        errors.append("con_id")
    if order.local_symbol != attempt.local_symbol:
        errors.append("local_symbol")
    if order.side != attempt.side:
        errors.append("side")
    if order.order_type != attempt.order_type:
        errors.append("order_type")
    if order.requested_qty != attempt.requested_qty:
        errors.append("requested_qty")
    if (
        attempt.broker_order_id is not None
        and order.broker_order_id != attempt.broker_order_id
    ):
        errors.append("broker_order_id")
    if (
        attempt.broker_perm_id is not None
        and order.broker_perm_id is not None
        and order.broker_perm_id != attempt.broker_perm_id
    ):
        errors.append("broker_perm_id")
    return tuple(errors)


def _fill_route_errors(
    *,
    fill: BrokerFillFactV1,
    current: BrokerOperationSnapshot,
) -> tuple[str, ...]:
    attempt = current.attempt
    errors: list[str] = []
    if fill.account_id != current.operation.account_id:
        errors.append("account")
    if fill.con_id != attempt.con_id:
        errors.append("con_id")
    if fill.local_symbol != attempt.local_symbol:
        errors.append("local_symbol")
    if fill.side != attempt.side:
        errors.append("side")
    if (
        attempt.broker_order_id is not None
        and fill.broker_order_id != attempt.broker_order_id
    ):
        errors.append("broker_order_id")
    if (
        attempt.broker_perm_id is not None
        and fill.broker_perm_id is not None
        and fill.broker_perm_id != attempt.broker_perm_id
    ):
        errors.append("broker_perm_id")
    return tuple(errors)


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
    completed = [item for item in group if item.source == BrokerOrderSource.COMPLETED]
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
    parts = [
        value
        for value in (
            order.completed_status,
            order.warning_text,
        )
        if value
    ]
    return "; ".join(parts) or None


def _broker_status(order: BrokerOrderFactV1) -> str:
    if order.completed_status:
        return f"{order.status}/{order.completed_status}"
    return order.status


def _fills_for_order(
    fills: tuple[BrokerFillFactV1, ...],
    *,
    order: BrokerOrderFactV1,
) -> tuple[BrokerFillFactV1, ...]:
    return tuple(item for item in fills if _same_fill_identity(item, order))


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


def reconcile_broker_attempt_snapshot(
    *,
    broker_snapshot: BrokerReconciliationSnapshotV1,
    current: BrokerOperationSnapshot,
) -> BrokerAttemptReconciliationResult:
    if not isinstance(broker_snapshot, BrokerReconciliationSnapshotV1):
        raise BrokerReconciliationDomainError(
            "broker_snapshot must be BrokerReconciliationSnapshotV1"
        )
    if not isinstance(current, BrokerOperationSnapshot):
        raise BrokerReconciliationDomainError(
            "current must be BrokerOperationSnapshot"
        )
    if not broker_snapshot.requests_complete:
        raise BrokerReconciliationDomainError(
            "broker reconciliation snapshot is incomplete"
        )
    if broker_snapshot.account_id != current.operation.account_id:
        raise BrokerReconciliationDomainError(
            "broker reconciliation snapshot account differs from operation account"
        )
    if current.attempt.state == BrokerAttemptState.PREPARING:
        raise BrokerReconciliationDomainError(
            "PREPARING attempt has no broker exposure to reconcile"
        )

    order_ref = current.attempt.order_ref
    all_orders = tuple(
        item
        for item in (*broker_snapshot.open_orders, *broker_snapshot.completed_orders)
        if item.order_ref == order_ref
    )
    orders = _matching_orders(broker_snapshot, order_ref=order_ref)
    fills = _matching_fills(broker_snapshot, order_ref=order_ref)

    fill_route_errors = sorted(
        {
            error
            for fill in fills
            for error in _fill_route_errors(fill=fill, current=current)
        }
    )
    if fill_route_errors:
        observation = _uncertain(
            attempt_order_ref=order_ref,
            captured_at_utc=broker_snapshot.captured_at_utc,
            outcome=BrokerObservationOutcome.AMBIGUOUS,
            detail=(
                "matching execution route differs from persisted attempt: "
                + ",".join(fill_route_errors)
            ),
        )
        return BrokerAttemptReconciliationResult(
            observation=observation,
            fills=fills,
            source_session_id=broker_snapshot.source_session_id,
            captured_at_utc=broker_snapshot.captured_at_utc,
        )

    if len(all_orders) > 1 and len(orders) != 1:
        observation = _uncertain(
            attempt_order_ref=order_ref,
            captured_at_utc=broker_snapshot.captured_at_utc,
            outcome=BrokerObservationOutcome.AMBIGUOUS,
            detail=(
                "multiple broker order identities matched the persisted order_ref"
            ),
        )
        return BrokerAttemptReconciliationResult(
            observation=observation,
            fills=fills,
            source_session_id=broker_snapshot.source_session_id,
            captured_at_utc=broker_snapshot.captured_at_utc,
        )

    if not orders:
        groups = _fill_groups(fills)
        if not groups:
            observation = _uncertain(
                attempt_order_ref=order_ref,
                captured_at_utc=broker_snapshot.captured_at_utc,
                outcome=BrokerObservationOutcome.NOT_FOUND,
                detail=(
                    "no open/completed order or execution matched the persisted "
                    "order_ref in the complete read-only broker snapshot"
                ),
            )
        elif len(groups) != 1:
            observation = _uncertain(
                attempt_order_ref=order_ref,
                captured_at_utc=broker_snapshot.captured_at_utc,
                outcome=BrokerObservationOutcome.AMBIGUOUS,
                detail="executions with one order_ref map to multiple broker identities",
            )
        else:
            group = groups[0]
            filled_qty = sum(item.shares for item in group)
            if filled_qty == current.attempt.requested_qty:
                first = group[0]
                observation = BrokerOrderObservationV1(
                    order_ref=order_ref,
                    outcome=BrokerObservationOutcome.FILLED,
                    observed_at_utc=broker_snapshot.captured_at_utc,
                    broker_order_id=first.broker_order_id,
                    broker_perm_id=first.broker_perm_id,
                    broker_status="FILLED_FROM_EXECUTIONS",
                    requested_qty=current.attempt.requested_qty,
                    filled_qty=filled_qty,
                    remaining_qty=0,
                    detail=(
                        "full quantity proven by immutable executions; order row "
                        "was absent from the broker order snapshots"
                    ),
                )
            else:
                observation = _uncertain(
                    attempt_order_ref=order_ref,
                    captured_at_utc=broker_snapshot.captured_at_utc,
                    outcome=BrokerObservationOutcome.AMBIGUOUS,
                    detail=(
                        "partial executions exist but no exact order row proves the "
                        "remaining order state"
                    ),
                )
        return BrokerAttemptReconciliationResult(
            observation=observation,
            fills=fills,
            source_session_id=broker_snapshot.source_session_id,
            captured_at_utc=broker_snapshot.captured_at_utc,
        )

    order = orders[0]
    route_errors = _route_errors(order=order, current=current)
    if route_errors:
        observation = _uncertain(
            attempt_order_ref=order_ref,
            captured_at_utc=broker_snapshot.captured_at_utc,
            outcome=BrokerObservationOutcome.AMBIGUOUS,
            detail=(
                "broker order route differs from persisted attempt: "
                + ",".join(route_errors)
            ),
        )
        return BrokerAttemptReconciliationResult(
            observation=observation,
            fills=fills,
            source_session_id=broker_snapshot.source_session_id,
            captured_at_utc=broker_snapshot.captured_at_utc,
        )

    order_fills = _fills_for_order(fills, order=order)
    unrelated = tuple(item for item in fills if item not in order_fills)
    if unrelated:
        observation = _uncertain(
            attempt_order_ref=order_ref,
            captured_at_utc=broker_snapshot.captured_at_utc,
            outcome=BrokerObservationOutcome.AMBIGUOUS,
            detail=(
                "matching order_ref executions disagree with the exact broker order "
                "identity"
            ),
        )
        return BrokerAttemptReconciliationResult(
            observation=observation,
            fills=fills,
            source_session_id=broker_snapshot.source_session_id,
            captured_at_utc=broker_snapshot.captured_at_utc,
        )

    executed_qty = sum(item.shares for item in order_fills)
    if executed_qty > order.filled_qty or executed_qty > order.requested_qty:
        observation = _uncertain(
            attempt_order_ref=order_ref,
            captured_at_utc=broker_snapshot.captured_at_utc,
            outcome=BrokerObservationOutcome.AMBIGUOUS,
            detail=(
                "execution quantity exceeds the matching broker order fill quantity"
            ),
        )
        return BrokerAttemptReconciliationResult(
            observation=observation,
            fills=order_fills,
            source_session_id=broker_snapshot.source_session_id,
            captured_at_utc=broker_snapshot.captured_at_utc,
        )

    outcome = _order_outcome(order)
    if outcome is None:
        observation = _uncertain(
            attempt_order_ref=order_ref,
            captured_at_utc=broker_snapshot.captured_at_utc,
            outcome=BrokerObservationOutcome.AMBIGUOUS,
            detail=(
                "unsupported or conflicting IB order status: "
                f"status={order.status!r}, completed_status={order.completed_status!r}"
            ),
        )
    else:
        observation = BrokerOrderObservationV1(
            order_ref=order_ref,
            outcome=outcome,
            observed_at_utc=broker_snapshot.captured_at_utc,
            broker_order_id=order.broker_order_id,
            broker_perm_id=order.broker_perm_id,
            broker_status=_broker_status(order),
            requested_qty=order.requested_qty,
            filled_qty=order.filled_qty,
            remaining_qty=order.remaining_qty,
            detail=_order_detail(order),
        )

    return BrokerAttemptReconciliationResult(
        observation=observation,
        fills=order_fills,
        source_session_id=broker_snapshot.source_session_id,
        captured_at_utc=broker_snapshot.captured_at_utc,
    )
