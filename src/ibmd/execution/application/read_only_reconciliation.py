from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ibmd.execution.domain.broker_attempt import (
    BrokerOperationSnapshot,
    apply_broker_observation,
    begin_reconciliation,
)
from ibmd.execution.domain.ib_reconciliation import (
    BrokerAttemptReconciliationResult,
    reconcile_broker_attempt_snapshot,
)
from ibmd.foundation.time import format_utc, utc_now
from ibmd.public_contracts.broker_execution import (
    BrokerAttemptState,
    BrokerOperationState,
    BrokerOrderObservationV1,
)
from ibmd.public_contracts.broker_reconciliation import (
    BrokerReconciliationSnapshotV1,
)


class BrokerAttemptSource(Protocol):
    def read_snapshot(self, operation_id: str) -> BrokerOperationSnapshot | None: ...

    def read_unresolved(self) -> tuple[BrokerOperationSnapshot, ...]: ...

    def publish_state(
        self,
        snapshot: BrokerOperationSnapshot,
        *,
        observation: BrokerOrderObservationV1 | None = None,
    ) -> BrokerOperationSnapshot: ...


class BrokerReconciliationRepository(Protocol):
    def read_commission_pending_operation_ids(self) -> tuple[str, ...]: ...

    def publish(
        self,
        *,
        current: BrokerOperationSnapshot,
        updated: BrokerOperationSnapshot,
        result: BrokerAttemptReconciliationResult,
    ) -> BrokerOperationSnapshot: ...


class ReadOnlyBrokerSnapshotSource(Protocol):
    async def read_snapshot(
        self,
        *,
        account_id: str,
    ) -> BrokerReconciliationSnapshotV1: ...


@dataclass(frozen=True)
class ReconciledBrokerAttempt:
    before: BrokerOperationSnapshot
    after: BrokerOperationSnapshot
    result: BrokerAttemptReconciliationResult


@dataclass(frozen=True)
class ReadOnlyReconciliationRun:
    broker_snapshot: BrokerReconciliationSnapshotV1
    reconciled: tuple[ReconciledBrokerAttempt, ...]
    skipped_operation_ids: tuple[str, ...]


_ELIGIBLE_OPERATION_STATES = {
    BrokerOperationState.SUBMITTING,
    BrokerOperationState.LIVE,
    BrokerOperationState.RECONCILING,
    BrokerOperationState.UNKNOWN_OUTCOME,
    BrokerOperationState.SUCCEEDED,
}
_ELIGIBLE_ATTEMPT_STATES = {
    BrokerAttemptState.SUBMITTING,
    BrokerAttemptState.LIVE,
    BrokerAttemptState.UNKNOWN_OUTCOME,
    BrokerAttemptState.FILLED,
}


class ReadOnlyBrokerReconciliationService:
    def __init__(
        self,
        *,
        account_id: str,
        broker_source: ReadOnlyBrokerSnapshotSource,
        attempt_source: BrokerAttemptSource,
        reconciliation_store: BrokerReconciliationRepository,
    ) -> None:
        self.account_id = str(account_id or "").strip()
        if not self.account_id:
            raise ValueError("account_id is required")
        self.broker_source = broker_source
        self.attempt_source = attempt_source
        self.reconciliation_store = reconciliation_store

    def _candidates(self) -> tuple[BrokerOperationSnapshot, ...]:
        by_operation = {
            item.operation.operation_id: item
            for item in self.attempt_source.read_unresolved()
        }
        for operation_id in (
            self.reconciliation_store.read_commission_pending_operation_ids()
        ):
            if operation_id in by_operation:
                continue
            snapshot = self.attempt_source.read_snapshot(operation_id)
            if snapshot is not None:
                by_operation[operation_id] = snapshot
        return tuple(
            by_operation[key]
            for key in sorted(by_operation)
        )

    async def run_once(self) -> ReadOnlyReconciliationRun:
        candidates = self._candidates()
        broker_snapshot = await self.broker_source.read_snapshot(
            account_id=self.account_id
        )
        reconciled: list[ReconciledBrokerAttempt] = []
        skipped: list[str] = []

        for original in candidates:
            if (
                original.operation.state not in _ELIGIBLE_OPERATION_STATES
                or original.attempt.state not in _ELIGIBLE_ATTEMPT_STATES
            ):
                skipped.append(original.operation.operation_id)
                continue

            current = original
            if original.operation.state in {
                BrokerOperationState.SUBMITTING,
                BrokerOperationState.LIVE,
                BrokerOperationState.UNKNOWN_OUTCOME,
            }:
                reconciling = begin_reconciliation(
                    original,
                    observed_at_utc=broker_snapshot.captured_at_utc,
                )
                current = self.attempt_source.publish_state(reconciling)

            result = reconcile_broker_attempt_snapshot(
                broker_snapshot=broker_snapshot,
                current=current,
            )
            if current.operation.state == BrokerOperationState.SUCCEEDED:
                updated = current
            else:
                updated = apply_broker_observation(
                    current,
                    observation=result.observation,
                )
            stored = self.reconciliation_store.publish(
                current=current,
                updated=updated,
                result=result,
            )
            reconciled.append(
                ReconciledBrokerAttempt(
                    before=original,
                    after=stored,
                    result=result,
                )
            )

        return ReadOnlyReconciliationRun(
            broker_snapshot=broker_snapshot,
            reconciled=tuple(reconciled),
            skipped_operation_ids=tuple(skipped),
        )


def reconciliation_run_payload(run: ReadOnlyReconciliationRun) -> dict:
    return {
        "captured_at_utc": run.broker_snapshot.captured_at_utc,
        "source_session_id": run.broker_snapshot.source_session_id,
        "requests_complete": run.broker_snapshot.requests_complete,
        "open_order_count": len(run.broker_snapshot.open_orders),
        "completed_order_count": len(run.broker_snapshot.completed_orders),
        "fill_count": len(run.broker_snapshot.fills),
        "commission_complete_count": (
            run.broker_snapshot.commission_complete_count
        ),
        "reconciled": [
            {
                "operation_id": item.after.operation.operation_id,
                "attempt_id": item.after.attempt.attempt_id,
                "order_ref": item.after.attempt.order_ref,
                "outcome": item.result.observation.outcome.value,
                "operation_state": item.after.operation.state.value,
                "attempt_state": item.after.attempt.state.value,
                "filled_qty": item.after.operation.filled_qty,
                "remaining_qty": item.after.operation.remaining_qty,
                "persisted_exec_ids": [
                    fill.exec_id for fill in item.result.fills
                ],
                "commission_complete": item.result.commission_complete,
            }
            for item in run.reconciled
        ],
        "skipped_operation_ids": list(run.skipped_operation_ids),
        "completed_at_utc": format_utc(utc_now()),
        "broker_mutations_enabled": False,
    }
