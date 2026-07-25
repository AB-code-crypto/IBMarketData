from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Protocol

from ibmd.execution.domain.protection import apply_protective_observation
from ibmd.execution.domain.protective_submission import (
    ProtectiveOrderReconciliationResult,
    mark_protective_order_submitting,
    mark_protective_order_unknown,
    readiness_for_protection,
    reconcile_protective_order_snapshot,
)
from ibmd.foundation.time import format_utc, parse_utc, utc_now
from ibmd.ib_gateway.paper_orders import (
    PaperOrderGateway,
    PaperOrderRoute,
    PaperOrderSubmissionReceipt,
    PaperProtectiveOrderRequest,
)
from ibmd.public_contracts.broker_execution import BrokerObservationOutcome
from ibmd.public_contracts.broker_reconciliation import (
    BrokerReconciliationSnapshotV1,
)
from ibmd.public_contracts.execution import (
    ExecutionReadinessV1,
    StrategyPositionStatus,
    StrategyPositionV1,
)
from ibmd.public_contracts.positions import (
    BrokerPositionSnapshotStatus,
    BrokerPositionSnapshotV1,
)
from ibmd.public_contracts.protection import (
    PositionEpisodeStatus,
    PositionEpisodeV1,
    ProtectionSetStatus,
    ProtectionStateV1,
    ProtectiveOrderKind,
    ProtectiveOrderState,
    ProtectiveOrderV1,
)


class PaperProtectiveSubmitError(RuntimeError):
    pass


class ProtectionStateSource(Protocol):
    def read_episode(
        self,
        position_episode_id: str,
    ) -> PositionEpisodeV1 | None: ...

    def read_protection_by_episode(
        self,
        position_episode_id: str,
    ) -> ProtectionStateV1 | None: ...


class ProtectiveSubmitRepository(Protocol):
    def publish_state_and_readiness(
        self,
        *,
        current: ProtectionStateV1,
        updated: ProtectionStateV1,
        readiness: ExecutionReadinessV1,
    ) -> ProtectionStateV1: ...


class ExecutionStateSource(Protocol):
    def read_position(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> StrategyPositionV1 | None: ...

    def read_readiness(
        self,
        *,
        account_id: str,
        strategy_id: str,
        deployment_id: str,
        instrument_id: str,
    ) -> ExecutionReadinessV1 | None: ...


class PositionSnapshotSource(Protocol):
    def read_latest_complete(self) -> BrokerPositionSnapshotV1 | None: ...


class BrokerSnapshotSource(Protocol):
    async def read_snapshot(
        self,
        *,
        account_id: str,
    ) -> BrokerReconciliationSnapshotV1: ...


@dataclass(frozen=True)
class PaperProtectiveSubmitPolicy:
    account_id: str
    environment: str
    confirmed_paper_account_id: str
    strategy_id: str
    strategy_version: int
    deployment_id: str
    instrument_id: str
    order_route: PaperOrderRoute
    position_max_age_seconds: float = 10.0
    proof_max_age_seconds: float = 15.0
    reconciliation_read_attempts: int = 5
    reconciliation_poll_seconds: float = 1.0

    def __post_init__(self) -> None:
        for field_name in (
            "account_id",
            "environment",
            "confirmed_paper_account_id",
            "strategy_id",
            "deployment_id",
            "instrument_id",
        ):
            text = str(getattr(self, field_name) or "").strip()
            if not text:
                raise PaperProtectiveSubmitError(
                    f"{field_name} is required"
                )
            object.__setattr__(self, field_name, text)
        object.__setattr__(self, "environment", self.environment.lower())
        version = int(self.strategy_version)
        if version <= 0:
            raise PaperProtectiveSubmitError(
                "strategy_version must be positive"
            )
        object.__setattr__(self, "strategy_version", version)
        if not isinstance(self.order_route, PaperOrderRoute):
            raise PaperProtectiveSubmitError(
                "order_route must be PaperOrderRoute"
            )
        if self.order_route.instrument_id != self.instrument_id:
            raise PaperProtectiveSubmitError(
                "paper route belongs to another instrument"
            )
        for field_name in (
            "position_max_age_seconds",
            "proof_max_age_seconds",
        ):
            value = float(getattr(self, field_name))
            if value <= 0.0:
                raise PaperProtectiveSubmitError(
                    f"{field_name} must be positive"
                )
            object.__setattr__(self, field_name, value)
        attempts = int(self.reconciliation_read_attempts)
        if attempts <= 0:
            raise PaperProtectiveSubmitError(
                "reconciliation_read_attempts must be positive"
            )
        object.__setattr__(self, "reconciliation_read_attempts", attempts)
        poll = float(self.reconciliation_poll_seconds)
        if poll < 0.0:
            raise PaperProtectiveSubmitError(
                "reconciliation_poll_seconds must be non-negative"
            )
        object.__setattr__(self, "reconciliation_poll_seconds", poll)


@dataclass(frozen=True)
class PaperProtectiveSubmitRun:
    episode: PositionEpisodeV1
    before: ProtectionStateV1
    after: ProtectionStateV1
    order_kind: ProtectiveOrderKind | None
    submission_performed: bool
    receipt: PaperOrderSubmissionReceipt | None
    submission_error: str | None
    reconciliation_result: ProtectiveOrderReconciliationResult | None
    broker_snapshot_reads: int


_EXPOSED_STATES = {
    ProtectiveOrderState.SUBMITTING,
    ProtectiveOrderState.LIVE,
    ProtectiveOrderState.UNKNOWN_OUTCOME,
}
_TERMINAL_OR_DISABLED_STATES = {
    ProtectiveOrderState.FILLED,
    ProtectiveOrderState.CANCELLED,
    ProtectiveOrderState.REJECTED,
    ProtectiveOrderState.FAILED,
    ProtectiveOrderState.NOT_REQUIRED,
}


def require_paper_protective_gate(
    policy: PaperProtectiveSubmitPolicy,
) -> None:
    if policy.environment != "paper":
        raise PaperProtectiveSubmitError(
            "protective broker mutation requires IBMD_ENVIRONMENT=paper"
        )
    if policy.confirmed_paper_account_id != policy.account_id:
        raise PaperProtectiveSubmitError(
            "paper account confirmation differs from configured account"
        )
    if not policy.account_id.upper().startswith("D"):
        raise PaperProtectiveSubmitError(
            "configured account does not look like an IB paper account: "
            f"{policy.account_id!r}"
        )


def _scope(episode: PositionEpisodeV1) -> tuple[str, str, int, str, str]:
    return (
        episode.account_id,
        episode.strategy_id,
        episode.strategy_version,
        episode.deployment_id,
        episode.instrument_id,
    )


def _validate_episode(
    episode: PositionEpisodeV1,
    policy: PaperProtectiveSubmitPolicy,
) -> None:
    expected = (
        policy.account_id,
        policy.strategy_id,
        policy.strategy_version,
        policy.deployment_id,
        policy.instrument_id,
    )
    if _scope(episode) != expected:
        raise PaperProtectiveSubmitError(
            "position episode belongs to another paper protection scope"
        )
    if episode.status != PositionEpisodeStatus.OPEN:
        raise PaperProtectiveSubmitError(
            f"position episode is not OPEN: {episode.status.value}"
        )
    route = policy.order_route
    if (
        episode.con_id != route.con_id
        or episode.local_symbol != route.local_symbol
        or route.sec_type != "FUT"
    ):
        raise PaperProtectiveSubmitError(
            "position episode and paper route disagree"
        )


def _load_execution_state(
    source: ExecutionStateSource,
    episode: PositionEpisodeV1,
) -> tuple[StrategyPositionV1, ExecutionReadinessV1]:
    position = source.read_position(
        account_id=episode.account_id,
        strategy_id=episode.strategy_id,
        deployment_id=episode.deployment_id,
        instrument_id=episode.instrument_id,
    )
    readiness = source.read_readiness(
        account_id=episode.account_id,
        strategy_id=episode.strategy_id,
        deployment_id=episode.deployment_id,
        instrument_id=episode.instrument_id,
    )
    if position is None or readiness is None:
        raise PaperProtectiveSubmitError(
            "execution position/readiness is incomplete before protection"
        )
    if (
        position.projection_status != StrategyPositionStatus.OPEN
        or position.position_episode_id != episode.position_episode_id
        or position.side != episode.side
        or position.quantity != episode.quantity
    ):
        raise PaperProtectiveSubmitError(
            "strategy position does not prove the open position episode"
        )
    if (
        not readiness.broker_actions_enabled
        or not readiness.reconciliation_complete
        or not readiness.clock_healthy
    ):
        raise PaperProtectiveSubmitError(
            "protective submission requires broker actions, reconciliation and "
            "clock health"
        )
    foreign_reasons = tuple(
        item
        for item in readiness.blocking_reasons
        if not item.startswith("protection:")
    )
    if foreign_reasons:
        raise PaperProtectiveSubmitError(
            "non-protection execution blockers prevent protective submission: "
            f"{foreign_reasons}"
        )
    return position, readiness


def _require_fresh_broker_position(
    snapshot: BrokerPositionSnapshotV1 | None,
    *,
    episode: PositionEpisodeV1,
    observed_at_utc: str,
    max_age_seconds: float,
) -> None:
    if snapshot is None:
        raise PaperProtectiveSubmitError(
            "no COMPLETE broker position snapshot is available"
        )
    if snapshot.status != BrokerPositionSnapshotStatus.COMPLETE:
        raise PaperProtectiveSubmitError(
            "broker position snapshot is not COMPLETE"
        )
    if snapshot.account_id != episode.account_id:
        raise PaperProtectiveSubmitError(
            "broker position snapshot account mismatch"
        )
    freshness = snapshot.freshness(
        observed_at_utc=observed_at_utc,
        max_age_seconds=max_age_seconds,
    )
    if not freshness.is_fresh:
        raise PaperProtectiveSubmitError(
            "broker position snapshot is stale before protection: "
            f"age={freshness.age_seconds:.6f}s"
        )
    matching = [
        row
        for row in snapshot.rows
        if row.con_id == episode.con_id
        and str(row.local_symbol or "") == episode.local_symbol
    ]
    if len(matching) != 1:
        raise PaperProtectiveSubmitError(
            "broker snapshot does not contain exactly one protected contract row"
        )
    expected_signed = (
        episode.quantity
        if episode.side.value == "LONG"
        else -episode.quantity
    )
    actual = float(matching[0].signed_quantity)
    if abs(actual - expected_signed) > 1e-9:
        raise PaperProtectiveSubmitError(
            "broker position does not match position episode: "
            f"expected={expected_signed}, actual={actual}"
        )
    competing = [
        row
        for row in snapshot.rows
        if row is not matching[0]
        and (
            row.symbol.upper() == episode.instrument_id.upper()
            or str(row.local_symbol or "").upper().startswith(
                episode.instrument_id.upper()
            )
        )
    ]
    if competing:
        raise PaperProtectiveSubmitError(
            "broker snapshot contains a competing instrument contract"
        )


def _order_by_kind(
    protection: ProtectionStateV1,
    kind: ProtectiveOrderKind,
) -> ProtectiveOrderV1:
    values = [item for item in protection.orders if item.kind == kind]
    if len(values) != 1:
        raise PaperProtectiveSubmitError(
            f"protection has no unique {kind.value} order"
        )
    return values[0]


def _proof_is_fresh(
    order: ProtectiveOrderV1,
    *,
    observed_at_utc: str,
    max_age_seconds: float,
) -> bool:
    if order.last_broker_proof_at_utc is None:
        return False
    age = (
        parse_utc(observed_at_utc)
        - parse_utc(order.last_broker_proof_at_utc)
    ).total_seconds()
    return 0.0 <= age <= max_age_seconds


def _submission_context(
    result: ProtectiveOrderReconciliationResult,
    submission_error: str | None,
) -> ProtectiveOrderReconciliationResult:
    if submission_error is None or result.observation.outcome not in {
        BrokerObservationOutcome.NOT_FOUND,
        BrokerObservationOutcome.AMBIGUOUS,
    }:
        return result
    observation = result.observation
    detail = "; ".join(
        item
        for item in (
            observation.detail,
            f"submit_call_error={submission_error}",
        )
        if item
    )
    return ProtectiveOrderReconciliationResult(
        observation=type(observation)(
            order_ref=observation.order_ref,
            outcome=observation.outcome,
            observed_at_utc=observation.observed_at_utc,
            broker_order_id=None,
            broker_perm_id=None,
            broker_status=None,
            requested_qty=None,
            filled_qty=None,
            remaining_qty=None,
            detail=detail,
        ),
        fills=result.fills,
        source_session_id=result.source_session_id,
        captured_at_utc=result.captured_at_utc,
    )


class PaperProtectiveSubmitCoordinator:
    def __init__(
        self,
        *,
        policy: PaperProtectiveSubmitPolicy,
        protection_source: ProtectionStateSource,
        protection_repository: ProtectiveSubmitRepository,
        execution_state_source: ExecutionStateSource,
        position_snapshot_source: PositionSnapshotSource,
        order_gateway: PaperOrderGateway,
        broker_snapshot_source: BrokerSnapshotSource,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.policy = policy
        self.protection_source = protection_source
        self.protection_repository = protection_repository
        self.execution_state_source = execution_state_source
        self.position_snapshot_source = position_snapshot_source
        self.order_gateway = order_gateway
        self.broker_snapshot_source = broker_snapshot_source
        self.clock = clock

    def _load(
        self,
        position_episode_id: str,
    ) -> tuple[PositionEpisodeV1, ProtectionStateV1]:
        episode = self.protection_source.read_episode(position_episode_id)
        if episode is None:
            raise PaperProtectiveSubmitError(
                f"position episode does not exist: {position_episode_id}"
            )
        protection = self.protection_source.read_protection_by_episode(
            position_episode_id
        )
        if protection is None:
            raise PaperProtectiveSubmitError(
                "position episode has no protection state"
            )
        _validate_episode(episode, self.policy)
        if protection.position_episode_id != episode.position_episode_id:
            raise PaperProtectiveSubmitError(
                "protection state belongs to another position episode"
            )
        return episode, protection

    def _publish(
        self,
        *,
        current: ProtectionStateV1,
        updated: ProtectionStateV1,
        current_readiness: ExecutionReadinessV1,
        observed_at_utc: str,
    ) -> ProtectionStateV1:
        readiness = readiness_for_protection(
            current_readiness,
            protection=updated,
            observed_at_utc=observed_at_utc,
        )
        return self.protection_repository.publish_state_and_readiness(
            current=current,
            updated=updated,
            readiness=readiness,
        )

    async def _reconcile(
        self,
        *,
        episode: PositionEpisodeV1,
        current: ProtectionStateV1,
        readiness: ExecutionReadinessV1,
        kind: ProtectiveOrderKind,
        before: ProtectionStateV1,
        submission_performed: bool,
        receipt: PaperOrderSubmissionReceipt | None,
        submission_error: str | None,
    ) -> PaperProtectiveSubmitRun:
        reads = 0
        last_result: ProtectiveOrderReconciliationResult | None = None
        read_errors: list[str] = []
        working = current
        for index in range(self.policy.reconciliation_read_attempts):
            try:
                snapshot = await self.broker_snapshot_source.read_snapshot(
                    account_id=self.policy.account_id
                )
                reads += 1
                result = reconcile_protective_order_snapshot(
                    broker_snapshot=snapshot,
                    episode=episode,
                    protection=working,
                    kind=kind,
                )
                result = _submission_context(result, submission_error)
                last_result = result
                if (
                    result.observation.outcome
                    == BrokerObservationOutcome.NOT_FOUND
                    and index + 1 < self.policy.reconciliation_read_attempts
                ):
                    if self.policy.reconciliation_poll_seconds:
                        await asyncio.sleep(
                            self.policy.reconciliation_poll_seconds
                        )
                    continue
                updated = apply_protective_observation(
                    protection=working,
                    kind=kind,
                    observation=result.observation,
                    position_open=True,
                )
                after = self._publish(
                    current=working,
                    updated=updated,
                    current_readiness=readiness,
                    observed_at_utc=result.captured_at_utc,
                )
                return PaperProtectiveSubmitRun(
                    episode=episode,
                    before=before,
                    after=after,
                    order_kind=kind,
                    submission_performed=submission_performed,
                    receipt=receipt,
                    submission_error=submission_error,
                    reconciliation_result=result,
                    broker_snapshot_reads=reads,
                )
            except Exception as exc:
                read_errors.append(f"{type(exc).__name__}: {exc}")
                if index + 1 < self.policy.reconciliation_read_attempts:
                    if self.policy.reconciliation_poll_seconds:
                        await asyncio.sleep(
                            self.policy.reconciliation_poll_seconds
                        )
                    continue
        reason_parts = [
            "protective_order_outcome_unproven_after_bounded_reconciliation"
        ]
        if submission_error:
            reason_parts.append(f"submit_call_error={submission_error}")
        if last_result is not None and last_result.observation.detail:
            reason_parts.append(last_result.observation.detail)
        if read_errors:
            reason_parts.append("read_errors=" + " | ".join(read_errors))
        observed = format_utc(self.clock())
        unknown = mark_protective_order_unknown(
            working,
            kind=kind,
            observed_at_utc=observed,
            reason="; ".join(reason_parts),
        )
        after = self._publish(
            current=working,
            updated=unknown,
            current_readiness=readiness,
            observed_at_utc=observed,
        )
        return PaperProtectiveSubmitRun(
            episode=episode,
            before=before,
            after=after,
            order_kind=kind,
            submission_performed=submission_performed,
            receipt=receipt,
            submission_error=submission_error,
            reconciliation_result=last_result,
            broker_snapshot_reads=reads,
        )

    async def _submit(
        self,
        *,
        episode: PositionEpisodeV1,
        protection: ProtectionStateV1,
        readiness: ExecutionReadinessV1,
        kind: ProtectiveOrderKind,
    ) -> PaperProtectiveSubmitRun:
        broker_order_id = await self.order_gateway.allocate_order_id(
            account_id=self.policy.account_id
        )
        observed = format_utc(self.clock())
        submitting = mark_protective_order_submitting(
            protection,
            kind=kind,
            broker_order_id=broker_order_id,
            observed_at_utc=observed,
        )
        submitting = self._publish(
            current=protection,
            updated=submitting,
            current_readiness=readiness,
            observed_at_utc=observed,
        )
        submitting_order = _order_by_kind(submitting, kind)
        request = PaperProtectiveOrderRequest(
            account_id=self.policy.account_id,
            broker_order_id=broker_order_id,
            order_ref=submitting_order.order_ref,
            kind=submitting_order.kind,
            side=submitting_order.side,
            order_type=submitting_order.order_type,
            quantity=submitting_order.quantity,
            route=self.policy.order_route,
            stop_price=submitting_order.stop_price,
            limit_price=submitting_order.limit_price,
            time_in_force=submitting_order.time_in_force,
            outside_rth=submitting_order.outside_rth,
            oca_group=submitting_order.oca_group,
        )
        receipt: PaperOrderSubmissionReceipt | None = None
        submission_error: str | None = None
        try:
            receipt = await self.order_gateway.submit_protective_order(request)
        except Exception as exc:
            submission_error = f"{type(exc).__name__}: {exc}"
        return await self._reconcile(
            episode=episode,
            current=submitting,
            readiness=readiness,
            kind=kind,
            before=protection,
            submission_performed=True,
            receipt=receipt,
            submission_error=submission_error,
        )

    async def run_once(
        self,
        *,
        position_episode_id: str,
    ) -> PaperProtectiveSubmitRun:
        require_paper_protective_gate(self.policy)
        episode_id = str(position_episode_id or "").strip()
        if not episode_id:
            raise PaperProtectiveSubmitError(
                "position_episode_id is required"
            )
        episode, protection = self._load(episode_id)
        _position, readiness = _load_execution_state(
            self.execution_state_source,
            episode,
        )
        observed = format_utc(self.clock())
        _require_fresh_broker_position(
            self.position_snapshot_source.read_latest_complete(),
            episode=episode,
            observed_at_utc=observed,
            max_age_seconds=self.policy.position_max_age_seconds,
        )

        if protection.status in {
            ProtectionSetStatus.EXITED,
            ProtectionSetStatus.CLOSED,
            ProtectionSetStatus.OPERATOR_REQUIRED,
        }:
            return PaperProtectiveSubmitRun(
                episode=episode,
                before=protection,
                after=protection,
                order_kind=None,
                submission_performed=False,
                receipt=None,
                submission_error=None,
                reconciliation_result=None,
                broker_snapshot_reads=0,
            )

        stop = protection.stop_order
        if stop.state == ProtectiveOrderState.PLANNED:
            return await self._submit(
                episode=episode,
                protection=protection,
                readiness=readiness,
                kind=ProtectiveOrderKind.STOP_LOSS,
            )
        if stop.state in {
            ProtectiveOrderState.SUBMITTING,
            ProtectiveOrderState.UNKNOWN_OUTCOME,
        }:
            return await self._reconcile(
                episode=episode,
                current=protection,
                readiness=readiness,
                kind=ProtectiveOrderKind.STOP_LOSS,
                before=protection,
                submission_performed=False,
                receipt=None,
                submission_error=None,
            )
        if stop.state in _TERMINAL_OR_DISABLED_STATES:
            return PaperProtectiveSubmitRun(
                episode=episode,
                before=protection,
                after=protection,
                order_kind=ProtectiveOrderKind.STOP_LOSS,
                submission_performed=False,
                receipt=None,
                submission_error=None,
                reconciliation_result=None,
                broker_snapshot_reads=0,
            )
        if stop.state != ProtectiveOrderState.LIVE:
            raise PaperProtectiveSubmitError(
                f"unsupported STOP state: {stop.state.value}"
            )
        if not _proof_is_fresh(
            stop,
            observed_at_utc=observed,
            max_age_seconds=self.policy.proof_max_age_seconds,
        ):
            return await self._reconcile(
                episode=episode,
                current=protection,
                readiness=readiness,
                kind=ProtectiveOrderKind.STOP_LOSS,
                before=protection,
                submission_performed=False,
                receipt=None,
                submission_error=None,
            )

        tp = protection.take_profit_order
        if tp is None or tp.state == ProtectiveOrderState.NOT_REQUIRED:
            return PaperProtectiveSubmitRun(
                episode=episode,
                before=protection,
                after=protection,
                order_kind=None,
                submission_performed=False,
                receipt=None,
                submission_error=None,
                reconciliation_result=None,
                broker_snapshot_reads=0,
            )
        if tp.state == ProtectiveOrderState.PLANNED:
            return await self._submit(
                episode=episode,
                protection=protection,
                readiness=readiness,
                kind=ProtectiveOrderKind.TAKE_PROFIT,
            )
        if tp.state in _EXPOSED_STATES:
            if (
                tp.state == ProtectiveOrderState.LIVE
                and _proof_is_fresh(
                    tp,
                    observed_at_utc=observed,
                    max_age_seconds=self.policy.proof_max_age_seconds,
                )
            ):
                return PaperProtectiveSubmitRun(
                    episode=episode,
                    before=protection,
                    after=protection,
                    order_kind=ProtectiveOrderKind.TAKE_PROFIT,
                    submission_performed=False,
                    receipt=None,
                    submission_error=None,
                    reconciliation_result=None,
                    broker_snapshot_reads=0,
                )
            return await self._reconcile(
                episode=episode,
                current=protection,
                readiness=readiness,
                kind=ProtectiveOrderKind.TAKE_PROFIT,
                before=protection,
                submission_performed=False,
                receipt=None,
                submission_error=None,
            )
        return PaperProtectiveSubmitRun(
            episode=episode,
            before=protection,
            after=protection,
            order_kind=ProtectiveOrderKind.TAKE_PROFIT,
            submission_performed=False,
            receipt=None,
            submission_error=None,
            reconciliation_result=None,
            broker_snapshot_reads=0,
        )


def paper_protective_submit_payload(
    run: PaperProtectiveSubmitRun,
) -> dict:
    result = run.reconciliation_result
    selected = (
        None
        if run.order_kind is None
        else _order_by_kind(run.after, run.order_kind)
    )
    return {
        "position_episode_id": run.episode.position_episode_id,
        "protection_set_id": run.after.protection_set_id,
        "protection_status": run.after.status.value,
        "order_kind": (
            None if run.order_kind is None else run.order_kind.value
        ),
        "protective_order_id": (
            None if selected is None else selected.protective_order_id
        ),
        "order_ref": None if selected is None else selected.order_ref,
        "order_state": None if selected is None else selected.state.value,
        "broker_order_id": (
            None if selected is None else selected.broker_order_id
        ),
        "broker_perm_id": (
            None if selected is None else selected.broker_perm_id
        ),
        "broker_status": None if selected is None else selected.broker_status,
        "filled_qty": None if selected is None else selected.filled_qty,
        "remaining_qty": None if selected is None else selected.remaining_qty,
        "blocking_reason": run.after.blocking_reason,
        "submission_performed": run.submission_performed,
        "submission_receipt": (
            None
            if run.receipt is None
            else {
                "broker_order_id": run.receipt.broker_order_id,
                "order_ref": run.receipt.order_ref,
                "submitted_at_utc": run.receipt.submitted_at_utc,
            }
        ),
        "submission_error": run.submission_error,
        "broker_snapshot_reads": run.broker_snapshot_reads,
        "reconciliation_outcome": (
            None if result is None else result.observation.outcome.value
        ),
        "persisted_exec_ids": (
            [] if result is None else [item.exec_id for item in result.fills]
        ),
        "commission_complete": (
            None if result is None else result.commission_complete
        ),
        "automatic_retry_enabled": False,
        "cancel_enabled": False,
        "liquidation_enabled": False,
        "paper_protective_mutation_enabled": run.submission_performed,
    }
